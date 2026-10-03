from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from telethon import Button, TelegramClient, events, functions, types, errors
from telethon.sessions import StringSession

load_dotenv()

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"].strip()
BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
OWNER_ID = int(os.environ["OWNER_ID"])
USER_SESSION = os.getenv("USER_SESSION", "").strip()

DB_PATH = os.getenv("DB_PATH", "data/bot.sqlite3").strip() or "data/bot.sqlite3"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("anitoons")

bot = TelegramClient("bot_memory", API_ID, API_HASH)
user: TelegramClient | None = None
if USER_SESSION:
    user = TelegramClient(StringSession(USER_SESSION), API_ID, API_HASH)

active_client: TelegramClient = bot
user_session_ok = False
auto_reactions = True
channels: dict[int, dict[str, Any]] = {}
pending: dict[int, dict[str, Any]] = {}

DEFAULT_REACTIONS = ["❤️", "🔥", "👍"]
REACTION_CHOICES = ["❤️", "🔥", "👍", "😂", "😍", "😢", "😡", "👏", "🎉", "💯"]

def db_connect() -> sqlite3.Connection:
    path = Path(DB_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=10)
    connection.row_factory = sqlite3.Row
    return connection

def init_storage() -> None:
    with db_connect() as db:
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS channels (
                chat_id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                username TEXT,
                reference TEXT NOT NULL,
                reactions TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                delay_min REAL NOT NULL DEFAULT 1,
                delay_max REAL NOT NULL DEFAULT 4,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                reaction TEXT,
                status TEXT NOT NULL,
                detail TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            );
            """
        )
    log.info("SQLite storage ready: %s", DB_PATH)

def owner_only(event) -> bool:
    return bool(event.is_private and event.sender_id == OWNER_ID)

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def worker_name() -> str:
    return "user session" if user_session_ok and user else "bot session"

async def db_load() -> None:
    global auto_reactions
    try:
        def read():
            with db_connect() as db:
                rows = db.execute("SELECT * FROM channels").fetchall()
                setting = db.execute(
                    "SELECT value FROM settings WHERE key = ?",
                    ("auto_reactions",),
                ).fetchone()
                return rows, setting

        rows, setting = await asyncio.to_thread(read)
        channels.clear()
        for row in rows:
            channels[int(row["chat_id"])] = {
                "chat_id": int(row["chat_id"]),
                "title": row["title"],
                "username": row["username"],
                "reference": row["reference"],
                "reactions": json.loads(row["reactions"]) or DEFAULT_REACTIONS[:],
                "enabled": bool(row["enabled"]),
                "delay_min": float(row["delay_min"]),
                "delay_max": float(row["delay_max"]),
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
        if setting is not None:
            auto_reactions = setting["value"] == "1"
    except Exception:
        log.exception("Failed to load SQLite data")

async def db_save_channel(config: dict[str, Any]) -> None:
    payload = dict(config)

    def write():
        with db_connect() as db:
            db.execute(
                """
                INSERT INTO channels
                (chat_id, title, username, reference, reactions, enabled,
                 delay_min, delay_max, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    title=excluded.title,
                    username=excluded.username,
                    reference=excluded.reference,
                    reactions=excluded.reactions,
                    enabled=excluded.enabled,
                    delay_min=excluded.delay_min,
                    delay_max=excluded.delay_max,
                    updated_at=excluded.updated_at
                """,
                (
                    int(payload["chat_id"]),
                    str(payload.get("title", "Channel")),
                    payload.get("username"),
                    str(payload.get("reference", "")),
                    json.dumps(payload.get("reactions") or DEFAULT_REACTIONS, ensure_ascii=False),
                    int(bool(payload.get("enabled", True))),
                    float(payload.get("delay_min", 1)),
                    float(payload.get("delay_max", 4)),
                    str(payload.get("created_at", now_iso())),
                    str(payload.get("updated_at", now_iso())),
                ),
            )

    await asyncio.to_thread(write)

async def db_delete_channel(chat_id: int) -> None:
    def delete():
        with db_connect() as db:
            db.execute("DELETE FROM channels WHERE chat_id = ?", (int(chat_id),))
    await asyncio.to_thread(delete)

async def db_set_global(value: bool) -> None:
    global auto_reactions
    auto_reactions = value

    def write():
        with db_connect() as db:
            db.execute(
                """
                INSERT INTO settings(key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
                """,
                ("auto_reactions", "1" if value else "0"),
            )

    await asyncio.to_thread(write)

async def db_log(
    chat_id: int,
    message_id: int,
    reaction: str | None,
    status: str,
    detail: str = "",
) -> None:
    def write():
        with db_connect() as db:
            db.execute(
                """
                INSERT INTO logs
                (chat_id, message_id, reaction, status, detail, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    int(chat_id),
                    int(message_id),
                    reaction,
                    status,
                    detail[:500],
                    now_iso(),
                ),
            )

    await asyncio.to_thread(write)

async def recent_logs(limit: int = 12) -> list[dict[str, Any]]:
    def read():
        with db_connect() as db:
            rows = db.execute(
                """
                SELECT chat_id, message_id, reaction, status, detail, created_at
                FROM logs
                ORDER BY id DESC
                LIMIT ?
                """,
                (max(1, min(int(limit), 50)),),
            ).fetchall()
            return [dict(row) for row in rows]

    return await asyncio.to_thread(read)

async def clear_local_storage() -> None:
    global auto_reactions

    def clear():
        with db_connect() as db:
            db.execute("DELETE FROM channels")
            db.execute("DELETE FROM logs")
            db.execute("DELETE FROM settings")

    await asyncio.to_thread(clear)
    channels.clear()
    pending.clear()
    auto_reactions = True

def main_buttons():
    return [
        [Button.inline("📊 Dashboard", b"dashboard"), Button.inline("📡 Channels", b"channels")],
        [Button.inline("➕ Add Channel", b"add"), Button.inline("😀 Reactions", b"reaction_menu")],
        [Button.inline("⏱ Delay", b"delay_menu"), Button.inline("📋 Logs", b"logs")],
        [Button.inline("▶️ Start", b"global_on"), Button.inline("⏸ Stop", b"global_off")],
        [Button.inline("🔍 Status", b"status"), Button.inline("❔ Help", b"help")],
        [Button.inline("🧹 Clear Local Storage", b"clear_storage")],
    ]

async def dashboard_text() -> str:
    enabled = sum(1 for x in channels.values() if x.get("enabled", True))
    state = "🟢 ON" if auto_reactions else "🔴 OFF"
    return (
        "🎬 AniToons 1Bot Control Panel\n\n"
        f"Auto reactions: {state}\n"
        f"Channels: {len(channels)} ({enabled} enabled)\n"
        f"Worker: {worker_name()}\n"
        f"Storage: 🟢 SQLite ({DB_PATH})\n\n"
        "Use the buttons below."
    )

async def edit_or_reply(event, text: str, buttons=None) -> None:
    try:
        if hasattr(event, "edit"):
            await event.edit(text, buttons=buttons)
        else:
            await event.reply(text, buttons=buttons)
    except errors.MessageNotModifiedError:
        pass

async def channel_entity(ref: str):
    entity = await active_client.get_entity(ref)
    if not isinstance(entity, types.Channel) or getattr(entity, "megagroup", False):
        raise ValueError("That target is not a broadcast channel.")
    return entity

async def add_channel_config(ref: str, entity) -> dict[str, Any]:
    chat_id = active_client.get_peer_id(entity)
    current = channels.get(chat_id, {})
    config = {
        "chat_id": chat_id,
        "title": getattr(entity, "title", "Channel"),
        "username": getattr(entity, "username", None),
        "reference": ref,
        "reactions": current.get("reactions", DEFAULT_REACTIONS),
        "enabled": current.get("enabled", True),
        "delay_min": float(current.get("delay_min", 1)),
        "delay_max": float(current.get("delay_max", 4)),
        "created_at": current.get("created_at", now_iso()),
        "updated_at": now_iso(),
    }
    channels[chat_id] = config
    await db_save_channel(config)
    return config

def channel_label(config: dict[str, Any]) -> str:
    username = config.get("username")
    suffix = f" @{username}" if username else ""
    return f"{config.get('title', 'Channel')}{suffix}"

def channels_buttons(prefix: str = "view"):
    rows = []
    for config in list(channels.values())[:30]:
        chat_id = int(config["chat_id"])
        status = "🟢" if config.get("enabled", True) else "🔴"
        rows.append([Button.inline(f"{status} {channel_label(config)[:35]}", f"{prefix}:{chat_id}".encode())])
    rows.append([Button.inline("⬅️ Back", b"dashboard")])
    return rows

async def channel_view(chat_id: int):
    config = channels.get(chat_id)
    if not config:
        return "Channel not found.", [[Button.inline("⬅️ Channels", b"channels")]]
    status = "🟢 Enabled" if config.get("enabled", True) else "🔴 Paused"
    reactions = ", ".join(config.get("reactions", DEFAULT_REACTIONS))
    delay = f"{float(config.get('delay_min', 1)):g}-{float(config.get('delay_max', 4)):g}s"
    buttons = [
        [Button.inline("⏯ Toggle", f"toggle:{chat_id}".encode()), Button.inline("🧪 Test", f"test:{chat_id}".encode())],
        [Button.inline("😀 Reactions", f"reactions:{chat_id}".encode()), Button.inline("⏱ Delay", f"delay:{chat_id}".encode())],
        [Button.inline("🗑 Remove", f"remove:{chat_id}".encode())],
        [Button.inline("⬅️ Channels", b"channels")],
    ]
    return (
        f"📡 {config.get('title', 'Channel')}\n\n"
        f"Status: {status}\n"
        f"Reactions: {reactions}\n"
        f"Delay: {delay}\n"
        f"Reference: {config.get('reference', '')}"
    ), buttons

def reaction_buttons(chat_id: int, selected: list[str]):
    rows = []
    row = []
    for emoji in REACTION_CHOICES:
        mark = "✅ " if emoji in selected else ""
        row.append(Button.inline(f"{mark}{emoji}", f"pick:{chat_id}:{emoji}".encode()))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([Button.inline("💾 Save", f"save_reactions:{chat_id}".encode()), Button.inline("⬅️ Back", f"view:{chat_id}".encode())])
    rows.append([Button.inline("✏️ Custom emojis", f"custom:{chat_id}".encode())])
    return rows

async def messages(event):
    if not owner_only(event):
        return
    text = (event.raw_text or "").strip()
    state = pending.get(OWNER_ID)
    if state and not text.startswith("/"):
        try:
            if state.get("action") == "add_channel":
                entity = await channel_entity(text)
                config = await add_channel_config(text, entity)
                pending.pop(OWNER_ID, None)
                await event.reply(f"✅ Added {config['title']}.", buttons=[[Button.inline("📡 Channels", b"channels")], [Button.inline("🏠 Dashboard", b"dashboard")]])
                return
            if state.get("action") == "delay":
                parts = text.replace(",", " ").split()
                if len(parts) != 2:
                    raise ValueError("Send two numbers, for example: 2 5")
                minimum, maximum = float(parts[0]), float(parts[1])
                if minimum < 0 or maximum < minimum:
                    raise ValueError("Use 0 <= minimum <= maximum")
                chat_id = int(state["chat_id"])
                channels[chat_id]["delay_min"] = minimum
                channels[chat_id]["delay_max"] = maximum
                channels[chat_id]["updated_at"] = now_iso()
                await db_save_channel(channels[chat_id])
                pending.pop(OWNER_ID, None)
                await event.reply("✅ Delay updated.", buttons=[[Button.inline("⬅️ Channel", f"view:{chat_id}".encode())]])
                return
            if state.get("action") == "custom_reactions":
                reactions = [x.strip() for x in text.split(",") if x.strip()]
                if not reactions or len(reactions) > 10:
                    raise ValueError("Send 1-10 emojis separated by commas")
                chat_id = int(state["chat_id"])
                channels[chat_id]["reactions"] = reactions
                channels[chat_id]["updated_at"] = now_iso()
                await db_save_channel(channels[chat_id])
                pending.pop(OWNER_ID, None)
                await event.reply("✅ Custom reactions saved.", buttons=[[Button.inline("⬅️ Channel", f"view:{chat_id}".encode())]])
                return
        except (ValueError, errors.RPCError) as exc:
            await event.reply(f"❌ {exc}")
            return
    if text in {"/start", "/panel"}:
        await event.reply(await dashboard_text(), buttons=main_buttons())
    elif text == "/clearstorage":
        await event.reply(
            "⚠️ This deletes the bot's local SQLite channels, logs, and settings. "
            "Use the button below to confirm.",
            buttons=[[Button.inline("✅ CONFIRM CLEAR", b"clear_confirm"),
                       Button.inline("❌ Cancel", b"dashboard")]],
        )
    elif text == "/help":
        await event.reply("Use /panel to open the button dashboard.", buttons=main_buttons())

async def callbacks(event):
    if event.sender_id != OWNER_ID:
        await event.answer("Access denied.", alert=True)
        return
    data = event.data.decode("utf-8", errors="ignore")
    try:
        await event.answer()
        if data == "dashboard":
            pending.pop(OWNER_ID, None)
            await edit_or_reply(event, await dashboard_text(), main_buttons())
            return
        if data == "channels":
            pending.pop(OWNER_ID, None)
            text = "📡 Configured Channels\n\n" + ("Tap a channel for controls." if channels else "No channels configured yet.")
            await edit_or_reply(event, text, channels_buttons())
            return
        if data == "add":
            pending[OWNER_ID] = {"action": "add_channel"}
            await edit_or_reply(event, "➕ Add Channel\n\nSend the channel username or link in your next message.\nExample: @YourChannel", [[Button.inline("❌ Cancel", b"dashboard")]])
            return
        if data == "reaction_menu":
            await edit_or_reply(event, "😀 Reaction Settings\n\nSelect a channel.", channels_buttons("reactions"))
            return
        if data == "delay_menu":
            await edit_or_reply(event, "⏱ Delay Settings\n\nSelect a channel.", channels_buttons("delay"))
            return
        if data == "status":
            bot_me = await bot.get_me()
            user_state = "authorized" if user_session_ok else ("revoked/not authorized" if user else "not configured")
            await edit_or_reply(event, f"🔍 Status\n\nBot: @{getattr(bot_me, 'username', 'unknown')}\nWorker: {worker_name()}\nUSER_SESSION: {user_state}\nStorage: SQLite ({DB_PATH})\nAuto reactions: {'ON' if auto_reactions else 'OFF'}", [[Button.inline("🔄 Refresh", b"status"), Button.inline("⬅️ Back", b"dashboard")]])
            return
        if data == "clear_storage":
            await edit_or_reply(
                event,
                "⚠️ CLEAR LOCAL STORAGE\n\n"
                "This deletes the bot's locally stored channels, logs, "
                "and settings from the SQLite database.\n\n"
                "This cannot be undone.",
                [[
                    Button.inline("✅ CONFIRM CLEAR", b"clear_confirm"),
                    Button.inline("❌ Cancel", b"dashboard"),
                ]],
            )
            return

        if data == "clear_confirm":
            try:
                await clear_local_storage()
                await edit_or_reply(
                    event,
                    "✅ Local SQLite storage cleared.\n\n"
                    "Channels, logs, and settings have been deleted.",
                    [[Button.inline("🏠 Dashboard", b"dashboard")]],
                )
            except Exception as exc:
                await edit_or_reply(
                    event,
                    f"❌ Local storage was not cleared.\n\n{type(exc).__name__}: {exc}",
                    [[
                        Button.inline("🔍 Status", b"status"),
                        Button.inline("⬅️ Dashboard", b"dashboard"),
                    ]],
                )
            return

        if data == "logs":
            entries = await recent_logs()
            if not entries:
                text = "📋 Logs\n\nNo local logs available yet."
            else:
                lines = ["📋 Recent Logs", ""]
                for item in entries:
                    lines.append(f"{item.get('status', '?')} | {item.get('reaction') or '-'} | msg {item.get('message_id')} | {item.get('created_at', '')[:19]}")
                text = "\n".join(lines)
            await edit_or_reply(event, text, [[Button.inline("⬅️ Back", b"dashboard")]])
            return
        if data == "help":
            await edit_or_reply(event, "❔ How to use\n\n1. Add a channel.\n2. Choose reactions.\n3. Set delay.\n4. Keep Auto Reactions ON.\n\nStorage uses SQLite locally, so MONGODB is not required. Bot-only mode requires the bot to be a member of the channel. USER_SESSION enables the user-account worker.", [[Button.inline("⬅️ Dashboard", b"dashboard")]])
            return
        if data == "global_on":
            await db_set_global(True)
            await edit_or_reply(event, await dashboard_text(), main_buttons())
            return
        if data == "global_off":
            await db_set_global(False)
            await edit_or_reply(event, await dashboard_text(), main_buttons())
            return
        if ":" in data:
            action, raw_id, *rest = data.split(":")
            chat_id = int(raw_id)
            config = channels.get(chat_id)
            if config is None:
                await event.answer("Channel not found.", alert=True)
                return
            if action == "view":
                pending.pop(OWNER_ID, None)
                text, buttons = await channel_view(chat_id)
                await edit_or_reply(event, text, buttons)
                return
            if action == "toggle":
                config["enabled"] = not bool(config.get("enabled", True))
                config["updated_at"] = now_iso()
                await db_save_channel(config)
                text, buttons = await channel_view(chat_id)
                await edit_or_reply(event, text, buttons)
                return
            if action == "remove":
                pending.pop(OWNER_ID, None)
                channels.pop(chat_id, None)
                await db_delete_channel(chat_id)
                await edit_or_reply(event, "✅ Channel removed.", [[Button.inline("⬅️ Channels", b"channels")]])
                return
            if action in {"reactions", "delay"}:
                if action == "reactions":
                    selected = list(config.get("reactions", DEFAULT_REACTIONS))
                    pending[OWNER_ID] = {"action": "reaction_select", "chat_id": chat_id, "selected": selected}
                    await edit_or_reply(event, "😀 Choose reactions\n\nTap emojis, then Save.", reaction_buttons(chat_id, selected))
                else:
                    pending[OWNER_ID] = {"action": "delay", "chat_id": chat_id}
                    await edit_or_reply(event, "⏱ Set delay\n\nSend minimum and maximum seconds.\nExample: 2 5", [[Button.inline("❌ Cancel", f"view:{chat_id}".encode())]])
                return
            if action == "pick":
                emoji = rest[0] if rest else ""
                state = pending.setdefault(OWNER_ID, {"action": "reaction_select", "chat_id": chat_id, "selected": []})
                selected = state.setdefault("selected", [])
                if emoji in selected:
                    selected.remove(emoji)
                else:
                    selected.append(emoji)
                if not selected:
                    selected.append(emoji)
                await edit_or_reply(event, "😀 Choose reactions\n\nTap emojis, then Save.", reaction_buttons(chat_id, selected))
                return
            if action == "save_reactions":
                state = pending.get(OWNER_ID, {})
                selected = state.get("selected") or DEFAULT_REACTIONS
                config["reactions"] = selected[:10]
                config["updated_at"] = now_iso()
                await db_save_channel(config)
                pending.pop(OWNER_ID, None)
                text, buttons = await channel_view(chat_id)
                await edit_or_reply(event, text, buttons)
                return
            if action == "custom":
                pending[OWNER_ID] = {"action": "custom_reactions", "chat_id": chat_id}
                await edit_or_reply(event, "✏️ Custom reactions\n\nSend emojis separated by commas.\nExample: ❤️,🔥,😂", [[Button.inline("❌ Cancel", f"view:{chat_id}".encode())]])
                return
            if action == "test":
                messages = await active_client.get_messages(chat_id, limit=1)
                if not messages:
                    await event.answer("No post found.", alert=True)
                    return
                message = messages[0]
                reaction = random.choice(config.get("reactions") or DEFAULT_REACTIONS)
                try:
                    await active_client(functions.messages.SendReactionRequest(peer=chat_id, msg_id=message.id, reaction=[types.ReactionEmoji(emoticon=reaction)]))
                    await db_log(chat_id, message.id, reaction, "TEST")
                    await event.answer(f"Sent {reaction}", alert=True)
                except errors.RPCError as exc:
                    await event.answer(f"Reaction failed: {exc}", alert=True)
                return
        await event.answer("Unknown action.", alert=True)
    except Exception as exc:
        log.exception("Button handler failure")
        try:
            await event.answer(f"Error: {type(exc).__name__}", alert=True)
        except Exception:
            pass

async def react_to_post(event, client: TelegramClient) -> None:
    if not auto_reactions:
        return
    message = event.message
    if not message or not getattr(message, "post", False):
        return
    chat = await event.get_chat()
    if not isinstance(chat, types.Channel) or getattr(chat, "megagroup", False):
        return
    chat_id = client.get_peer_id(chat)
    config = channels.get(chat_id)
    if not config or not config.get("enabled", True):
        return
    delay = random.uniform(float(config.get("delay_min", 1)), float(config.get("delay_max", 4)))
    if delay:
        await asyncio.sleep(delay)
    reaction = random.choice(config.get("reactions") or DEFAULT_REACTIONS)
    try:
        await client(functions.messages.SendReactionRequest(peer=chat, msg_id=message.id, reaction=[types.ReactionEmoji(emoticon=reaction)]))
        log.info("Reacted %s in %s to message %s", reaction, chat.title, message.id)
        await db_log(chat_id, message.id, reaction, "SUCCESS")
    except errors.FloodWaitError as exc:
        log.warning("Flood wait: %ss", exc.seconds)
        await db_log(chat_id, message.id, reaction, "FLOOD_WAIT", f"{exc.seconds}s")
    except errors.RPCError as exc:
        log.warning("Reaction failed: %s", exc)
        await db_log(chat_id, message.id, reaction, "ERROR", str(exc))

async def user_post_handler(event):
    try:
        await react_to_post(event, user)
    except Exception:
        log.exception("User reaction error")

async def bot_post_handler(event):
    try:
        await react_to_post(event, bot)
    except Exception:
        log.exception("Bot reaction error")

async def main():
    global active_client, user_session_ok
    init_storage()
    await bot.start(bot_token=BOT_TOKEN)
    bot.add_event_handler(messages, events.NewMessage(incoming=True))
    bot.add_event_handler(callbacks, events.CallbackQuery)
    active_client = bot
    if user:
        try:
            await user.connect()
            if await user.is_user_authorized():
                user_session_ok = True
                active_client = user
                user.add_event_handler(user_post_handler, events.NewMessage(incoming=True))
                log.info("User worker session connected.")
            else:
                log.warning("USER_SESSION is not authorized; using bot-only mode.")
                await user.disconnect()
        except errors.SessionRevokedError:
            user_session_ok = False
            log.warning("USER_SESSION was revoked; using bot-only mode.")
            try:
                await user.disconnect()
            except Exception:
                pass
        except Exception as exc:
            user_session_ok = False
            log.warning("User session unavailable; using bot-only mode: %s", exc)
            try:
                await user.disconnect()
            except Exception:
                pass
    bot.add_event_handler(bot_post_handler, events.NewMessage(incoming=True))
    await db_load()
    log.info("AniToons_1Bot online | mode=%s | sqlite=%s", worker_name(), DB_PATH)
    await bot.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
