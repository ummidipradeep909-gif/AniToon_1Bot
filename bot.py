from __future__ import annotations

import asyncio
import logging
import os
import random
from datetime import datetime, timezone
from urllib.parse import quote, unquote, urlsplit, urlunsplit
from typing import Any

from dotenv import load_dotenv
from pymongo import MongoClient
from telethon import Button, TelegramClient, events, functions, types, errors
from telethon.sessions import StringSession

load_dotenv()

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"].strip()
BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
OWNER_ID = int(os.environ["OWNER_ID"])
USER_SESSION = os.getenv("USER_SESSION", "").strip()
MONGODB = os.getenv("MONGODB", "").strip()

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

mongo: MongoClient | None = None
channel_collection = None
log_collection = None
settings_collection = None
mongo_error = ""

def normalized_mongodb_uri(uri: str) -> str:
    """Normalize URI credentials so reserved password characters work."""
    parsed = urlsplit(uri.strip())
    if not parsed.scheme or not parsed.hostname:
        return uri.strip()
    if parsed.username is None:
        return uri.strip()

    username = quote(unquote(parsed.username), safe="")
    password = ""
    if parsed.password is not None:
        password = quote(unquote(parsed.password), safe="")
    host = parsed.hostname
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    if parsed.port:
        host = f"{host}:{parsed.port}"
    userinfo = username
    if parsed.password is not None:
        userinfo += ":" + password
    userinfo += "@"
    rebuilt = urlunsplit((parsed.scheme, userinfo + host, parsed.path, parsed.query, parsed.fragment))
    return rebuilt

def configure_mongo(client: MongoClient) -> None:
    global mongo, channel_collection, log_collection, settings_collection
    mongo = client
    db = client.get_database("anitoons_1bot")
    channel_collection = db.get_collection("channels")
    log_collection = db.get_collection("logs")
    settings_collection = db.get_collection("settings")

def connect_mongo() -> None:
    global mongo_error
    if not MONGODB:
        mongo_error = "MONGODB environment variable is empty."
        raise RuntimeError(mongo_error)

    uri = normalized_mongodb_uri(MONGODB)
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    configure_mongo(client)
    mongo_error = ""
    log.info("MongoDB connected.")

try:
    connect_mongo()
except Exception as exc:
    mongo_error = str(exc)
    log.warning("MongoDB unavailable; using memory: %s", exc)
    mongo = None
    channel_collection = None
    log_collection = None
    settings_collection = None

def owner_only(event) -> bool:
    return bool(event.is_private and event.sender_id == OWNER_ID)

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def worker_name() -> str:
    return "user session" if user_session_ok and user else "bot session"

async def db_load() -> None:
    global auto_reactions
    if channel_collection is None:
        return
    try:
        docs = await asyncio.to_thread(lambda: list(channel_collection.find({})))
        for item in docs:
            item.pop("_id", None)
            channels[int(item["chat_id"])] = item
        setting = await asyncio.to_thread(lambda: settings_collection.find_one({"_id": "global"})) if settings_collection is not None else None
        if setting:
            auto_reactions = bool(setting.get("auto_reactions", True))
    except Exception:
        log.exception("Failed to load MongoDB data")

async def db_save_channel(config: dict[str, Any]) -> None:
    if channel_collection is None:
        return
    payload = dict(config)
    await asyncio.to_thread(lambda: channel_collection.replace_one({"chat_id": int(payload["chat_id"])}, payload, upsert=True))

async def db_delete_channel(chat_id: int) -> None:
    if channel_collection is not None:
        await asyncio.to_thread(lambda: channel_collection.delete_one({"chat_id": int(chat_id)}))

async def db_set_global(value: bool) -> None:
    global auto_reactions
    auto_reactions = value
    if settings_collection is not None:
        await asyncio.to_thread(lambda: settings_collection.update_one({"_id": "global"}, {"$set": {"auto_reactions": value}}, upsert=True))

async def db_log(chat_id: int, message_id: int, reaction: str | None, status: str, detail: str = "") -> None:
    if log_collection is None:
        return
    payload = {"chat_id": int(chat_id), "message_id": int(message_id), "reaction": reaction, "status": status, "detail": detail[:500], "created_at": now_iso()}
    await asyncio.to_thread(lambda: log_collection.insert_one(payload))

async def recent_logs(limit: int = 12) -> list[dict[str, Any]]:
    if log_collection is None:
        return []
    docs = await asyncio.to_thread(lambda: list(log_collection.find({}).sort("_id", -1).limit(limit)))
    for item in docs:
        item.pop("_id", None)
    return docs

async def clear_mongo_storage() -> None:
    global mongo, channel_collection, log_collection, settings_collection
    # Reconnect for every clear request. This handles credentials fixed in Render
    # without relying on a MongoClient that failed during process startup.
    await asyncio.to_thread(connect_mongo)
    if mongo is None or channel_collection is None or log_collection is None or settings_collection is None:
        raise RuntimeError("MongoDB connection could not be established.")
    await asyncio.to_thread(channel_collection.delete_many, {})
    await asyncio.to_thread(log_collection.delete_many, {})
    await asyncio.to_thread(settings_collection.delete_many, {})
    channels.clear()
    pending.clear()

def main_buttons():
    return [
        [Button.inline("📊 Dashboard", b"dashboard"), Button.inline("📡 Channels", b"channels")],
        [Button.inline("➕ Add Channel", b"add"), Button.inline("😀 Reactions", b"reaction_menu")],
        [Button.inline("⏱ Delay", b"delay_menu"), Button.inline("📋 Logs", b"logs")],
        [Button.inline("▶️ Start", b"global_on"), Button.inline("⏸ Stop", b"global_off")],
        [Button.inline("🔍 Status", b"status"), Button.inline("❔ Help", b"help")],
        [Button.inline("🧹 Clear MongoDB", b"clear_storage")],
    ]

async def dashboard_text() -> str:
    enabled = sum(1 for x in channels.values() if x.get("enabled", True))
    state = "🟢 ON" if auto_reactions else "🔴 OFF"
    mongo_state = "🟢 Connected" if mongo else "🟡 Not connected"
    return (
        "🎬 AniToons 1Bot Control Panel\n\n"
        f"Auto reactions: {state}\n"
        f"Channels: {len(channels)} ({enabled} enabled)\n"
        f"Worker: {worker_name()}\n"
        f"MongoDB: {mongo_state}\n\n"
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
            "⚠️ This deletes the bot's MongoDB channels, logs, and settings. "
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
            await edit_or_reply(event, f"🔍 Status\n\nBot: @{getattr(bot_me, 'username', 'unknown')}\nWorker: {worker_name()}\nUSER_SESSION: {user_state}\nMongoDB: {'connected' if mongo else 'not connected'}\nAuto reactions: {'ON' if auto_reactions else 'OFF'}", [[Button.inline("🔄 Refresh", b"status"), Button.inline("⬅️ Back", b"dashboard")]])
            return
        if data == "clear_storage":
            await edit_or_reply(
                event,
                "⚠️ CLEAR MONGODB STORAGE\n\n"
                "This will delete only this bot's stored channels, logs, "
                "and settings from the anitoons_1bot database.\n\n"
                "This cannot be undone.",
                [[
                    Button.inline("✅ CONFIRM CLEAR", b"clear_confirm"),
                    Button.inline("❌ Cancel", b"dashboard"),
                ]],
            )
            return

        if data == "clear_confirm":
            try:
                await clear_mongo_storage()
                await edit_or_reply(
                    event,
                    "✅ MongoDB storage cleared.\n\n"
                    "Channels, logs, and settings have been deleted.",
                    [[Button.inline("🏠 Dashboard", b"dashboard")]],
                )
            except Exception as exc:
                await edit_or_reply(
                    event,
                    f"❌ MongoDB storage was not cleared.\n\n{type(exc).__name__}: {exc}",
                    [[
                        Button.inline("🔍 Status", b"status"),
                        Button.inline("⬅️ Dashboard", b"dashboard"),
                    ]],
                )
            return

        if data == "logs":
            entries = await recent_logs()
            if not entries:
                text = "📋 Logs\n\nNo MongoDB logs available yet."
            else:
                lines = ["📋 Recent Logs", ""]
                for item in entries:
                    lines.append(f"{item.get('status', '?')} | {item.get('reaction') or '-'} | msg {item.get('message_id')} | {item.get('created_at', '')[:19]}")
                text = "\n".join(lines)
            await edit_or_reply(event, text, [[Button.inline("⬅️ Back", b"dashboard")]])
            return
        if data == "help":
            await edit_or_reply(event, "❔ How to use\n\n1. Add a channel.\n2. Choose reactions.\n3. Set delay.\n4. Keep Auto Reactions ON.\n\nBot-only mode requires the bot to be a member of the channel. USER_SESSION enables the user-account worker.", [[Button.inline("⬅️ Dashboard", b"dashboard")]])
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
    log.info("AniToons_1Bot online | mode=%s | mongodb=%s", worker_name(), bool(mongo))
    await bot.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())