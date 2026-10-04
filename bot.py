from __future__ import annotations

import asyncio
import html
import json
import logging
import os
import secrets
import shutil
import time
from contextlib import suppress
from dataclasses import asdict, dataclass
from datetime import datetime, timezone, timedelta
from urllib.parse import urlsplit
from typing import Any

from dotenv import load_dotenv
from telethon import Button, TelegramClient, errors, events, functions, types
from telethon.sessions import MemorySession

from file_inspector import Report, format_report, format_section
from media_probe import (
    ProbeBudgetExceeded,
    ProbeCancelled,
    cancel_probe,
    get_probe,
    inspect_telegram_player,
    purge_probes,
)
from mongo_store import (
    get_user_clone,
    list_user_clones,
    mark_clone_removed,
    owner_7day_summary,
    owner_clone_records,
    owner_user_scans,
    load_web_report,
    ensure_mongodb,
    mongodb_is_configured,
    mongodb_is_connected,
    record_clone_request,
    user_scan_summary,
    record_scan,
    record_user,
    record_bot_user,
    list_bot_users,
    save_web_report,
    update_clone_stats,
    record_group_chat,
    list_group_chats,
    update_group_onboarding,
)

load_dotenv()

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"].strip()
BOT_TOKEN = os.environ["BOT_TOKEN"].strip()

FILE_CHECKER_PRIVATE_ONLY = (
    os.getenv("FILE_CHECKER_PRIVATE_ONLY", "0").strip().lower()
    not in {"0", "false", "no", "off"}
)
MAX_CONCURRENT_CHECKS = max(1, min(int(os.getenv("MAX_CONCURRENT_CHECKS", "10")), 10))
SCAN_TIMEOUT_SECONDS = max(30, min(int(os.getenv("SCAN_TIMEOUT_SECONDS", "300")), 300))
REPORT_LINK_TTL_SECONDS = 5 * 60
PENDING_SCAN_TTL_SECONDS = 10 * 60
MAX_STORED_RESULTS = 100
PUBLIC_WEB_URL = (
    os.getenv("PUBLIC_WEB_URL", "https://anitoons-1bot-oa44.onrender.com")
    .strip()
    .rstrip("/")
)
CLONE_BOT_USERNAME = os.getenv("CLONE_BOT_USERNAME", "").strip().lstrip("@")
BOT_USERNAME = os.getenv("BOT_USERNAME", "AniToon_1Bot").strip().lstrip("@")
OWNER_ID = int(os.getenv("OWNER_ID", "0") or "0")
# Clone bots are live in memory only; scan execution is globally queued.
MAX_ACTIVE_SCANS_PER_USER = max(1, min(int(os.getenv("MAX_ACTIVE_SCANS_PER_USER", "1")), 2))
SCAN_COOLDOWN_SECONDS = max(0, min(int(os.getenv("SCAN_COOLDOWN_SECONDS", "2")), 10))
WEB_EGRESS_GUARD_BYTES = 4 * 1024 * 1024 * 1024

# Ask Telegram to pre-enable all group admin permissions when the user adds AniToon.
# Telegram still lets the group owner change any permission before confirming.
GROUP_ADMIN_PERMISSIONS = (
    "change_info+delete_messages+restrict_members+invite_users+pin_messages+"
    "manage_topics+promote_members+manage_video_chats+anonymous+manage_chat+"
    "post_stories+edit_stories+delete_stories"
)
ADD_TO_GROUP_URL = (
    f"https://t.me/{BOT_USERNAME}?startgroup&admin={GROUP_ADMIN_PERMISSIONS}"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("anitoons-file-checker")

bot = TelegramClient("file_checker_bot", API_ID, API_HASH)
bot.flood_sleep_threshold = 15 * 60
check_semaphore = asyncio.Semaphore(MAX_CONCURRENT_CHECKS)
scan_queue_lock = asyncio.Lock()
active_processes = 0
queued_processes = 0
web_bytes_sent = 0
started_at = datetime.now(timezone.utc)

checks_total = 0
checks_ok = 0
checks_failed = 0


@dataclass(slots=True)
class ScanState:
    source_message: Any
    report: Report
    created_at: float
    web_token: str
    busy: bool = False


@dataclass(slots=True)
class PendingScan:
    source_message: Any
    created_at: float
    user_id: int | None = None
    client: Any = None
    bot_username: str = BOT_USERNAME
    include_clone: bool = False
    show_privacy: bool = True
    busy: bool = False


# Live clone bot clients. Each clone runs on this same asyncio event loop.
clone_clients: dict[int, TelegramClient] = {}
clone_owners: dict[int, int] = {}
clone_usernames: dict[int, str] = {}
clone_owner_names: dict[int, str] = {}
clone_client_ids: dict[int, int] = {}
clone_stats: dict[int, dict[str, Any]] = {}
clone_message_pending: dict[int, int] = {}
clone_monitor_task: asyncio.Task | None = None
active_scan_clients: dict[str, Any] = {}
bot_identity_ids: dict[int, int] = {}
known_group_chats: dict[tuple[int, int], dict[str, Any]] = {}
group_touch_cache: dict[tuple[int, int], float] = {}
owner_broadcast_pending: dict[int, float] = {}
group_onboarding_bootstrap: dict[tuple[int, int], float] = {}
bot_audience_touch_cache: dict[tuple[int, int], float] = {}


scan_states: dict[tuple[int, int], ScanState] = {}
web_states: dict[str, ScanState] = {}
pending_scans: dict[str, PendingScan] = {}
active_scans: dict[str, asyncio.Task] = {}
active_scan_users: dict[str, int | None] = {}
clone_setup_pending: dict[int, float] = {}
last_scan_by_user: dict[int, float] = {}
CLONE_SETUP_TTL_SECONDS = 5 * 60
OWNER_BROADCAST_TTL_SECONDS = 10 * 60
GROUP_TOUCH_INTERVAL_SECONDS = 15 * 60
GROUP_ONBOARDING_LOOP_SECONDS = 300
GROUP_ONBOARDING_DAYS = 7

def add_to_group_url(bot_username: str = BOT_USERNAME) -> str:
    permissions = GROUP_ADMIN_PERMISSIONS
    return f"https://t.me/{bot_username}?startgroup&admin={permissions}"


def safe_filename(message: Any) -> str:
    name = getattr(getattr(message, "file", None), "name", None)
    return str(name or "telegram_file")


def metadata_button(token: str):
    return [[Button.inline("🔎 Scan File Info", f"scan:{token}".encode("ascii"))]]


def cancel_button(token: str):
    return [[Button.inline("❌ Cancel Scan", f"cancel:{token}".encode("ascii"))]]


def clone_setup_buttons():
    return [[
        Button.url("🤖 Open @BotFather", "https://t.me/BotFather"),
        Button.inline("⬅️ Cancel", b"clone:cancel"),
    ]]

def clone_manager_buttons(records: list[dict[str, Any]]) -> list[list[Any]]:
    buttons: list[list[Any]] = []
    for record in records[:2]:
        clone_id = int(record["clone_id"])
        username = str(record.get("clone_username") or "").strip().lstrip("@")
        if username:
            buttons.append([Button.url(f"🤖 @{username}", f"https://t.me/{username}")])
        buttons.append([
            Button.inline("📊 Stats", f"clone:stats:{clone_id}".encode("ascii")),
            Button.inline("🗑 Remove", f"clone:remove:{clone_id}".encode("ascii")),
        ])
    if len(records) < 2:
        buttons.append([Button.inline("🧬 Create Clone", b"clone:create")])
    buttons.append([Button.inline("⬅️ Home", b"home:back")])
    return buttons


def web_report_button(
    token: str,
    bot_username: str = BOT_USERNAME,
    *,
    include_clone: bool = True,
):
    buttons = [
        [Button.url("🌐 Open File Info", f"{PUBLIC_WEB_URL}/report/{token}")],
    ]
    if include_clone and CLONE_BOT_USERNAME:
        buttons.append([
            Button.url("🤖 Clone Bot", f"https://t.me/{CLONE_BOT_USERNAME}")
        ])
    buttons.append([
        Button.url("➕ Add Me to Your Group", add_to_group_url(bot_username))
    ])
    return buttons


def compact_scan_result(report: Report) -> str:
    return (
        "✅ <b>METADATA SCAN COMPLETE</b>\n\n"
        "🌐 Tap <b>Open File Info</b> below to view the complete file metadata."
    )


HOME_TEXT = (
    "⛩ <b>Welcome to AniToon</b> ⛩\n\n"
    "🎞️ <b>File Metadata • Clone Bots • Smart Reports</b>\n"
    "⚡ Fast, bounded media inspection with a clean web report.\n\n"
    "✨ Simple, fast and easy to use."
)

CLONE_HOME_TEXT = (
    "⛩ <b>AniToon Media Info Clone</b> ⛩\n\n"
    "🔎 Scan Telegram videos and documents for detailed media information.\n"
    "🌐 Open the complete file report in your browser."
)

CLONE_HELP_TEXT = (
    "📖 <b>How to Use AniToon Media Info Bot</b>\n\n"
    "1️⃣ Send a Telegram <b>video or document</b> to the bot.\n"
    "2️⃣ Press <b>🔎 Scan File Info</b>.\n"
    "3️⃣ Open <b>🌐 Open File Info</b> for the complete report.\n\n"
    "📋 <b>Only command</b>\n"
    "/start — Open Home"
)

HELP_TEXT = (
    "📖 <b>How to Use AniToon Media Info Bot</b>\n\n"
    "1️⃣ Send a Telegram <b>video or document</b> to the bot.\n"
    "2️⃣ Press <b>🔎 Scan File Info</b>.\n"
    "3️⃣ Wait for the metadata scan to finish.\n"
    "4️⃣ Press <b>🌐 Open File Info</b> for the full web report.\n\n"
    "🤖 <b>Clone Bots</b>\n"
    "Use <b>🤖 Clone Manager</b> to create and manage up to <b>2</b> clone bots.\n\n"
    "📋 <b>Commands</b>\n"
    "/start — Open Home\n"
    "/help — Open this guide\n"
    "/stats — View your 7-day scan stats\n"

    "/about — About AniToon\n"
    "/addtogroup — Add AniToon to a group\n"
    "/clone — Create/connect a clone bot\n"
    "/clones — View and manage your clones\n"
    "/myclones — Same as /clones\n"
    "/cancel — Cancel your running scan"
)

ABOUT_TEXT = (
    "ℹ️ <b>About AniToon</b> ✨\n\n"
    "🤖 <b>AniToon Bot</b>\n"
    "A Telegram bot that detects your video or document and scans its available metadata using bounded reads.\n\n"
    "🌐 <b>AniToon Web</b>\n"
    "The web page presents the complete file information in a clean, easy-to-read report.\n\n"
    "📦 <b>What you get</b>\n"
    "Video • Audio • Subtitles • Container • Technical metadata\n\n"
    "⚡ <b>Simple workflow</b>\n"
    "Send file → tap Scan File Info → open the web report."
)


def cache_state(
    status_message: Any,
    source_message: Any,
    report: Report,
    web_token: str | None = None,
) -> ScanState:
    token = web_token or secrets.token_urlsafe(18)
    state = ScanState(
        source_message=source_message,
        report=report,
        created_at=time.monotonic(),
        web_token=token,
    )
    web_states[token] = state
    while len(web_states) > MAX_STORED_RESULTS:
        oldest = next(iter(web_states), None)
        if oldest is None:
            break
        web_states.pop(oldest, None)
    return state


def purge_pending_scans() -> None:
    now = time.monotonic()
    for token, pending in list(pending_scans.items()):
        if now - pending.created_at > PENDING_SCAN_TTL_SECONDS:
            pending_scans.pop(token, None)


def _purge_states() -> None:
    now = time.monotonic()
    expired = [
        token for token, state in web_states.items()
        if now - state.created_at > REPORT_LINK_TTL_SECONDS
    ]
    for token in expired:
        web_states.pop(token, None)
    pending_expired = [
        token for token, pending in pending_scans.items()
        if now - pending.created_at > PENDING_SCAN_TTL_SECONDS
    ]
    for token in pending_expired:
        pending_scans.pop(token, None)


def home_buttons(
    bot_username: str = BOT_USERNAME,
    *,
    include_clone: bool = True,
    user_id: int | None = None,
    show_privacy: bool = True,
):
    scan = Button.inline("🔎 Scan Files", b"home:scan")
    stats = Button.inline("📊 My Stats", b"home:stats")
    help_btn = Button.inline("📖 Help", b"home:help")
    about = Button.inline("ℹ️ About", b"home:about")
    add_group = Button.url("➕ Add to Group", add_to_group_url(bot_username))

    if include_clone:
        clone = Button.inline("🤖 Clone Manager", b"home:clones")
        buttons = [
            [scan, clone],
            [stats, help_btn],
            [about, add_group],
        ]
        if _owner_allowed(user_id):
            buttons[-1] = [
                about,
                Button.inline("👑 Owner Dashboard", b"owner:dashboard"),
            ]
            buttons.append([add_group])
    else:
        buttons = [
            [scan, stats],
            [help_btn, about],
            [add_group],
        ]
    return buttons


def back_buttons() -> list[list[Any]]:
    return [[Button.inline("⬅️ Back", b"home:back")]]


def help_buttons() -> list[list[Any]]:
    return back_buttons()


def scan_page_buttons() -> list[list[Any]]:
    return back_buttons()




def _new_clone_stats() -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    return {
        "messages_received": 0,
        "scans_started": 0,
        "scans_completed": 0,
        "scans_failed": 0,
        "scans_cancelled": 0,
        "created_at": now,
        "last_activity": now,
    }


def _ensure_clone_stats(clone_id: int, record: dict[str, Any] | None = None) -> dict[str, Any]:
    clone_id = int(clone_id)
    stats = clone_stats.setdefault(clone_id, _new_clone_stats())
    if record:
        for key in (
            "messages_received",
            "scans_started",
            "scans_completed",
            "scans_failed",
            "scans_cancelled",
        ):
            if key in record:
                stats[key] = int(record.get(key) or 0)
        if record.get("created_at") is not None:
            stats["created_at"] = record["created_at"]
        if record.get("last_activity") is not None:
            stats["last_activity"] = record["last_activity"]
    return stats


def _clone_id_for_client(client: Any) -> int | None:
    return clone_client_ids.get(id(client))


async def bump_clone_stat(client: Any, field: str, amount: int = 1) -> None:
    clone_id = _clone_id_for_client(client)
    if clone_id is None:
        return

    stats = _ensure_clone_stats(clone_id)
    stats[field] = int(stats.get(field, 0)) + int(amount)
    stats["last_activity"] = datetime.now(timezone.utc)

    # Batch high-frequency message counters to reduce database traffic.
    if field == "messages_received":
        pending = clone_message_pending.get(clone_id, 0) + int(amount)
        clone_message_pending[clone_id] = pending
        if pending < 20:
            return
        clone_message_pending[clone_id] = 0
        amount = pending

    try:
        await update_clone_stats(clone_id=clone_id, **{field: int(amount)})
    except Exception:
        log.exception(
            "Failed to persist clone stat | clone_id=%s | field=%s",
            clone_id,
            field,
        )


def _fmt_clone_time(value: Any) -> str:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(float(value), tz=timezone.utc)
    else:
        return "Not available"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M UTC")


async def _user_clone_records(user_id: int) -> list[dict[str, Any]]:
    records = await list_user_clones(int(user_id))

    # MongoDB is optional. During this session, live clones remain visible even
    # when persistent storage is unavailable.
    if not records:
        for clone_id, owner_id in clone_owners.items():
            if int(owner_id) != int(user_id):
                continue
            client = clone_clients.get(int(clone_id))
            stats = _ensure_clone_stats(int(clone_id))
            records.append({
                "user_id": int(user_id),
                "clone_id": int(clone_id),
                "clone_username": clone_usernames.get(int(clone_id)),
                "status": "online" if client and client.is_connected() else "offline",
                **stats,
            })

    for record in records:
        clone_id = int(record["clone_id"])
        stats = _ensure_clone_stats(clone_id, record)
        for key in (
            "messages_received",
            "scans_started",
            "scans_completed",
            "scans_failed",
            "scans_cancelled",
        ):
            record[key] = int(stats.get(key, record.get(key, 0)) or 0)
        record["clone_username"] = record.get("clone_username") or clone_usernames.get(clone_id)
        record["status"] = (
            "online"
            if clone_id in clone_clients and clone_clients[clone_id].is_connected()
            else "offline"
        )

    records.sort(
        key=lambda item: item.get("created_at") or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    return records


def _clone_manager_buttons(records: list[dict[str, Any]]) -> list[list[Any]]:
    return clone_manager_buttons(records)


async def render_clone_list(event, user_id: int, *, edit: bool = True) -> None:
    records = await _user_clone_records(int(user_id))
    count = min(len(records), 2)
    lines = [
        "🤖 <b>Clone Manager</b>",
        f"🧩 Slots used: <b>{count}/2</b>",
        "",
    ]

    if records:
        for index, record in enumerate(records[:2], 1):
            username = str(record.get("clone_username") or "Unnamed").lstrip("@")
            status = "🟢 Online" if record.get("status") == "online" else "🔴 Offline"
            scans = int(record.get("scans_started", 0) or 0)
            lines.append(
                f"{index}. <b>@{html.escape(username)}</b> • {status} • 📊 {scans}"
            )
    else:
        lines.append("No clone bot connected yet.")

    buttons = clone_manager_buttons(records)
    if edit:
        await event.edit("\n".join(lines), parse_mode="html", buttons=buttons)
    else:
        await event.reply("\n".join(lines), parse_mode="html", buttons=buttons)


async def render_clone_stats(event, user_id: int, clone_id: int) -> None:
    record = await get_user_clone(int(user_id), int(clone_id))
    if record is None and clone_owners.get(int(clone_id)) != int(user_id):
        await event.answer("That clone is not owned by you.", alert=True)
        return

    if record is None:
        record = {
            "user_id": int(user_id),
            "clone_id": int(clone_id),
            "clone_username": clone_usernames.get(int(clone_id)),
            "status": "online",
        }

    clone_id = int(record["clone_id"])
    stats = _ensure_clone_stats(clone_id, record)
    username = str(
        record.get("clone_username")
        or clone_usernames.get(clone_id)
        or "Unnamed"
    ).lstrip("@")
    live = clone_id in clone_clients and clone_clients[clone_id].is_connected()

    text = (
        "📊 <b>Clone Bot Stats</b>\n\n"
        f"🤖 <b>@{html.escape(username)}</b>\n"
        f"{'🟢 Online' if live else '🔴 Offline'}\n\n"
        f"📨 Messages received: <b>{int(stats['messages_received'])}</b>\n"
        f"🔎 Scans started: <b>{int(stats['scans_started'])}</b>\n"
        f"✅ Scans completed: <b>{int(stats['scans_completed'])}</b>\n"
        f"❌ Scans failed: <b>{int(stats['scans_failed'])}</b>\n"
        f"🛑 Scans cancelled: <b>{int(stats['scans_cancelled'])}</b>\n\n"
        f"📅 Created: {_fmt_clone_time(stats.get('created_at'))}\n"
        f"🕒 Last activity: {_fmt_clone_time(stats.get('last_activity'))}"
    )

    buttons = []
    if username != "Unnamed":
        buttons.append([Button.url("🤖 Open Clone Bot", f"https://t.me/{username}")])
    buttons.append([
        Button.inline("🗑 Remove Clone", f"clone:remove:{clone_id}".encode("ascii")),
        Button.inline("⬅️ My Clones", b"clone:list"),
    ])
    await event.edit(text, parse_mode="html", buttons=buttons)


async def remove_clone_for_user(user_id: int, clone_id: int) -> bool:
    record = await get_user_clone(int(user_id), int(clone_id))
    owner = clone_owners.get(int(clone_id))
    if record is None and owner != int(user_id):
        return False
    if record is not None and int(record.get("user_id", user_id)) != int(user_id):
        return False

    client = clone_clients.pop(int(clone_id), None)
    clone_owners.pop(int(clone_id), None)
    clone_usernames.pop(int(clone_id), None)
    clone_stats.pop(int(clone_id), None)
    clone_message_pending.pop(int(clone_id), None)

    if client is not None:
        clone_client_ids.pop(id(client), None)
        for token, task in list(active_scans.items()):
            if active_scan_clients.get(token) is client:
                task.cancel()
        with suppress(Exception):
            await client.disconnect()

    await mark_clone_removed(int(user_id), int(clone_id))
    return True


def runtime_resource_stats() -> dict[str, Any]:
    """Return app-local resource signals; exact workspace billing remains in Render."""
    rss_mb = 0.0
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("VmRSS:"):
                    rss_mb = float(line.split()[1]) / 1024.0
                    break
    except (OSError, ValueError):
        pass

    ram_limit_mb = 512.0
    ram_pct = (rss_mb / ram_limit_mb * 100.0) if ram_limit_mb else 0.0

    try:
        disk = shutil.disk_usage("/")
        disk_used_pct = disk.used / disk.total * 100.0 if disk.total else 0.0
        disk_used_gb = disk.used / (1024**3)
    except OSError:
        disk_used_pct = 0.0
        disk_used_gb = 0.0

    uptime_hours = (datetime.now(timezone.utc) - started_at).total_seconds() / 3600.0
    uptime_pct = min(100.0, uptime_hours / 750.0 * 100.0)
    egress_gb = web_bytes_sent / (1024**3)
    egress_pct = min(100.0, egress_gb / 4.0 * 100.0)

    return {
        "rss_mb": rss_mb,
        "ram_limit_mb": ram_limit_mb,
        "ram_pct": ram_pct,
        "disk_used_pct": disk_used_pct,
        "disk_used_gb": disk_used_gb,
        "uptime_hours": uptime_hours,
        "uptime_pct": uptime_pct,
        "web_egress_gb": egress_gb,
        "web_egress_pct": egress_pct,
    }


def memory_pressure_high() -> bool:
    return runtime_resource_stats()["ram_pct"] >= 80.0


def _owner_allowed(user_id: int | None) -> bool:
    return OWNER_ID > 0 and user_id is not None and int(user_id) == OWNER_ID

def _owner_user_label(record: dict[str, Any]) -> str:
    username = str(record.get("username") or "").strip().lstrip("@")
    name = " ".join(str(record.get(key) or "").strip() for key in ("first_name", "last_name")).strip()
    return f"@{username}" if username else (name or f"User {record.get('user_id', '?')}")

async def render_owner_resources(event, user_id: int) -> None:
    if not _owner_allowed(user_id):
        await event.answer("Owner access only.", alert=True)
        return

    stats = runtime_resource_stats()
    ram_state = "🟢 Normal" if stats["ram_pct"] < 70 else ("🟡 Watch" if stats["ram_pct"] < 80 else "🔴 High")
    egress_state = "🟢 Low" if stats["web_egress_pct"] < 70 else ("🟡 Watch" if stats["web_egress_pct"] < 85 else "🔴 High")

    text = (
        "🖥️ <b>Render Free Resource Guard</b>\n\n"
        f"🧠 RAM: <b>{stats['rss_mb']:.1f} / 512 MB</b> • <b>{stats['ram_pct']:.1f}%</b> {ram_state}\n"
        f"📡 Tracked web egress: <b>{stats['web_egress_gb']:.3f} / 4 GB app guard</b> • <b>{stats['web_egress_pct']:.1f}%</b> {egress_state}\n"
        f"⏱️ Current process uptime: <b>{stats['uptime_hours']:.2f} h</b>\n"
        f"📊 Uptime vs 750h allowance: <b>{stats['uptime_pct']:.1f}%</b>\n"
        f"💾 Local filesystem currently used: <b>{stats['disk_used_gb']:.2f} GB</b> • {stats['disk_used_pct']:.1f}%\n\n"
        "🛡️ New heavy scans are paused when RAM pressure reaches 80%.\n"
        "⚠️ Render's exact workspace billing meter is still shown in Render Billing/Metrics; "
        "the egress figure above is only traffic tracked by this process."
    )
    await event.edit(
        text,
        parse_mode="html",
        buttons=[
            [Button.inline("🔄 Refresh", b"owner:resources")],
            [Button.inline("⬅️ Dashboard", b"owner:dashboard")],
        ],
    )


async def render_owner_dashboard(event, user_id: int, *, edit: bool = True) -> None:
    if not _owner_allowed(user_id):
        await event.answer("Owner access only.", alert=True)
        return

    summary = await owner_7day_summary(7)
    mongo = "🟢 Connected" if mongodb_is_connected() else (
        "🟠 Configured / reconnecting" if mongodb_is_configured() else "🔴 Not configured"
    )
    total = int(summary.get("completed", 0) or 0) + int(summary.get("failed", 0) or 0)
    success = (int(summary.get("completed", 0) or 0) / total * 100) if total else 0.0
    uptime = (datetime.now(timezone.utc) - started_at).total_seconds() / 3600

    if summary.get("available"):
        text = (
            "👑 <b>AniToon Owner Control Center</b>\n\n"
            "📅 <b>Last 7 Days</b>\n"
            f"👥 Active users: <b>{int(summary.get('total_users', 0) or 0)}</b>\n"
            f"📁 Scans: <b>{int(summary.get('total_scans', 0) or 0)}</b> • ✅ {int(summary.get('completed', 0) or 0)}\n"
            f"❌ Failed: <b>{int(summary.get('failed', 0) or 0)}</b> • 🛑 {int(summary.get('cancelled', 0) or 0)}\n"
            f"📈 Success rate: <b>{success:.1f}%</b>\n\n"
            "⚡ <b>Live</b>\n"
            f"🔎 Active: <b>{active_processes}/{MAX_CONCURRENT_CHECKS}</b>\n"
            f"⏳ Queue: <b>{queued_processes}</b>\n"
            f"🗄️ MongoDB: <b>{mongo}</b>\n"
            f"⏱️ Uptime: <b>{uptime:.1f} h</b>"
        )
    else:
        text = (
            "👑 <b>AniToon Owner Control Center</b>\n\n"
            "⚠️ MongoDB statistics are temporarily unavailable.\n\n"
            f"🔎 Active: <b>{active_processes}/{MAX_CONCURRENT_CHECKS}</b>\n"
            f"⏳ Queue: <b>{queued_processes}</b>\n"
            f"🗄️ MongoDB: <b>{mongo}</b>\n"
            f"⏱️ Uptime: <b>{uptime:.1f} h</b>"
        )

    buttons = [
        [
            Button.inline("👥 Users", b"owner:users:0"),
            Button.inline("🤖 Clones", b"owner:clones"),
        ],
        [
            Button.inline("🖥 Resources", b"owner:resources"),
            Button.inline("💚 Bot Status", b"home:status"),
        ],
        [
            Button.inline("📢 Broadcast", b"owner:broadcast"),
            Button.inline("🔄 Refresh", b"owner:dashboard"),
        ],
        [Button.inline("⬅️ Home", b"home:back")],
    ]
    if edit:
        await event.edit(text, parse_mode="html", buttons=buttons)
    else:
        await event.reply(text, parse_mode="html", buttons=buttons)

async def render_owner_users(event, owner_id: int, page: int = 0) -> None:
    if not _owner_allowed(owner_id):
        await event.answer("Owner access only.", alert=True)
        return

    summary = await owner_7day_summary(7)
    users = summary.get("users", []) if summary.get("available") else []
    page_size = 8
    max_page = max(0, (len(users) - 1) // page_size)
    page = max(0, min(int(page), max_page))
    chunk = users[page * page_size:(page + 1) * page_size]

    if not users:
        await event.edit(
            "👥 <b>Users</b> • 7 days\n\nNo activity found.",
            parse_mode="html",
            buttons=[
                [Button.inline("🔄 Refresh", b"owner:users:0")],
                [Button.inline("⬅️ Dashboard", b"owner:dashboard")],
            ],
        )
        return

    lines = [f"👥 <b>Users</b> • {len(users)} active / 7d", ""]
    buttons: list[list[Any]] = []
    for item in chunk:
        name = _owner_user_label(item)
        scans = int(item.get("scans", 0) or 0)
        completed = int(item.get("completed", 0) or 0)
        failed = int(item.get("failed", 0) or 0)
        icon = "✅" if scans and failed == 0 else ("⚠️" if failed else "ℹ️")
        lines.append(f"{icon} <b>{html.escape(name)}</b> • 📁 {scans} • ✅ {completed}")
        buttons.append([
            Button.inline(
                f"👤 {name[:24]}",
                f"owner:user:{int(item['user_id'])}".encode("ascii"),
            )
        ])

    nav: list[Any] = []
    if page > 0:
        nav.append(Button.inline("◀️", f"owner:users:{page-1}".encode("ascii")))
    if page < max_page:
        nav.append(Button.inline("▶️", f"owner:users:{page+1}".encode("ascii")))
    if nav:
        buttons.append(nav)
    buttons.extend([
        [Button.inline("🔄 Refresh", f"owner:users:{page}".encode("ascii"))],
        [Button.inline("⬅️ Dashboard", b"owner:dashboard")],
    ])
    await event.edit(
        "\n".join(lines) + f"\n\nPage {page + 1}/{max_page + 1}",
        parse_mode="html",
        buttons=buttons,
    )

async def render_owner_user_scans(
    event,
    owner_id: int,
    target_user_id: int,
    page: int = 0,
) -> None:
    if not _owner_allowed(owner_id):
        await event.answer("Owner access only.", alert=True)
        return

    page_size = 10
    page = max(0, int(page))
    records = await owner_user_scans(
        int(target_user_id),
        7,
        page_size,
        page * page_size,
    )

    if not records and page == 0:
        await event.edit(
            f"👤 <b>User {int(target_user_id)}</b>\n\nNo scans in the last 7 days.",
            parse_mode="html",
            buttons=[[Button.inline("⬅️ Users", b"owner:users:0")]],
        )
        return

    lines = [
        f"👤 <b>User {int(target_user_id)}</b> • Scan History",
        "📅 Last 7 days",
        "",
    ]
    for item in records:
        filename = html.escape(str(item.get("filename") or "telegram_file")[:80])
        status = str(item.get("status") or "unknown").lower()
        icon = {"completed": "✅", "failed": "❌", "cancelled": "🛑"}.get(status, "ℹ️")
        when = item.get("created_at")
        when_text = when.strftime("%d %b %H:%M") if isinstance(when, datetime) else "—"
        source = html.escape(str(item.get("source_bot") or BOT_USERNAME))
        lines.append(f"{icon} <code>{filename}</code> • {source} • {when_text}")

    buttons: list[list[Any]] = []
    nav: list[Any] = []
    if page > 0:
        nav.append(Button.inline("◀️", f"owner:user:{int(target_user_id)}:{page-1}".encode("ascii")))
    if len(records) == page_size:
        nav.append(Button.inline("▶️", f"owner:user:{int(target_user_id)}:{page+1}".encode("ascii")))
    if nav:
        buttons.append(nav)
    buttons.extend([
        [Button.inline("👥 Users", b"owner:users:0")],
        [Button.inline("👑 Dashboard", b"owner:dashboard")],
    ])
    await event.edit("\n".join(lines), parse_mode="html", buttons=buttons)


async def remove_clone_as_owner(owner_id: int, clone_id: int) -> bool:
    if not _owner_allowed(owner_id):
        return False
    real_owner = clone_owners.get(int(clone_id))
    if real_owner is None:
        return False

    client = clone_clients.pop(int(clone_id), None)
    clone_owners.pop(int(clone_id), None)
    clone_usernames.pop(int(clone_id), None)
    clone_owner_names.pop(int(clone_id), None)
    clone_stats.pop(int(clone_id), None)
    clone_message_pending.pop(int(clone_id), None)

    if client is not None:
        clone_client_ids.pop(id(client), None)
        for token, task in list(active_scans.items()):
            if active_scan_clients.get(token) is client:
                task.cancel()
        with suppress(Exception):
            await client.disconnect()

    await mark_clone_removed(int(real_owner), int(clone_id))
    return True


async def render_owner_clones(event, owner_id: int, page: int = 0) -> None:
    if not _owner_allowed(owner_id):
        await event.answer("Owner access only.", alert=True)
        return

    persisted = await owner_clone_records()
    by_id: dict[int, dict[str, Any]] = {
        int(item["clone_id"]): dict(item)
        for item in persisted
        if item.get("clone_id") is not None
    }

    for clone_id, owner_user_id in clone_owners.items():
        cid = int(clone_id)
        item = by_id.setdefault(cid, {})
        item.update({
            "clone_id": cid,
            "user_id": int(owner_user_id),
            "clone_username": clone_usernames.get(cid),
            "owner_name": clone_owner_names.get(cid) or item.get("owner_name") or f"User {owner_user_id}",
            "status": "online",
        })

    items = []
    for cid, item in by_id.items():
        client = clone_clients.get(cid)
        item["clone_username"] = item.get("clone_username") or clone_usernames.get(cid)
        item["owner_name"] = item.get("owner_name") or f"User {int(item.get('user_id') or 0)}"
        item["online"] = bool(client and client.is_connected())
        items.append(item)

    items.sort(
        key=lambda x: x.get("created_at") or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )

    page_size = 5
    max_page = max(0, (len(items) - 1) // page_size)
    page = max(0, min(int(page), max_page))
    chunk = items[page * page_size:(page + 1) * page_size]

    if not items:
        await event.edit(
            "👑 <b>Clone Bots of AniToon</b>\n\nNo connected clone bots.",
            parse_mode="html",
            buttons=[[Button.inline("⬅️ Dashboard", b"owner:dashboard")]],
        )
        return

    lines = [
        f"👑 <b>Clone Bots of AniToon</b> — {len(items)} registered",
        "",
    ]
    buttons = []

    for item in chunk:
        clone_id = int(item["clone_id"])
        username = str(item.get("clone_username") or "").lstrip("@")
        owner_id = int(item.get("user_id") or 0)
        owner_name = html.escape(str(item.get("owner_name") or f"User {owner_id}"))
        online = bool(item.get("online"))
        status = "🟢 Online" if online else "🔴 Offline"
        lines.append(
            f"🤖 <b>{html.escape('@' + username) if username else 'Clone Bot #' + str(clone_id)}</b> — {status}\n"
            f"👤 <a href=\"tg://user?id={owner_id}\">{owner_name}</a>"
        )
        if username:
            buttons.append([
                Button.url("🤖 Open Clone Bot", f"https://t.me/{username}")
            ])
        buttons.append([
            Button.inline(
                "🗑 Remove Clone",
                f"owner:clone_remove:{clone_id}".encode("ascii"),
            )
        ])

    nav = []
    if page > 0:
        nav.append(Button.inline("◀️ Previous", f"owner:clones:{page-1}".encode("ascii")))
    if page < max_page:
        nav.append(Button.inline("Next ▶️", f"owner:clones:{page+1}".encode("ascii")))
    if nav:
        buttons.append(nav)
    buttons.append([Button.inline("🔄 Refresh", f"owner:clones:{page}".encode("ascii"))])
    buttons.append([Button.inline("⬅️ Dashboard", b"owner:dashboard")])

    await event.edit(
        "\n".join(lines) + f"\n\nPage {page + 1}/{max_page + 1}",
        parse_mode="html",
        buttons=buttons,
    )


async def edit_status(message, text: str, *, buttons=None) -> None:
    with suppress(Exception):
        await message.edit(text, parse_mode="html", buttons=buttons)


def status_text(filename: str, line: str) -> str:
    low = line.lower()

    if "stage 1/4" in low:
        pct = 15
    elif "stage 2/4" in low:
        pct = 35
    elif "stage 3/4" in low:
        pct = 70
    elif "stage 4/4" in low:
        pct = 92
    else:
        pct = 50

    filled = pct // 10
    bar = "█" * filled + "░" * (10 - filled)

    clean = line
    for prefix in ("🧭 ", "🎯 ", "🔎 ", "🧩 "):
        clean = clean.replace(prefix, "")
    if "•" in clean:
        clean = clean.split("•", 1)[1].strip()

    return (
        "🔎 <b>SCANNING METADATA</b>\n\n"
        f"<code>[{bar}] {pct}%</code>\n"
        f"{html.escape(clean)}"
    )


async def run_scan(
    source_message: Any,
    status_message: Any,
    *,
    scan_token: str,
    client: Any,
) -> Report:
    filename = safe_filename(source_message)

    async def progress(line: str):
        await edit_status(
            status_message,
            status_text(filename, line),
            buttons=cancel_button(scan_token),
        )

    return await asyncio.wait_for(
        inspect_telegram_player(
            client,
            source_message,
            scan_token,
            progress=progress,
            budget=int(os.getenv("FILE_DEEP_PROBE_BYTES", "33554432")),
            port=int(os.getenv("PORT", "10000")),
        ),
        timeout=SCAN_TIMEOUT_SECONDS,
    )


async def acquire_scan_slot(
    status_message: Any,
    scan_token: str,
    filename: str,
) -> None:
    global active_processes, queued_processes

    queued = False
    async with scan_queue_lock:
        if active_processes >= MAX_CONCURRENT_CHECKS:
            queued_processes += 1
            position = queued_processes
            queued = True
        else:
            position = 0

    if queued:
        await edit_status(
            status_message,
            "⏳ <b>Bot is full</b>\n\n"
            f"10 active users are being processed. Your scan is queued at <b>#{position}</b>.\n"
            "Please wait — your scan will start automatically.",
            buttons=cancel_button(scan_token),
        )

    acquired = False
    try:
        await check_semaphore.acquire()
        acquired = True
    except asyncio.CancelledError:
        if queued:
            async with scan_queue_lock:
                queued_processes = max(0, queued_processes - 1)
        raise

    async with scan_queue_lock:
        if queued:
            queued_processes = max(0, queued_processes - 1)
        active_processes += 1


async def release_scan_slot() -> None:
    global active_processes
    async with scan_queue_lock:
        active_processes = max(0, active_processes - 1)
    check_semaphore.release()


async def analyze_source(
    source_message: Any,
    status_message: Any,
    scan_token: str,
    user_id: int | None = None,
    *,
    client: Any = bot,
    bot_username: str = BOT_USERNAME,
    include_clone: bool = True,
    show_privacy: bool = True,
) -> None:
    global checks_total, checks_ok, checks_failed
    checks_total += 1
    filename = safe_filename(source_message)
    outcome = "failed"

    async def persist_outcome(status: str) -> None:
        await record_scan(
            user_id=user_id,
            source_message=source_message,
            report=None,
            status=status,
            source_bot=bot_username,
        )

    slot_acquired = False
    try:
        await acquire_scan_slot(status_message, scan_token, filename)
        slot_acquired = True

        if client is not bot:
            await bump_clone_stat(client, "scans_started")

        await edit_status(
            status_message,
            "🔎 <b>SCANNING METADATA</b>\n\n"
            "<code>[░░░░░░░░░░] 0%</code>\n"
            "Starting scan…",
            buttons=cancel_button(scan_token),
        )

        report = await run_scan(
            source_message,
            status_message,
            scan_token=scan_token,
            client=client,
        )

        state = cache_state(
            status_message,
            source_message,
            report,
            web_token=scan_token,
        )
        if state is None:
            raise RuntimeError("Could not create web report link")

        state.web_token = scan_token
        web_states[scan_token] = state

        try:
            await save_web_report(
                scan_token,
                asdict(report),
                datetime.now(timezone.utc) + timedelta(seconds=REPORT_LINK_TTL_SECONDS),
            )
        except Exception:
            log.exception("Failed to persist web report")
        await record_scan(
            user_id=user_id,
            source_message=source_message,
            report=report,
            status="completed",
            source_bot=bot_username,
        )

        await edit_status(
            status_message,
            compact_scan_result(report),
            buttons=web_report_button(
                scan_token,
                bot_username,
                include_clone=include_clone,
            ),
        )
        checks_ok += 1
        outcome = "completed"

    except asyncio.CancelledError:
        outcome = "cancelled"
        await persist_outcome("cancelled")
        checks_failed += 1
        await cancel_probe(scan_token)
        await edit_status(
            status_message,
            "❌ <b>Metadata scan cancelled.</b>",
            buttons=home_buttons(
                bot_username,
                include_clone=include_clone,
                show_privacy=show_privacy,
            ),
        )
        raise

    except ProbeBudgetExceeded:
        await persist_outcome("failed")
        checks_failed += 1
        await edit_status(
            status_message,
            "🛑 <b>Safe scan limit reached.</b>\n\n"
            "The player engine stopped before downloading the complete file.",
            buttons=home_buttons(
                bot_username,
                include_clone=include_clone,
                show_privacy=show_privacy,
            ),
        )

    except ProbeCancelled:
        outcome = "cancelled"
        await persist_outcome("cancelled")
        checks_failed += 1
        await edit_status(
            status_message,
            "❌ <b>Metadata scan cancelled.</b>",
            buttons=home_buttons(
                bot_username,
                include_clone=include_clone,
                show_privacy=show_privacy,
            ),
        )

    except asyncio.TimeoutError:
        await persist_outcome("failed")
        checks_failed += 1
        await edit_status(
            status_message,
            f"⏰ <b>Metadata scan reached the {SCAN_TIMEOUT_SECONDS}-second safety limit.</b>",
            buttons=home_buttons(
                bot_username,
                include_clone=include_clone,
                show_privacy=show_privacy,
            ),
        )

    except errors.FloodWaitError as exc:
        await persist_outcome("failed")
        checks_failed += 1
        await edit_status(
            status_message,
            f"⏳ Telegram temporarily rate-limited this scan for {int(exc.seconds)} seconds.",
            buttons=home_buttons(
                bot_username,
                include_clone=include_clone,
                show_privacy=show_privacy,
            ),
        )

    except Exception as exc:
        await persist_outcome("failed")
        checks_failed += 1
        log.exception("File metadata scan failed for %s", filename)
        await edit_status(
            status_message,
            "❌ <b>Metadata scan failed.</b>\n\n"
            f"<code>{html.escape(type(exc).__name__)}</code>",
            buttons=home_buttons(
                bot_username,
                include_clone=include_clone,
                show_privacy=show_privacy,
            ),
        )

    finally:
        if slot_acquired:
            await release_scan_slot()

        if scan_token in active_scans:
            await cancel_probe(scan_token)
        active_scans.pop(scan_token, None)
        active_scan_users.pop(scan_token, None)
        active_scan_clients.pop(scan_token, None)

        if client is not bot:
            if outcome == "completed":
                await bump_clone_stat(client, "scans_completed")
            elif outcome == "cancelled":
                await bump_clone_stat(client, "scans_cancelled")
            else:
                await bump_clone_stat(client, "scans_failed")


async def analyze(
    event,
    *,
    client: Any = bot,
    bot_username: str = BOT_USERNAME,
    include_clone: bool = True,
) -> None:
    purge_pending_scans()

    token = secrets.token_urlsafe(18)
    sender = await event.get_sender()
    sender_id = getattr(sender, "id", None)
    pending_scans[token] = PendingScan(
        source_message=event.message,
        created_at=time.monotonic(),
        user_id=int(sender_id) if sender_id is not None else None,
        client=client,
        bot_username=bot_username,
        include_clone=include_clone,
        show_privacy=bool(getattr(event, "is_private", True)),
    )

    filename = html.escape(safe_filename(event.message))[:120]
    await event.reply(
        "📦 <b>FILE DETECTED</b>\n\n"
        f"📄 <code>{filename}</code>\n"
        "🔎 Click below to check the file information.",
        parse_mode="html",
        buttons=metadata_button(token),
    )


async def cancel_user_scan(user_id: int, client: Any = None) -> bool:
    for token, pending in list(pending_scans.items()):
        if pending.user_id == user_id and (client is None or pending.client is client):
            pending_scans.pop(token, None)
            return True

    for token, task in list(active_scans.items()):
        if active_scan_users.get(token) == user_id:
            task_client = active_scan_clients.get(token)
            if client is None or task_client is client:
                task.cancel()
                return True

    return False


def _clean_bot_token(value: str) -> str:
    value = value.strip().strip(chr(96)).strip()
    return value


GROUP_ONBOARDING_MESSAGES = (
    "👋 <b>Hi everyone! I’m new here — AniToon Media Info Bot.</b>\n\n"
    "🔎 I help you see what is inside your Telegram video or document files.\n"
    "🎬 Quality • resolution • codec • bitrate • duration\n"
    "🎧 Audio tracks • languages • channels\n"
    "💬 Subtitles • languages • formats\n"
    "📦 Container and technical file details\n\n"
    "📤 Send a media file here and tap <b>🔎 Scan File Info</b>.\n"
    "🌐 Open the full report in the web page.\n"
    "🧬 You can also create your own AniToon clone bot for personal use.",
    "🎬 <b>Day 2 • Check Video Quality</b>\n\n"
    "See the video resolution, codec, pixel format, profile and other available quality details before you download or share a file.\n\n"
    "📤 Send the video and tap <b>🔎 Scan File Info</b>.",
    "🎧 <b>Day 3 • Check Audio Tracks</b>\n\n"
    "See audio track count, language, codec, channels, layout, sample rate and bitrate when available.\n\n"
    "📤 Send the file and scan its media information.",
    "💬 <b>Day 4 • Find Subtitles</b>\n\n"
    "Check embedded subtitle tracks, languages and subtitle formats so you know what is available before downloading the file.\n\n"
    "📤 Send the media file and tap <b>🔎 Scan File Info</b>.",
    "📦 <b>Day 5 • Check File & Container Details</b>\n\n"
    "AniToon can identify the container or media format and show technical details such as runtime, MIME type and average bitrate when available.\n\n"
    "🌐 Everything is organized in one clean report.",
    "🌐 <b>Day 6 • Open the Full Web Report</b>\n\n"
    "After the scan, use <b>🌐 Open File Info</b> to see video, audio, subtitle and technical details in an easy-to-read browser page.\n\n"
    "⚡ Useful for checking a file quickly without downloading a complete copy.",
    "✨ <b>Day 7 • Make AniToon Part of Your Workflow</b>\n\n"
    "Use AniToon for quality checks, audio and subtitle inspection, container details and complete web reports.\n\n"
    "🧬 You can also create your own clone bot for personal use.\n"
    "📤 Send a file anytime and tap <b>🔎 Scan File Info</b>."
)


def _bot_id_for_client(client: Any) -> int | None:
    return bot_identity_ids.get(id(client))

async def _ensure_bot_identity(client: Any) -> int | None:
    existing = _bot_id_for_client(client)
    if existing is not None:
        return int(existing)
    try:
        me = await client.get_me()
        bot_id = getattr(me, "id", None)
        if bot_id is not None:
            bot_identity_ids[id(client)] = int(bot_id)
            return int(bot_id)
    except Exception:
        log.debug("Unable to resolve bot identity", exc_info=True)
    return None

async def _touch_group_chat(
    event,
    *,
    client: Any,
    bot_username: str,
) -> None:
    if not getattr(event, "is_group", False):
        return
    chat_id = getattr(event, "chat_id", None)
    if chat_id is None:
        return
    bot_id = await _ensure_bot_identity(client)
    if bot_id is None:
        return
    clone_id = _clone_id_for_client(client) or 0
    key = (int(bot_id), int(chat_id))
    now_mono = time.monotonic()
    if now_mono - group_touch_cache.get(key, 0.0) < GROUP_TOUCH_INTERVAL_SECONDS:
        return
    group_touch_cache[key] = now_mono

    title = "Telegram group"
    try:
        chat = await event.get_chat()
        title = str(
            getattr(chat, "title", None)
            or getattr(chat, "first_name", None)
            or "Telegram group"
        )[:200]
    except Exception:
        pass

    record = {
        "bot_id": int(bot_id),
        "bot_username": str(bot_username).lstrip("@"),
        "chat_id": int(chat_id),
        "title": title,
        "clone_id": int(clone_id),
        "last_seen": datetime.now(timezone.utc),
    }
    known_group_chats[key] = record
    with suppress(Exception):
        await record_group_chat(
            bot_id=int(bot_id),
            bot_username=bot_username,
            chat_id=int(chat_id),
            title=title,
            clone_id=int(clone_id),
        )

async def _send_group_onboarding_day(
    client: Any,
    group: dict[str, Any],
    day: int,
) -> bool:
    if not 1 <= int(day) <= GROUP_ONBOARDING_DAYS:
        return False

    chat_id = int(group["chat_id"])
    bot_id = int(group["bot_id"])
    try:
        message = await client.send_message(
            chat_id,
            GROUP_ONBOARDING_MESSAGES[int(day) - 1],
            parse_mode="html",
        )
    except Exception:
        log.warning(
            "Group onboarding send failed | bot_id=%s | chat_id=%s | day=%s",
            bot_id,
            chat_id,
            day,
            exc_info=True,
        )
        return False

    old_message_id = group.get("last_onboarding_message_id")
    if old_message_id and int(old_message_id) != int(getattr(message, "id", 0) or 0):
        with suppress(Exception):
            await client.delete_messages(chat_id, int(old_message_id))

    message_id = getattr(message, "id", None)
    active = int(day) < GROUP_ONBOARDING_DAYS
    with suppress(Exception):
        await update_group_onboarding(
            bot_id=bot_id,
            chat_id=chat_id,
            day=int(day),
            message_id=int(message_id) if message_id is not None else None,
            active=active,
        )

    current = known_group_chats.setdefault((bot_id, chat_id), dict(group))
    current.update({
        "onboarding_day": int(day),
        "last_onboarding_message_id": int(message_id) if message_id is not None else None,
        "onboarding_active": active,
    })
    return True

async def _handle_bot_added_to_group(
    event,
    *,
    client: Any,
    bot_username: str,
) -> None:
    if not getattr(event, "is_group", False):
        return
    if not (getattr(event, "user_added", False) or getattr(event, "user_joined", False)):
        return

    bot_id = await _ensure_bot_identity(client)
    if bot_id is None:
        return
    added_user_id = getattr(event, "user_id", None)
    if added_user_id is None or int(added_user_id) != int(bot_id):
        return

    chat_id = getattr(event, "chat_id", None)
    if chat_id is None:
        return
    key = (int(bot_id), int(chat_id))
    if time.monotonic() - group_onboarding_bootstrap.get(key, 0.0) < 30:
        return
    group_onboarding_bootstrap[key] = time.monotonic()

    title = "Telegram group"
    try:
        chat = await event.get_chat()
        title = str(
            getattr(chat, "title", None)
            or getattr(chat, "first_name", None)
            or "Telegram group"
        )[:200]
    except Exception:
        pass

    clone_id = _clone_id_for_client(client) or 0
    saved = await record_group_chat(
        bot_id=int(bot_id),
        bot_username=bot_username,
        chat_id=int(chat_id),
        title=title,
        clone_id=int(clone_id),
        joined_at=datetime.now(timezone.utc),
        reset_onboarding=True,
    )
    group = {
        "bot_id": int(bot_id),
        "bot_username": str(bot_username).lstrip("@"),
        "chat_id": int(chat_id),
        "title": title,
        "clone_id": int(clone_id),
        "joined_at": datetime.now(timezone.utc),
        "onboarding_day": 0,
        "last_onboarding_message_id": None,
    }
    if saved:
        group.update(saved)
    known_group_chats[key] = group
    await _send_group_onboarding_day(client, group, 1)

async def _group_onboarding_loop() -> None:
    while True:
        try:
            groups = await list_group_chats()
            merged: dict[tuple[int, int], dict[str, Any]] = {}
            for group in groups:
                try:
                    key = (int(group["bot_id"]), int(group["chat_id"]))
                    merged[key] = dict(group)
                except (KeyError, TypeError, ValueError):
                    continue
            for key, group in known_group_chats.items():
                merged.setdefault(key, dict(group))

            now = datetime.now(timezone.utc)
            for key, group in merged.items():
                joined_at = group.get("joined_at")
                if not isinstance(joined_at, datetime):
                    continue
                if joined_at.tzinfo is None:
                    joined_at = joined_at.replace(tzinfo=timezone.utc)
                age_seconds = max(0.0, (now - joined_at).total_seconds())
                day = int(age_seconds // 86400) + 1
                last_day = int(group.get("onboarding_day", 0) or 0)

                if day > GROUP_ONBOARDING_DAYS:
                    if last_day and bool(group.get("onboarding_active", True)):
                        with suppress(Exception):
                            await update_group_onboarding(
                                bot_id=key[0],
                                chat_id=key[1],
                                day=last_day,
                                message_id=group.get("last_onboarding_message_id"),
                                active=False,
                            )
                    continue

                if last_day >= day:
                    continue

                bot_id, chat_id = key
                clone_id = int(group.get("clone_id") or 0)
                target = bot if bot_id == _bot_id_for_client(bot) else clone_clients.get(clone_id)
                if target is None:
                    continue
                await _send_group_onboarding_day(target, group, day)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Group onboarding loop failed")
        await asyncio.sleep(GROUP_ONBOARDING_LOOP_SECONDS)

async def _broadcast_owner_message(message_text: str) -> dict[str, int]:
    clean = (message_text or "").strip()
    if not clean:
        return {
            "users": 0,
            "main_users": 0,
            "clone_users": 0,
            "groups": 0,
            "group_sent": 0,
            "failed": 0,
            "bots": 0,
        }

    payload = "📢 <b>AniToon Announcement</b>\n\n" + html.escape(clean)
    users_sent = main_users = clone_users = group_sent = failed = 0
    bot_count = 0

    async def send_to_bot(target: TelegramClient, bot_id: int, clone_id: int = 0) -> tuple[int, int]:
        nonlocal failed, users_sent, main_users, clone_users
        user_ids = await list_bot_users(int(bot_id), bot_username, 1_000_000)
        sent_local = 0
        for uid in user_ids:
            try:
                await target.send_message(int(uid), payload, parse_mode="html")
                sent_local += 1
                users_sent += 1
                if clone_id:
                    clone_users += 1
                else:
                    main_users += 1
            except errors.FloodWaitError as exc:
                sleep_for = min(max(1, int(exc.seconds)), 120)
                await asyncio.sleep(sleep_for)
                try:
                    await target.send_message(int(uid), payload, parse_mode="html")
                    sent_local += 1
                    users_sent += 1
                    if clone_id:
                        clone_users += 1
                    else:
                        main_users += 1
                except Exception:
                    failed += 1
            except Exception:
                failed += 1
            await asyncio.sleep(0.06)
        return sent_local, len(user_ids)

    try:
        main_bot_id = await _ensure_bot_identity(bot)
    except Exception:
        main_bot_id = None

    if main_bot_id is not None:
        bot_count += 1
        await send_to_bot(bot, int(main_bot_id), 0)

    for clone_id, target in list(clone_clients.items()):
        try:
            clone_bot_id = await _ensure_bot_identity(target)
            if clone_bot_id is None:
                clone_bot_id = int(clone_id)
            bot_count += 1
            await send_to_bot(target, int(clone_bot_id), int(clone_id))
        except Exception:
            failed += 1
            log.warning("Clone broadcast audience failed | clone_id=%s", clone_id, exc_info=True)

    groups = await list_group_chats()
    merged: dict[tuple[int, int], dict[str, Any]] = {}
    for group in groups:
        try:
            merged[(int(group["bot_id"]), int(group["chat_id"]))] = dict(group)
        except (KeyError, TypeError, ValueError):
            continue
    for key, group in known_group_chats.items():
        merged.setdefault(key, dict(group))

    for (bot_id, chat_id), group in merged.items():
        clone_id = int(group.get("clone_id") or 0)
        target = bot if bot_id == _bot_id_for_client(bot) else clone_clients.get(clone_id)
        if target is None:
            failed += 1
            continue
        try:
            await target.send_message(chat_id, payload, parse_mode="html")
            group_sent += 1
        except errors.FloodWaitError as exc:
            await asyncio.sleep(min(max(1, int(exc.seconds)), 120))
            try:
                await target.send_message(chat_id, payload, parse_mode="html")
                group_sent += 1
            except Exception:
                failed += 1
        except Exception:
            failed += 1
            log.warning(
                "Owner group broadcast failed | bot_id=%s | chat_id=%s | clone_id=%s",
                bot_id, chat_id, clone_id, exc_info=True,
            )
        await asyncio.sleep(0.08)

    return {
        "users": users_sent,
        "main_users": main_users,
        "clone_users": clone_users,
        "groups": len(merged),
        "group_sent": group_sent,
        "failed": failed,
        "bots": bot_count,
    }


def _owner_broadcast_pending(user_id: int) -> bool:
    created = owner_broadcast_pending.get(int(user_id))
    if created is None:
        return False
    if time.monotonic() - created > OWNER_BROADCAST_TTL_SECONDS:
        owner_broadcast_pending.pop(int(user_id), None)
        return False
    return True

async def _set_bot_commands(client: TelegramClient, *, include_clone: bool) -> None:
    if not include_clone:
        commands = [
            types.BotCommand(command="start", description="Open Home"),
        ]
    else:
        commands = [
            types.BotCommand(command="start", description="Open Home"),
            types.BotCommand(command="help", description="How to use AniToon Media Info Bot"),
            types.BotCommand(command="stats", description="View your 7-day stats"),
            types.BotCommand(command="about", description="About AniToons"),
            types.BotCommand(command="addtogroup", description="Add the bot to a group"),
            types.BotCommand(command="clones", description="View your clone bots"),
            types.BotCommand(command="myclones", description="View your clone bots"),
            types.BotCommand(command="cancel", description="Cancel your scan"),
            types.BotCommand(command="clone", description="Create a clone bot"),
        ]

    await client(functions.bots.SetBotCommandsRequest(
        scope=types.BotCommandScopeDefault(),
        lang_code="en",
        commands=commands,
    ))


def _bind_bot_handlers(
    client: TelegramClient,
    bot_username: str,
    *,
    include_clone: bool,
) -> None:
    async def on_message(event):
        try:
            await _touch_group_chat(
                event,
                client=client,
                bot_username=bot_username,
            )
        except Exception:
            log.debug("Group tracking failed", exc_info=True)
        await handle_new_message(
            event,
            client=client,
            bot_username=bot_username,
            include_clone=include_clone,
        )

    async def on_callback(event):
        await handle_callback(
            event,
            client=client,
            bot_username=bot_username,
            include_clone=include_clone,
        )

    async def on_chat_action(event):
        try:
            await _handle_bot_added_to_group(
                event,
                client=client,
                bot_username=bot_username,
            )
        except Exception:
            log.exception("Group join/onboarding handler failed")

    client.add_event_handler(on_message, events.NewMessage(incoming=True))
    client.add_event_handler(on_callback, events.CallbackQuery)
    client.add_event_handler(on_chat_action, events.ChatAction())


async def _start_clone_bot(
    token: str,
    owner_user_id: int,
    owner_name: str | None = None,
):
    client = TelegramClient(MemorySession(), API_ID, API_HASH)
    client.flood_sleep_threshold = 15 * 60

    try:
        await asyncio.wait_for(client.start(bot_token=token), timeout=30)
        me = await asyncio.wait_for(client.get_me(), timeout=15)

        clone_id = getattr(me, "id", None)
        username = getattr(me, "username", None)
        if clone_id is None or not username:
            raise RuntimeError("Telegram did not return a usable clone bot identity")

        old = clone_clients.get(int(clone_id))
        if old is not None:
            with suppress(Exception):
                await old.disconnect()

        _bind_bot_handlers(client, username, include_clone=False)

        try:
            await _set_bot_commands(client, include_clone=False)
        except Exception:
            log.exception("Failed to set command menu for clone @%s", username)

        clone_clients[int(clone_id)] = client
        clone_owners[int(clone_id)] = int(owner_user_id)
        clone_usernames[int(clone_id)] = username
        if owner_name:
            clone_owner_names[int(clone_id)] = owner_name
        clone_client_ids[id(client)] = int(clone_id)
        _ensure_clone_stats(int(clone_id))

        await record_clone_request(
            user_id=int(owner_user_id),
            clone_id=int(clone_id),
            clone_username=username,
            clone_first_name=getattr(me, "first_name", None),
            owner_name=owner_name,
            token=token,
        )
        log.info("Clone bot online as @%s | owner_id=%s", username, owner_user_id)
        return me

    except Exception:
        with suppress(Exception):
            await client.disconnect()
        raise


async def monitor_clone_bots() -> None:
    while True:
        await asyncio.sleep(300)
        for clone_id, client in list(clone_clients.items()):
            try:
                await asyncio.wait_for(client.get_me(), timeout=15)
            except (
                errors.UnauthorizedError,
                errors.AuthKeyUnregisteredError,
                errors.UserDeactivatedError,
            ):
                owner_id = clone_owners.get(int(clone_id))
                username = clone_usernames.get(int(clone_id)) or f"Clone Bot #{int(clone_id)}"
                log.warning(
                    "Clone token revoked/deactivated | clone_id=%s | username=%s",
                    clone_id,
                    username,
                )
                if owner_id:
                    with suppress(Exception):
                        await bot.send_message(
                            int(owner_id),
                            "⚠️ <b>Your clone bot was disconnected because its bot token is no longer valid.</b>",
                            parse_mode="html",
                            buttons=[[Button.inline("🤖 Clone Manager", b"home:clones")]],
                        )
                if owner_id:
                    await remove_clone_for_user(int(owner_id), int(clone_id))
            except Exception:
                # Transient network errors do not remove a valid clone.
                log.debug(
                    "Temporary clone health-check failure | clone_id=%s",
                    clone_id,
                    exc_info=True,
                )


def _clone_pending(user_id: int) -> bool:
    created = clone_setup_pending.get(user_id)
    if created is None:
        return False
    if time.monotonic() - created > CLONE_SETUP_TTL_SECONDS:
        clone_setup_pending.pop(user_id, None)
        return False
    return True


async def begin_clone_setup(event) -> None:
    if not event.is_private:
        await event.reply(
            "🔐 <b>Clone setup is available in private chat only.</b>",
            parse_mode="html",
        )
        return

    sender = await event.get_sender()
    user_id = getattr(sender, "id", None)
    if user_id is None:
        return

    existing_clones = await list_user_clones(int(user_id))
    stored_ids = {
        int(item["clone_id"])
        for item in existing_clones
        if item.get("clone_id") is not None
    }
    runtime_ids = {
        int(clone_id)
        for clone_id, owner_id in clone_owners.items()
        if int(owner_id) == int(user_id)
    }
    if len(stored_ids | runtime_ids) >= 2:
        await event.reply(
            "⚠️ <b>Clone limit reached</b>\n\n"
            "You already have <b>2/2 clone bots</b>.\n"
            "Remove one before creating another.",
            parse_mode="html",
            buttons=[
                [Button.inline("🤖 Clone Manager", b"home:clones")],
                [Button.inline("⬅️ Home", b"home:back")],
            ],
        )
        return

    clone_setup_pending[int(user_id)] = time.monotonic()

    await event.reply(
        "🧬 <b>Create a Clone Bot</b>\n\n"
        "1️⃣ Open @BotFather and create your bot.\n"
        "2️⃣ Send the BotFather token here.\n\n"
        "🔒 <b>Limit:</b> 2 clone bots per user.",
        parse_mode="html",
        buttons=clone_setup_buttons(),
    )


async def handle_clone_token_message(event) -> bool:
    if not event.is_private or not (event.raw_text or "").strip():
        return False

    sender = await event.get_sender()
    user_id = getattr(sender, "id", None)
    if user_id is None or not _clone_pending(int(user_id)):
        return False

    clone_setup_pending.pop(int(user_id), None)
    token = _clean_bot_token(event.raw_text or "")

    with suppress(Exception):
        await event.message.delete()

    if ":" not in token or len(token) < 20 or len(token) > 200:
        await event.reply(
            "❌ <b>Invalid BotFather token.</b>\n\n"
            "Please send the token exactly as provided by @BotFather.",
            parse_mode="html",
            buttons=clone_setup_buttons(),
        )
        return True

    existing_clones = await list_user_clones(int(user_id))
    live_owned = {
        int(item["clone_id"])
        for item in existing_clones
        if item.get("clone_id") is not None
    }
    runtime_owned = {
        int(clone_id)
        for clone_id, owner_id in clone_owners.items()
        if int(owner_id) == int(user_id)
    }
    if len(live_owned | runtime_owned) >= 2:
        await event.reply(
            "⚠️ <b>Clone limit reached</b>\n\n"
            "You already have <b>2/2 clone bots</b>.\n"
            "Remove one clone before creating another.",
            parse_mode="html",
            buttons=[
                [Button.inline("🤖 Clone Manager", b"home:clones")],
                [Button.inline("⬅️ Home", b"home:back")],
            ],
        )
        return True

    sender_name = " ".join(
        part for part in (
            str(getattr(sender, "first_name", "") or "").strip(),
            str(getattr(sender, "last_name", "") or "").strip(),
        ) if part
    ) or (
        f"@{getattr(sender, 'username', '')}"
        if getattr(sender, "username", None)
        else f"User {int(user_id)}"
    )

    try:
        me = await _start_clone_bot(
            token,
            int(user_id),
            sender_name,
        )
    except Exception:
        log.exception("Clone bot startup failed")
        await event.reply(
            "❌ <b>Clone bot could not be started.</b>\n\n"
            "Please check the BotFather token and make sure the bot is active.",
            parse_mode="html",
            buttons=clone_setup_buttons(),
        )
        return True

    username = getattr(me, "username", None)
    label = "@" + username if username else str(getattr(me, "first_name", "your bot"))
    await event.reply(
        "✅ <b>Your bot is created and ready to use!</b>\n\n"
        f"🤖 <b>{html.escape(label)}</b> is now online.\n"
        "📥 Send a video or document to your new bot to scan its metadata.",
        parse_mode="html",
        buttons=[
            [Button.url("🤖 Open Clone Bot", f"https://t.me/{username}")],
            [Button.inline("🤖 Clone Manager", b"home:clones")],
            [Button.inline("⬅️ Home", b"home:back")],
        ],
    )
    return True


async def render_user_stats(event, user_id: int, *, edit: bool = True) -> None:
    summary = await user_scan_summary(int(user_id), 7)
    if not summary.get("available"):
        text = (
            "📊 <b>My Stats</b>\n\n"
            "⚠️ Scan history is temporarily unavailable."
        )
    else:
        total = int(summary.get("scans", 0) or 0)
        completed = int(summary.get("completed", 0) or 0)
        failed = int(summary.get("failed", 0) or 0)
        cancelled = int(summary.get("cancelled", 0) or 0)
        finished = completed + failed
        success = (completed / finished * 100) if finished else 0.0
        text = (
            "📊 <b>My Stats</b>\n"
            "📅 <i>Last 7 days</i>\n\n"
            f"📁 <b>Total Scans</b> • {total}\n"
            f"✅ <b>Completed</b> • {completed}\n"
            f"❌ <b>Failed</b> • {failed}\n"
            f"🛑 <b>Cancelled</b> • {cancelled}\n"
            f"📈 <b>Success Rate</b> • {success:.1f}%\n\n"
            "✨ Keep sending files — every scan is counted."
        )
    if edit:
        await event.edit(text, parse_mode="html", buttons=back_buttons())
    else:
        await event.reply(text, parse_mode="html", buttons=back_buttons())


async def render_public_status(event, *, edit: bool = True) -> None:
    resources = runtime_resource_stats()
    if mongodb_is_connected():
        mongo = "🟢 Connected"
    elif mongodb_is_configured():
        mongo = "🟠 Configured but unreachable"
    else:
        mongo = "🔴 URI not configured"
    queue = (
        f"⚡ Active scans: <b>{active_processes}/{MAX_CONCURRENT_CHECKS}</b>\n"
        f"⏳ Queued scans: <b>{queued_processes}</b>"
    )
    text = (
        "💚 <b>AniToon Status</b>\n\n"
        f"🤖 Telegram: <b>{'Connected' if bot.is_connected() else 'Disconnected'}</b>\n"
        f"🗄️ MongoDB: <b>{mongo}</b>\n"
        f"{queue}\n"
        f"🧠 RAM: <b>{resources['ram_pct']:.1f}%</b>\n"
        f"📡 Tracked web egress: <b>{resources['web_egress_pct']:.1f}%</b>"
    )
    if edit:
        await event.edit(text, parse_mode="html", buttons=back_buttons())
    else:
        await event.reply(text, parse_mode="html", buttons=back_buttons())


PRIVACY_TEXT = (
    "🔐 <b>AniToon Privacy</b>\n\n"
    "🛡️ Media is inspected with bounded reads instead of creating a full local copy.\n"
    "📊 Scan history is stored in MongoDB for the owner/user statistics.\n"
    "🔑 Clone BotFather tokens are encrypted before being stored.\n"
    "🗑️ Removing a clone removes its stored credential and disconnects the clone."
)


def is_checkable_message(event) -> bool:
    """Return True only for Telegram video/document messages that can be scanned."""
    message = getattr(event, "message", None)
    if message is None:
        return False
    if getattr(message, "video", None) is not None:
        return True
    if getattr(message, "document", None) is not None:
        return True
    return False


async def handle_new_message(
    event,
    *,
    client: Any = bot,
    bot_username: str = BOT_USERNAME,
    include_clone: bool = True,
):
    text = (event.raw_text or "").strip()
    command = text.split(maxsplit=1)[0].split("@", 1)[0].lower() if text else ""

    # Keep a per-bot private audience so owner broadcasts can reach users of
    # the main bot and each clone through the bot they actually used.
    if getattr(event, "is_private", False):
        try:
            sender = await event.get_sender()
            uid = getattr(sender, "id", None)
            bot_id = await _ensure_bot_identity(client)
            if uid is not None and bot_id is not None:
                audience_key = (int(bot_id), int(uid))
                now_mono = time.monotonic()
                if now_mono - bot_audience_touch_cache.get(audience_key, 0.0) >= 6 * 3600:
                    bot_audience_touch_cache[audience_key] = now_mono
                    await record_bot_user(
                        bot_id=int(bot_id),
                        bot_username=bot_username,
                        user_id=int(uid),
                    )
        except Exception:
            log.debug("Bot audience tracking failed", exc_info=True)

    if not include_clone:
        await bump_clone_stat(client, "messages_received")
        if text.startswith("/") and text.split(maxsplit=1)[0].split("@", 1)[0].lower() != "/start":
            return

    if include_clone and await handle_clone_token_message(event):
        return

    if include_clone:
        sender_for_broadcast = await event.get_sender()
        owner_user_id = getattr(sender_for_broadcast, "id", None)
        if (
            owner_user_id is not None
            and _owner_allowed(owner_user_id)
            and _owner_broadcast_pending(int(owner_user_id))
        ):
            if text:
                owner_broadcast_pending.pop(int(owner_user_id), None)
                result = await _broadcast_owner_message(text)
                await event.reply(
                    "✅ <b>Broadcast finished</b>\n\n"
                    f"👤 Users reached: <b>{result['users']}</b>\n"
                    f"🤖 Main-bot users: <b>{result['main_users']}</b>\n"
                    f"🧬 Clone-bot users: <b>{result['clone_users']}</b>\n"
                    f"👥 Groups reached: <b>{result['group_sent']}/{result['groups']}</b>\n"
                    f"🤖 Bot sources: <b>{result['bots']}</b>\n"
                    f"⚠️ Failed deliveries: <b>{result['failed']}</b>",
                    parse_mode="html",
                    buttons=[[Button.inline("👑 Owner Dashboard", b"owner:dashboard")]],
                )
                return

    home_text = HOME_TEXT if include_clone else CLONE_HOME_TEXT
    help_text = HELP_TEXT if include_clone else CLONE_HELP_TEXT

    if command == "/start":
        await record_user(event)
        sender = await event.get_sender()
        await event.reply(
            home_text,
            parse_mode="html",
            buttons=home_buttons(
                bot_username,
                include_clone=include_clone,
                user_id=getattr(sender, "id", None),
                show_privacy=bool(getattr(event, "is_private", True)),
            ),
        )
        return

    if command == "/help":
        await record_user(event)
        sender = await event.get_sender()
        await event.reply(help_text, parse_mode="html", buttons=help_buttons())
        return

    if command == "/about":
        await record_user(event)
        sender = await event.get_sender()
        await event.reply(
            ABOUT_TEXT,
            parse_mode="html",
            buttons=[
                [Button.url("🌐 Open AniToon Web", PUBLIC_WEB_URL)],
                [Button.url("🤖 Open AniToon Bot", f"https://t.me/{BOT_USERNAME}")],
                [Button.inline("⬅️ Home", b"home:back")],
            ],
        )
        return

    if command == "/addtogroup":
        await record_user(event)
        await event.reply(
            "➕ <b>Add AniToon to your group</b>",
            parse_mode="html",
            buttons=[
                [Button.url(
                    "➕ Add Me to Your Group",
                    add_to_group_url(bot_username),
                )],
                [Button.inline("⬅️ Back", b"home:back")],
            ],
        )
        return

    if command == "/clone":
        await record_user(event)
        if include_clone:
            await begin_clone_setup(event)
        else:
            await event.reply(
                f"🧬 Clone creation is managed by @{html.escape(BOT_USERNAME)}.",
                parse_mode="html",
                buttons=[
                    [Button.url(
                        "🤖 Open AniToon",
                        f"https://t.me/{BOT_USERNAME}",
                    )],
                    [Button.inline("⬅️ Back", b"home:back")],
                ],
            )
        return

    if command in {"/myclones", "/stats"}:
        await record_user(event)
        sender = await event.get_sender()
        if command == "/stats":
            await render_user_stats(event, int(sender.id), edit=False)
        else:
            if include_clone:
                await render_clone_list(event, int(sender.id), edit=False)
            else:
                await event.reply(
                    "🤖 <b>Clone management</b> is available from the main AniToon bot.",
                    parse_mode="html",
                    buttons=back_buttons(),
                )
        return

    if command == "/status":
        sender = await event.get_sender()
        user_id = getattr(sender, "id", None)
        if not _owner_allowed(user_id):
            await event.reply("ℹ️ System status is available to the owner only.")
            return
        await record_user(event)
        await render_public_status(event, edit=False)
        return

    if command == "/privacy":
        await record_user(event)
        await event.reply(PRIVACY_TEXT, parse_mode="html", buttons=back_buttons())
        return

    if command == "/clones":
        await record_user(event)
        if include_clone:
            await render_clone_list(event, int((await event.get_sender()).id), edit=False)
        else:
            await event.reply(
                f"🤖 Manage your clones from @{html.escape(BOT_USERNAME)}.",
                parse_mode="html",
            )
        return

    if command == "/owner":
        sender = await event.get_sender()
        user_id = getattr(sender, "id", None)
        if _owner_allowed(user_id):
            await render_owner_dashboard(event, int(user_id), edit=False)
        else:
            await event.reply("⛔ Owner access only.")
        return

    if command == "/cancel":
        user_id = getattr(getattr(event, "sender", None), "id", None)
        if user_id is not None and await cancel_user_scan(int(user_id), client):
            await event.reply("❌ Your metadata scan has been cancelled.")
        else:
            await event.reply("ℹ️ You do not have a running metadata scan.")
        return

    if not is_checkable_message(event):
        return

    # Never drop a media message because another scan was just received.
    # Every file gets its own Download Metadata button. Scan execution is
    # bounded by the global semaphore/queue, so bursts are queued instead
    # of producing "Please wait" messages or losing files.
    await record_user(event)
    await analyze(
        event,
        client=client,
        bot_username=bot_username,
        include_clone=include_clone,
    )


async def handle_callback(
    event,
    *,
    client: Any = bot,
    bot_username: str = BOT_USERNAME,
    include_clone: bool = True,
):
    purge_pending_scans()
    data = (event.data or b"").decode("ascii", "ignore")

    if data.startswith("owner:clones"):
        await event.answer()
        sender = await event.get_sender()
        user_id = getattr(sender, "id", None)
        if not _owner_allowed(user_id):
            await event.answer("Owner access only.", alert=True)
            return
        try:
            page = int(data.split(":", 2)[2]) if data.count(":") >= 2 else 0
        except ValueError:
            page = 0
        await render_owner_clones(event, int(user_id), page)
        return

    if data.startswith("owner:clone_remove:"):
        await event.answer()
        sender = await event.get_sender()
        user_id = getattr(sender, "id", None)
        if not _owner_allowed(user_id):
            await event.answer("Owner access only.", alert=True)
            return
        try:
            clone_id = int(data.split(":", 2)[2])
        except ValueError:
            await event.answer("Invalid clone.", alert=True)
            return

        clone_username = clone_usernames.get(clone_id) or "this clone"
        await event.edit(
            f"⚠️ <b>Remove @{html.escape(clone_username.lstrip('@'))}?</b>\n\n"
            "This will disconnect the clone bot immediately.",
            parse_mode="html",
            buttons=[
                [Button.inline("✅ Yes, Remove", f"owner:clone_remove_confirm:{clone_id}".encode("ascii"))],
                [Button.inline("⬅️ Cancel", b"owner:clones")],
            ],
        )
        return

    if data.startswith("owner:clone_remove_confirm:"):
        await event.answer()
        sender = await event.get_sender()
        user_id = getattr(sender, "id", None)
        if not _owner_allowed(user_id):
            await event.answer("Owner access only.", alert=True)
            return
        try:
            clone_id = int(data.split(":", 2)[2])
        except ValueError:
            await event.answer("Invalid clone.", alert=True)
            return
        if not await remove_clone_as_owner(int(user_id), clone_id):
            await event.answer("Clone not found or already removed.", alert=True)
            return
        await event.edit(
            "✅ <b>Clone removed by owner.</b>",
            parse_mode="html",
            buttons=[
                [Button.inline("🤖 Active Clone Bots", b"owner:clones")],
                [Button.inline("⬅️ Dashboard", b"owner:dashboard")],
            ],
        )
        return

    if data == "owner:broadcast":
        await event.answer()
        sender = await event.get_sender()
        user_id = getattr(sender, "id", None)
        if not _owner_allowed(user_id):
            await event.answer("Owner access only.", alert=True)
            return
        owner_broadcast_pending[int(user_id)] = time.monotonic()
        await event.edit(
            "📢 <b>Broadcast Message</b>\n\n"
            "Send one message for AniToon users, clone-bot users, and every registered group.\n\n"
            "⚡ Delivery uses the bot each user or group has interacted with.",
            parse_mode="html",
            buttons=[[Button.inline("❌ Cancel", b"owner:broadcast_cancel")]],
        )
        return

    if data == "owner:broadcast_cancel":
        await event.answer()
        sender = await event.get_sender()
        user_id = getattr(sender, "id", None)
        if user_id is not None:
            owner_broadcast_pending.pop(int(user_id), None)
        if _owner_allowed(user_id):
            await render_owner_dashboard(event, int(user_id))
        return

    if data == "owner:resources":
        await event.answer()
        sender = await event.get_sender()
        user_id = getattr(sender, "id", None)
        if not _owner_allowed(user_id):
            await event.answer("Owner access only.", alert=True)
            return
        await render_owner_resources(event, int(user_id))
        return

    if data == "owner:dashboard":
        await event.answer()
        sender = await event.get_sender()
        user_id = getattr(sender, "id", None)
        if not _owner_allowed(user_id):
            await event.answer("Owner access only.", alert=True)
            return
        await render_owner_dashboard(event, int(user_id))
        return

    if data.startswith("owner:users:"):
        await event.answer()
        sender = await event.get_sender()
        user_id = getattr(sender, "id", None)
        if not _owner_allowed(user_id):
            await event.answer("Owner access only.", alert=True)
            return
        try:
            page = int(data.split(":", 2)[2])
        except ValueError:
            page = 0
        await render_owner_users(event, int(user_id), page)
        return

    if data.startswith("owner:user:"):
        await event.answer()
        sender = await event.get_sender()
        user_id = getattr(sender, "id", None)
        if not _owner_allowed(user_id):
            await event.answer("Owner access only.", alert=True)
            return
        parts = data.split(":")
        try:
            target_user_id = int(parts[2])
            page = int(parts[3]) if len(parts) > 3 else 0
        except ValueError:
            await event.answer("Invalid user.", alert=True)
            return
        await render_owner_user_scans(
            event,
            int(user_id),
            target_user_id,
            page,
        )
        return

    if data == "home:clones":
        await event.answer()
        sender = await event.get_sender()
        user_id = getattr(sender, "id", None)
        if user_id is None or not include_clone:
            await event.answer("Clone management is available from the main AniToon bot.", alert=True)
            return
        await render_clone_list(event, int(user_id))
        return

    if data == "clone:list":
        await event.answer()
        sender = await event.get_sender()
        user_id = getattr(sender, "id", None)
        if user_id is None or not include_clone:
            await event.answer("Clone management is available from the main AniToon bot.", alert=True)
            return
        await render_clone_list(event, int(user_id))
        return

    if data.startswith("clone:stats:"):
        await event.answer()
        if not include_clone:
            await event.answer("Clone management is available from the main AniToon bot.", alert=True)
            return
        try:
            clone_id = int(data.split(":", 2)[2])
        except ValueError:
            await event.answer("Invalid clone.", alert=True)
            return
        sender = await event.get_sender()
        user_id = getattr(sender, "id", None)
        if user_id is None:
            await event.answer("User not found.", alert=True)
            return
        await render_clone_stats(event, int(user_id), clone_id)
        return

    if data.startswith("clone:remove_confirm:"):
        await event.answer()
        if not include_clone:
            await event.answer("Clone management is available from the main AniToon bot.", alert=True)
            return
        try:
            clone_id = int(data.split(":", 2)[2])
        except ValueError:
            await event.answer("Invalid clone.", alert=True)
            return
        sender = await event.get_sender()
        user_id = getattr(sender, "id", None)
        if user_id is None:
            await event.answer("User not found.", alert=True)
            return
        removed = await remove_clone_for_user(int(user_id), clone_id)
        if not removed:
            await event.answer("That clone is not owned by you.", alert=True)
            return
        await event.edit(
            "✅ <b>Clone bot removed.</b>\n\n"
            "The bot has been disconnected and will not be restored.",
            parse_mode="html",
            buttons=[
                [Button.inline("🤖 Clone Manager", b"home:clones")],
                [Button.inline("⬅️ Home", b"home:back")],
            ],
        )
        return

    if data.startswith("clone:remove:"):
        await event.answer()
        if not include_clone:
            await event.answer("Clone management is available from the main AniToon bot.", alert=True)
            return
        try:
            clone_id = int(data.split(":", 2)[2])
        except ValueError:
            await event.answer("Invalid clone.", alert=True)
            return
        sender = await event.get_sender()
        user_id = getattr(sender, "id", None)
        if user_id is None:
            await event.answer("User not found.", alert=True)
            return
        record = await get_user_clone(int(user_id), clone_id)
        if record is None and clone_owners.get(clone_id) != int(user_id):
            await event.answer("That clone is not owned by you.", alert=True)
            return
        username = str(
            (record or {}).get("clone_username")
            or clone_usernames.get(clone_id)
            or "this clone"
        ).lstrip("@")
        await event.edit(
            f"⚠️ <b>Remove @{html.escape(username)}?</b>\n\n"
            "This disconnects the clone and stops it from being restored.",
            parse_mode="html",
            buttons=[
                [Button.inline("✅ Yes, Remove", f"clone:remove_confirm:{clone_id}".encode("ascii"))],
                [Button.inline("⬅️ Cancel", b"clone:list")],
            ],
        )
        return

    if data == "home:stats":
        await event.answer()
        sender = await event.get_sender()
        user_id = getattr(sender, "id", None)
        if user_id is None:
            return
        await render_user_stats(event, int(user_id))
        return

    if data == "home:status":
        await event.answer()
        sender = await event.get_sender()
        user_id = getattr(sender, "id", None)
        if not _owner_allowed(user_id):
            await event.answer("Owner access only.", alert=True)
            return
        await render_public_status(event)
        return

    if data == "home:privacy":
        await event.answer()
        await event.edit(
            PRIVACY_TEXT,
            parse_mode="html",
            buttons=back_buttons(),
        )
        return

    if data == "home:help":
        await event.answer()
        text = HELP_TEXT if include_clone else CLONE_HELP_TEXT

        await event.edit(
            text,
            parse_mode="html",
            buttons=help_buttons(),
        )
        return

    if data == "home:scan":
        await event.answer()
        await event.edit(
            "🔎 <b>Ready to Scan</b>\n\n"
            "📤 <b>Send me a video or document.</b>",
            parse_mode="html",
            buttons=scan_page_buttons(),
        )
        return

    if data == "clone:create":
        if include_clone:
            await event.answer()
            await begin_clone_setup(event)
        else:
            await event.answer(
                "Clone management is available from the main AniToon bot.",
                alert=True,
            )
        return

    if data == "home:clone":
        if include_clone:
            await event.answer()
            await begin_clone_setup(event)
        else:
            await event.answer(
                "Clone management is available from the main AniToon bot.",
                alert=True,
            )
        return


    if data == "clone:cancel":
        await event.answer()
        sender = await event.get_sender()
        user_id = getattr(sender, "id", None)
        if user_id is not None:
            clone_setup_pending.pop(int(user_id), None)
        await event.edit(
            HOME_TEXT,
            parse_mode="html",
            buttons=home_buttons(
                bot_username,
                include_clone=include_clone,
                user_id=getattr(await event.get_sender(), "id", None),
                show_privacy=bool(getattr(event, "is_private", True)),
            ),
        )
        return

    if data == "home:about":
        await event.answer()
        await event.edit(
            ABOUT_TEXT,
            parse_mode="html",
            buttons=[
                [Button.url("🌐 Open AniToon Web", PUBLIC_WEB_URL)],
                [Button.url("🤖 Open AniToon Bot", f"https://t.me/{BOT_USERNAME}")],
                [Button.inline("⬅️ Home", b"home:back")],
            ],
        )
        return

    if data == "home:back":
        await event.answer()
        home_text = HOME_TEXT if include_clone else CLONE_HOME_TEXT
        sender = await event.get_sender()
        await event.edit(
            home_text,
            parse_mode="html",
            buttons=home_buttons(
                bot_username,
                include_clone=include_clone,
                user_id=getattr(sender, "id", None),
                show_privacy=bool(getattr(event, "is_private", True)),
            ),
        )
        return

    if data.startswith("scan:"):
        token = data[5:].strip()
        pending = pending_scans.pop(token, None)

        if pending is None:
            if token in active_scans:
                await event.answer("Metadata scan is already running.", alert=True)
            else:
                await event.answer(
                    "This metadata request expired. Send the file again.",
                    alert=True,
                )
            return

        if pending.client is not client:
            pending_scans[token] = pending
            await event.answer("This scan belongs to another bot.", alert=True)
            return

        async with scan_queue_lock:
            queue_full = active_processes >= MAX_CONCURRENT_CHECKS
            queue_position = queued_processes + 1 if queue_full else 0

        if queue_full:
            await event.answer(f"Bot is full with {MAX_CONCURRENT_CHECKS} active scans. Your scan is queued at #{queue_position}.", alert=True)
        else:
            await event.answer("Metadata scan starting…")

        status_message = await event.get_message()
        await edit_status(
            status_message,
            "🔎 <b>SCANNING METADATA</b>\n\n"
            "<code>[░░░░░░░░░░] 0%</code>\n"
            "Starting scan…",
            buttons=cancel_button(token),
        )

        task = asyncio.create_task(
            analyze_source(
                pending.source_message,
                status_message,
                token,
                pending.user_id,
                client=pending.client,
                bot_username=pending.bot_username,
                include_clone=pending.include_clone,
                show_privacy=pending.show_privacy,
            )
        )
        active_scans[token] = task
        active_scan_users[token] = pending.user_id
        active_scan_clients[token] = pending.client
        return

    if data.startswith("cancel:"):
        token = data[7:].strip()
        task = active_scans.get(token)

        if not task:
            await event.answer("This scan is no longer running.", alert=True)
            return

        if active_scan_clients.get(token) is not client:
            await event.answer("This scan belongs to another bot.", alert=True)
            return

        await event.answer("Cancelling scan…")
        task.cancel()
        return

    await event.answer()


def web_section(report: Report, section: str) -> str:
    return format_section(report, section).replace("\n", "<br>")


def home_page(report_token: str | None = None) -> bytes:
    channels = [
        ("🎬", "Movies", "https://t.me/+KEz_Up14hfFhOTI1"),
        ("🍿", "All Animes", "https://t.me/anitoons_ani"),
        ("🎧", "Dual Content", "https://t.me/ani_engjaphin"),
        ("📚", "Manga", "https://t.me/mangauniverse_ani"),
        ("🏴‍☠️", "One Piece", "https://t.me/ani_pocket_monster"),
        ("⚔️", "Jujutsu Kaisen", "https://t.me/jjk_anitoon"),
        ("🍥", "Naruto Shippuden", "https://t.me/naruto_shippuden_in_telugudub"),
    ]
    completed = [
        ("🤖", "Doraemon", "https://t.me/ani_seas"),
        ("🌻", "Shin-Chan", "https://t.me/shin_seas"),
        ("⚡", "Beyblade", "https://t.me/Ani_beyblade"),
        ("⚡", "Pokemon", "https://t.me/poketmonster_01"),
    ]

    def card(icon: str, name: str, url: str) -> str:
        return (
            f'<a class="channel" href="{html.escape(url)}" target="_blank" rel="noopener noreferrer">'
            f'<span class="channel-icon">{icon}</span>'
            f'<span class="channel-name">{html.escape(name)}</span>'
            f'<span class="channel-arrow">↗</span>'
            f'</a>'
        )

    current_html = "".join(card(*item) for item in channels)
    completed_html = "".join(card(*item) for item in completed)

    report_embed = ""
    if report_token:
        safe_token = html.escape(report_token)
        report_embed = f"""
        <section class="section report-section">
          <div class="section-head">
            <div>
              <small>LIVE REPORT</small>
              <h2>🔬 File Intelligence</h2>
            </div>
            <a class="mini-link" href="/report/{safe_token}">Open ↗</a>
          </div>
          <iframe src="/report/{safe_token}?embed=1" title="AniToon Media Metadata" loading="lazy"></iframe>
        </section>
        """

    document = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#050611">
<meta name="description" content="AniToon Media Info — creative media channels and file intelligence.">
<title>AniToon Media Info</title>
<style>
:root {{
  color-scheme:dark;
  --bg:#04050d;--panel:rgba(11,14,31,.72);--line:rgba(255,255,255,.09);
  --text:#f7f7fb;--muted:#9ba2bd;--a:#9a8cff;--b:#5ee7ff;--good:#7cf4b0;
}}
*{{box-sizing:border-box}}
html{{scroll-behavior:smooth}}
body{{margin:0;min-height:100vh;background:var(--bg);color:var(--text);font:14px/1.55 system-ui,-apple-system,"Segoe UI",sans-serif;overflow-x:hidden}}
body::before{{content:"";position:fixed;inset:0;z-index:-4;background:
radial-gradient(900px 620px at 8% 2%,rgba(106,89,255,.22),transparent 68%),
radial-gradient(760px 520px at 94% 10%,rgba(45,216,255,.16),transparent 70%),
radial-gradient(1000px 680px at 50% 100%,rgba(191,67,255,.10),transparent 72%),
linear-gradient(180deg,#080918 0%,#050611 55%,#03040a 100%)}}
body::after{{content:"";position:fixed;inset:-35%;z-index:-3;background:conic-gradient(from 0deg,transparent 0 20%,rgba(154,140,255,.11) 28%,transparent 38% 56%,rgba(94,231,255,.09) 64%,transparent 78%);filter:blur(40px);animation:spin 26s linear infinite;pointer-events:none}}
.grid{{position:fixed;inset:0;z-index:-2;opacity:.20;background-image:linear-gradient(rgba(255,255,255,.028) 1px,transparent 1px),linear-gradient(90deg,rgba(255,255,255,.028) 1px,transparent 1px);background-size:38px 38px;mask-image:linear-gradient(to bottom,black,transparent 88%)}}
.float{{position:fixed;width:250px;height:250px;border-radius:50%;z-index:-1;pointer-events:none;filter:blur(8px);opacity:.35}}
.float.a{{left:-120px;top:22%;background:radial-gradient(circle,rgba(154,140,255,.42),transparent 70%);animation:float1 16s ease-in-out infinite}}
.float.b{{right:-120px;top:62%;background:radial-gradient(circle,rgba(94,231,255,.34),transparent 70%);animation:float2 20s ease-in-out infinite}}
.topline{{height:3px;background:linear-gradient(90deg,transparent,var(--a),var(--b),transparent);background-size:200% 100%;animation:flow 5s linear infinite}}
.wrap{{max-width:1100px;margin:auto;padding:18px 14px 48px}}
.nav{{display:flex;justify-content:space-between;align-items:center;gap:12px;padding:11px 14px;border:1px solid var(--line);border-radius:16px;background:rgba(8,10,24,.68);backdrop-filter:blur(16px)}}
.brand{{font-weight:900;letter-spacing:.08em;text-transform:uppercase;font-size:11px}}
.nav-link{{color:var(--muted);text-decoration:none;font-size:12px;font-weight:800}}
.hero{{position:relative;overflow:hidden;margin-top:14px;padding:30px 24px;border:1px solid var(--line);border-radius:28px;background:linear-gradient(145deg,rgba(15,18,43,.87),rgba(7,9,21,.58));box-shadow:0 30px 90px rgba(0,0,0,.38);backdrop-filter:blur(22px);animation:reveal .8s cubic-bezier(.2,1,.2,1) both}}
.hero::before{{content:"";position:absolute;inset:0;background:linear-gradient(105deg,transparent,rgba(255,255,255,.045),transparent);transform:translateX(-110%);animation:sheen 9s ease-in-out infinite;pointer-events:none}}
.eyebrow{{display:inline-flex;align-items:center;gap:8px;padding:7px 10px;border-radius:999px;background:rgba(124,244,176,.06);border:1px solid rgba(124,244,176,.14);color:var(--good);font-size:9px;font-weight:900;letter-spacing:.14em;text-transform:uppercase}}
.dot{{width:7px;height:7px;border-radius:50%;background:var(--good);box-shadow:0 0 16px rgba(124,244,176,.7);animation:pulse 1.8s ease-in-out infinite}}
h1{{margin:15px 0 7px;font-size:clamp(30px,7vw,56px);line-height:1;letter-spacing:-.045em}}
.lead{{max-width:720px;color:#c3c7da;font-size:14px}}
.cta-row{{display:flex;gap:9px;flex-wrap:wrap;margin-top:18px}}
.cta{{display:inline-flex;align-items:center;justify-content:center;text-decoration:none;padding:10px 13px;border-radius:13px;font-weight:850;font-size:12px;border:1px solid var(--line);color:var(--text);background:rgba(255,255,255,.045);transition:.2s ease}}
.cta.primary{{background:linear-gradient(135deg,rgba(154,140,255,.23),rgba(94,231,255,.11));border-color:rgba(154,140,255,.28)}}
.cta:hover{{transform:translateY(-2px);border-color:rgba(154,140,255,.34);background:rgba(255,255,255,.07)}}
.section{{margin-top:18px;border:1px solid var(--line);border-radius:22px;background:var(--panel);backdrop-filter:blur(18px);box-shadow:0 22px 56px rgba(0,0,0,.22);overflow:hidden;animation:reveal .7s cubic-bezier(.2,1,.2,1) both}}
.section-head{{display:flex;justify-content:space-between;align-items:center;gap:12px;padding:16px 18px;border-bottom:1px solid var(--line);background:linear-gradient(90deg,rgba(255,255,255,.035),transparent)}}
.section-head small{{display:block;color:var(--a);font-size:9px;font-weight:900;letter-spacing:.14em}}
.section-head h2{{margin:2px 0 0;font-size:18px}}
.mini-link{{color:var(--b);text-decoration:none;font-size:11px;font-weight:850}}
.list{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:9px;padding:13px}}
.channel{{display:flex;align-items:center;gap:12px;min-height:58px;padding:11px 12px;border:1px solid var(--line);border-radius:16px;color:var(--text);text-decoration:none;background:linear-gradient(145deg,rgba(255,255,255,.035),rgba(255,255,255,.012));transition:.20s ease}}
.channel:hover{{transform:translateY(-3px);border-color:rgba(154,140,255,.30);box-shadow:0 14px 30px rgba(0,0,0,.22)}}
.channel-icon{{width:36px;height:36px;display:grid;place-items:center;flex:0 0 auto;border-radius:12px;background:rgba(154,140,255,.08);border:1px solid rgba(154,140,255,.13);font-size:18px}}
.channel-name{{flex:1;min-width:0;font-weight:800}}
.channel-arrow{{color:var(--muted);font-size:18px}}
.report-section iframe{{display:block;width:100%;height:1900px;border:0;background:#050611}}
.footer{{display:flex;justify-content:space-between;gap:12px;margin-top:18px;color:#777d98;font-size:10px;padding:0 4px}}
.footer b{{color:#b7b8ca}}
@keyframes spin{{to{{transform:rotate(360deg)}}}}
@keyframes float1{{0%,100%{{transform:translate3d(0,0,0)}}50%{{transform:translate3d(70px,45px,0) scale(1.08)}}}}
@keyframes float2{{0%,100%{{transform:translate3d(0,0,0)}}50%{{transform:translate3d(-75px,-30px,0) scale(1.1)}}}}
@keyframes pulse{{0%,100%{{transform:scale(.85);opacity:.8}}50%{{transform:scale(1.15);opacity:1}}}}
@keyframes flow{{0%{{background-position:0% 50%}}100%{{background-position:200% 50%}}}}
@keyframes sheen{{0%,100%{{transform:translateX(-110%)}}50%{{transform:translateX(100%)}}}}
@keyframes reveal{{from{{opacity:0;transform:translateY(16px);filter:blur(6px)}}to{{opacity:1;transform:none;filter:none}}}}
@media(max-width:700px){{.list{{grid-template-columns:1fr}}.hero{{padding:24px 18px;border-radius:22px}}.footer{{flex-direction:column;align-items:flex-start}}.report-section iframe{{height:2050px}}}}
@media(prefers-reduced-motion:reduce){{*,*::before,*::after{{animation:none!important;transition:none!important;scroll-behavior:auto!important}}}}
</style>
</head>
<body>
<div class="topline"></div><div class="grid"></div><div class="float a"></div><div class="float b"></div>
<div class="wrap">
  <nav class="nav">
    <div class="brand">AniToon Media Info</div>
    <a class="nav-link" href="https://t.me/AniToon_1Bot" target="_blank" rel="noopener noreferrer">Open Bot ↗</a>
  </nav>

  <header class="hero">
    <span class="eyebrow"><span class="dot"></span> Media intelligence</span>
    <h1>⛩ AniToon</h1>
    <div class="lead">A focused media hub for discovering AniToon channels and inspecting video, audio, subtitle and technical metadata.</div>
    <div class="cta-row">
      <a class="cta primary" href="https://t.me/AniToon_1Bot" target="_blank" rel="noopener noreferrer">🤖 Open AniToon Bot</a>
      <a class="cta" href="https://t.me/Anitoon_group" target="_blank" rel="noopener noreferrer">👥 Community</a>
    </div>
  </header>

  {report_embed}

  <section class="section">
    <div class="section-head"><div><small>NOW LIVE</small><h2>🎞️ Channels</h2></div></div>
    <div class="list">{current_html}</div>
  </section>

  <section class="section">
    <div class="section-head"><div><small>ARCHIVE</small><h2>✅ Completed collections</h2></div></div>
    <div class="list">{completed_html}</div>
  </section>

  <footer class="footer">
    <span><b>AniToon</b> • Media intelligence</span>
    <span>Official Bot & Web</span>
  </footer>
</div>
</body>
</html>"""
    return document.encode("utf-8")


def web_page(report: Report, report_token: str | None = None) -> bytes:
    filename = html.escape(report.filename or "Telegram media file")
    generated = datetime.now(timezone.utc)
    generated_text = generated.strftime("%d %b %Y • %H:%M UTC")

    audio = list(report.audio.get("tracks", []) or [])
    video = list(report.video.get("tracks", []) or [])
    subtitles = list(report.subtitles or [])

    def esc(value: Any) -> str:
        return html.escape(str(value))

    def human_size(value: Any) -> str:
        try:
            size = float(value)
        except Exception:
            return "Not available"
        units = ("B", "KB", "MB", "GB", "TB")
        i = 0
        while size >= 1024 and i < len(units) - 1:
            size /= 1024
            i += 1
        return f"{size:.1f} {units[i]}" if i else f"{int(size)} {units[i]}"

    def icon_for(kind: str) -> str:
        return {"Video": "🎬", "Audio": "🎧", "Subtitle": "💬"}.get(kind, "◈")

    def track_cards(items: list[dict[str, Any]], kind: str) -> str:
        if not items:
            return (
                f'<div class="empty-state">'
                f'<span class="empty-icon">{icon_for(kind)}</span>'
                f'<div><strong>No {esc(kind.lower())} tracks detected</strong>'
                f'<p>The inspected metadata did not expose a confirmed {esc(kind.lower())} stream.</p></div>'
                f'</div>'
            )

        cards: list[str] = []
        for index, track in enumerate(items, 1):
            name = track.get("name") or track.get("display_name") or f"{kind} Track {index}"
            language = track.get("language_name") or track.get("language")
            codec = track.get("codec_name") or track.get("codec")
            details: list[tuple[str, Any]] = []

            if language:
                details.append(("Language", language))
            if codec:
                details.append(("Codec", codec))

            if kind == "Audio":
                details.extend([
                    ("Channels", track.get("channels")),
                    ("Layout", track.get("layout")),
                    ("Sample rate", track.get("sample_rate")),
                    ("Bitrate", track.get("bitrate")),
                ])
            elif kind == "Video":
                details.extend([
                    ("Resolution", track.get("dimensions")),
                    ("Pixel format", track.get("pixel_format")),
                    ("Profile", track.get("profile")),
                    ("Frame rate", track.get("frame_rate")),
                ])
            else:
                details.append(("Format", track.get("subtitle_format") or codec))

            details = [(label, value) for label, value in details if value]
            rows = "".join(
                f'<div class="spec"><span>{esc(label)}</span><strong>{esc(value)}</strong></div>'
                for label, value in details
            )

            flags: list[str] = []
            for key, label in (
                ("default", "DEFAULT"),
                ("original", "ORIGINAL"),
                ("commentary", "COMMENTARY"),
                ("forced", "FORCED"),
                ("hearing_impaired", "HI"),
                ("visual_impaired", "VI"),
            ):
                if track.get(key) == "yes":
                    flags.append(label)

            badges = "".join(f'<span class="badge">{esc(flag)}</span>' for flag in flags)
            cards.append(
                f"""
                <article class="track-card">
                  <div class="track-orb">{icon_for(kind)}</div>
                  <div class="track-content">
                    <div class="track-heading">
                      <div>
                        <div class="track-number">{index:02d}</div>
                        <h3>{esc(name)}</h3>
                        <p>{esc(kind)} stream</p>
                      </div>
                      <div class="badges">{badges}</div>
                    </div>
                    <div class="spec-grid">{rows}</div>
                  </div>
                </article>
                """
            )
        return "".join(cards)

    runtime = report.container.get("runtime") or "Not available"
    container_name = report.detected or "Detected media"
    mime = report.mime or "application/octet-stream"
    size_text = human_size(report.size)
    sampled_text = human_size(report.sampled)

    first_video = video[0] if video else {}
    quality = first_video.get("dimensions") or "Not available"
    primary_codec = first_video.get("codec_name") or first_video.get("codec") or "Not available"
    bitrate = report.container.get("average_bitrate") or "Not available"
    title = report.container.get("title")

    report_id = esc((report_token or "local")[:14])

    document = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#070816">
<title>AniToon Media Intelligence — {filename}</title>
<style>
:root {{
  color-scheme: dark;
  --bg:#050611;
  --panel:rgba(12,15,34,.72);
  --panel-strong:rgba(15,19,43,.88);
  --line:rgba(255,255,255,.10);
  --line-strong:rgba(139,124,255,.32);
  --text:#f7f7fb;
  --muted:#9ea4bf;
  --accent:#9a8cff;
  --accent-2:#5ee7ff;
  --good:#7cf4b0;
  --shadow:0 28px 80px rgba(0,0,0,.42);
}}
* {{ box-sizing:border-box; }}
html {{ scroll-behavior:smooth; }}
body {{
  margin:0;
  min-height:100vh;
  color:var(--text);
  background:var(--bg);
  font:14px/1.55 Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
  overflow-x:hidden;
}}
body::before {{
  content:"";
  position:fixed;
  inset:0;
  z-index:-4;
  background:
    radial-gradient(900px 620px at 8% 5%, rgba(110,95,255,.24), transparent 68%),
    radial-gradient(780px 560px at 92% 10%, rgba(49,224,255,.17), transparent 70%),
    radial-gradient(900px 680px at 50% 100%, rgba(176,76,255,.12), transparent 72%),
    linear-gradient(180deg,#070816 0%,#050611 55%,#03040b 100%);
}}
body::after {{
  content:"";
  position:fixed;
  inset:-35%;
  z-index:-3;
  background:
    conic-gradient(from 0deg at 50% 50%, transparent 0 18%, rgba(114,99,255,.12) 25%, transparent 35% 55%, rgba(70,229,255,.10) 63%, transparent 75% 100%);
  filter:blur(34px);
  animation:aurora 24s linear infinite;
  pointer-events:none;
}}
.bg-grid {{
  position:fixed;
  inset:0;
  z-index:-2;
  opacity:.23;
  background-image:
    linear-gradient(rgba(255,255,255,.025) 1px,transparent 1px),
    linear-gradient(90deg,rgba(255,255,255,.025) 1px,transparent 1px);
  background-size:36px 36px;
  mask-image:linear-gradient(to bottom,black 0%,transparent 85%);
}}
.orb {{
  position:fixed;
  width:260px;height:260px;border-radius:50%;
  z-index:-1;pointer-events:none;
  filter:blur(2px);
  opacity:.42;
  mix-blend-mode:screen;
}}
.orb.a {{
  top:12%;left:-110px;
  background:radial-gradient(circle at 50% 50%,rgba(157,122,255,.38),transparent 68%);
  animation:floatA 16s ease-in-out infinite;
}}
.orb.b {{
  top:58%;right:-120px;
  background:radial-gradient(circle at 50% 50%,rgba(56,215,255,.28),transparent 68%);
  animation:floatB 20s ease-in-out infinite;
}}
.top-glow {{
  position:fixed;top:0;left:0;right:0;height:3px;z-index:20;
  background:linear-gradient(90deg,transparent,var(--accent),var(--accent-2),transparent);
  background-size:200% 100%;
  animation:scanline 5s linear infinite;
}}
.wrap {{ max-width:1120px; margin:auto; padding:22px 16px 54px; }}
.nav {{
  display:flex;justify-content:space-between;align-items:center;gap:12px;
  margin-bottom:16px;padding:10px 13px;
  border:1px solid var(--line);border-radius:16px;
  background:rgba(9,11,25,.66);backdrop-filter:blur(18px);
}}
.nav .brand {{font-weight:900;letter-spacing:.08em;text-transform:uppercase;font-size:11px;color:#dddafe;}}
.nav a {{color:var(--muted);text-decoration:none;font-size:12px;font-weight:750;}}
.hero {{
  position:relative;overflow:hidden;
  padding:26px;border:1px solid var(--line);border-radius:28px;
  background:linear-gradient(145deg,rgba(15,18,43,.86),rgba(8,10,23,.60));
  box-shadow:var(--shadow);
  backdrop-filter:blur(22px);
  animation:reveal .75s cubic-bezier(.2,1,.2,1) both;
}}
.hero::before {{
  content:"";position:absolute;inset:0;pointer-events:none;
  background:
    linear-gradient(110deg,transparent 0%,rgba(255,255,255,.04) 28%,transparent 48%),
    radial-gradient(420px 180px at 0% 0%,rgba(154,140,255,.15),transparent 72%);
  transform:translateX(-20%);
  animation:sheen 9s ease-in-out infinite;
}}
.eyebrow {{
  display:inline-flex;align-items:center;gap:8px;
  padding:7px 10px;border:1px solid rgba(124,244,176,.16);border-radius:999px;
  background:rgba(124,244,176,.07);color:var(--good);
  font-size:10px;font-weight:900;letter-spacing:.12em;text-transform:uppercase;
}}
.dot {{width:7px;height:7px;border-radius:50%;background:var(--good);box-shadow:0 0 16px rgba(124,244,176,.7);animation:pulse 1.8s ease-in-out infinite;}}
h1 {{ margin:14px 0 5px;font-size:clamp(25px,5vw,42px);line-height:1.05;letter-spacing:-.035em; }}
.file {{color:#c7cbe1;overflow-wrap:anywhere;font-size:13px;max-width:850px;}}
.hero-meta {{display:flex;gap:8px;flex-wrap:wrap;margin-top:14px;}}
.pill {{display:inline-flex;align-items:center;gap:7px;padding:8px 10px;border:1px solid var(--line);border-radius:999px;background:rgba(255,255,255,.035);color:var(--muted);font-size:11px;}}
.summary {{
  display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:10px;margin-top:18px;
}}
.stat {{
  position:relative;padding:14px;border:1px solid var(--line);border-radius:17px;
  background:linear-gradient(180deg,rgba(255,255,255,.05),rgba(255,255,255,.015));
  overflow:hidden;transition:transform .2s ease,border-color .2s ease,background .2s ease;
}}
.stat:hover {{transform:translateY(-3px);border-color:var(--line-strong);background:rgba(255,255,255,.065);}}
.stat::after {{
  content:"";position:absolute;width:70px;height:70px;right:-20px;top:-24px;border-radius:50%;
  background:radial-gradient(circle,rgba(154,140,255,.17),transparent 70%);
}}
.stat b {{display:block;font-size:21px;letter-spacing:-.02em;}}
.stat span {{color:var(--muted);font-size:11px;}}
.section {{
  margin-top:16px;border:1px solid var(--line);border-radius:22px;
  background:var(--panel);backdrop-filter:blur(18px);box-shadow:0 20px 50px rgba(0,0,0,.20);
  overflow:hidden;animation:reveal .7s cubic-bezier(.2,1,.2,1) both;
}}
.section:nth-of-type(2){{animation-delay:.08s}} .section:nth-of-type(3){{animation-delay:.14s}}
.section:nth-of-type(4){{animation-delay:.20s}} .section:nth-of-type(5){{animation-delay:.26s}}
.section-head {{
  display:flex;align-items:center;justify-content:space-between;gap:12px;
  padding:16px 18px;border-bottom:1px solid var(--line);
  background:linear-gradient(90deg,rgba(255,255,255,.035),transparent);
}}
.section-head h2 {{margin:0;font-size:17px;letter-spacing:-.02em;}}
.section-body {{padding:14px;}}
.track-card {{
  display:flex;gap:14px;padding:16px;border:1px solid var(--line);border-radius:18px;
  background:linear-gradient(145deg,rgba(255,255,255,.035),rgba(255,255,255,.012));
  margin-bottom:10px;transition:transform .2s ease,border-color .2s ease,box-shadow .2s ease;
}}
.track-card:last-child {{margin-bottom:0;}}
.track-card:hover {{transform:translateY(-2px);border-color:rgba(154,140,255,.30);box-shadow:0 16px 35px rgba(0,0,0,.22);}}
.track-orb {{
  width:46px;height:46px;border-radius:15px;display:grid;place-items:center;flex:0 0 auto;
  background:radial-gradient(circle at 30% 20%,rgba(154,140,255,.24),rgba(94,231,255,.08));
  border:1px solid rgba(154,140,255,.20);font-size:20px;
}}
.track-content {{min-width:0;flex:1;}}
.track-heading {{display:flex;justify-content:space-between;gap:12px;align-items:flex-start;}}
.track-number {{color:var(--accent);font-size:10px;font-weight:900;letter-spacing:.12em;}}
.track-heading h3 {{margin:2px 0 1px;font-size:16px;overflow-wrap:anywhere;}}
.track-heading p {{margin:0;color:var(--muted);font-size:11px;}}
.badges {{display:flex;gap:5px;flex-wrap:wrap;justify-content:flex-end;}}
.badge {{padding:4px 7px;border-radius:999px;border:1px solid rgba(124,244,176,.15);background:rgba(124,244,176,.06);color:var(--good);font-size:9px;font-weight:900;letter-spacing:.08em;}}
.spec-grid {{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;margin-top:12px;}}
.spec {{display:flex;justify-content:space-between;gap:12px;padding:9px 10px;border-radius:11px;background:rgba(255,255,255,.035);}}
.spec span {{color:var(--muted);font-size:11px;}}
.spec strong {{font-size:11px;text-align:right;overflow-wrap:anywhere;}}
.empty-state {{display:flex;gap:12px;align-items:center;padding:14px;border:1px dashed rgba(255,255,255,.10);border-radius:16px;background:rgba(255,255,255,.02);}}
.empty-icon {{font-size:20px;opacity:.7;}}
.empty-state strong {{font-size:13px;}}
.empty-state p {{margin:2px 0 0;color:var(--muted);font-size:11px;}}
.tech-grid {{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:9px;}}
.tech {{
  padding:13px;border:1px solid var(--line);border-radius:15px;background:rgba(255,255,255,.028);
}}
.tech small {{display:block;color:var(--muted);font-size:10px;margin-bottom:4px;}}
.tech b {{display:block;font-size:12px;overflow-wrap:anywhere;}}
.footer {{
  display:flex;justify-content:space-between;gap:12px;align-items:center;margin-top:18px;
  color:#7f849d;font-size:10px;padding:0 3px;
}}
.footer strong {{color:#b9bad0;}}
.countdown {{color:var(--accent-2);font-weight:900;}}
.reveal-line {{height:1px;background:linear-gradient(90deg,transparent,rgba(154,140,255,.45),transparent);margin:2px 0 0;}}
@keyframes aurora {{to {{transform:rotate(360deg)}}}}
@keyframes floatA {{0%,100%{{transform:translate3d(0,0,0) scale(1)}}50%{{transform:translate3d(80px,40px,0) scale(1.12)}}}}
@keyframes floatB {{0%,100%{{transform:translate3d(0,0,0) scale(1)}}50%{{transform:translate3d(-80px,-30px,0) scale(1.10)}}}}
@keyframes pulse {{0%,100%{{transform:scale(.85);opacity:.8}}50%{{transform:scale(1.15);opacity:1}}}}
@keyframes scanline {{0%{{background-position:0% 50%}}100%{{background-position:200% 50%}}}}
@keyframes sheen {{0%,100%{{transform:translateX(-25%)}}50%{{transform:translateX(45%)}}}}
@keyframes reveal {{from{{opacity:0;transform:translateY(18px);filter:blur(7px)}}to{{opacity:1;transform:none;filter:none}}}}
@media(max-width:860px){{.summary{{grid-template-columns:repeat(3,minmax(0,1fr))}}.tech-grid{{grid-template-columns:repeat(2,minmax(0,1fr))}}}}
@media(max-width:620px){{.wrap{{padding:12px 10px 35px}}.hero{{padding:20px;border-radius:22px}}.summary{{grid-template-columns:repeat(2,minmax(0,1fr))}}.spec-grid,.tech-grid{{grid-template-columns:1fr}}.track-card{{padding:13px}}.track-heading{{flex-direction:column}}.badges{{justify-content:flex-start}}.footer{{flex-direction:column;align-items:flex-start}}}}
@media(prefers-reduced-motion:reduce){{*,*::before,*::after{{animation:none!important;transition:none!important;scroll-behavior:auto!important}}}}
</style>
</head>
<body>
<div class="top-glow"></div>
<div class="bg-grid"></div>
<div class="orb a"></div><div class="orb b"></div>

<div class="wrap">
  <nav class="nav">
    <div class="brand">AniToon Media Intelligence</div>
    <a href="/">Home ↗</a>
  </nav>

  <header class="hero">
    <span class="eyebrow"><span class="dot"></span> Analysis Complete</span>
    <h1>Media Metadata Report</h1>
    <div class="file">{filename}</div>
    {f'<div class="pill" style="margin-top:10px;display:inline-flex;">🎞️ {esc(title)}</div>' if title else ''}
    <div class="hero-meta">
      <span class="pill">📦 {esc(container_name)}</span>
      <span class="pill">📏 {esc(size_text)}</span>
      <span class="pill">⏱️ <span id="countdown" class="countdown">05:00</span></span>
    </div>

    <div class="summary">
      <div class="stat"><b>{len(video)}</b><span>Video</span></div>
      <div class="stat"><b>{len(audio)}</b><span>Audio</span></div>
      <div class="stat"><b>{len(subtitles)}</b><span>Subtitles</span></div>
      <div class="stat"><b>{esc(quality)}</b><span>Quality</span></div>
      <div class="stat"><b>{esc(runtime)}</b><span>Runtime</span></div>
    </div>
  </header>

  <section class="section">
    <div class="section-head">
      <h2>🎬 Video</h2>
      <span class="pill">{esc(primary_codec)}</span>
    </div>
    <div class="section-body">{track_cards(video, "Video")}</div>
  </section>

  <section class="section">
    <div class="section-head">
      <h2>🎧 Audio</h2>
      <span class="pill">{len(audio)} track{'s' if len(audio) != 1 else ''}</span>
    </div>
    <div class="section-body">{track_cards(audio, "Audio")}</div>
  </section>

  <section class="section">
    <div class="section-head">
      <h2>💬 Subtitles</h2>
      <span class="pill">{len(subtitles)} track{'s' if len(subtitles) != 1 else ''}</span>
    </div>
    <div class="section-body">{track_cards(subtitles, "Subtitle")}</div>
  </section>

  <section class="section">
    <div class="section-head">
      <h2>⚙️ Technical</h2>
      <span class="pill">ID {report_id}</span>
    </div>
    <div class="section-body">
      <div class="tech-grid">
        <div class="tech"><small>Container</small><b>{esc(container_name)}</b></div>
        <div class="tech"><small>MIME type</small><b>{esc(mime)}</b></div>
        <div class="tech"><small>Runtime</small><b>{esc(runtime)}</b></div>
        <div class="tech"><small>File size</small><b>{esc(size_text)}</b></div>
        <div class="tech"><small>Average bitrate</small><b>{esc(bitrate)}</b></div>
        <div class="tech"><small>Metadata sampled</small><b>{esc(sampled_text)}</b></div>
      </div>
    </div>
  </section>

  <div class="footer">
    <span><strong>AniToon</strong> • Media intelligence</span>
    <span>Generated {generated_text} • Secure report link</span>
  </div>
</div>

<script>
(() => {{
  let left = 300;
  const countdown = document.getElementById("countdown");
  const tick = () => {{
    if (countdown) {{
      const m = Math.floor(left / 60);
      const s = left % 60;
      countdown.textContent = String(m).padStart(2,"0") + ":" + String(s).padStart(2,"0");
    }}
    if (left > 0) {{
      left -= 1;
      setTimeout(tick,1000);
    }}
  }};
  tick();
}})();
</script>
</body>
</html>"""
    return document.encode("utf-8")


async def health_server():
    port = int(os.getenv("PORT", "10000"))

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        global web_bytes_sent
        try:
            raw = await reader.read(4096)
            first = raw.split(b"\r\n", 1)[0].decode("latin1", "replace")
            parts = first.split(" ", 2)
            target = parts[1] if len(parts) > 1 else "/"
            parsed_url = urlsplit(target)
            path = parsed_url.path
            query = parsed_url.query
            purge_probes()

            if path.startswith("/probe/"):
                token = path[len("/probe/"):].strip("/")
                session = get_probe(token)
                method = first.split(" ", 1)[0].upper()

                if not session:
                    body = b"Probe session expired"
                    head = b"Content-Type: text/plain; charset=utf-8\r\n"
                    code = b"404 Not Found"
                    body_for_send = body
                elif method not in {"GET", "HEAD"}:
                    body = b"Method Not Allowed"
                    head = b"Allow: GET, HEAD\r\nContent-Type: text/plain; charset=utf-8\r\n"
                    code = b"405 Method Not Allowed"
                    body_for_send = body
                else:
                    range_value = next(
                        (line.split(":", 1)[1].strip() for line in raw.decode("latin1", "replace").split("\r\n")
                         if line.lower().startswith("range:")),
                        "",
                    )

                    total = session.total
                    start = 0
                    end = min(
                        (total - 1) if total is not None else session.chunk_size - 1,
                        session.chunk_size - 1,
                    )
                    partial = False

                    if method == "HEAD" and not range_value:
                        body_for_send = b""
                        body = body_for_send
                        code = b"200 OK"
                        head = (
                            b"Accept-Ranges: bytes\r\n"
                            + (
                                f"Content-Length: {total if total is not None else 0}\r\n".encode("ascii")
                            )
                            + b"Content-Type: application/octet-stream\r\n"
                        )
                    elif range_value.lower().startswith("bytes="):
                        spec = range_value[6:].split(",", 1)[0].strip()
                        if "-" not in spec:
                            body = b"Invalid Range"
                            head = b"Content-Type: text/plain; charset=utf-8\r\n"
                            code = b"416 Range Not Satisfiable"
                            body_for_send = body
                        else:
                            left, right = spec.split("-", 1)
                            try:
                                if left:
                                    start = int(left)
                                    if right:
                                        end = int(right)
                                    elif total is not None:
                                        end = total - 1
                                    else:
                                        end = start + session.chunk_size - 1
                                else:
                                    suffix = int(right)
                                    if total is None:
                                        raise ValueError
                                    start = max(0, total - suffix)
                                    end = total - 1

                                if total is not None:
                                    if start < 0 or start >= total:
                                        raise ValueError
                                    end = min(end, total - 1)
                                if end < start:
                                    raise ValueError
                                partial = True

                                body_for_send = (
                                    b"" if method == "HEAD"
                                    else await session.read(start, end - start + 1)
                                )
                                actual_end = start + len(body_for_send) - 1
                                if method == "HEAD":
                                    actual_end = end
                                body = body_for_send
                                code = b"206 Partial Content"
                                head = (
                                    b"Accept-Ranges: bytes\r\n"
                                    + f"Content-Range: bytes {start}-{actual_end}/{total}\r\n".encode("ascii")
                                    if total is not None
                                    else b"Accept-Ranges: bytes\r\n"
                                )
                                head += b"Content-Type: application/octet-stream\r\n"
                                body_for_send = body
                            except (ValueError, ProbeBudgetExceeded, ProbeCancelled):
                                body = b"Requested media range is unavailable"
                                head = b"Content-Type: text/plain; charset=utf-8\r\n"
                                code = b"416 Range Not Satisfiable"
                                body_for_send = body
                    else:
                        try:
                            if method == "HEAD":
                                body_for_send = b""
                            else:
                                body_for_send = await session.read(0, min(session.chunk_size, session.total or session.chunk_size))
                            actual_end = start + len(body_for_send) - 1
                            body = body_for_send
                            code = b"206 Partial Content"
                            head = (
                                b"Accept-Ranges: bytes\r\n"
                                + (
                                    f"Content-Range: bytes 0-{actual_end}/{total}\r\n".encode("ascii")
                                    if total is not None else b""
                                )
                                + b"Content-Type: application/octet-stream\r\n"
                            )
                        except (ProbeBudgetExceeded, ProbeCancelled):
                            body = b"Probe budget exceeded"
                            head = b"Content-Type: text/plain; charset=utf-8\r\n"
                            code = b"509 Bandwidth Limit Exceeded"
                            body_for_send = body

                    writer.write(
                        b"HTTP/1.1 " + code + b"\r\n"
                        + head
                        + f"Content-Length: {len(body_for_send)}\r\n".encode("ascii")
                        + f'ETag: "probe-{token}"\r\n'.encode("ascii")
                        + b"Cache-Control: no-store\r\nConnection: close\r\n\r\n"
                        + body_for_send
                    )
                    await writer.drain()
                    return

                writer.write(
                    b"HTTP/1.1 " + code + b"\r\n"
                    + head
                    + f"Content-Length: {len(body_for_send)}\r\n".encode("ascii")
                    + b"Cache-Control: no-store\r\nConnection: close\r\n\r\n"
                    + body_for_send
                )
                await writer.drain()
                return


            if path == "/health":
                _purge_states()
                payload = {
                    "status": "ok",
                    "service": "anitoons-file-intelligence",
                    "telegram_connected": bot.is_connected(),
                    "mongodb_connected": mongodb_is_connected(),
                    "checks_total": checks_total,
                    "checks_ok": checks_ok,
                    "checks_failed": checks_failed,
                    "web_reports": len(web_states),
                    "clones": len(clone_clients),
                    "active_scans": active_processes,
                    "queued_scans": queued_processes,
                    "max_active_scans": MAX_CONCURRENT_CHECKS,
                    "ram_pct": runtime_resource_stats()["ram_pct"],
                    "web_egress_gb": runtime_resource_stats()["web_egress_gb"],
                    "uptime_seconds": int(
                        (datetime.now(timezone.utc) - started_at).total_seconds()
                    ),
                }
                body = json.dumps(
                    payload,
                    separators=(",", ":"),
                ).encode("utf-8")
                head = b"Content-Type: application/json; charset=utf-8\r\n"
                code = b"200 OK"

            elif path.startswith("/report/"):
                _purge_states()
                token = path[len("/report/"):].strip("/")

                if query != "embed=1":
                    state = web_states.get(token)
                    if state:
                        writer.write(
                            b"HTTP/1.1 302 Found\r\n"
                            + f"Location: /?report={token}\r\n".encode("utf-8")
                            + b"Cache-Control: no-store\r\n"
                            + b"Content-Length: 0\r\n"
                            + b"Connection: close\r\n\r\n"
                        )
                        await writer.drain()
                        return

                state = web_states.get(token)

                if not state:
                    try:
                        stored_report = await load_web_report(token)
                    except Exception:
                        stored_report = None
                    if stored_report:
                        try:
                            restored_report = Report(**stored_report)
                            state = ScanState(
                                source_message=None,
                                report=restored_report,
                                created_at=time.monotonic(),
                                web_token=token,
                            )
                            web_states[token] = state
                        except Exception:
                            log.exception("Failed to reconstruct Mongo web report")

                if not state:
                    body = """<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Report expired</title></head>
<body style="font-family:system-ui;padding:32px">
<h2>🔎 File report expired</h2>
<p>This report is no longer stored. Send the Telegram file to the bot again to create a new report.</p>
</body></html>""".encode("utf-8")
                    head = b"Content-Type: text/html; charset=utf-8\r\n"
                    code = b"404 Not Found"
                else:
                    body = web_page(state.report, token)
                    head = b"Content-Type: text/html; charset=utf-8\r\n"
                    code = b"200 OK"

            elif path == "/" or path == "":
                report_token = None
                for part in query.split("&"):
                    if part.startswith("report="):
                        report_token = part.split("=", 1)[1].strip()
                        break
                body = home_page(report_token=report_token)
                head = b"Content-Type: text/html; charset=utf-8\r\n"
                code = b"200 OK"

            else:
                body = b"Not found"
                head = b"Content-Type: text/plain; charset=utf-8\r\n"
                code = b"404 Not Found"

            if web_bytes_sent + len(body) > WEB_EGRESS_GUARD_BYTES:
                body = (
                    "Web response guard reached its 4 GB app budget. "
                    "Please use the Telegram report again later."
                ).encode("utf-8")
                head = b"Content-Type: text/plain; charset=utf-8\r\n"
                code = b"503 Service Unavailable"
            web_bytes_sent += len(body)
            writer.write(
                b"HTTP/1.1 " + code + b"\r\n" + head
                + f"Content-Length: {len(body)}\r\n".encode("ascii")
                + b"Cache-Control: no-store\r\n"
                + b"Connection: close\r\n\r\n"
                + body
            )
            await writer.drain()

        except Exception:
            log.exception("Health/web request failed")
        finally:
            writer.close()
            with suppress(Exception):
                await writer.wait_closed()

    server = await asyncio.start_server(
        handler,
        "0.0.0.0",
        port,
    )
    log.info("Health/web endpoint listening on 0.0.0.0:%s", port)
    return server


async def main():
    clone_monitor_task: asyncio.Task | None = None
    group_onboarding_task: asyncio.Task | None = None
    _bind_bot_handlers(bot, BOT_USERNAME, include_clone=True)
    health = await health_server()

    try:
        while True:
            try:
                await bot.start(bot_token=BOT_TOKEN)
                break
            except errors.FloodWaitError as exc:
                wait_seconds = max(1, int(exc.seconds) + 5)
                log.warning(
                    "Telegram authorization rate-limited; waiting %s seconds before retry",
                    wait_seconds,
                )
                await asyncio.sleep(wait_seconds)

        try:
            mongo_ok = await ensure_mongodb()
            log.info(
                "MongoDB startup check | configured=%s | connected=%s",
                mongodb_is_configured(),
                mongo_ok,
            )
            if mongo_ok:
                from mongo_store import purge_expired_web_reports
                await purge_expired_web_reports()
        except Exception:
            mongo_ok = False
            log.exception("MongoDB startup check failed")

        try:
            await _set_bot_commands(bot, include_clone=True)
        except Exception:
            log.exception("Failed to set command menu for main bot")

        restored = 0

        try:
            from mongo_store import load_clone_requests, mark_clone_removed
            saved_clones = await load_clone_requests()
        except Exception:
            saved_clones = []
            log.exception("Failed to load saved clone configurations")

        restored = 0
        per_user_restored: dict[int, int] = {}
        for saved in saved_clones:
            try:
                uid = int(saved["user_id"])
                clone_id = saved.get("clone_id")
                if per_user_restored.get(uid, 0) >= 2:
                    if clone_id is not None:
                        await mark_clone_removed(uid, int(clone_id))
                    continue

                if clone_id is not None:
                    _ensure_clone_stats(int(clone_id), saved)

                await _start_clone_bot(
                    saved["token"],
                    uid,
                    saved.get("owner_name"),
                )
                per_user_restored[uid] = per_user_restored.get(uid, 0) + 1
                restored += 1
            except (errors.UnauthorizedError, errors.AuthKeyUnregisteredError, errors.UserDeactivatedError):
                try:
                    await mark_clone_removed(
                        int(saved.get("user_id") or 0),
                        int(saved.get("clone_id") or 0),
                    )
                except Exception:
                    log.exception("Failed to remove revoked clone record")
                log.warning(
                    "Removed revoked/deactivated clone | clone_id=%s",
                    saved.get("clone_id"),
                )
            except Exception:
                log.exception(
                    "Failed to restore clone bot id=%s",
                    saved.get("clone_id"),
                )

        me = await bot.get_me()
        username = getattr(me, "username", None) or BOT_USERNAME.lstrip("@") or "AniToon_1Bot"

        clone_monitor_task = asyncio.create_task(monitor_clone_bots())
        group_onboarding_task = asyncio.create_task(_group_onboarding_loop())

        log.info(
            "Telegram bot online as @%s | private_only=%s | clones=%s | concurrency=%s | scan_timeout=%ss | web=%s",
            username,
            FILE_CHECKER_PRIVATE_ONLY,
            restored,
            MAX_CONCURRENT_CHECKS,
            SCAN_TIMEOUT_SECONDS,
            PUBLIC_WEB_URL,
        )

        await bot.run_until_disconnected()

    finally:
        if group_onboarding_task is not None:
            group_onboarding_task.cancel()
            with suppress(asyncio.CancelledError):
                await group_onboarding_task
        if clone_monitor_task is not None:
            clone_monitor_task.cancel()
            with suppress(asyncio.CancelledError):
                await clone_monitor_task
        for client in list(clone_clients.values()):
            with suppress(Exception):
                await client.disconnect()
        clone_clients.clear()
        clone_owners.clear()
        clone_usernames.clear()
        clone_owner_names.clear()
        clone_client_ids.clear()
        clone_stats.clear()
        active_scan_clients.clear()

        health.close()
        await health.wait_closed()
        with suppress(Exception):
            await bot.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
