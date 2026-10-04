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
from datetime import datetime, timezone
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
    mongodb_is_connected,
    record_clone_request,
    user_scan_summary,
    record_scan,
    record_user,
    save_web_report,
    update_clone_stats,
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


scan_states: dict[tuple[int, int], ScanState] = {}
web_states: dict[str, ScanState] = {}
pending_scans: dict[str, PendingScan] = {}
active_scans: dict[str, asyncio.Task] = {}
active_scan_users: dict[str, int | None] = {}
clone_setup_pending: dict[int, float] = {}
last_scan_by_user: dict[int, float] = {}
CLONE_SETUP_TTL_SECONDS = 5 * 60

def add_to_group_url(bot_username: str = BOT_USERNAME) -> str:
    permissions = GROUP_ADMIN_PERMISSIONS
    return f"https://t.me/{bot_username}?startgroup&admin={permissions}"


def safe_filename(message: Any) -> str:
    name = getattr(getattr(message, "file", None), "name", None)
    return str(name or "telegram_file")


def metadata_button(token: str):
    return [[Button.inline("📥 Download Metadata", f"scan:{token}".encode("ascii"))]]


def cancel_button(token: str):
    return [[Button.inline("❌ Cancel Scan", f"cancel:{token}".encode("ascii"))]]


def clone_buttons():
    return [
        [Button.inline("🔐 Enter Clone Token", b"clone:token")],
        [Button.url("🤖 Open @BotFather", "https://t.me/BotFather")],
        [Button.inline("⬅️ Home", b"home:back")],
    ]


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
    "✨ Choose a feature below."
)

HELP_TEXT = (
    "📖 <b>How to Use AniToon</b>\n\n"
    "1️⃣ Send a Telegram <b>video or document</b> to the bot.\n"
    "2️⃣ Press <b>📥 Download Metadata</b>.\n"
    "3️⃣ Wait for the metadata scan to finish.\n"
    "4️⃣ Press <b>🌐 Open File Info</b> for the full web report.\n\n"
    "🤖 <b>Clone Bots</b>\n"
    "Use <code>/clone</code> or <b>🧬 Create Clone</b>, then send the BotFather token. "
    "Each account can manage up to <b>2</b> clones.\n\n"
    "📋 <b>Commands</b>\n"
    "/start — Open Home\n"
    "/help — Open this guide\n"
    "/stats — View your 7-day scan stats\n"
    "/status — View current bot/queue status\n"
    "/privacy — View the data handling policy\n"
    "/about — About AniToon\n"
    "/addtogroup — Add AniToon to a group\n"
    "/clone — Create/connect a clone bot\n"
    "/clones — View and manage your clones\n"
    "/myclones — Same as /clones\n"
    "/cancel — Cancel your running scan"
)

ABOUT_TEXT = (
    "ℹ️ <b>About AniToon</b>\n\n"
    "AniToon scans Telegram media without intentionally downloading complete large files.\n"
    "It uses bounded range reads to inspect container, video, audio and subtitle metadata."
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
):
    buttons = [
        [Button.inline("🔎 Scan Files", b"home:scan")],
        [Button.inline("📊 My Stats", b"home:stats")],
        [Button.inline("💚 Bot Status", b"home:status")],
        [Button.inline("🔐 Privacy", b"home:privacy")],
        [Button.inline("📖 Help", b"home:help")],
        [Button.inline("ℹ️ About", b"home:about")],
        [Button.url("➕ Add Me to Your Group", add_to_group_url(bot_username))],
    ]
    if include_clone:
        buttons.insert(1, [Button.inline("🤖 My Clones", b"home:clones")])
        buttons.insert(2, [Button.inline("🧬 Create Clone", b"home:clone")])
    if include_clone and _owner_allowed(user_id):
        buttons.insert(-1, [Button.inline("👑 Owner Dashboard", b"owner:dashboard")])
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
        return "Unknown"
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
            else record.get("status", "offline")
        )

    records.sort(
        key=lambda item: item.get("created_at") or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    return records


def _clone_manager_buttons(records: list[dict[str, Any]]) -> list[list[Any]]:
    buttons: list[list[Any]] = []
    for record in records[:20]:
        clone_id = int(record["clone_id"])
        username = str(record.get("clone_username") or "").strip().lstrip("@")
        if username:
            buttons.append([Button.url(f"🤖 @{username}", f"https://t.me/{username}")])
        buttons.append([
            Button.inline("📊 Stats", f"clone:stats:{clone_id}".encode("ascii")),
            Button.inline("🗑 Remove", f"clone:remove:{clone_id}".encode("ascii")),
        ])
    buttons.append([Button.inline("➕ Create Clone", b"home:clone")])
    buttons.append([Button.inline("⬅️ Home", b"home:back")])
    return buttons


async def render_clone_list(event, user_id: int, *, edit: bool = True) -> None:
    records = await _user_clone_records(int(user_id))
    if not records:
        text = "🤖 <b>My Clone Bots</b>\n\nYou have not created a clone bot yet."
        buttons = [
            [Button.inline("➕ Create Clone", b"home:clone")],
            [Button.inline("⬅️ Home", b"home:back")],
        ]
    else:
        lines = ["🤖 <b>My Clone Bots</b>", ""]
        for index, record in enumerate(records[:20], 1):
            username = str(record.get("clone_username") or "Unnamed").lstrip("@")
            status = "🟢 Online" if record.get("status") == "online" else "🔴 Offline"
            lines.append(
                f"{index}. <b>@{html.escape(username)}</b> — {status}  "
                f"📊 {int(record.get('scans_started', 0) or 0)} scans"
            )
        if len(records) > 20:
            lines.append(f"\n…and {len(records) - 20} more clone(s).")
        text = "\n".join(lines)
        buttons = _clone_manager_buttons(records)

    if edit:
        await event.edit(text, parse_mode="html", buttons=buttons)
    else:
        await event.reply(text, parse_mode="html", buttons=buttons)


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
    if not summary.get("available"):
        text = ("👑 <b>Owner Dashboard</b>\n\n"
                "⚠️ MongoDB is not connected.\n"
                "Set <code>MONGODB_URI</code> in Render to keep the last 7 days of user and scan history.")
        buttons = [[Button.inline("🔄 Refresh", b"owner:dashboard")], [Button.inline("⬅️ Home", b"home:back")]]
    else:
        text = ("👑 <b>Owner Dashboard</b>\n\n"
                "📅 <b>Last 7 Days</b>\n\n"
                f"👥 Users active in last 7 days: <b>{summary['total_users']}</b>\n"
                f"📁 Scan files: <b>{summary['total_scans']}</b>\n"
                f"✅ Completed: <b>{summary['completed']}</b>\n"
                f"❌ Failed: <b>{summary['failed']}</b>\n"
                f"🛑 Cancelled: <b>{summary['cancelled']}</b>")
        buttons = [
            [Button.inline("👥 Users & Scan Files", b"owner:users:0")],
            [Button.inline("🤖 Active Clone Bots", b"owner:clones")],
            [Button.inline("🔄 Refresh", b"owner:dashboard")],
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
    page_size = 10
    max_page = max(0, (len(users) - 1) // page_size)
    page = max(0, min(int(page), max_page))
    chunk = users[page * page_size:(page + 1) * page_size]
    if not users:
        await event.edit("👥 <b>Users — Last 7 Days</b>\n\nNo scan activity found.", parse_mode="html", buttons=[[Button.inline("👑 Dashboard", b"owner:dashboard")]])
        return
    lines = ["👥 <b>Users — Last 7 Days</b>", ""]
    buttons = []
    for item in chunk:
        name = _owner_user_label(item)
        lines.append(f"👤 <b>{html.escape(name)}</b> — 📁 {int(item.get('scans', 0) or 0)} • ID {int(item['user_id'])}")
        buttons.append([Button.inline(f"📁 {name[:28]}", f"owner:user:{int(item['user_id'])}".encode("ascii"))])
    nav = []
    if page > 0:
        nav.append(Button.inline("◀️ Previous", f"owner:users:{page-1}".encode("ascii")))
    if page < max_page:
        nav.append(Button.inline("Next ▶️", f"owner:users:{page+1}".encode("ascii")))
    if nav:
        buttons.append(nav)
    buttons.append([Button.inline("👑 Dashboard", b"owner:dashboard")])
    await event.edit("\n".join(lines) + f"\n\nPage {page + 1}/{max_page + 1}", parse_mode="html", buttons=buttons)

async def render_owner_user_scans(
    event,
    owner_id: int,
    target_user_id: int,
    page: int = 0,
) -> None:
    if not _owner_allowed(owner_id):
        await event.answer("Owner access only.", alert=True)
        return

    page_size = 15
    page = max(0, int(page))
    records = await owner_user_scans(
        int(target_user_id),
        7,
        page_size,
        page * page_size,
    )

    if not records and page == 0:
        await event.edit(
            f"📁 <b>User {int(target_user_id)} — Scan Files</b>\n\n"
            "No scans in the last 7 days.",
            parse_mode="html",
            buttons=[[Button.inline("⬅️ Users", b"owner:users:0")]],
        )
        return

    lines = [
        f"📁 <b>User {int(target_user_id)} — Scan Files</b>",
        "📅 Last 7 Days",
        "",
    ]
    for index, item in enumerate(records, page * page_size + 1):
        filename = str(item.get("filename") or "telegram_file")
        status = str(item.get("status") or "unknown")
        when = item.get("created_at")
        when_text = (
            when.strftime("%d %b %H:%M UTC")
            if isinstance(when, datetime)
            else "Unknown"
        )
        source = str(item.get("source_bot") or BOT_USERNAME)
        lines.append(
            f"{index}. <b>{html.escape(filename[:110])}</b>\n"
            f"   {html.escape(status)} • {html.escape(source)} • "
            f"{html.escape(when_text)}"
        )

    buttons = []
    nav = []
    if page > 0:
        nav.append(
            Button.inline(
                "◀️ Previous",
                f"owner:user:{int(target_user_id)}:{page - 1}".encode("ascii"),
            )
        )
    if len(records) == page_size:
        nav.append(
            Button.inline(
                "Next ▶️",
                f"owner:user:{int(target_user_id)}:{page + 1}".encode("ascii"),
            )
        )
    if nav:
        buttons.append(nav)
    buttons.append([Button.inline("⬅️ Users", b"owner:users:0")])
    buttons.append([Button.inline("👑 Dashboard", b"owner:dashboard")])

    await event.edit(
        "\n".join(lines) + f"\n\nPage {page + 1}",
        parse_mode="html",
        buttons=buttons,
    )


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
            f"🤖 <b>@{html.escape(username or 'unknown')}</b> — {status}\n"
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
            budget=int(os.getenv("FILE_DEEP_PROBE_BYTES", "4194304")),
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
            buttons=home_buttons(bot_username, include_clone=include_clone),
        )
        raise

    except ProbeBudgetExceeded:
        await persist_outcome("failed")
        checks_failed += 1
        await edit_status(
            status_message,
            "🛑 <b>Safe scan limit reached.</b>\n\n"
            "The player engine stopped before downloading the complete file.",
            buttons=home_buttons(bot_username, include_clone=include_clone),
        )

    except ProbeCancelled:
        outcome = "cancelled"
        await persist_outcome("cancelled")
        checks_failed += 1
        await edit_status(
            status_message,
            "❌ <b>Metadata scan cancelled.</b>",
            buttons=home_buttons(bot_username, include_clone=include_clone),
        )

    except asyncio.TimeoutError:
        await persist_outcome("failed")
        checks_failed += 1
        await edit_status(
            status_message,
            "⏰ <b>Metadata scan reached the 5-minute limit.</b>",
            buttons=home_buttons(bot_username, include_clone=include_clone),
        )

    except errors.FloodWaitError as exc:
        await persist_outcome("failed")
        checks_failed += 1
        await edit_status(
            status_message,
            f"⏳ Telegram temporarily rate-limited this scan for {int(exc.seconds)} seconds.",
            buttons=home_buttons(bot_username, include_clone=include_clone),
        )

    except Exception as exc:
        await persist_outcome("failed")
        checks_failed += 1
        log.exception("File metadata scan failed for %s", filename)
        await edit_status(
            status_message,
            "❌ <b>Metadata scan failed.</b>\n\n"
            f"<code>{html.escape(type(exc).__name__)}</code>",
            buttons=home_buttons(bot_username, include_clone=include_clone),
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
    )

    await event.reply(
        "📥 <b>Download Metadata</b>",
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


async def _set_bot_commands(client: TelegramClient, *, include_clone: bool) -> None:
    commands = [
        types.BotCommand(command="start", description="Open Home"),
        types.BotCommand(command="help", description="How to use AniToon"),
        types.BotCommand(command="stats", description="View your 7-day stats"),
        types.BotCommand(command="status", description="View bot and queue status"),
        types.BotCommand(command="privacy", description="View data handling"),
        types.BotCommand(command="about", description="About AniToons"),
        types.BotCommand(command="addtogroup", description="Add the bot to a group"),
        types.BotCommand(command="clones", description="View your clone bots"),
        types.BotCommand(command="myclones", description="View your clone bots"),
        types.BotCommand(command="cancel", description="Cancel your scan"),
    ]
    if include_clone:
        commands.insert(8, types.BotCommand(command="clone", description="Create a clone bot"))

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

    client.add_event_handler(on_message, events.NewMessage(incoming=True))
    client.add_event_handler(on_callback, events.CallbackQuery)


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
                username = clone_usernames.get(int(clone_id), "unknown")
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
                            buttons=[[Button.inline("🤖 My Clones", b"home:clones")]],
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
    if user_id is not None:
        clone_setup_pending[int(user_id)] = time.monotonic()

    await event.reply(
        "🔐 <b>Send your BotFather token</b>\n\n"
        "Paste it in your next message.\n"
        "⚠️ Keep your token private.\n"
        "🗑️ The token message will be deleted after processing.",
        parse_mode="html",
        buttons=[[Button.inline("⬅️ Cancel", b"clone:cancel")]],
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
            buttons=clone_buttons(),
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
            "⚠️ <b>You already have 2 connected clone bots.</b>\n\n"
            "Remove one before creating another.",
            parse_mode="html",
            buttons=[
                [Button.inline("🤖 My Clones", b"home:clones")],
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
            buttons=clone_buttons(),
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
            [Button.inline("🤖 My Clones", b"home:clones")],
            [Button.inline("🧬 Create Another Clone", b"home:clone")],
            [Button.inline("⬅️ Home", b"home:back")],
        ],
    )
    return True


async def render_user_stats(event, user_id: int, *, edit: bool = True) -> None:
    summary = await user_scan_summary(int(user_id), 7)
    if not summary.get("available"):
        text = (
            "📊 <b>My Stats</b>\n\n"
            "MongoDB history is currently unavailable.\n"
            "Your live scans are still protected by the queue."
        )
    else:
        text = (
            "📊 <b>My Stats — Last 7 Days</b>\n\n"
            f"📁 Total scans: <b>{summary['scans']}</b>\n"
            f"✅ Completed: <b>{summary['completed']}</b>\n"
            f"❌ Failed: <b>{summary['failed']}</b>\n"
            f"🛑 Cancelled: <b>{summary['cancelled']}</b>"
        )
    if edit:
        await event.edit(text, parse_mode="html", buttons=back_buttons())
    else:
        await event.reply(text, parse_mode="html", buttons=back_buttons())


async def render_public_status(event, *, edit: bool = True) -> None:
    resources = runtime_resource_stats()
    mongo = "🟢 Connected" if mongodb_is_connected() else "🔴 Not connected"
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


async def handle_new_message(
    event,
    *,
    client: Any = bot,
    bot_username: str = BOT_USERNAME,
    include_clone: bool = True,
):
    text = (event.raw_text or "").strip()
    command = text.split(maxsplit=1)[0].split("@", 1)[0].lower() if text else ""

    if not include_clone:
        await bump_clone_stat(client, "messages_received")

    if include_clone and await handle_clone_token_message(event):
        return

    home_text = HOME_TEXT if include_clone else (
        "⛩ <b>AniToon Clone Bot</b> ⛩\n\n"
        "🔎 Scan Telegram media files for detailed metadata.\n"
        "🌐 View complete file information in your browser.\n\n"
        "Choose an option below."
    )
    help_text = HELP_TEXT if include_clone else HELP_TEXT.replace(
        "/clone — Start clone-bot setup with a BotFather token\n", ""
    )

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
        await event.reply(ABOUT_TEXT, parse_mode="html", buttons=back_buttons())
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

    sender_id = getattr(getattr(event, "sender", None), "id", None)
    if sender_id is not None:
        now = time.monotonic()
        last = last_scan_by_user.get(int(sender_id), 0.0)
        if now - last < SCAN_COOLDOWN_SECONDS:
            remaining = max(1, int(SCAN_COOLDOWN_SECONDS - (now - last)))
            await event.reply(f"⏳ Please wait {remaining}s before starting another scan.")
            return

        last_scan_by_user[int(sender_id)] = now

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
                [Button.inline("🤖 My Clones", b"home:clones")],
                [Button.inline("➕ Create Clone", b"home:clone")],
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
        text = HELP_TEXT if include_clone else HELP_TEXT.replace(
            "/clone — Start clone-bot setup with a BotFather token\n", ""
        )
        await event.edit(
            text,
            parse_mode="html",
            buttons=help_buttons(),
        )
        return

    if data == "home:scan":
        await event.answer()
        await event.edit(
            "🔎 <b>Scan Files</b>\n\n"
            "Send a Telegram video or document in this chat.\n"
            "The scan starts only after you press <b>📥 Download Metadata</b>.\n\n"
            "🛡️ Large files are inspected with bounded byte-range reads.",
            parse_mode="html",
            buttons=scan_page_buttons(),
        )
        return

    if data == "home:clone":
        if include_clone:
            await event.answer()
            await begin_clone_setup(event)
        else:
            await event.answer(
                "Clone creation is available from the main AniToon bot.",
                alert=True,
            )
        return

    if data == "clone:token":
        await event.answer()
        if include_clone:
            await begin_clone_setup(event)
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
            ),
        )
        return

    if data == "home:about":
        await event.answer()
        await event.edit(
            ABOUT_TEXT,
            parse_mode="html",
            buttons=back_buttons(),
        )
        return

    if data == "home:back":
        await event.answer()
        home_text = HOME_TEXT if include_clone else (
            "⛩ <b>AniToon Clone Bot</b> ⛩\n\n"
            "🔎 Scan Telegram media files for detailed metadata.\n"
            "🌐 View complete file information in your browser.\n\n"
            "Choose an option below."
        )
        sender = await event.get_sender()
        await event.edit(
            home_text,
            parse_mode="html",
            buttons=home_buttons(
                bot_username,
                include_clone=include_clone,
                user_id=getattr(sender, "id", None),
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
        ("🎬", "Movies Channel", "https://t.me/+KEz_Up14hfFhOTI1", False),
        ("🍿", "All Animes Channel", "https://t.me/anitoons_ani", False),
        ("🎧", "Dual Content Channel", "https://t.me/ani_engjaphin", True),
        ("📚", "Manga Channel", "https://t.me/mangauniverse_ani", False),
        ("🏴‍☠️", "One Piece All New Episodes", "https://t.me/ani_pocket_monster", False),
        ("⚔️", "Jujutsu Kaisen Channel", "https://t.me/jjk_anitoon", False),
        ("🍥", "Naruto Shippuden Channel", "https://t.me/naruto_shippuden_in_telugudub", False),
    ]

    completed = [
        ("🤖", "Doraemon All Movies & Seasons", "https://t.me/ani_seas"),
        ("🌻", "Shin-Chan All Seasons & Movies", "https://t.me/shin_seas"),
        ("⚡", "Beyblade Channel", "https://t.me/Ani_beyblade"),
        ("⚡", "Pokemon All Seasons & Movies", "https://t.me/poketmonster_01"),
    ]

    def card(icon, name, url, stopped=False):
        badge = '<span class="stopped">STOPPED</span>' if stopped else ""
        return f"""
        <a class="channel" href="{html.escape(url)}" target="_blank" rel="noopener noreferrer">
          <span class="icon">{icon}</span>
          <span class="name">{html.escape(name)}</span>
          {badge}
          <span class="arrow">↗</span>
        </a>
        """

    current_html = "".join(card(*item) for item in channels)
    completed_html = "".join(card(*item) for item in completed)

    report_embed = ""
    if report_token:
        report_embed = f"""<section class="section">
    <div class="section-title"><span>🔬 File Metadata</span><span class="line"></span></div>
    <div style="border:1px solid var(--border);border-radius:20px;overflow:hidden;background:var(--panel)">
      <iframe src="/report/{html.escape(report_token)}?embed=1" title="AniToons File Metadata" style="display:block;width:100%;height:1800px;border:0;background:#070b14"></iframe>
    </div>
  </section>"""
    
    document = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#080b16">
<meta name="description" content="AniToon's official channel list and AniToons File Intelligence.">
<title>⛩ AniToon's List ⛩</title>
<style>
:root {{
  color-scheme: dark;
  --bg:#070a12;
  --panel:#0f1422;
  --panel2:#12192a;
  --border:rgba(255,255,255,.09);
  --text:#f6f7fb;
  --muted:#98a3b8;
  --accent:#f5c76a;
  --accent2:#8b5cf6;
  --danger:#ff8a8a;
}}
* {{ box-sizing:border-box; }}
html {{ scroll-behavior:smooth; }}
body {{
  margin:0;
  min-height:100vh;
  background:
    radial-gradient(800px 420px at 50% -10%, rgba(139,92,246,.18), transparent 65%),
    radial-gradient(700px 360px at 100% 20%, rgba(245,199,106,.10), transparent 70%),
    var(--bg);
  color:var(--text);
  font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
}}
.wrap {{ max-width:900px; margin:auto; padding:18px 14px 50px; }}
.hero {{
  position:relative;
  text-align:center;
  padding:28px 18px 22px;
  border:1px solid var(--border);
  border-radius:24px;
  background:linear-gradient(145deg,rgba(19,24,38,.96),rgba(10,14,25,.92));
  box-shadow:0 20px 70px rgba(0,0,0,.35);
  overflow:hidden;
}}
.hero:after {{
  content:"";
  position:absolute; inset:auto 8% -40px;
  height:90px;
  background:radial-gradient(circle,rgba(245,199,106,.12),transparent 70%);
  pointer-events:none;
}}
.kicker {{
  color:var(--accent);
  font-size:12px;
  font-weight:800;
  letter-spacing:.18em;
  text-transform:uppercase;
}}
h1 {{
  margin:8px 0 5px;
  font-size:clamp(26px,6vw,42px);
  line-height:1.08;
}}
.subtitle {{ color:var(--muted); font-size:13px; }}
.quick {{
  display:grid;
  grid-template-columns:repeat(2,minmax(0,1fr));
  gap:10px;
  margin-top:16px;
}}
.quick a {{
  text-decoration:none;
  color:var(--text);
  border:1px solid var(--border);
  border-radius:15px;
  padding:12px 10px;
  background:rgba(255,255,255,.035);
  font-weight:700;
  font-size:12px;
}}
.quick a:hover,.channel:hover {{ transform:translateY(-1px); background:rgba(255,255,255,.06); }}
.section {{ margin-top:16px; }}
.section-title {{
  display:flex; align-items:center; gap:10px;
  padding:14px 2px 10px;
  font-size:17px; font-weight:850;
}}
.line {{
  height:1px; flex:1;
  background:linear-gradient(90deg,rgba(255,255,255,.16),transparent);
}}
.list {{ display:grid; gap:9px; }}
.channel {{
  display:flex;
  align-items:center;
  gap:12px;
  text-decoration:none;
  color:var(--text);
  min-height:58px;
  padding:12px 13px;
  border-radius:16px;
  border:1px solid var(--border);
  background:linear-gradient(135deg,rgba(18,25,42,.92),rgba(12,17,29,.92));
  transition:.15s ease;
}}
.icon {{
  width:36px; height:36px; flex:0 0 auto;
  display:grid; place-items:center;
  border-radius:12px;
  background:rgba(245,199,106,.08);
  border:1px solid rgba(245,199,106,.12);
  font-size:18px;
}}
.name {{ flex:1; min-width:0; font-weight:700; line-height:1.3; }}
.stopped {{
  font-size:9px; font-weight:900; letter-spacing:.08em;
  color:var(--danger);
  border:1px solid rgba(255,138,138,.20);
  background:rgba(255,138,138,.07);
  padding:4px 7px;
  border-radius:999px;
}}
.arrow {{ color:var(--muted); font-size:18px; }}
.info-grid {{
  display:grid;
  grid-template-columns:repeat(2,minmax(0,1fr));
  gap:10px;
}}
.info-card {{
  display:block;
  padding:15px;
  border:1px solid var(--border);
  border-radius:16px;
  color:var(--text);
  text-decoration:none;
  background:var(--panel);
}}
.info-card b {{ display:block; margin-bottom:3px; }}
.info-card span {{ color:var(--muted); font-size:12px; }}
.footer {{
  text-align:center;
  margin-top:24px;
  color:var(--muted);
  font-size:11px;
}}
.footer a {{ color:var(--accent); text-decoration:none; }}
@keyframes reportReveal {{
  from {{ opacity:0; transform:translateY(14px); filter:blur(5px); }}
  to {{ opacity:1; transform:translateY(0); filter:blur(0); }}
}}
.hero-actions {{ display:flex; flex-wrap:wrap; gap:8px; margin-top:14px; }}
.action-btn {{
  appearance:none; border:1px solid var(--border); border-radius:12px;
  background:rgba(148,163,184,.07); color:var(--text); padding:9px 12px;
  font:inherit; font-size:12px; font-weight:750; cursor:pointer;
  transition:transform .18s ease, background-color .18s ease, border-color .18s ease;
}}
.action-btn:hover {{ transform:translateY(-1px); background:rgba(148,163,184,.12); border-color:rgba(125,211,252,.35); }}
.action-btn:focus-visible {{ outline:2px solid var(--accent); outline-offset:2px; }}
.copy-status {{ min-height:18px; color:var(--good); font-size:12px; margin-top:5px; }}
.section {{ animation:reportReveal .65s cubic-bezier(.2,1,.2,1) both; }}
@media (prefers-reduced-motion: reduce) {{
  *, *::before, *::after {{
    scroll-behavior:auto !important;
    animation:none !important;
    transition:none !important;
  }}
}}
@media print {{
  body::before, body::after {{ display:none !important; }}
  .site-nav, .hero-actions, .copy-status {{ display:none !important; }}
  .section, .hero {{ box-shadow:none !important; break-inside:avoid; }}
}}
@media(max-width:650px) {{
  .quick,.info-grid {{ grid-template-columns:1fr; }}
  .wrap {{ padding-left:10px; padding-right:10px; }}
}}
</style>
</head>
<body>
<div class="wrap">
  <header class="hero">
    <div class="kicker">AniToon's</div>
    <h1>⛩ AniToon's List ⛩</h1>
    <div class="subtitle">Official channels, groups and social links</div>
    <div class="quick">
      <a href="/health">💚 Bot Status</a>
      <a href="https://t.me/AniToon_1Bot" target="_blank" rel="noopener noreferrer">🤖 Open Bot</a>
    </div>
  </header>

  {report_embed}

  <section class="section">
    <div class="section-title"><span>📡 Active Channels</span><span class="line"></span></div>
    <div class="list">{current_html}</div>
  </section>

  <section class="section">
    <div class="section-title"><span>✅ Completed Channels of Us</span><span class="line"></span></div>
    <div class="list">{completed_html}</div>
  </section>

  <section class="section">
    <div class="section-title"><span>👥 Community & Support</span><span class="line"></span></div>
    <div class="info-grid">
      <a class="info-card" href="https://t.me/Anitoon_group" target="_blank" rel="noopener noreferrer">
        <b>👉 Main Group Chats</b><span>AniToon's Group ↗</span>
      </a>
      <a class="info-card" href="https://t.me/Anitoon_edit" target="_blank" rel="noopener noreferrer">
        <b>👉 BackUp Channel</b><span>@Anitoon_edit ↗</span>
      </a>
      <a class="info-card" href="https://t.me/Anitoon_edit/155?single" target="_blank" rel="noopener noreferrer">
        <b>👉 Tutorial To Clear Ads</b><span>Watch Video ↗</span>
      </a>
    </div>
  </section>

  <section class="section">
    <div class="section-title"><span>↗️ Follow Us</span><span class="line"></span></div>
    <div class="info-grid">
      <a class="info-card" href="https://www.instagram.com/ani_toon_edits?igsh=Y2syejF5bG1wN3ps" target="_blank" rel="noopener noreferrer">
        <b>Instagram</b><span>@ani_toon_edits ↗</span>
      </a>
      <a class="info-card" href="https://youtube.com/@teluguanitoons-a?si=HMMXIAjTbwgyKSZk" target="_blank" rel="noopener noreferrer">
        <b>YouTube</b><span>Telugu AniToons ↗</span>
      </a>
    </div>
  </section>

  <div class="footer">
    ⛩ AniToon's • <a href="/health">System status</a>
  </div>
</div>

</body>
</html>"""
    return document.encode("utf-8")

def web_page(report: Report, report_token: str | None = None) -> bytes:
    filename = html.escape(report.filename)
    generated = datetime.now(timezone.utc)
    generated_text = generated.strftime("%Y-%m-%d %H:%M UTC")

    audio = report.audio.get("tracks", [])
    video = report.video.get("tracks", [])
    subtitles = report.subtitles or []

    def esc(value: Any) -> str:
        return html.escape(str(value))

    def track_cards(items: list[dict[str, Any]], kind: str) -> str:
        if not items:
            return (
                '<div class="empty">No confirmed '
                + esc(kind)
                + ' track was exposed by the player engine within the bounded probe.</div>'
            )

        cards = []
        for index, track in enumerate(items, 1):
            name = track.get("name") or track.get("display_name") or "Unnamed track"
            language = track.get("language_name") or track.get("language") or "Unknown"
            codec = track.get("codec_name") or track.get("codec") or "Unknown"

            details = [
                ("Language", language),
                ("Codec", codec),
            ]

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
                ])
            else:
                details.append(("Format", track.get("subtitle_format") or codec))

            for label, value in details:
                if value:
                    pass
                else:
                    continue
                details_html = ""

            rows = "".join(
                f'<div class="kv"><span>{esc(label)}</span><strong>{esc(value)}</strong></div>'
                for label, value in details
                if value
            )

            flags = []
            if track.get("default") == "yes":
                flags.append("DEFAULT")
            if track.get("original") == "yes":
                flags.append("ORIGINAL")
            if track.get("commentary") == "yes":
                flags.append("COMMENTARY")
            if track.get("forced") == "yes":
                flags.append("FORCED")
            if track.get("hearing_impaired") == "yes":
                flags.append("HI")
            if track.get("visual_impaired") == "yes":
                flags.append("VI")

            badges = "".join(f'<span class="badge">{esc(flag)}</span>' for flag in flags)
            source = esc(track.get("name_source", "player metadata"))

            cards.append(
                f"""
                <article class="track">
                  <div class="track-top">
                    <div class="index">{index:02d}</div>
                    <div class="track-main">
                      <h3>{esc(name)}</h3>
                      <div class="subline">{esc(kind)} • {esc(source)}</div>
                    </div>
                    <div class="badges">{badges}</div>
                  </div>
                  <div class="grid">{rows}</div>
                </article>
                """
            )
        return "".join(cards)

    runtime = report.container.get("runtime") or "Unknown"
    container_name = report.detected or "Unknown"
    mime = report.mime or "Unknown"
    sampled = f"{report.sampled / 1024 / 1024:.2f} MiB"
    wallpaper_seed = html.escape(report_token or secrets.token_urlsafe(10))
    wallpaper_url = f"https://picsum.photos/seed/{wallpaper_seed}/1920/1080"
    audio_names = [str(t.get("name")) for t in audio if t.get("name")]
    subtitle_names = [str(t.get("name")) for t in subtitles if t.get("name")]

    document = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#0b1020">
<title>AniToons File Intelligence — {filename}</title>
<style>
:root {{
  color-scheme: dark;
  --bg: #070b14;
  --panel: rgba(17,24,39,.88);
  --border: rgba(148,163,184,.18);
  --muted: #94a3b8;
  --text: #f8fafc;
  --accent: #7dd3fc;
  --good: #86efac;
}}
* {{ box-sizing: border-box; }}
body {{
  margin: 0;
  min-height: 100vh;
  position: relative;
  isolation: isolate;
  background: var(--bg);
  color: var(--text);
  font: 14px/1.6 system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
}}
body::before {{
  content: "";
  position: fixed;
  inset: 0;
  z-index: -2;
  background:
    linear-gradient(180deg, rgba(4,8,18,.72), rgba(4,8,18,.92)),
    url("{wallpaper_url}") center/cover no-repeat;
  filter: saturate(1.08) contrast(1.03);
  transform: scale(1.03);
}}
body::after {{
  content: "";
  position: fixed;
  inset: -10%;
  z-index: -1;
  background:
    radial-gradient(900px 420px at 10% 0%, rgba(125,211,252,.12), transparent 65%),
    radial-gradient(850px 420px at 100% 0%, rgba(168,85,247,.12), transparent 65%);
  pointer-events: none;
  animation: glowDrift 18s ease-in-out infinite alternate;
}}
@keyframes glowDrift {{
  from {{ transform:translate3d(-1%,0,0) scale(1); opacity:.88; }}
  to {{ transform:translate3d(1%,1%,0) scale(1.03); opacity:1; }}
}}
@media (prefers-reduced-motion: reduce) {{
  body::after {{ animation:none !important; }}
}}
.wrap {{ max-width: 1080px; margin:auto; padding:20px 14px 50px; }}
.hero {{
  padding:24px;
  border:1px solid var(--border);
  border-radius:24px;
  background:linear-gradient(135deg,rgba(15,23,42,.78),rgba(17,24,39,.66));
  box-shadow:0 20px 60px rgba(0,0,0,.26);
}}
.logo {{ font-size:13px; letter-spacing:.12em; text-transform:uppercase; color:var(--accent); font-weight:800; }}
h1 {{ margin:8px 0 6px; font-size:clamp(22px,4vw,34px); line-height:1.2; }}
.file {{ color:#cbd5e1; overflow-wrap:anywhere; }}
.meta {{ margin-top:10px; color:var(--muted); font-size:12px; display:flex; gap:10px; flex-wrap:wrap; }}
.pill {{
  display:inline-flex; align-items:center; gap:7px;
  padding:7px 10px; border-radius:999px;
  background:rgba(148,163,184,.08); border:1px solid var(--border);
}}
.summary {{
  display:grid; grid-template-columns:repeat(4,minmax(0,1fr));
  gap:10px; margin-top:18px;
}}
.stat {{
  padding:15px; border:1px solid var(--border); border-radius:16px;
  background:rgba(2,6,23,.28);
}}
.stat b {{ display:block; font-size:22px; margin-bottom:2px; }}
.stat span {{ color:var(--muted); font-size:12px; }}
.section {{
  margin-top:16px; border:1px solid var(--border); border-radius:20px;
  background:var(--panel); overflow:hidden;
}}
.section-head {{
  padding:16px 18px; display:flex; justify-content:space-between; align-items:center; gap:12px;
  border-bottom:1px solid var(--border);
}}
.section-head h2 {{ margin:0; font-size:18px; }}
.section-body {{ padding:14px; }}
.track {{
  padding:16px; border:1px solid var(--border); border-radius:16px;
  background:rgba(2,6,23,.22); margin-bottom:10px;
}}
.track:last-child {{ margin-bottom:0; }}
.track-top {{ display:flex; gap:12px; align-items:flex-start; }}
.index {{
  width:38px; height:38px; display:grid; place-items:center; flex:0 0 auto;
  border-radius:12px; background:rgba(125,211,252,.10); color:var(--accent); font-weight:800;
}}
.track-main {{ min-width:0; flex:1; }}
.track-main h3 {{ margin:0; font-size:16px; overflow-wrap:anywhere; }}
.subline {{ color:var(--muted); font-size:12px; margin-top:2px; }}
.badges {{ display:flex; gap:5px; flex-wrap:wrap; justify-content:flex-end; }}
.badge {{
  padding:3px 7px; border-radius:999px; background:rgba(134,239,172,.09);
  border:1px solid rgba(134,239,172,.18); color:var(--good); font-size:10px; font-weight:800;
}}
.grid {{
  margin-top:13px; display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:8px;
}}
.kv {{
  display:flex; justify-content:space-between; gap:14px;
  padding:8px 10px; border-radius:10px; background:rgba(148,163,184,.05);
}}
.kv span {{ color:var(--muted); }}
.kv strong {{ text-align:right; overflow-wrap:anywhere; }}
.empty {{ color:var(--muted); padding:10px; }}
.note {{
  margin-top:16px; padding:14px 16px; border:1px solid var(--border); border-radius:16px;
  background:rgba(15,23,42,.72); color:#cbd5e1;
}}
.countdown {{ color:var(--accent); font-weight:800; }}
@keyframes reportReveal {{
  from {{ opacity:0; transform:translateY(14px); filter:blur(5px); }}
  to {{ opacity:1; transform:translateY(0); filter:blur(0); }}
}}
.section {{ animation:reportReveal .65s cubic-bezier(.2,1,.2,1) both; }}
.section:nth-of-type(2) {{ animation-delay:.08s; }}
.section:nth-of-type(3) {{ animation-delay:.14s; }}
.section:nth-of-type(4) {{ animation-delay:.20s; }}
@media(max-width:720px) {{
  .summary {{ grid-template-columns:repeat(2,minmax(0,1fr)); }}
  .grid {{ grid-template-columns:1fr; }}
  .badges {{ justify-content:flex-start; }}
}}
</style>
</head>
<body>
<div class="wrap">
  <nav class="site-nav" style="margin-bottom:14px;display:flex;justify-content:space-between;align-items:center;gap:12px;padding:12px 14px;border:1px solid var(--border);border-radius:14px;background:rgba(15,20,34,.88);">
    <a href="/" style="color:var(--text);text-decoration:none;font-weight:850;">⛩ AniToon's List ⛩</a>
    <a href="/" style="color:var(--accent);text-decoration:none;font-size:12px;font-weight:750;">Home ↗</a>
  </nav>
<div class="wrap">
  <header class="hero">
    <div class="logo">AniToons File Intelligence</div>
    <h1>Media Metadata Report</h1>
    <div class="file">{filename}</div>
    <div class="meta">
      <span class="pill">Generated {generated_text}</span>
      <span class="pill">⏳ Link valid for <span id="countdown" class="countdown">05:00</span></span>
      <span class="pill">🛡️ No complete file download</span>
    </div>
    <div class="hero-actions">
      <button class="action-btn" type="button" onclick="copyFilename()">📋 Copy filename</button>
      <button class="action-btn" type="button" onclick="window.print()">🖨️ Print</button>
    </div>
    <div id="copy-status" class="copy-status" aria-live="polite"></div>

    <div class="summary">
      <div class="stat"><b>{len(video)}</b><span>Video tracks</span></div>
      <div class="stat"><b>{len(audio)}</b><span>Audio tracks</span></div>
      <div class="stat"><b>{len(subtitles)}</b><span>Subtitle tracks</span></div>
      <div class="stat"><b>{esc(runtime)}</b><span>Runtime</span></div>
    </div>
  </header>

  <section class="section">
    <div class="section-head"><h2>🎬 Video</h2><span class="pill">{esc(container_name)}</span></div>
    <div class="section-body">{track_cards(video, "Video")}</div>
  </section>

  <section class="section">
    <div class="section-head">
      <h2>🔊 Audio</h2>
      <span class="pill">{esc(", ".join(audio_names) if audio_names else "Not detected")}</span>
    </div>
    <div class="section-body">{track_cards(audio, "Audio")}</div>
  </section>

  <section class="section">
    <div class="section-head">
      <h2>💬 Subtitles</h2>
      <span class="pill">{esc(", ".join(subtitle_names) if subtitle_names else "Not detected")}</span>
    </div>
    <div class="section-body">{track_cards(subtitles, "Subtitle")}</div>
  </section>

  <section class="section">
    <div class="section-head"><h2>⚙️ Technical</h2></div>
    <div class="section-body">
      <div class="grid">
        <div class="kv"><span>Container</span><strong>{esc(container_name)}</strong></div>
        <div class="kv"><span>MIME</span><strong>{esc(mime)}</strong></div>
        <div class="kv"><span>Runtime</span><strong>{esc(runtime)}</strong></div>
        <div class="kv"><span>Sample read</span><strong>{esc(sampled)}</strong></div>
        <div class="kv"><span>Average bitrate</span><strong>{esc(report.container.get("average_bitrate", "Unknown"))}</strong></div>
        <div class="kv"><span>Probe ranges</span><strong>{len(report.probe_ranges)}</strong></div>
      </div>
    </div>
  </section>

  <div class="note">
    <b>Privacy / bandwidth:</b> this page is a metadata report. The scanner does not create a complete local copy of the Telegram file.
    The browser report token is held in memory and expires after 5 minutes or when the service restarts.
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
      countdown.textContent =
        String(m).padStart(2, "0") + ":" + String(s).padStart(2, "0");
    }}
    if (left > 0) {{
      left -= 1;
      setTimeout(tick, 1000);
    }}
  }};
  tick();

  const status = document.getElementById("copy-status");
  const button = document.querySelector(".action-btn");
  window.copyFilename = () => {{
    if (!navigator.clipboard) {{
      if (status) status.textContent = "Clipboard is not available.";
      return;
    }}
    navigator.clipboard.writeText(FILE_NAME).then(() => {{
      if (status) status.textContent = "Filename copied.";
      setTimeout(() => {{ if (status) status.textContent = ""; }}, 1800);
    }}).catch(() => {{
      if (status) status.textContent = "Could not copy filename.";
    }});
  }};
}})();
</script></body>
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
            log.info("MongoDB startup check | connected=%s", mongo_ok)
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
        username = getattr(me, "username", "unknown")

        clone_monitor_task = asyncio.create_task(monitor_clone_bots())

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
