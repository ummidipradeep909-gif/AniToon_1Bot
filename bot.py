from __future__ import annotations

import asyncio
import base64
import html
import json
import logging
import os
import secrets
import shutil
import tempfile
import time
from contextlib import suppress
from dataclasses import asdict, dataclass
from datetime import datetime, timezone, timedelta
from urllib.parse import urlsplit
from typing import Any

from dotenv import load_dotenv
from telethon import Button, TelegramClient, errors, events, functions, types
from telethon.sessions import MemorySession

from file_inspector import Report, inspect_telegram_message, format_report, format_section
from media_probe import (
    generate_video_previews,
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
# Free-instance safe default: keep compute concurrency deliberately conservative.
MAX_CONCURRENT_CHECKS = max(1, min(int(os.getenv("MAX_CONCURRENT_CHECKS", "4")), 4))
# Fail fast instead of leaving a Free-instance worker hung for minutes.
SCAN_TIMEOUT_SECONDS = max(20, min(int(os.getenv("SCAN_TIMEOUT_SECONDS", "60")), 120))
REPORT_LINK_TTL_SECONDS = 5 * 60
PENDING_SCAN_TTL_SECONDS = 10 * 60
MAX_STORED_RESULTS = 100
PUBLIC_WEB_URL = (
    os.getenv("PUBLIC_WEB_URL", "https://anitoons-1bot-oa44.onrender.com")
    .strip()
    .rstrip("/")
)

# Private Telegram channel used for automatic media archiving.
STORAGE_CHANNEL = os.getenv("STORAGE_CHANNEL", "https://t.me/+TlTvvw02fcViNjM9").strip()
STORAGE_SEND_TIMEOUT = 20
CLONE_BOT_USERNAME = os.getenv("CLONE_BOT_USERNAME", "").strip().lstrip("@")
BOT_USERNAME = os.getenv("BOT_USERNAME", "AniToon_1Bot").strip().lstrip("@")
OWNER_ID = int(os.getenv("OWNER_ID", "0") or "0")
# Clone bots are live in memory only; scan execution is globally queued.
MAX_ACTIVE_SCANS_PER_USER = max(1, min(int(os.getenv("MAX_ACTIVE_SCANS_PER_USER", "1")), 2))
SCAN_COOLDOWN_SECONDS = max(0, min(int(os.getenv("SCAN_COOLDOWN_SECONDS", "2")), 10))
# Render's current Hobby workspace includes 5 GB/month of outbound bandwidth.
# This app deliberately stops heavy work at 80% of that as a safety buffer.
WEB_EGRESS_GUARD_BYTES = 4 * 1024 * 1024 * 1024
WEB_EGRESS_PAUSE_BYTES = int(WEB_EGRESS_GUARD_BYTES * 0.80)
RAM_PAUSE_PCT = 80.0

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
    expires_at: datetime | None = None
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
preview_tasks: set[asyncio.Task] = set()
storage_peers: dict[int, Any] = {}


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

ANIME_WALLPAPER_SVG = "<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 1600 900\" preserveAspectRatio=\"xMidYMid slice\">\n<defs>\n<linearGradient id=\"sky\" x1=\"0\" y1=\"0\" x2=\"0\" y2=\"1\"><stop stop-color=\"#070b25\"/><stop offset=\".52\" stop-color=\"#18266b\"/><stop offset=\"1\" stop-color=\"#31164f\"/></linearGradient>\n<radialGradient id=\"moon\"><stop stop-color=\"#fffde8\"/><stop offset=\".6\" stop-color=\"#bfeaff\"/><stop offset=\"1\" stop-color=\"#7b86ff\" stop-opacity=\"0\"/></radialGradient>\n<linearGradient id=\"mount\" x1=\"0\" y1=\"0\" x2=\"0\" y2=\"1\"><stop stop-color=\"#111a48\"/><stop offset=\"1\" stop-color=\"#060816\"/></linearGradient>\n<linearGradient id=\"water\" x1=\"0\" y1=\"0\" x2=\"0\" y2=\"1\"><stop stop-color=\"#122d63\"/><stop offset=\"1\" stop-color=\"#080b23\"/></linearGradient>\n<filter id=\"glow\"><feGaussianBlur stdDeviation=\"8\"/></filter><filter id=\"soft\"><feGaussianBlur stdDeviation=\"2.5\"/></filter>\n</defs>\n<rect width=\"1600\" height=\"900\" fill=\"url(#sky)\"/><circle cx=\"1210\" cy=\"170\" r=\"155\" fill=\"url(#moon)\" opacity=\".78\"/>\n<g fill=\"#dbe9ff\" opacity=\".9\"><circle cx=\"980\" cy=\"90\" r=\"2\"/><circle cx=\"1060\" cy=\"130\" r=\"2\"/><circle cx=\"1280\" cy=\"80\" r=\"2.5\"/><circle cx=\"1400\" cy=\"160\" r=\"2\"/><circle cx=\"1160\" cy=\"235\" r=\"1.8\"/><circle cx=\"820\" cy=\"150\" r=\"1.5\"/><circle cx=\"690\" cy=\"90\" r=\"2\"/><circle cx=\"1510\" cy=\"95\" r=\"1.7\"/></g>\n<g opacity=\".55\" filter=\"url(#glow)\" fill=\"#7fdfff\"><circle cx=\"1040\" cy=\"180\" r=\"3\"/><circle cx=\"1370\" cy=\"120\" r=\"4\"/><circle cx=\"890\" cy=\"210\" r=\"3\"/></g>\n<path d=\"M500 650L760 390 930 560 1130 300 1540 650Z\" fill=\"url(#mount)\"/><path d=\"M730 420l40-30 30 36 55 45-120-4z\" fill=\"#dce5ff\" opacity=\".34\"/>\n<path d=\"M0 680L220 505 410 615 650 430 820 650 1080 510 1290 650 1450 500 1600 610V900H0Z\" fill=\"#080c25\"/>\n<path d=\"M0 700Q400 660 800 705T1600 690V900H0Z\" fill=\"url(#water)\"/>\n<g opacity=\".22\" fill=\"#9acfff\"><ellipse cx=\"1100\" cy=\"760\" rx=\"360\" ry=\"16\"/><ellipse cx=\"850\" cy=\"820\" rx=\"250\" ry=\"11\"/><ellipse cx=\"1320\" cy=\"850\" rx=\"180\" ry=\"9\"/></g>\n<g stroke=\"#271d50\" stroke-width=\"16\" fill=\"none\" opacity=\".95\"><path d=\"M120 600V410M260 600V410M90 430H290M105 385H275\"/></g>\n<g stroke=\"#ff6cae\" stroke-width=\"10\" stroke-linecap=\"round\" opacity=\".7\" filter=\"url(#soft)\"><path d=\"M0 150Q170 90 360 250T610 130\"/><path d=\"M40 175Q190 120 330 265\"/></g>\n<g fill=\"#ff8ec7\" opacity=\".92\"><circle cx=\"120\" cy=\"150\" r=\"13\"/><circle cx=\"160\" cy=\"120\" r=\"10\"/><circle cx=\"230\" cy=\"170\" r=\"15\"/><circle cx=\"310\" cy=\"215\" r=\"11\"/><circle cx=\"400\" cy=\"200\" r=\"14\"/><circle cx=\"470\" cy=\"155\" r=\"9\"/><circle cx=\"570\" cy=\"190\" r=\"12\"/><circle cx=\"70\" cy=\"215\" r=\"8\"/></g>\n<g fill=\"#ffbfdc\" opacity=\".65\"><circle cx=\"190\" cy=\"250\" r=\"5\"/><circle cx=\"360\" cy=\"110\" r=\"6\"/><circle cx=\"520\" cy=\"260\" r=\"5\"/><circle cx=\"455\" cy=\"285\" r=\"4\"/></g>\n<g transform=\"translate(1310 500)\" fill=\"#050713\" stroke=\"#10163d\" stroke-width=\"3\"><circle cx=\"0\" cy=\"-100\" r=\"43\"/><path d=\"M-78 55Q-72-30 0-48Q72-30 78 55L110 200H-110Z\"/><path d=\"M-54-42L0 5 54-42 82 12 35 55 0 32-35 55-82 12Z\" fill=\"#0a0d21\"/><path d=\"M-63 62Q0 95 63 62\" fill=\"none\" stroke=\"#27336e\" stroke-width=\"9\"/><path d=\"M-90 25Q0 0 90 25\" fill=\"none\" stroke=\"#0d1230\" stroke-width=\"20\"/><path d=\"M-24 92L-62 180M24 92L62 180\" stroke=\"#080a18\" stroke-width=\"32\" stroke-linecap=\"round\"/><path d=\"M-50 192L-82 218M50 192L82 218\" stroke=\"#111631\" stroke-width=\"18\" stroke-linecap=\"round\"/></g>\n<g fill=\"#c9d7ff\" opacity=\".28\"><circle cx=\"1280\" cy=\"610\" r=\"6\"/><circle cx=\"1450\" cy=\"560\" r=\"4\"/><circle cx=\"1160\" cy=\"690\" r=\"5\"/></g>\n<rect width=\"1600\" height=\"900\" fill=\"#050611\" opacity=\".17\"/>\n</svg>"
ANIME_WALLPAPER_DATA_URI = "data:image/svg+xml;base64," + base64.b64encode(
    ANIME_WALLPAPER_SVG.encode("utf-8")
).decode("ascii")

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
    "📋 <b>Commands on clone bots</b>\n"
    "/start — Open Home\n"
    "/help — Open this guide\n"
    "/stats — View your 7-day stats\n"
    "/about — About AniToon\n"
    "/addtogroup — Add the bot to a group\n"
    "/privacy — Privacy information\n"
    "/cancel — Cancel your running scan\n\n"
    "🤖 Clone management stays on the main AniToon bot."
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
    "/resources — Render Free Resource Guard (owner)\n"
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
    expires_at: datetime | None = None,
) -> ScanState:
    token = web_token or secrets.token_urlsafe(18)
    expiry = expires_at or (datetime.now(timezone.utc) + timedelta(seconds=REPORT_LINK_TTL_SECONDS))
    state = ScanState(
        source_message=source_message,
        report=report,
        created_at=time.monotonic(),
        web_token=token,
        expires_at=expiry,
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
    now_mono = time.monotonic()
    now_utc = datetime.now(timezone.utc)
    expired = [
        token for token, state in web_states.items()
        if (
            state.expires_at is not None
            and now_utc >= state.expires_at
        ) or (
            state.expires_at is None
            and now_mono - state.created_at > REPORT_LINK_TTL_SECONDS
        )
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


APP_TMP_DIR = os.path.join(tempfile.gettempdir(), "anitoon")
APP_TMP_LIMIT_BYTES = 32 * 1024 * 1024
APP_TMP_RETENTION_SECONDS = 10 * 60


def cleanup_app_temp() -> tuple[int, int]:
    os.makedirs(APP_TMP_DIR, exist_ok=True)
    now = time.time()
    files: list[tuple[str, int, float]] = []
    removed = 0
    removed_bytes = 0

    for root, _, names in os.walk(APP_TMP_DIR):
        for name in names:
            path = os.path.join(root, name)
            try:
                size = os.path.getsize(path)
                mtime = os.path.getmtime(path)
            except OSError:
                continue
            if now - mtime >= APP_TMP_RETENTION_SECONDS:
                try:
                    os.remove(path)
                except OSError:
                    continue
                removed += 1
                removed_bytes += size
                continue
            files.append((path, size, mtime))

    total = sum(size for _, size, _ in files)
    if total > APP_TMP_LIMIT_BYTES:
        for path, size, _ in sorted(files, key=lambda item: item[2]):
            if total <= APP_TMP_LIMIT_BYTES:
                break
            try:
                os.remove(path)
            except OSError:
                continue
            total -= size
            removed += 1
            removed_bytes += size

    for root, dirs, _ in os.walk(APP_TMP_DIR, topdown=False):
        for name in dirs:
            with suppress(OSError):
                os.rmdir(os.path.join(root, name))

    return removed, removed_bytes


def app_temp_usage_bytes() -> int:
    os.makedirs(APP_TMP_DIR, exist_ok=True)
    total = 0
    for root, _, names in os.walk(APP_TMP_DIR):
        for name in names:
            with suppress(OSError):
                total += os.path.getsize(os.path.join(root, name))
    return total


async def temp_cleanup_loop() -> None:
    while True:
        try:
            cleanup_app_temp()
        except Exception:
            log.exception("Temporary storage cleanup failed")
        await asyncio.sleep(300)


def runtime_resource_stats() -> dict[str, Any]:
    """Return app-owned resource signals; host/root filesystem is not app-controlled."""
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
    tmp_bytes = app_temp_usage_bytes()
    tmp_mb = tmp_bytes / (1024**2)
    tmp_pct = min(100.0, tmp_bytes / APP_TMP_LIMIT_BYTES * 100.0)

    uptime_hours = (datetime.now(timezone.utc) - started_at).total_seconds() / 3600.0
    uptime_pct = min(100.0, uptime_hours / 750.0 * 100.0)
    egress_gb = web_bytes_sent / (1024**3)
    egress_pct = min(100.0, egress_gb / 4.0 * 100.0)

    return {
        "rss_mb": rss_mb,
        "ram_limit_mb": ram_limit_mb,
        "ram_pct": ram_pct,
        "tmp_mb": tmp_mb,
        "tmp_bytes": tmp_bytes,
        "tmp_pct": tmp_pct,
        "tmp_limit_mb": APP_TMP_LIMIT_BYTES / (1024**2),
        "uptime_hours": uptime_hours,
        "uptime_pct": uptime_pct,
        "web_egress_gb": egress_gb,
        "web_egress_pct": egress_pct,
    }

def memory_pressure_high() -> bool:
    return runtime_resource_stats()["ram_pct"] >= RAM_PAUSE_PCT


def resource_guard_reason() -> str | None:
    stats = runtime_resource_stats()
    if stats["ram_pct"] >= RAM_PAUSE_PCT:
        return (
            f"RAM pressure is {stats['ram_pct']:.1f}% (pause threshold {RAM_PAUSE_PCT:.0f}%). "
            f"New heavy scans stay paused until memory falls below {RAM_PAUSE_PCT:.0f}%."
        )
    if web_bytes_sent >= WEB_EGRESS_PAUSE_BYTES:
        return (
            f"Tracked web egress reached {stats['web_egress_gb']:.2f} GB. "
            f"Heavy work is paused before the {WEB_EGRESS_GUARD_BYTES / (1024**3):.0f} GB app guard."
        )
    return None


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
    ram_state = (
        "🟢 Normal"
        if stats["ram_pct"] < 70
        else ("🟡 Watch" if stats["ram_pct"] < RAM_PAUSE_PCT else "🔴 High")
    )
    egress_state = (
        "🟢 Low"
        if stats["web_egress_pct"] < 70
        else ("🟡 Watch" if stats["web_egress_pct"] < 85 else "🔴 High")
    )
    guard = resource_guard_reason()
    guard_line = "✅ Guard is clear — new heavy scans are allowed." if not guard else f"🛑 <b>Guard active:</b> {html.escape(guard)}"

    text = (
        "🖥️ <b>Render Resource Guard</b>\n"
        f"🧠 RAM <b>{stats['rss_mb']:.1f}/512 MB</b> • {stats['ram_pct']:.1f}% {ram_state}\n"
        f"📡 Web egress <b>{stats['web_egress_gb']:.3f}/4 GB</b> • {stats['web_egress_pct']:.1f}% {egress_state}\n"
        f"⏱️ Uptime <b>{stats['uptime_hours']:.2f} h</b> • 📊 allowance {stats['uptime_pct']:.1f}%\n"
        f"🧹 Bot temp <b>{stats['tmp_mb']:.1f}/{stats['tmp_limit_mb']:.0f} MB</b> • {stats['tmp_pct']:.1f}%\n"
        f"🔎 Load <b>{active_processes}/{MAX_CONCURRENT_CHECKS}</b> active • {queued_processes} queued\n"
        f"🛡️ Guard: <b>{'PAUSED' if guard else 'CLEAR'}</b>\n\n"
        "ℹ️ RAM ↑ = workers/media parsing. Web ↑ = browser traffic/previews. "
        "Uptime ↑ = process running. Bot temp is auto-cleaned every 5 min; old files expire after 10 min.\n"
        "⚠️ Render host/root disk usage is not app-owned and cannot be safely cleaned by the bot."
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


class ResourceGuardPause(RuntimeError):
    """Raised when the free-instance protection gate temporarily blocks a new scan."""


status_edit_cache: dict[int, tuple[float, str]] = {}
STATUS_EDIT_MIN_INTERVAL = 0.9

async def edit_status(message, text: str, *, buttons=None) -> None:
    message_id = getattr(message, "id", None)
    now = time.monotonic()
    key = int(message_id) if message_id is not None else id(message)
    previous = status_edit_cache.get(key)
    if previous and previous[1] == text:
        return
    if previous and now - previous[0] < STATUS_EDIT_MIN_INTERVAL:
        return
    status_edit_cache[key] = (now, text)
    with suppress(errors.MessageNotModifiedError, errors.FloodWaitError):
        await message.edit(text, parse_mode="html", buttons=buttons)


def progress_details(line: str) -> tuple[int, str]:
    low = line.lower()

    if "stage 1/4" in low:
        pct = 14
    elif "stage 2/4" in low:
        pct = 38
    elif "stage 3/4" in low:
        pct = 68
    elif "stage 4/4" in low:
        pct = 90
    elif "stage 2/2" in low or "reading available media metadata" in low:
        pct = 76
    elif "starting scan" in low:
        pct = 1
    else:
        pct = 50

    clean = line
    for prefix in ("🧭 ", "🎯 ", "🔎 ", "🧩 "):
        clean = clean.replace(prefix, "")
    if "•" in clean:
        clean = clean.split("•", 1)[1].strip()
    return pct, clean


def status_text(filename: str, line: str, pct: int | None = None) -> str:
    parsed_pct, clean = progress_details(line)
    current_pct = parsed_pct if pct is None else int(pct)
    pct = max(0, min(99, current_pct))
    if pct >= 99:
        pct = 99
    filled = pct // 5
    bar = "▰" * filled + "▱" * (20 - filled)
    return (
        "🔎 <b>SCANNING METADATA</b>\n"
        f"📄 <b>{html.escape(filename)}</b>\n\n"
        f"<code>{bar}</code> <b>{pct}%</b>\n"
        f"⚙️ {html.escape(clean)}"
    )


async def run_scan(
    source_message: Any,
    status_message: Any,
    *,
    scan_token: str,
    client: Any,
) -> Report:
    filename = safe_filename(source_message)

    progress_state = {"pct": 0, "label": "Starting scan…"}

    async def progress(line: str):
        parsed_pct, label = progress_details(line)
        progress_state["pct"] = max(int(progress_state["pct"]), parsed_pct)
        progress_state["label"] = label
        await edit_status(
            status_message,
            status_text(filename, label, progress_state["pct"]),
            buttons=cancel_button(scan_token),
        )

    heartbeat_stop = asyncio.Event()
    async def heartbeat():
        pulse = ("·", "••", "•••")
        index = 0
        while not heartbeat_stop.is_set():
            await asyncio.sleep(2)
            if heartbeat_stop.is_set():
                break

            # Keep the bar visibly moving while the worker is doing bounded
            # network/metadata work. Real parser callbacks can jump it forward.
            current = int(progress_state["pct"])
            if current < 14:
                current = 14

            if current < 76:
                visual_pct = min(35, current + 2)
            else:
                visual_pct = min(96, current + 2)

            progress_state["pct"] = max(current, visual_pct)
            label = progress_state["label"]
            await edit_status(
                status_message,
                status_text(
                    filename,
                    f"⚡ {label} {pulse[index % len(pulse)]}",
                    progress_state["pct"],
                ),
                buttons=cancel_button(scan_token),
            )
            index += 1

    heartbeat_task = asyncio.create_task(heartbeat())
    try:
        async def fast_worker():
            # Telegram I/O remains async and every source read stays inside the bounded probe budget.
            return await inspect_telegram_message(
                client,
                source_message,
                progress=progress,
                deep=False,
            )

        report = await asyncio.wait_for(
            fast_worker(),
            timeout=SCAN_TIMEOUT_SECONDS,
        )
        progress_state["pct"] = max(int(progress_state["pct"]), 96)
        progress_state["label"] = "✅ Worker finished — building report…"
        return report
    finally:
        heartbeat_stop.set()
        heartbeat_task.cancel()
        with suppress(asyncio.CancelledError):
            await heartbeat_task


async def acquire_scan_slot(
    status_message: Any,
    scan_token: str,
    filename: str,
) -> None:
    global active_processes, queued_processes

    guard = resource_guard_reason()
    if guard:
        raise ResourceGuardPause(guard)

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
        guard = resource_guard_reason()
        if guard:
            check_semaphore.release()
            acquired = False
            if queued:
                async with scan_queue_lock:
                    queued_processes = max(0, queued_processes - 1)
            raise ResourceGuardPause(guard)
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


async def _generate_previews_background(
    client: Any,
    source_message: Any,
    scan_token: str,
    state: ScanState,
    expires_at: datetime,
) -> None:
    try:
        previews = await generate_video_previews(client, source_message, scan_token)
        if not previews:
            return
        state.report.previews = previews
        await save_web_report(scan_token, asdict(state.report), expires_at)
        log.info("Web previews ready | token=%s | count=%s", scan_token, len(previews))
    except Exception:
        log.exception("Background web preview generation failed | token=%s", scan_token)


def _schedule_preview_generation(
    client: Any,
    source_message: Any,
    scan_token: str,
    state: ScanState,
    expires_at: datetime,
) -> None:
    task = asyncio.create_task(
        _generate_previews_background(
            client,
            source_message,
            scan_token,
            state,
            expires_at,
        )
    )
    preview_tasks.add(task)
    task.add_done_callback(preview_tasks.discard)


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
            "<code>▰▱▱▱▱▱▱▱▱▱▱▱▱▱▱▱▱▱▱▱</code> <b>1%</b>\n"
            "⚡ Starting fast worker…",
            buttons=cancel_button(scan_token),
        )

        report = await run_scan(
            source_message,
            status_message,
            scan_token=scan_token,
            client=client,
        )

        expires_at = datetime.now(timezone.utc) + timedelta(seconds=REPORT_LINK_TTL_SECONDS)
        state = cache_state(
            status_message,
            source_message,
            report,
            web_token=scan_token,
            expires_at=expires_at,
        )
        if state is None:
            raise RuntimeError("Could not create web report link")

        state.web_token = scan_token
        web_states[scan_token] = state

        try:
            await save_web_report(
                scan_token,
                asdict(report),
                expires_at,
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

        archive_ok = await archive_scanned_file(
            client,
            source_message,
            user_id=user_id,
            bot_username=bot_username,
        )
        resend_ok = False
        if user_id is not None:
            resend_ok = await resend_scanned_file(
                client,
                int(user_id),
                source_message,
                filename=filename,
            )

        final_result_text = compact_scan_result(report)
        result_buttons = web_report_button(
            scan_token,
            bot_username,
            include_clone=include_clone,
        )

        # Completion is sent as a fresh message so the edit throttle can never
        # hide the final result behind the temporary 99% processing message.
        try:
            final_message = await status_message.reply(
                final_result_text
                + ("\n\n📤 <b>Your file copy was sent.</b>" if resend_ok else "\n\n⚠️ <b>File copy could not be sent.</b>")
                + ("\n🗄️ <b>Archived in private storage.</b>" if archive_ok else "\n🗄️ <b>Storage archive unavailable.</b>"),
                parse_mode="html",
                buttons=result_buttons,
            )
            if final_message is not None:
                log.info("Final scan result sent | token=%s", scan_token)
        except Exception:
            log.exception("Failed to send final scan result message | token=%s", scan_token)
            raise

        with suppress(Exception):
            await status_message.delete()

        if report.video.get("tracks"):
            _schedule_preview_generation(
                client,
                source_message,
                scan_token,
                state,
                expires_at,
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

    except ResourceGuardPause as exc:
        await edit_status(
            status_message,
            "🛡️ <b>Free Resource Guard Paused This Scan</b>\n\n"
            f"{html.escape(str(exc))}\n\n"
            "Please try again when the guard returns to normal.",
            buttons=home_buttons(
                bot_username,
                include_clone=include_clone,
                show_privacy=show_privacy,
            ),
        )
        outcome = "paused"

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
        "🔎 Click below to check the file information.\n"
        "🗄️ Scanned files are automatically copied to the private AniToon storage channel.",
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

async def _resolve_storage_peer(client: Any) -> Any | None:
    key = id(client)
    if key in storage_peers:
        return storage_peers[key]
    if not STORAGE_CHANNEL:
        return None
    try:
        peer = await asyncio.wait_for(client.get_entity(STORAGE_CHANNEL), timeout=12)
        storage_peers[key] = peer
        return peer
    except Exception:
        log.warning(
            "Storage channel unavailable | bot=%s | bot must be a member with post permission | %s",
            _bot_id_for_client(client) or "main",
            STORAGE_CHANNEL,
        )
        return None


async def archive_scanned_file(
    client: Any,
    source_message: Any,
    *,
    user_id: int | None,
    bot_username: str,
) -> bool:
    media = getattr(source_message, "media", None)
    if not media:
        return False
    peer = await _resolve_storage_peer(client)
    if peer is None:
        return False

    filename = safe_filename(source_message)
    source_name = str(bot_username).lstrip("@") or "AniToon"
    caption = (
        "📦 <b>AniToon Storage</b>\n"
        f"📄 <code>{html.escape(filename)}</code>\n"
        f"🤖 <b>@{html.escape(source_name)}</b>"
    )
    if user_id is not None:
        caption += f"\n🆔 User ID: <code>{int(user_id)}</code>"

    try:
        await asyncio.wait_for(
            client.send_file(
                peer,
                media,
                caption=caption,
                parse_mode="html",
                allow_cache=True,
            ),
            timeout=STORAGE_SEND_TIMEOUT,
        )
        return True
    except Exception:
        log.warning("Storage archive failed | file=%s", filename, exc_info=True)
        return False


async def resend_scanned_file(
    client: Any,
    destination: Any,
    source_message: Any,
    *,
    filename: str,
) -> bool:
    media = getattr(source_message, "media", None)
    if destination is None or not media:
        return False
    try:
        await asyncio.wait_for(
            client.send_file(
                destination,
                media,
                caption=f"📁 <b>Your scanned file</b>\n<code>{html.escape(filename)}</code>",
                parse_mode="html",
                allow_cache=True,
            ),
            timeout=STORAGE_SEND_TIMEOUT,
        )
        return True
    except Exception:
        log.warning("Resend file failed | file=%s", filename, exc_info=True)
        return False



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
        audience_username = BOT_USERNAME if not clone_id else clone_usernames.get(int(clone_id))
        user_ids = await list_bot_users(int(bot_id), audience_username, 1_000_000)
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
    common = [
        types.BotCommand(command="start", description="Open Home"),
        types.BotCommand(command="help", description="How to use AniToon"),
        types.BotCommand(command="stats", description="View your 7-day stats"),
        types.BotCommand(command="about", description="About AniToon"),
        types.BotCommand(command="addtogroup", description="Add the bot to a group"),
        types.BotCommand(command="privacy", description="Privacy information"),
        types.BotCommand(command="cancel", description="Cancel your scan"),
    ]
    if include_clone:
        commands = common + [
            types.BotCommand(command="clones", description="View your clone bots"),
            types.BotCommand(command="myclones", description="View your clone bots"),
            types.BotCommand(command="clone", description="Create a clone bot"),
            types.BotCommand(command="resources", description="Render resource guard (owner)"),
        ]
    else:
        # Clone bots expose the same user commands; clone-management actions redirect to AniToon.
        commands = common + [
            types.BotCommand(command="clones", description="View your clone bots"),
            types.BotCommand(command="myclones", description="View your clone bots"),
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

    if command in {"/status", "/resources"}:
        sender = await event.get_sender()
        user_id = getattr(sender, "id", None)
        if not _owner_allowed(user_id):
            await event.reply("ℹ️ Render resource details are available to the owner only.")
            return
        await record_user(event)
        if command == "/resources":
            await render_owner_resources(event, int(user_id))
        else:
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
            if token in web_states:
                await event.answer("✅ This scan has already finished. Open the File Info report.", alert=True)
            else:
                await event.answer("ℹ️ This scan already stopped. Send the file again to start a fresh scan.", alert=True)
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
    socials = [
        ("◎", "Instagram", "@AniToonHQ", "https://www.instagram.com/AniToonHQ"),
        ("▶", "YouTube", "AniToon HQ", "https://www.youtube.com/channel/UC5LrPauKQX6PkO8mLd-DxEg"),
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

    social_html = "".join(
        f'<a class="social-channel" href="{html.escape(url)}" target="_blank" rel="noopener noreferrer">'
        f'<span class="social-icon">{icon}</span>'
        f'<span class="social-main"><strong>{html.escape(name)}</strong><small>{html.escape(handle)}</small></span>'
        f'<span class="channel-arrow">↗</span>'
        f'</a>'
        for icon, name, handle, url in socials
    )

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
.anime-wallpaper{{position:fixed;inset:0;z-index:-5;background:url("{ANIME_WALLPAPER_DATA_URI}") center/cover no-repeat;opacity:.34;filter:saturate(1.06) contrast(1.05);transform:scale(1.03);animation:wallpaperZoom 18s ease-in-out infinite alternate;pointer-events:none}}
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
.nav-tools{{display:flex;align-items:center;gap:8px;flex-wrap:wrap;justify-content:flex-end}}
.display-controls{{display:flex;align-items:center;gap:4px;padding:3px;border:1px solid var(--line);border-radius:12px;background:rgba(255,255,255,.035);backdrop-filter:blur(10px)}}
.display-controls button{{border:0;border-radius:8px;padding:6px 8px;background:transparent;color:var(--text);font:800 11px/1 system-ui;cursor:pointer}}
.display-controls button:hover{{background:rgba(255,255,255,.08)}}
.display-controls #zoom-label{{min-width:44px;color:var(--muted)}}
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
.social-channel{{display:flex;align-items:center;gap:12px;min-height:72px;padding:12px 13px;border:1px solid var(--line);border-radius:16px;color:var(--text);text-decoration:none;background:linear-gradient(145deg,rgba(255,255,255,.045),rgba(255,255,255,.012));transition:.20s ease}}
.social-channel:hover{{transform:translateY(-3px);border-color:rgba(94,231,255,.30);box-shadow:0 14px 30px rgba(0,0,0,.22)}}
.social-icon{{width:40px;height:40px;display:grid;place-items:center;flex:0 0 auto;border-radius:13px;background:linear-gradient(145deg,rgba(154,140,255,.13),rgba(94,231,255,.08));border:1px solid rgba(255,255,255,.10);font-size:20px;font-weight:900}}
.social-main{{display:flex;flex-direction:column;gap:1px;flex:1;min-width:0}}
.social-main strong{{font-size:12px}}
.social-main small{{color:var(--muted);font-size:10px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.report-section iframe{{display:block;width:100%;height:1900px;border:0;background:#050611}}
.footer{{display:flex;justify-content:space-between;gap:12px;margin-top:18px;color:#777d98;font-size:10px;padding:0 4px}}
.footer b{{color:#b7b8ca}}
@keyframes wallpaperZoom{{from{{transform:scale(1.03)}}to{{transform:scale(1.08)}}}}
@keyframes spin{{to{{transform:rotate(360deg)}}}}
@keyframes float1{{0%,100%{{transform:translate3d(0,0,0)}}50%{{transform:translate3d(70px,45px,0) scale(1.08)}}}}
@keyframes float2{{0%,100%{{transform:translate3d(0,0,0)}}50%{{transform:translate3d(-75px,-30px,0) scale(1.1)}}}}
@keyframes pulse{{0%,100%{{transform:scale(.85);opacity:.8}}50%{{transform:scale(1.15);opacity:1}}}}
@keyframes flow{{0%{{background-position:0% 50%}}100%{{background-position:200% 50%}}}}
@keyframes sheen{{0%,100%{{transform:translateX(-110%)}}50%{{transform:translateX(100%)}}}}
@keyframes reveal{{from{{opacity:0;transform:translateY(16px);filter:blur(6px)}}to{{opacity:1;transform:none;filter:none}}}}
@media(max-width:700px){{.list{{grid-template-columns:1fr}}.hero{{padding:24px 18px;border-radius:22px}}.footer{{flex-direction:column;align-items:flex-start}}.report-section iframe{{height:2050px}}}}
html[data-theme="light"]{{color-scheme:light;--bg:#eef2ff;--panel:rgba(255,255,255,.72);--line:rgba(37,45,80,.12);--text:#18203b;--muted:#5c6686;--a:#6757e8;--b:#0b7ea0;--good:#128b52}}
html[data-theme="light"] body::before{{background:linear-gradient(180deg,rgba(247,249,255,.88),rgba(229,235,255,.84))}}
html[data-theme="light"] body::after{{opacity:.22}}
html[data-theme="light"] .nav,html[data-theme="light"] .hero,html[data-theme="light"] .section{{background:rgba(255,255,255,.70);box-shadow:0 22px 50px rgba(44,55,100,.12)}}
html[data-theme="light"] .lead{{color:#4e5878}}
html[data-theme="light"] .channel,html[data-theme="light"] .social-channel,html[data-theme="light"] .cta{{color:var(--text);background:rgba(255,255,255,.62)}}
html[data-theme="light"] .display-controls{{background:rgba(255,255,255,.7)}}
@media(prefers-reduced-motion:reduce){{*,*::before,*::after{{animation:none!important;transition:none!important;scroll-behavior:auto!important}}}}
</style>
</head>
<body>
<div class="anime-wallpaper"></div><div class="topline"></div><div class="grid"></div><div class="float a"></div><div class="float b"></div>
<div class="wrap">
  <nav class="nav">
    <div class="brand">AniToon Media Info</div>
    <div class="nav-tools">
      <a class="nav-link" href="https://t.me/AniToon_1Bot" target="_blank" rel="noopener noreferrer">Open Bot ↗</a>
      <div class="display-controls" aria-label="Display controls">
        <button type="button" id="theme-toggle" title="Toggle dark/light theme">☀️ Light</button>
        <button type="button" id="zoom-out" title="Zoom out">−</button>
        <button type="button" id="zoom-label" title="Reset zoom">100%</button>
        <button type="button" id="zoom-in" title="Zoom in">+</button>
      </div>
    </div>
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

  <section class="section">
    <div class="section-head"><div><small>CONNECT</small><h2>📲 Social Accounts</h2></div></div>
    <div class="list">{social_html}</div>
  </section>

  <footer class="footer">
    <span><b>AniToon</b> • Media intelligence</span>
    <span>Official Bot & Web</span>
  </footer>
</div>
<script>
(() => {{
  const root = document.documentElement;
  const wrap = document.querySelector(".wrap");
  const themeBtn = document.getElementById("theme-toggle");
  const zoomOut = document.getElementById("zoom-out");
  const zoomIn = document.getElementById("zoom-in");
  const zoomLabel = document.getElementById("zoom-label");
  const read = (key, fallback) => {{ try {{ return localStorage.getItem(key) ?? fallback; }} catch (_) {{ return fallback; }} }};
  const write = (key, value) => {{ try {{ localStorage.setItem(key, value); }} catch (_) {{}} }};
  const applyTheme = (theme) => {{
    root.dataset.theme = theme;
    if (themeBtn) themeBtn.textContent = theme === "light" ? "🌙 Dark" : "☀️ Light";
    write("anitoon-theme", theme);
  }};
  const applyZoom = (value) => {{
    const zoom = Math.max(0.80, Math.min(1.20, Number(value) || 1));
    if (wrap) wrap.style.zoom = zoom;
    if (zoomLabel) zoomLabel.textContent = Math.round(zoom * 100) + "%";
    write("anitoon-zoom", String(zoom));
  }};
  applyTheme(read("anitoon-theme", "dark") === "light" ? "light" : "dark");
  applyZoom(Number(read("anitoon-zoom", "1")));
  themeBtn?.addEventListener("click", () => applyTheme(root.dataset.theme === "light" ? "dark" : "light"));
  zoomOut?.addEventListener("click", () => applyZoom(Number(read("anitoon-zoom","1")) - 0.10));
  zoomIn?.addEventListener("click", () => applyZoom(Number(read("anitoon-zoom","1")) + 0.10));
  zoomLabel?.addEventListener("click", () => applyZoom(1));
}})();
</script>
</body>
</html>"""
    return document.encode("utf-8")


def web_page(
    report: Report,
    report_token: str | None = None,
    expires_at: datetime | None = None,
) -> bytes:
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

    previews = list(getattr(report, "previews", []) or [])
    preview_cards = ""
    if previews:
        cards = []
        for index, preview in enumerate(previews[:5], 1):
            data = str(preview.get("data") or "")
            if not data:
                continue
            ratio = int(preview.get("ratio") or 0)
            preview_label = str(preview.get("label") or "").strip()
            try:
                seconds = int(float(preview.get("seconds") or 0))
            except Exception:
                seconds = 0
            minutes, secs = divmod(max(0, seconds), 60)
            hours, minutes = divmod(minutes, 60)
            stamp = f"{hours}:{minutes:02d}:{secs:02d}" if hours else f"{minutes}:{secs:02d}"
            cards.append(
                f'<article class="preview-card">'
                f'<div class="preview-frame"><img src="data:image/jpeg;base64,{data}" alt="Video preview {index}" loading="lazy"></div>'
                f'<div class="preview-meta"><b>Preview {index}</b><span>{html.escape(preview_label or (str(ratio) + "%"))} • {stamp}</span></div>'
                f'</article>'
            )
        preview_cards = "".join(cards)

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
<title>AniToon • {filename}</title>
<style>
:root {{
  color-scheme:dark;
  --bg:#050611;--panel:rgba(12,15,34,.76);--line:rgba(255,255,255,.10);
  --line2:rgba(155,140,255,.28);--text:#f7f7fb;--muted:#9fa6c1;
  --accent:#9b8cff;--cyan:#5ee7ff;--good:#7cf4b0;--shadow:0 28px 80px rgba(0,0,0,.38);
}}
*{{box-sizing:border-box}}
html{{scroll-behavior:smooth}}
body{{margin:0;min-height:100vh;background:var(--bg);color:var(--text);font:14px/1.55 Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;overflow-x:hidden}}
.anime-wallpaper{{position:fixed;inset:0;z-index:-6;background:url("{ANIME_WALLPAPER_DATA_URI}") center/cover no-repeat;opacity:.22;filter:saturate(1.05) contrast(1.03);transform:scale(1.04);animation:wall 24s ease-in-out infinite alternate;pointer-events:none}}
body::before{{content:"";position:fixed;inset:0;z-index:-5;background:radial-gradient(780px 520px at 0 0,rgba(126,107,255,.20),transparent 70%),radial-gradient(700px 480px at 100% 10%,rgba(42,217,255,.12),transparent 72%),linear-gradient(180deg,#070816 0%,#050611 52%,#03040a 100%);pointer-events:none}}
.bg-grid{{position:fixed;inset:0;z-index:-4;opacity:.15;background-image:linear-gradient(rgba(255,255,255,.028) 1px,transparent 1px),linear-gradient(90deg,rgba(255,255,255,.028) 1px,transparent 1px);background-size:32px 32px;mask-image:linear-gradient(to bottom,#000,transparent 88%);pointer-events:none}}
.topline{{position:fixed;top:0;left:0;right:0;height:3px;z-index:40;background:linear-gradient(90deg,transparent,var(--accent),var(--cyan),transparent);background-size:200% 100%;animation:scan 5s linear infinite}}
.wrap{{max-width:1180px;margin:auto;padding:18px 16px 56px}}
.nav{{position:sticky;top:10px;z-index:30;display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:14px;padding:10px 12px;border:1px solid var(--line);border-radius:18px;background:rgba(8,10,23,.72);backdrop-filter:blur(20px);box-shadow:0 14px 36px rgba(0,0,0,.16)}}
.brand{{font-size:11px;font-weight:950;letter-spacing:.11em;text-transform:uppercase;color:#e5e0ff}}
.nav-tools{{display:flex;align-items:center;gap:7px;flex-wrap:wrap;justify-content:flex-end}}
.nav a{{color:var(--muted);text-decoration:none;font-size:11px;font-weight:850}}
.controls{{display:flex;align-items:center;gap:3px;padding:3px;border:1px solid var(--line);border-radius:11px;background:rgba(255,255,255,.035)}}
.controls button{{border:0;border-radius:8px;background:transparent;color:var(--text);padding:6px 8px;font:850 11px/1 system-ui;cursor:pointer}}
.controls button:hover{{background:rgba(255,255,255,.08)}}
.controls #zoom-label{{min-width:44px;color:var(--muted)}}
.hero{{position:relative;display:grid;grid-template-columns:minmax(0,1.25fr) minmax(280px,.75fr);gap:18px;overflow:hidden;padding:20px;border:1px solid var(--line);border-radius:28px;background:linear-gradient(145deg,rgba(17,20,46,.88),rgba(7,9,22,.68));box-shadow:var(--shadow);backdrop-filter:blur(22px)}}
.hero-copy{{min-width:0;padding:5px 2px}}
.eyebrow{{display:inline-flex;align-items:center;gap:7px;padding:6px 9px;border:1px solid rgba(124,244,176,.15);border-radius:999px;background:rgba(124,244,176,.06);color:var(--good);font-size:9px;font-weight:950;letter-spacing:.13em;text-transform:uppercase}}
.eyebrow i{{width:7px;height:7px;border-radius:50%;background:var(--good);box-shadow:0 0 14px rgba(124,244,176,.70);animation:pulse 1.8s ease-in-out infinite}}
h1{{margin:13px 0 7px;font-size:clamp(28px,4.8vw,48px);line-height:1.02;letter-spacing:-.045em}}
.file{{max-width:850px;color:#cbd0e4;font-size:13px;overflow-wrap:anywhere}}
.hero-pills{{display:flex;gap:7px;flex-wrap:wrap;margin-top:13px}}
.pill{{display:inline-flex;align-items:center;gap:6px;padding:7px 9px;border:1px solid var(--line);border-radius:999px;background:rgba(255,255,255,.035);color:var(--muted);font-size:10px}}
.countdown{{color:var(--cyan);font-weight:950}}
.hero-media{{position:relative;min-height:230px;border-radius:20px;overflow:hidden;border:1px solid var(--line2);background:linear-gradient(145deg,rgba(155,140,255,.10),rgba(94,231,255,.04))}}
.hero-media::after{{content:"";position:absolute;inset:0;background:linear-gradient(180deg,transparent 45%,rgba(0,0,0,.58));pointer-events:none}}
.hero-media img{{display:block;width:100%;height:100%;min-height:230px;object-fit:cover}}
.media-label{{position:absolute;left:11px;bottom:10px;z-index:2;padding:6px 8px;border-radius:9px;background:rgba(5,6,17,.66);border:1px solid rgba(255,255,255,.12);backdrop-filter:blur(10px);font-size:10px;font-weight:900}}
.summary{{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:9px;margin-top:12px}}
.stat{{padding:12px;border:1px solid var(--line);border-radius:15px;background:rgba(255,255,255,.03);transition:.2s ease}}
.stat:hover{{transform:translateY(-2px);border-color:var(--line2)}}
.stat b{{display:block;font-size:18px;letter-spacing:-.02em;overflow-wrap:anywhere}}
.stat span{{color:var(--muted);font-size:10px}}
.quick{{position:sticky;top:78px;z-index:20;display:flex;gap:6px;flex-wrap:wrap;margin:10px 0 4px;padding:7px;border:1px solid var(--line);border-radius:15px;background:rgba(7,9,21,.60);backdrop-filter:blur(16px)}}
.quick a{{padding:6px 9px;border-radius:9px;color:var(--muted);text-decoration:none;font-size:10px;font-weight:900}}
.quick a:hover{{background:rgba(255,255,255,.06);color:var(--text)}}
.section{{margin-top:12px;border:1px solid var(--line);border-radius:20px;background:var(--panel);backdrop-filter:blur(18px);box-shadow:0 18px 46px rgba(0,0,0,.18);overflow:hidden}}
.section summary{{list-style:none;cursor:pointer}}
.section summary::-webkit-details-marker{{display:none}}
.section-head{{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:14px 16px;border-bottom:1px solid var(--line);background:linear-gradient(90deg,rgba(255,255,255,.035),transparent)}}
.section-head h2{{margin:0;font-size:16px;letter-spacing:-.02em}}
.chev{{color:var(--muted);transition:transform .2s ease}}
.section[open] .chev{{transform:rotate(180deg)}}
.section-body{{padding:12px}}
.toolbar{{display:flex;align-items:center;gap:8px;margin-bottom:10px}}
.search{{width:100%;padding:9px 11px;border:1px solid var(--line);border-radius:11px;background:rgba(255,255,255,.035);color:var(--text);outline:none;font:700 11px system-ui}}
.search:focus{{border-color:var(--line2);box-shadow:0 0 0 3px rgba(155,140,255,.08)}}
.track-card{{display:flex;gap:12px;padding:13px;border:1px solid var(--line);border-radius:16px;background:linear-gradient(145deg,rgba(255,255,255,.032),rgba(255,255,255,.012));margin-bottom:8px;transition:.18s ease}}
.track-card:last-child{{margin-bottom:0}}
.track-card:hover{{transform:translateY(-1px);border-color:rgba(155,140,255,.28)}}
.track-orb{{width:42px;height:42px;flex:0 0 auto;border-radius:13px;display:grid;place-items:center;background:radial-gradient(circle at 30% 20%,rgba(155,140,255,.22),rgba(94,231,255,.07));border:1px solid rgba(155,140,255,.18);font-size:18px}}
.track-content{{min-width:0;flex:1}}
.track-heading{{display:flex;justify-content:space-between;gap:10px;align-items:flex-start}}
.track-number{{color:var(--accent);font-size:9px;font-weight:950;letter-spacing:.12em}}
.track-heading h3{{margin:2px 0 1px;font-size:14px;overflow-wrap:anywhere}}
.track-heading p{{margin:0;color:var(--muted);font-size:10px}}
.badges{{display:flex;gap:4px;flex-wrap:wrap;justify-content:flex-end}}
.badge{{padding:4px 6px;border-radius:999px;border:1px solid rgba(124,244,176,.14);background:rgba(124,244,176,.05);color:var(--good);font-size:8px;font-weight:950;letter-spacing:.07em}}
.spec-grid{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:7px;margin-top:10px}}
.spec{{display:flex;justify-content:space-between;gap:10px;padding:8px 9px;border-radius:10px;background:rgba(255,255,255,.028)}}
.spec span{{color:var(--muted);font-size:10px}}
.spec strong{{font-size:10px;text-align:right;overflow-wrap:anywhere}}
.empty{{padding:14px;border:1px dashed rgba(255,255,255,.11);border-radius:14px;color:var(--muted);font-size:11px}}
.preview-section{{display:none}}
.preview-section.visible{{display:block}}
.preview-card{{overflow:hidden;border:1px solid var(--line2);border-radius:18px;background:rgba(255,255,255,.025)}}
.preview-frame{{aspect-ratio:16/9;background:#070816;overflow:hidden}}
.preview-frame img{{display:block;width:100%;height:100%;object-fit:cover}}
.preview-meta{{display:flex;justify-content:space-between;gap:8px;padding:9px 11px;font-size:10px}}
.preview-meta span{{color:var(--muted);text-align:right}}
.tech-grid{{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px}}
.tech{{padding:12px;border:1px solid var(--line);border-radius:14px;background:rgba(255,255,255,.028)}}
.tech small{{display:block;color:var(--muted);font-size:9px;margin-bottom:4px}}
.tech b{{display:block;font-size:11px;overflow-wrap:anywhere}}
.footer{{display:flex;justify-content:space-between;gap:10px;margin-top:15px;padding:0 3px;color:#777e9a;font-size:9px}}
.footer strong{{color:#bdc2d8}}
html[data-theme="light"]{{color-scheme:light;--bg:#eef2ff;--panel:rgba(255,255,255,.80);--line:rgba(37,45,80,.12);--line2:rgba(103,87,232,.27);--text:#18203b;--muted:#5d6788;--accent:#6757e8;--cyan:#0b7ea0;--good:#128b52;--shadow:0 24px 65px rgba(44,55,100,.14)}}
html[data-theme="light"] body::before{{background:linear-gradient(180deg,rgba(247,249,255,.90),rgba(228,234,255,.88))}}
html[data-theme="light"] .anime-wallpaper{{opacity:.12}}
html[data-theme="light"] .nav,html[data-theme="light"] .quick,html[data-theme="light"] .hero,html[data-theme="light"] .section{{background:rgba(255,255,255,.78)}}
html[data-theme="light"] .file{{color:#4e5878}}
html[data-theme="light"] .controls{{background:rgba(255,255,255,.72)}}
@keyframes wall{{from{{transform:scale(1.04)}}to{{transform:scale(1.08)}}}}
@keyframes scan{{0%{{background-position:0%}}100%{{background-position:200%}}}}
@keyframes pulse{{0%,100%{{transform:scale(.86);opacity:.78}}50%{{transform:scale(1.16);opacity:1}}}}
@media(max-width:900px){{.hero{{grid-template-columns:1fr}}.hero-media,.hero-media img{{min-height:210px}}.summary{{grid-template-columns:repeat(3,minmax(0,1fr))}}.tech-grid{{grid-template-columns:repeat(2,minmax(0,1fr))}}}}
@media(max-width:620px){{.wrap{{padding:10px 9px 30px}}.nav{{top:6px}}.brand{{font-size:9px}}.hero{{padding:15px;border-radius:22px}}.hero-media,.hero-media img{{min-height:180px}}h1{{font-size:29px}}.summary{{grid-template-columns:repeat(2,minmax(0,1fr))}}.spec-grid,.tech-grid{{grid-template-columns:1fr}}.track-heading{{flex-direction:column}}.badges{{justify-content:flex-start}}.quick{{top:64px;overflow:auto;flex-wrap:nowrap}}.quick a{{white-space:nowrap}}.footer{{flex-direction:column;align-items:flex-start}}}}
@media(prefers-reduced-motion:reduce){{*,*::before,*::after{{animation:none!important;transition:none!important;scroll-behavior:auto!important}}}}
</style>
</head>
<body>
<div class="anime-wallpaper"></div><div class="bg-grid"></div><div class="topline"></div>
<div class="wrap">
  <nav class="nav">
    <div class="brand">AniToon • Media Intelligence</div>
    <div class="nav-tools">
      <a href="/">Home ↗</a>
      <div class="controls">
        <button type="button" id="theme-toggle">☀️ Light</button>
        <button type="button" id="zoom-out" title="Zoom out">−</button>
        <button type="button" id="zoom-label" title="Reset zoom">100%</button>
        <button type="button" id="zoom-in" title="Zoom in">+</button>
      </div>
    </div>
  </nav>

  <header class="hero">
    <div class="hero-copy">
      <span class="eyebrow"><i></i> Analysis Complete</span>
      <h1>Media Intelligence</h1>
      <div class="file">{filename}</div>
      {f'<div class="hero-pills"><span class="pill">🎞️ {esc(title)}</span></div>' if title else ''}
      <div class="hero-pills">
        <span class="pill">📦 {esc(container_name)}</span>
        <span class="pill">📏 {esc(size_text)}</span>
        <span class="pill">⏱️ <span id="countdown" class="countdown">--:--</span></span>
      </div>
    </div>
    {f'<div class="hero-media" id="hero-media"><img src="data:image/jpeg;base64,{str(previews[0].get("data") or "")}" alt="Telegram thumbnail" loading="eager"><span class="media-label">🎞️ Telegram thumbnail</span></div>' if previews and previews[0].get("data") else ''}
  </header>

  <div class="summary" id="overview">
    <div class="stat"><b>{len(video)}</b><span>Video tracks</span></div>
    <div class="stat"><b>{len(audio)}</b><span>Audio tracks</span></div>
    <div class="stat"><b>{len(subtitles)}</b><span>Subtitle tracks</span></div>
    <div class="stat"><b>{esc(quality)}</b><span>Primary quality</span></div>
    <div class="stat"><b>{esc(runtime)}</b><span>Runtime</span></div>
  </div>

  <nav class="quick" aria-label="Quick navigation">
    <a href="#overview">Overview</a>
    {f'<a href="#preview-section" id="preview-nav">Thumbnail</a>' if video else ''}
    <a href="#video-section">Video</a>
    <a href="#audio-section">Audio</a>
    <a href="#subs-section">Subtitles</a>
    <a href="#technical-section">Technical</a>
  </nav>

  {f'''<section id="preview-section" class="section preview-section{' visible' if preview_cards else ''}">
    <div class="section-head"><h2>🎞️ Thumbnail preview</h2><span class="pill">1 image</span></div>
    <div class="section-body" id="preview-grid">{preview_cards}</div>
  </section>''' if video else ""}

  <details id="video-section" class="section" open>
    <summary class="section-head"><h2>🎬 Video</h2><span class="pill">{len(video)} track{'s' if len(video)!=1 else ''} <span class="chev">⌄</span></span></summary>
    <div class="section-body">
      <div class="toolbar"><input class="search" data-filter="video" placeholder="Filter video tracks…"></div>
      <div data-track-list="video">{track_cards(video,"Video")}</div>
    </div>
  </details>

  <details id="audio-section" class="section" open>
    <summary class="section-head"><h2>🎧 Audio</h2><span class="pill">{len(audio)} track{'s' if len(audio)!=1 else ''} <span class="chev">⌄</span></span></summary>
    <div class="section-body">
      <div class="toolbar"><input class="search" data-filter="audio" placeholder="Filter audio by language, codec, name…"></div>
      <div data-track-list="audio">{track_cards(audio,"Audio")}</div>
    </div>
  </details>

  <details id="subs-section" class="section" open>
    <summary class="section-head"><h2>💬 Subtitles</h2><span class="pill">{len(subtitles)} track{'s' if len(subtitles)!=1 else ''} <span class="chev">⌄</span></span></summary>
    <div class="section-body">
      <div class="toolbar"><input class="search" data-filter="subtitle" placeholder="Filter subtitle tracks…"></div>
      <div data-track-list="subtitle">{track_cards(subtitles,"Subtitle")}</div>
    </div>
  </details>

  <details id="technical-section" class="section" open>
    <summary class="section-head"><h2>⚙️ Technical</h2><span class="pill">ID {report_id} <span class="chev">⌄</span></span></summary>
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
  </details>

  <div class="footer"><span><strong>AniToon</strong> • Advanced media report</span><span>Generated {generated_text} • Secure report</span></div>
</div>

<script>
(() => {{
  const expiresAt = {int(expires_at.timestamp()*1000) if expires_at else 0};
  const countdown = document.getElementById("countdown");
  const tick = () => {{
    if (!countdown) return;
    if (!expiresAt) {{ countdown.textContent="ACTIVE"; return; }}
    const left = Math.max(0, Math.floor((expiresAt-Date.now())/1000));
    countdown.textContent = String(Math.floor(left/60)).padStart(2,"0")+":"+String(left%60).padStart(2,"0");
    if(left>0) setTimeout(tick,1000); else countdown.textContent="EXPIRED";
  }};
  tick();
}})();

(() => {{
  const root=document.documentElement, wrap=document.querySelector(".wrap");
  const themeBtn=document.getElementById("theme-toggle"), zo=document.getElementById("zoom-out"), zi=document.getElementById("zoom-in"), zl=document.getElementById("zoom-label");
  const read=(k,f)=>{{try{{return localStorage.getItem(k)??f}}catch(_){{return f}}}};
  const write=(k,v)=>{{try{{localStorage.setItem(k,v)}}catch(_){{}}}};
  const applyTheme=t=>{{root.dataset.theme=t;if(themeBtn)themeBtn.textContent=t==="light"?"🌙 Dark":"☀️ Light";write("anitoon-theme",t)}};
  const applyZoom=v=>{{const z=Math.max(.80,Math.min(1.20,Number(v)||1));if(wrap)wrap.style.zoom=z;if(zl)zl.textContent=Math.round(z*100)+"%";write("anitoon-zoom",String(z))}};
  applyTheme(read("anitoon-theme","dark")==="light"?"light":"dark"); applyZoom(Number(read("anitoon-zoom","1")));
  themeBtn?.addEventListener("click",()=>applyTheme(root.dataset.theme==="light"?"dark":"light"));
  zo?.addEventListener("click",()=>applyZoom(Number(read("anitoon-zoom","1"))-.10));
  zi?.addEventListener("click",()=>applyZoom(Number(read("anitoon-zoom","1"))+.10));
  zl?.addEventListener("click",()=>applyZoom(1));
}})();

(() => {{
  document.querySelectorAll("[data-filter]").forEach(input=>{{
    input.addEventListener("input",()=>{{
      const q=input.value.trim().toLowerCase(), key=input.dataset.filter;
      const list=document.querySelector('[data-track-list="'+key+'"]');
      if(!list)return;
      list.querySelectorAll(".track-card").forEach(card=>{{card.style.display=!q||card.textContent.toLowerCase().includes(q)?"":"none"}});
    }});
  }});
}})();

(() => {{
  const token="{html.escape(report_token or "")}", section=document.getElementById("preview-section"), grid=document.getElementById("preview-grid");
  if(!token||!section||!grid)return;
  const render=item=>{{
    if(!item||!item.data)return;
    const label=String(item.label||"").trim()||"Telegram thumbnail";
    grid.innerHTML='<article class="preview-card"><div class="preview-frame"><img src="data:image/jpeg;base64,'+String(item.data)+'" alt="Telegram thumbnail" loading="lazy"></div><div class="preview-meta"><b>Thumbnail</b><span>'+label+'</span></div></article>';
    section.classList.add("visible");
    if(!document.getElementById("hero-media")){{
      const hero=document.querySelector(".hero");
      if(hero){{
        const media=document.createElement("div"); media.className="hero-media"; media.id="hero-media";
        media.innerHTML='<img src="data:image/jpeg;base64,'+String(item.data)+'" alt="Telegram thumbnail" loading="eager"><span class="media-label">🎞️ '+label+'</span>';
        hero.appendChild(media);
      }}
    }}
  }};
  let attempts=0;
  const poll=async()=>{{
    if(attempts++>12)return;
    try{{
      const r=await fetch("/preview/"+encodeURIComponent(token),{{cache:"no-store"}});
      if(r.ok){{
        const p=await r.json();
        if(p.previews&&p.previews.length){{render(p.previews[0]);return;}}
      }}
    }}catch(_ ){{}}
    setTimeout(poll,1200);
  }};
  poll();
}})();
</script>
</body>
</html>"""    return document.encode("utf-8")


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
                    "preview_tasks": len(preview_tasks),
                    "max_active_scans": MAX_CONCURRENT_CHECKS,
                    "ram_pct": runtime_resource_stats()["ram_pct"],
                    "web_egress_gb": runtime_resource_stats()["web_egress_gb"],
                    "guard_active": bool(resource_guard_reason()),
                    "guard_reason": resource_guard_reason(),
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

            elif path.startswith("/preview/"):
                _purge_states()
                token = path[len("/preview/"):].strip("/")
                preview_items = []
                state = web_states.get(token)
                if state is not None:
                    preview_items = list(getattr(state.report, "previews", []) or [])[:1]
                else:
                    try:
                        stored_payload = await load_web_report(token)
                    except Exception:
                        stored_payload = None
                    if stored_payload:
                        report_payload = stored_payload.get("report") if isinstance(stored_payload, dict) else {}
                        preview_items = list((report_payload or {}).get("previews") or [])[:1]
                body = json.dumps(
                    {
                        "ready": len(preview_items) >= 1,
                        "previews": preview_items[:1],
                    },
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
                            stored_payload = stored_report if isinstance(stored_report, dict) else {"report": stored_report}
                            restored_report = Report(**(stored_payload.get("report") or {}))
                            stored_expiry = stored_payload.get("expires_at")
                            if isinstance(stored_expiry, str):
                                stored_expiry = datetime.fromisoformat(stored_expiry.replace("Z", "+00:00"))
                            if stored_expiry is None:
                                stored_expiry = datetime.now(timezone.utc) + timedelta(seconds=REPORT_LINK_TTL_SECONDS)
                            state = ScanState(
                                source_message=None,
                                report=restored_report,
                                created_at=time.monotonic(),
                                web_token=token,
                                expires_at=stored_expiry,
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
                    body = web_page(state.report, token, state.expires_at)
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
    temp_cleanup_task: asyncio.Task | None = None
    _bind_bot_handlers(bot, BOT_USERNAME, include_clone=True)
    cleanup_app_temp()
    temp_cleanup_task = asyncio.create_task(temp_cleanup_loop())
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
        if temp_cleanup_task is not None:
            temp_cleanup_task.cancel()
            with suppress(asyncio.CancelledError):
                await temp_cleanup_task
        for task in list(preview_tasks):
            task.cancel()
        for task in list(preview_tasks):
            with suppress(asyncio.CancelledError):
                await task
        preview_tasks.clear()
        status_edit_cache.clear()

        health.close()
        await health.wait_closed()
        with suppress(Exception):
            await bot.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
