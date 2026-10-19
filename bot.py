from __future__ import annotations

import asyncio
import base64
import html
import io
import json
import logging
import math
import os
import re
import secrets
import shutil
import tempfile
import time
from contextlib import suppress
from dataclasses import asdict, dataclass
from datetime import datetime, timezone, timedelta
from urllib.parse import urlsplit
from typing import Any

from PIL import Image, ImageEnhance, ImageFilter

from dotenv import load_dotenv
from telethon import Button, TelegramClient, errors, events, functions, types
from telethon.sessions import MemorySession

from file_inspector import Report, inspect_telegram_message, format_report, format_section, human
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
    owner_daily_summary,
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
    clone_daily_summary,
    record_clone_daily_activity,
    record_group_chat,
    list_group_chats,
    update_group_onboarding,
    load_scan_worker_cap,
    save_scan_worker_cap,
)
load_dotenv()

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"].strip()
BOT_TOKEN = os.environ["BOT_TOKEN"].strip()

FILE_CHECKER_PRIVATE_ONLY = (
    os.getenv("FILE_CHECKER_PRIVATE_ONLY", "0").strip().lower()
    not in {"0", "false", "no", "off"}
)
# Hard safety ceiling. The RAM controller decides how many of these workers may run now.
MAX_SCAN_WORKERS = 5
MAX_CONCURRENT_CHECKS = max(1, min(int(os.getenv("MAX_CONCURRENT_CHECKS", "5")), MAX_SCAN_WORKERS))
scan_worker_cap = MAX_CONCURRENT_CHECKS
scan_worker_cap_persisted = False
scan_worker_cap_source = "environment default"
# Fail fast instead of leaving a Free-instance worker hung for minutes.
SCAN_TIMEOUT_SECONDS = max(20, min(int(os.getenv("SCAN_TIMEOUT_SECONDS", "60")), 120))
REPORT_LINK_TTL_SECONDS = 10 * 60
PENDING_SCAN_TTL_SECONDS = 10 * 60
MAX_STORED_RESULTS = 100
MAX_REPORT_TRACKS = 100
MAX_REPORT_ARCHIVE_ENTRIES = 100
PUBLIC_WEB_URL = (
    os.getenv("PUBLIC_WEB_URL", "https://anitoons-1bot-oa44.onrender.com")
    .strip()
    .rstrip("/")
)

# Private Telegram channel used for automatic media archiving.
STORAGE_CHANNEL = os.getenv("STORAGE_CHANNEL", "").strip()
STORAGE_CHANNEL_ID_DEFAULT = "-1004491486679"
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
RAM_PAUSE_PCT = 75.0

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

# Bound Telegram network handshakes/retries so a data-center migration cannot
# leave the web process alive while the bot is stuck in startup indefinitely.
bot = TelegramClient(
    "file_checker_bot",
    API_ID,
    API_HASH,
    timeout=15,
    connection_retries=3,
    request_retries=3,
    retry_delay=2,
    auto_reconnect=True,
)
bot.flood_sleep_threshold = 15 * 60
# Reserve the full bounded pool so owner controls can raise the cap live without restart.
check_semaphore = asyncio.Semaphore(MAX_SCAN_WORKERS)
scan_queue_lock = asyncio.Lock()
scan_queue_refresh_lock = asyncio.Lock()
scan_queue_changed = asyncio.Event()
scan_queue_order: list[str] = []
queued_scan_statuses: dict[str, tuple[Any, str]] = {}
processing_scan_tokens: set[str] = set()
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
queue_refresh_tasks: set[asyncio.Task] = set()
storage_tasks: set[asyncio.Task] = set()
storage_semaphore = asyncio.Semaphore(2)
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


_anitoon_art_cache: str | None = None
_anitoon_art_photo_cache: bytes | None = None

def _anitoon_start_art_b64() -> str:
    """Load the replacement horizontal banner from split base64 source parts."""
    global _anitoon_art_cache
    if _anitoon_art_cache is not None:
        return _anitoon_art_cache

    parts_dir = os.path.join(os.path.dirname(__file__), "assets", "anitoon_start_parts")
    try:
        names = sorted(
            name for name in os.listdir(parts_dir)
            if name.startswith("part") and name.endswith(".b64chunk")
        )
        _anitoon_art_cache = "".join(
            open(os.path.join(parts_dir, name), "r", encoding="ascii").read().strip()
            for name in names
        )
    except OSError:
        _anitoon_art_cache = ""
    return _anitoon_art_cache


def _anitoon_start_art_bytes() -> bytes | None:
    """Return a Telegram-safe JPEG for the AniToon start photo."""
    global _anitoon_art_photo_cache
    if _anitoon_art_photo_cache is not None:
        return _anitoon_art_photo_cache

    def normalize(raw: bytes) -> bytes | None:
        try:
            with Image.open(io.BytesIO(raw)) as source:
                source.verify()
            with Image.open(io.BytesIO(raw)) as source:
                image = source.convert("RGB")
                # Prepare a high-resolution banner. Upscale smaller artwork to a
                # 4K long edge with a high-quality resampler while preserving the
                # original aspect ratio; never downscale artwork already larger.
                max_width, max_height = 3840, 2160
                width, height = image.size
                scale = min(max_width / width, max_height / height)
                if scale > 1:
                    resampling = getattr(Image, "Resampling", Image).LANCZOS
                    image = image.resize(
                        (max(1, round(width * scale)), max(1, round(height * scale))),
                        resampling,
                    )
                # Telegram is stricter about image decoding than Pillow; always
                # send a freshly encoded baseline JPEG rather than raw bytes.
                # Apply mild sharpening after resizing and avoid chroma subsampling
                # to retain cleaner edges and color detail in the source JPEG.
                image = image.filter(
                    ImageFilter.UnsharpMask(radius=0.8, percent=110, threshold=3)
                )
                out = io.BytesIO()
                image.save(
                    out,
                    format="JPEG",
                    quality=95,
                    optimize=True,
                    progressive=False,
                    subsampling=0,
                )
                return out.getvalue()
        except Exception:
            return None

    # Preferred new banner asset. A missing asset is handled gracefully.
    asset_candidates = (
        os.path.join(os.path.dirname(__file__), "assets", "anitoon_start_4k.jpg"),
        os.path.join(os.path.dirname(__file__), "assets", "anitoon_start_4k.png"),
    )
    for asset_path in asset_candidates:
        try:
            with open(asset_path, "rb") as handle:
                normalized = normalize(handle.read())
            if normalized:
                _anitoon_art_photo_cache = normalized
                return normalized
        except OSError:
            continue

    # Backward-compatible bundled artwork, also normalized before upload.
    data = _anitoon_start_art_b64()
    if data:
        try:
            raw = base64.b64decode(data, validate=True)
            normalized = normalize(raw)
            if normalized:
                _anitoon_art_photo_cache = normalized
                return normalized
        except Exception:
            log.exception("Failed to normalize bundled AniToon start artwork")

    log.error(
        "AniToon start artwork unavailable; check the horizontal start banner asset."
    )
    return None


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
        buttons.append([Button.inline("🧬 Create New Clone", b"clone:create")])
    buttons.append([Button.inline("⬅️ Home", b"home:back")])
    return buttons


def web_report_button(
    token: str,
    bot_username: str = BOT_USERNAME,
    *,
    include_clone: bool = True,
):
    # Keep Telegram clean: expose the report and the group-join action only.
    return [
        [Button.url("🌐 Open File Info", f"{PUBLIC_WEB_URL}/report/{token}")],
        [Button.url("➕ Add Me to Your Group", add_to_group_url(bot_username))],
    ]


def _language_names_for_report(report: Report, key: str) -> list[str]:
    tracks = report.video.get("tracks", []) if key == "video" else (
        report.audio.get("tracks", []) if key == "audio" else report.subtitles
    )
    names=[]
    for track in tracks if isinstance(tracks, list) else []:
        name = str(track.get("language_name") or "").strip()
        code = str(track.get("language") or "").strip()
        shown = name or (f"Unknown ({code})" if code else "")
        if shown and shown not in names:
            names.append(shown)
    return names[:12]


def compact_scan_result(report: Report) -> str:
    """Keep scan details on the web report instead of posting them as Telegram text."""
    return (
        "✅ <b>Scan complete</b>\n\n"
        "Your file report is ready in the browser.\n"
        "Tap <b>Open File Info</b> below to view the details."
    )

HOME_TEXT = (
    "⛩ <b>Welcome to AniToon Media Bot</b> ⛩\n\n"
    "🎞️ <b>File Metadata • Media Tools</b>\n"
    "⚡ Get clear, easy-to-read information about your media files."
)

CLONE_HOME_TEXT = (
    "⛩ <b>AniToon Media Info Clone</b> ⛩\n\n"
    "🔎 Inspect Telegram photos, videos, stickers, GIFs, PDFs, audio, documents and supported post media.\n"
    "🌐 Open the complete type-specific report in your browser."
)

CLONE_HELP_TEXT = (
    "📚 <b>AniToon Clone — Command Guide</b>\n\n"
    "📤 <b>Scan a file</b>\n"
    "Send a photo, video, GIF, sticker, PDF, ZIP archive, audio file or document. Tap <b>Scan Files</b>, then open <b>Open File Info</b> for the full report. ZIP reports include a contents listing when readable.\n\n"
    "⌨️ <b>Commands</b>\n"
    "/start — Open the bot menu\n"
    "/help — Show this command guide\n"
    "/about — Learn about AniToon\n"
    "/scan_files — Open the Scan Files instructions\n"
    "/stats — View your scan statistics\n"
    "/clone — Open Clone Manager on the main bot\n"
    "/addtogroup — Get the group invite link\n"
    "/cancel — Cancel all your queued and running scans"
)

HELP_TEXT = (
    "📚 <b>AniToon — Command Guide</b>\n\n"
    "🔎 <b>Scan a file</b>\n"
    "Send a photo, video, GIF, sticker, PDF, ZIP archive, audio file or document. Tap <b>Scan Files</b>, then open <b>Open File Info</b> for the full report. ZIP reports include a contents listing when readable.\n\n"
    "⌨️ <b>Commands</b>\n"
    "/start — Open the main menu\n"
    "/help — Show this command guide\n"
    "/about — Learn about AniToon\n"
    "/scan_files — Open Scan Files instructions\n"
    "/stats — View your scan statistics\n"
    "/clone — Manage your clone bots\n"
    "/addtogroup — Get the group invite link\n"
    "/cancel — Cancel queued and running scans"
)

SCAN_TEXT = (
    "🔎 <b>Scan Files</b>\n\n"
    "1️⃣ Send a photo, video, GIF, sticker, PDF, ZIP archive, audio file, document or supported post media.\n"
    "2️⃣ Tap <b>🔎 Scan File Info</b> under the file message.\n"
    "3️⃣ Tap <b>🌐 Open File Info</b> to view the complete web report.\n\n"
    "📄 The report shows the metadata available for that file."
)

ABOUT_TEXT = (
    "ℹ️ <b>About AniToon</b> ✨\n\n"
    "🤖 <b>AniToon Bot</b>\n"
    "A Telegram media inspector that reports available metadata for photos, images, videos, stickers, animations, PDFs, audio, documents and supported post attachments. Reads stay bounded for large files.\n\n"
    "🌐 <b>AniToon Web</b>\n"
    "The web page presents the complete file information in a clean, easy-to-read report.\n\n"
    "📦 <b>What you get</b>\n"
    "Images • Video • Audio • Stickers • GIFs • PDFs • ZIP contents • Subtitles • Post context • Technical metadata\n\n"
    "⚡ <b>Simple workflow</b>\n"
    "Send file → tap Scan File Info → open the web report.\n\n"
    "For help or questions, use the Help & Support button below."
)


def _coerce_report(value: Any) -> Report:
    """Normalize current/legacy scan state so web rendering never depends on a preview."""
    if isinstance(value, Report):
        report = value
    elif isinstance(value, tuple) and value and isinstance(value[0], Report):
        report = value[0]
    elif isinstance(value, dict):
        allowed = {
            "filename", "size", "mime", "ext", "detected", "media_kind",
            "sampled", "sample_hash", "notes", "video", "audio",
            "subtitles", "container", "probe_ranges", "previews", "media_info", "archive_entries",
        }
        report = Report(**{key: value.get(key) for key in allowed if key in value})
    else:
        raise TypeError(f"Unsupported report state type: {type(value).__name__}")

    # Repair legacy/malformed nested values instead of allowing one bad field
    # to abort the entire /report/<token> response.
    if not isinstance(report.video, dict):
        report.video = {"tracks": report.video} if isinstance(report.video, list) else {}
    if not isinstance(report.audio, dict):
        report.audio = {"tracks": report.audio} if isinstance(report.audio, list) else {}
    if not isinstance(report.container, dict):
        report.container = {}
    if not isinstance(getattr(report, "media_info", None), dict):
        report.media_info = {}

    video_tracks = report.video.get("tracks", [])
    audio_tracks = report.audio.get("tracks", [])
    subtitles = report.subtitles
    previews = report.previews

    if not isinstance(video_tracks, list):
        video_tracks = []
    if not isinstance(audio_tracks, list):
        audio_tracks = []
    if not isinstance(subtitles, list):
        subtitles = []
    if not isinstance(previews, list):
        previews = [previews] if isinstance(previews, dict) else []

    report.video["tracks"] = [item for item in video_tracks if isinstance(item, dict)][:MAX_REPORT_TRACKS]
    report.audio["tracks"] = [item for item in audio_tracks if isinstance(item, dict)][:MAX_REPORT_TRACKS]
    report.subtitles = [item for item in subtitles if isinstance(item, dict)][:MAX_REPORT_TRACKS]
    report.previews = [
        item for item in previews
        if isinstance(item, dict) and str(item.get("data") or "").strip()
    ][:1]

    archive_entries = getattr(report, "archive_entries", [])
    if not isinstance(archive_entries, list):
        archive_entries = []
    report.archive_entries = [
        item for item in archive_entries if isinstance(item, dict)
    ][:MAX_REPORT_ARCHIVE_ENTRIES]

    if isinstance(report.notes, str):
        report.notes = [report.notes]
    elif not isinstance(report.notes, list):
        report.notes = []

    if isinstance(report.probe_ranges, str):
        report.probe_ranges = [report.probe_ranges]
    elif not isinstance(report.probe_ranges, list):
        report.probe_ranges = []

    return report


def cache_state(
    status_message: Any,
    source_message: Any,
    report: Report,
    web_token: str | None = None,
    expires_at: datetime | None = None,
) -> ScanState:
    token = web_token or secrets.token_urlsafe(18)
    expiry = expires_at or (datetime.now(timezone.utc) + timedelta(seconds=REPORT_LINK_TTL_SECONDS))
    normalized_report = _coerce_report(report)
    state = ScanState(
        source_message=source_message,
        report=normalized_report,
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
    expired: list[str] = []
    for token, state in list(web_states.items()):
        expiry = state.expires_at
        if expiry is not None:
            # PyMongo may return BSON datetimes as naive UTC values.
            # Normalize them before comparing with our aware UTC clock.
            if expiry.tzinfo is None:
                expiry = expiry.replace(tzinfo=timezone.utc)
            else:
                expiry = expiry.astimezone(timezone.utc)
            state.expires_at = expiry
            if now_utc >= expiry:
                expired.append(token)
        elif now_mono - state.created_at > REPORT_LINK_TTL_SECONDS:
            expired.append(token)

    for token in expired:
        web_states.pop(token, None)
    pending_expired = [
        token for token, pending in pending_scans.items()
        if now_mono - pending.created_at > PENDING_SCAN_TTL_SECONDS
    ]
    for token in pending_expired:
        pending_scans.pop(token, None)


def main_bot_start_url(payload: str | None = None) -> str:
    base = f"https://t.me/{BOT_USERNAME.lstrip('@')}"
    return f"{base}?start={payload}" if payload else base


def clone_redirect_buttons(payload: str = "clone") -> list[list[Any]]:
    if payload == "clones":
        primary_label = "🤖 Open Clone Manager"
    else:
        primary_label = "🧬 Create Clone on Main Bot"
        payload = "clone"
    return [
        [Button.url(primary_label, main_bot_start_url(payload))],
        [
            Button.url("🤖 Main AniToon", main_bot_start_url()),
            Button.inline("⬅️ Back", b"home:back"),
        ],
    ]


async def show_main_bot_redirect(event, *, edit: bool = False) -> None:
    message = (
        "🤖 <b>Main AniToon Bot</b>\n\n"
        "Open the main bot for its home menu and clone creation."
    )
    buttons = [
        [Button.url("🤖 Open Main AniToon", main_bot_start_url())],
        [Button.url("🧬 Create Your Own Bot", main_bot_start_url("clone"))],
    ]
    if edit:
        await event.edit(message, parse_mode="html", buttons=buttons)
    else:
        await event.reply(message, parse_mode="html", buttons=buttons)


async def show_clone_redirect(event, *, payload: str = "clone", edit: bool = False) -> None:
    if payload == "clones":
        message = (
            "🤖 <b>Clone Manager</b>\n\n"
            "Open the main AniToon bot to view, manage or remove your clone bots."
        )
    else:
        message = (
            "🧬 <b>Create Your Own Bot</b>\n\n"
            "Clone bots cannot register other clone bots directly. "
            "Tap below to open the main AniToon bot and start clone creation."
        )
    buttons = clone_redirect_buttons(payload)
    if edit:
        await event.edit(message, parse_mode="html", buttons=buttons)
    else:
        await event.reply(message, parse_mode="html", buttons=buttons)


def home_buttons(
    bot_username: str = BOT_USERNAME,
    *,
    include_clone: bool = True,
    user_id: int | None = None,
    show_privacy: bool = True,
):
    scan = Button.inline("🔎  Scan Files", b"home:scan")
    stats = Button.inline("📊  My Stats", b"home:stats")
    help_btn = Button.inline("📚  Help Center", b"home:help")
    about = Button.inline("✨  About AniToon", b"home:about")
    add_group = Button.url("➕ Add to Group", add_to_group_url(bot_username))

    if include_clone:
        clone = Button.inline("🤖 Clone Manager", b"home:clones")
        buttons = [
            [scan],
            [clone, stats],
            [help_btn, about],
            [add_group],
        ]
        if _owner_allowed(user_id):
            buttons[-1] = [
                Button.inline("🛠️ Admin Studio", b"owner:dashboard"),
            ]
            buttons.append([add_group])
    else:
        create_clone = Button.url("🤖 Open Clone Manager", main_bot_start_url("clones"))
        buttons = [
            [scan],
            [stats, help_btn],
            [about],
            [create_clone],
            [add_group],
        ]
    return buttons


def back_buttons() -> list[list[Any]]:
    return [[Button.inline("⬅️ Back", b"home:back")]]


def help_buttons() -> list[list[Any]]:
    return [[Button.inline("⬅️ Back", b"home:back")]]


def about_buttons() -> list[list[Any]]:
    return [
        [Button.url("🌐 Open AniToon Web", PUBLIC_WEB_URL)],
        [Button.url("🤖 Open AniToon Bot", f"https://t.me/{BOT_USERNAME}")],
        [Button.url("❔ Help & Support", "https://t.me/AniToon_0Bot")],
        [Button.inline("⬅️ Home", b"home:back")],
    ]


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

    # Keep a precise 7-day activity history for the owner's clone dashboard.
    with suppress(Exception):
        await record_clone_daily_activity(
            clone_id,
            **{field: int(amount)},
        )

    # Batch high-frequency cumulative message counters to reduce database traffic.
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
        return "—"
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

    daily = await clone_daily_summary(clone_id, 7)
    days = daily.get("days", []) if daily.get("available") else []
    total_7 = {
        key: sum(int(day.get(key, 0) or 0) for day in days)
        for key in (
            "messages_received",
            "scans_started",
            "scans_completed",
            "scans_failed",
            "scans_cancelled",
        )
    }
    started = total_7["scans_completed"] + total_7["scans_failed"]
    success_rate = (total_7["scans_completed"] / started * 100) if started else 0.0

    lines = [
        "📊 <b>Clone Bot Analytics</b>",
        "",
        f"🤖 <b>@{html.escape(username)}</b>",
        f"{'🟢 Online' if live else '🔴 Offline'}",
        f"🆔 Clone ID: <code>{clone_id}</code>",
        "",
        "📈 <b>Last 7 Days</b>",
        f"📨 Messages: <b>{total_7['messages_received']}</b>",
        f"🔎 Scans started: <b>{total_7['scans_started']}</b>",
        f"✅ Completed: <b>{total_7['scans_completed']}</b>",
        f"❌ Failed: <b>{total_7['scans_failed']}</b>",
        f"🛑 Cancelled: <b>{total_7['scans_cancelled']}</b>",
        f"📊 Success rate: <b>{success_rate:.1f}%</b>",
        "",
        "🗓️ <b>Daily Activity</b>",
    ]

    if days:
        for day in days:
            dt = str(day.get("date") or "")
            try:
                label = datetime.strptime(dt, "%Y-%m-%d").strftime("%d %b")
            except ValueError:
                label = dt
            msg = int(day.get("messages_received", 0) or 0)
            scans = int(day.get("scans_started", 0) or 0)
            done = int(day.get("scans_completed", 0) or 0)
            failed = int(day.get("scans_failed", 0) or 0)
            cancelled = int(day.get("scans_cancelled", 0) or 0)
            lines.append(
                f"• <b>{label}</b> — 📨 {msg}  🔎 {scans}  ✅ {done}  ❌ {failed}  🛑 {cancelled}"
            )
    else:
        lines.append("• No daily activity recorded yet.")

    lines += [
        "",
        "📦 <b>All-Time</b>",
        f"📨 Messages: <b>{int(stats['messages_received'])}</b>",
        f"🔎 Scans: <b>{int(stats['scans_started'])}</b>",
        f"✅ Completed: <b>{int(stats['scans_completed'])}</b>",
        f"❌ Failed: <b>{int(stats['scans_failed'])}</b>",
        f"🛑 Cancelled: <b>{int(stats['scans_cancelled'])}</b>",
        "",
        f"📅 Created: {_fmt_clone_time(stats.get('created_at'))}",
        f"🕒 Last activity: {_fmt_clone_time(stats.get('last_activity'))}",
    ]

    buttons = []
    if username != "Unnamed":
        buttons.append([Button.url("🤖 Open Clone Bot", f"https://t.me/{username}")])
    buttons.append([
        Button.inline("🔄 Refresh", f"clone:stats:{clone_id}".encode("ascii")),
        Button.inline("🗑 Remove", f"clone:remove:{clone_id}".encode("ascii")),
    ])
    buttons.append([Button.inline("⬅️ My Clones", b"clone:list")])
    await event.edit("\n".join(lines), parse_mode="html", buttons=buttons)



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
    """Return container-aware memory and app-owned resource signals."""
    process_rss_mb = 0.0
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("VmRSS:"):
                    process_rss_mb = float(line.split()[1]) / 1024.0
                    break
    except (OSError, ValueError):
        pass

    # Prefer cgroup limits, as these reflect the memory budget of the Render
    # container instead of the much larger host. Fall back conservatively.
    ram_limit_mb = 512.0
    for limit_path in ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"):
        try:
            with open(limit_path, "r", encoding="utf-8") as handle:
                raw_limit = handle.read().strip()
            if raw_limit and raw_limit != "max":
                limit_bytes = int(raw_limit)
                if 64 * 1024 * 1024 <= limit_bytes < 2**60:
                    ram_limit_mb = limit_bytes / (1024**2)
                    break
        except (OSError, ValueError):
            continue

    # cgroup current usage includes the complete container, not just this
    # Python process, so it better captures parser/native-library memory.
    container_used_mb = None
    for current_path in ("/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory/memory.usage_in_bytes"):
        try:
            with open(current_path, "r", encoding="utf-8") as handle:
                current_bytes = int(handle.read().strip())
            if current_bytes > 0:
                container_used_mb = current_bytes / (1024**2)
                break
        except (OSError, ValueError):
            continue

    memory_used_mb = container_used_mb if container_used_mb is not None else process_rss_mb
    memory_source = "container cgroup" if container_used_mb is not None else "process RSS estimate"
    ram_pct = min(100.0, memory_used_mb / ram_limit_mb * 100.0) if ram_limit_mb else 0.0
    tmp_bytes = app_temp_usage_bytes()
    tmp_mb = tmp_bytes / (1024**2)
    tmp_pct = min(100.0, tmp_bytes / APP_TMP_LIMIT_BYTES * 100.0)

    uptime_hours = (datetime.now(timezone.utc) - started_at).total_seconds() / 3600.0
    uptime_pct = min(100.0, uptime_hours / 750.0 * 100.0)
    egress_gb = web_bytes_sent / (1024**3)
    egress_pct = min(100.0, egress_gb / 4.0 * 100.0)

    return {
        "rss_mb": process_rss_mb,
        "memory_used_mb": memory_used_mb,
        "memory_source": memory_source,
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


def runtime_cpu_cores() -> float | None:
    """Read this container's CPU quota, avoiding oversubscribed scans on tiny instances."""
    for quota_path, period_path in (
        ("/sys/fs/cgroup/cpu.max", None),
        ("/sys/fs/cgroup/cpu/cpu.cfs_quota_us", "/sys/fs/cgroup/cpu/cpu.cfs_period_us"),
    ):
        try:
            if period_path is None:
                with open(quota_path, "r", encoding="utf-8") as handle:
                    quota, period = handle.read().strip().split()[:2]
                if quota == "max":
                    continue
                quota_us, period_us = int(quota), int(period)
            else:
                with open(quota_path, "r", encoding="utf-8") as handle:
                    quota_us = int(handle.read().strip())
                with open(period_path, "r", encoding="utf-8") as handle:
                    period_us = int(handle.read().strip())
            if quota_us > 0 and period_us > 0:
                return max(0.01, quota_us / period_us)
        except (OSError, ValueError):
            continue
    return None


def worker_limit_for_cpu(cpu_cores: float | None, configured_cap: int | None = None) -> int:
    """Limit concurrent scans based on CPU quota; each extra worker contends for CPU."""
    cap = scan_worker_cap if configured_cap is None else int(configured_cap)
    cap = max(1, min(MAX_SCAN_WORKERS, cap))
    if cpu_cores is None or cpu_cores <= 0:
        return cap
    # Allow roughly three mostly-I/O-bound scans per CPU core, with one minimum
    # worker for Free instances (0.1 CPU) so scans still make progress.
    cpu_cap = max(1, min(MAX_SCAN_WORKERS, math.ceil(cpu_cores * 3)))
    return min(cap, cpu_cap)


def memory_pressure_high() -> bool:
    return runtime_resource_stats()["ram_pct"] >= RAM_PAUSE_PCT


def worker_limit_for_ram(ram_pct: float, configured_cap: int | None = None) -> int:
    """Choose 0-5 workers from measured RAM pressure, always respecting the owner cap."""
    cap = max(1, min(int(scan_worker_cap if configured_cap is None else configured_cap), MAX_SCAN_WORKERS))
    ram_pct = max(0.0, min(100.0, float(ram_pct)))
    if ram_pct >= RAM_PAUSE_PCT:
        return 0
    # Five-way operation is permitted only with plenty of free container RAM.
    # Concurrency is reduced before the hard pause to avoid OOM kills.
    if ram_pct >= 65.0:
        return min(cap, 1)
    if ram_pct >= 55.0:
        return min(cap, 2)
    if ram_pct >= 45.0:
        return min(cap, 3)
    if ram_pct >= 35.0:
        return min(cap, 4)
    return cap


def adaptive_scan_limit() -> int:
    """Cap scan starts using both container memory pressure and CPU quota."""
    ram_limit = worker_limit_for_ram(runtime_resource_stats()["ram_pct"], scan_worker_cap)
    cpu_limit = worker_limit_for_cpu(runtime_cpu_cores(), scan_worker_cap)
    return min(ram_limit, cpu_limit)


async def change_scan_worker_cap(new_cap: int) -> bool:
    """Change the live owner cap and persist it when MongoDB is reachable."""
    global scan_worker_cap, scan_worker_cap_persisted, scan_worker_cap_source
    scan_worker_cap = max(1, min(int(new_cap), MAX_SCAN_WORKERS))
    try:
        scan_worker_cap_persisted = await save_scan_worker_cap(scan_worker_cap)
    except Exception:
        log.exception("Could not persist scan worker cap")
        scan_worker_cap_persisted = False
    scan_worker_cap_source = "MongoDB" if scan_worker_cap_persisted else "runtime only until restart"
    scan_queue_changed.set()
    return scan_worker_cap_persisted

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
    scan_limit = adaptive_scan_limit()
    ram_state = (
        "🟢 Normal"
        if stats["ram_pct"] < 35
        else ("🟡 Watch" if stats["ram_pct"] < 45 else ("🟠 High" if stats["ram_pct"] < RAM_PAUSE_PCT else "🔴 Critical"))
    )
    worker_state = "PAUSED (RAM guard)" if scan_limit == 0 else f"{scan_limit} worker(s) allowed now"
    egress_state = (
        "🟢 Low"
        if stats["web_egress_pct"] < 70
        else ("🟡 Watch" if stats["web_egress_pct"] < 85 else "🔴 High")
    )
    guard = resource_guard_reason()
    guard_line = "✅ Guard is clear — new heavy scans are allowed." if not guard else f"🛑 <b>Guard active:</b> {html.escape(guard)}"
    persist_state = "saved in MongoDB" if scan_worker_cap_persisted else "runtime-only until restart"

    text = (
        "🧠 <b>Smart RAM Scan Controller</b>\n"
        f"🧠 RAM <b>{stats['memory_used_mb']:.1f}/{stats['ram_limit_mb']:.0f} MB</b> • {stats['ram_pct']:.1f}% {ram_state}\n"
        f"📍 Memory source: <i>{html.escape(stats['memory_source'])}</i>\n"
        f"⚙️ Worker cap: <b>{scan_worker_cap}/{MAX_SCAN_WORKERS}</b> • {scan_worker_cap_source}\n"
        f"🤖 Adaptive limit now: <b>{scan_limit if scan_limit else 'PAUSED'}</b> worker(s)\n"
        f"🔎 Active scans: <b>{active_processes}</b> • Waiting: <b>{queued_processes}</b>\n"
        f"📡 Web egress <b>{stats['web_egress_gb']:.3f}/4 GB</b> • {stats['web_egress_pct']:.1f}% {egress_state}\n"
        f"⏱️ Uptime <b>{stats['uptime_hours']:.2f} h</b> • 📊 allowance {stats['uptime_pct']:.1f}%\n"
        f"🧹 Bot temp <b>{stats['tmp_mb']:.1f}/{stats['tmp_limit_mb']:.0f} MB</b> • {stats['tmp_pct']:.1f}%\n"
        f"🛡️ Guard: <b>{'PAUSED' if guard else 'CLEAR'}</b>\n\n"
        "<b>Automatic RAM limits</b>\n"
        "Below 35%: up to 5 workers • 35–44%: 4 • 45–54%: 3 • 55–64%: 2 • 65–74%: 1 • 75%+: pause new scans.\n"
        "Use the controls below to change the maximum cap. The controller still reduces workers automatically for safety.\n"
        f"💾 Cap setting: <b>{persist_state}</b>\n"
        "Temp files are cleaned every 5 min; report links expire after 10 min.\n"
        "⚠️ A five-worker cap is a maximum, not a promise of five active scans when memory is elevated."
    )
    await event.edit(
        text,
        parse_mode="html",
        buttons=[
            [
                Button.inline("➖", b"owner:workers:down"),
                Button.inline(f"Worker cap {scan_worker_cap}/{MAX_SCAN_WORKERS}", b"owner:resources"),
                Button.inline("➕", b"owner:workers:up"),
            ],
            [
                Button.inline("🛡 Safe (2)", b"owner:workers:set:2"),
                Button.inline("⚖ Balanced (3)", b"owner:workers:set:3"),
                Button.inline("🚀 Max (5)", b"owner:workers:set:5"),
            ],
            [Button.inline("🔄 Refresh", b"owner:resources")],
            [Button.inline("⬅️ Dashboard", b"owner:dashboard")],
        ],
    )


async def render_owner_dashboard(
    event,
    user_id: int,
    *,
    edit: bool = True,
    selected_day: datetime | None = None,
) -> None:
    """Show a concise owner overview with rolling seven-day analytics and live status."""
    if not _owner_allowed(user_id):
        await event.answer("Owner access only.", alert=True)
        return

    now = datetime.now(timezone.utc)
    today = now.date()
    first_day = today - timedelta(days=6)
    summary = await owner_7day_summary(7)
    mongo = "🟢 Connected" if mongodb_is_connected() else (
        "🟠 Reconnecting" if mongodb_is_configured() else "🔴 Not configured"
    )
    active = int(summary.get("completed", 0) or 0)
    failed = int(summary.get("failed", 0) or 0)
    cancelled = int(summary.get("cancelled", 0) or 0)
    total = int(summary.get("total_scans", 0) or 0)
    success_rate = active / total * 100 if total else 0.0
    period = f"{first_day.strftime('%d %b')} – {today.strftime('%d %b %Y')} UTC"
    uptime = (now - started_at).total_seconds() / 3600

    if summary.get("available"):
        text = (
            "🛠️ <b>ANITOON OWNER DASHBOARD</b>\n"
            f"<i>Rolling 7-day overview • {html.escape(period)}</i>\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "📊 <b>USAGE (LAST 7 DAYS)</b>\n"
            f"👥 Active users: <b>{int(summary.get('total_users', 0) or 0)}</b>\n"
            f"📁 Total scans: <b>{total}</b>\n"
            f"✅ Completed: <b>{active}</b>   ❌ Failed: <b>{failed}</b>   🛑 Cancelled: <b>{cancelled}</b>\n"
            f"🎯 Completion rate: <b>{success_rate:.1f}%</b>\n\n"
            "🤖 <b>CLONES</b>\n"
            f"📦 Registered: <b>{int(summary.get('clone_registered', 0) or 0)}</b> "
            f"• 🟢 Online: <b>{int(summary.get('clone_online', 0) or 0)}</b>\n"
            f"🆕 Created in 7 days: <b>{int(summary.get('clone_created_7d', 0) or 0)}</b>\n"
            f"💬 Clone messages: <b>{int(summary.get('clone_messages_7d', 0) or 0)}</b>\n"
            f"🔎 Clone scans: <b>{int(summary.get('clone_scans_started_7d', 0) or 0)}</b> started "
            f"• ✅ {int(summary.get('clone_scans_completed_7d', 0) or 0)} "
            f"• ❌ {int(summary.get('clone_scans_failed_7d', 0) or 0)}\n\n"
            "⚡ <b>LIVE NOW</b>\n"
            f"🔎 Active scans: <b>{active_processes}/{adaptive_scan_limit()} allowed</b> • cap {scan_worker_cap}/{MAX_SCAN_WORKERS}\n"
            f"⏳ Waiting in queue: <b>{queued_processes}</b>\n"
            f"🗄️ Database: <b>{mongo}</b>\n"
            f"⏱️ Process uptime: <b>{uptime:.1f} hours</b>"
        )
    else:
        text = (
            "🛠️ <b>ANITOON OWNER DASHBOARD</b>\n"
            f"<i>Rolling 7-day overview • {html.escape(period)}</i>\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "⚠️ <b>Analytics temporarily unavailable</b>\n"
            "MongoDB could not provide the last-7-days statistics. No historical totals are guessed.\n\n"
            "⚡ <b>LIVE NOW</b>\n"
            f"🔎 Active scans: <b>{active_processes}/{adaptive_scan_limit()} allowed</b> • cap {scan_worker_cap}/{MAX_SCAN_WORKERS}\n"
            f"⏳ Waiting in queue: <b>{queued_processes}</b>\n"
            f"🗄️ Database: <b>{mongo}</b>\n"
            f"⏱️ Process uptime: <b>{uptime:.1f} hours</b>"
        )

    buttons = [
        [
            Button.inline("👥 Users • 7 days", b"owner:users:0"),
            Button.inline("🤖 Clone Fleet", b"owner:clones"),
        ],
        [
            Button.inline("📈 Performance & RAM", b"owner:resources"),
            Button.inline("🟢 Live Status", b"home:status"),
        ],
        [
            Button.inline("📣 Announcements", b"owner:broadcast"),
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


status_edit_cache: dict[tuple[int, int], tuple[float, str]] = {}
STATUS_EDIT_MIN_INTERVAL = 0.8

async def edit_status(message, text: str, *, buttons=None) -> None:
    """Apply non-critical progress updates without blocking or breaking a scan."""
    message_id = getattr(message, "id", None)
    chat_id = getattr(message, "chat_id", None)
    try:
        key = (
            int(chat_id) if chat_id is not None else 0,
            int(message_id) if message_id is not None else id(message),
        )
    except (TypeError, ValueError):
        key = (0, id(message))

    now = time.monotonic()
    previous = status_edit_cache.get(key)
    if previous and previous[1] == text:
        return

    lowered = text.casefold()
    terminal_update = any(marker in lowered for marker in (
        "metadata scan cancelled",
        "free resource guard paused",
        "safe scan limit reached",
        "metadata scan reached the",
        "temporarily rate-limited this scan",
        "metadata scan failed",
    ))
    # Queue positions are state transitions, not animation frames. They must
    # update immediately so the temporary "REGISTERING SCAN" message cannot
    # stick at 10% when the first queue-number edit happens inside the throttle.
    queue_state_update = (
        lowered.startswith("⏳ <b>queue ")
        and "registering scan" not in lowered
    )
    if (
        previous
        and not terminal_update
        and not queue_state_update
        and now - previous[0] < STATUS_EDIT_MIN_INTERVAL
    ):
        return

    try:
        await message.edit(text, parse_mode="html", buttons=buttons)
    except errors.MessageNotModifiedError:
        status_edit_cache[key] = (time.monotonic(), text)
    except errors.FloodWaitError as exc:
        # A skipped animation frame must never interrupt the file scan. The
        # next heartbeat/progress event will refresh the message when allowed.
        log.debug("Progress edit rate-limited for %s seconds", getattr(exc, "seconds", "?"))
    except Exception:
        # Telegram UI edits are secondary to the actual scan/report operation.
        log.debug("Progress message edit failed", exc_info=True)
    else:
        status_edit_cache[key] = (time.monotonic(), text)


def progress_details(line: str) -> tuple[int, str]:
    low = line.lower()

    if "starting scan" in low:
        pct = 1
    elif "stage 1/4" in low or "stage 1/3" in low:
        pct = 12
    elif "stage 2/4" in low:
        pct = 32
    elif "stage 2/3" in low:
        pct = 60
    elif "stage 3/4" in low or "stage 3/3" in low:
        pct = 88
    elif "stage 4/4" in low:
        pct = 92
    elif "stage 1/1" in low:
        pct = 20
    elif "stage 2/2" in low or "reading available media metadata" in low:
        pct = 72
    else:
        pct = 50

    clean = line
    for prefix in ("🧭 ", "🎯 ", "🔎 ", "🧩 "):
        clean = clean.replace(prefix, "")
    if "•" in clean:
        clean = clean.split("•", 1)[1].strip()
    return pct, clean


def status_text(
    filename: str,
    line: str,
    pct: int | None = None,
    queue_position: int = 1,
) -> str:
    parsed_pct, clean = progress_details(line)
    current_pct = parsed_pct if pct is None else int(pct)
    pct = max(0, min(100, current_pct))
    # Keep ten cells on mobile, but use a fractional first cell so 1-9%
    # doesn't look like a fully filled 10% block.
    filled, remainder = divmod(pct * 10, 100)
    partial_cells = ("", "▏", "▎", "▍", "▌", "▋", "▊", "▉")
    bar = "▓" * filled
    if remainder and filled < 10:
        partial_index = min(7, max(1, (remainder * 8 + 99) // 100))
        bar += partial_cells[partial_index]
    bar += "░" * (10 - len(bar))
    # Keep the percentage and bar together; use short labels to avoid mobile wrapping.
    return (
        "⚡ <b>PROCESSING</b>\n"
        f"📄 <b>{html.escape(filename)}</b>\n\n"
        f"<code>{bar}</code> <b>{pct:3d}%</b>\n"
        f"🔄 {html.escape(clean)}"
    )


async def run_scan(
    source_message: Any,
    status_message: Any,
    *,
    scan_token: str,
    client: Any,
) -> Report:
    filename = safe_filename(source_message)

    progress_state = {
        "shown_pct": 1,
        "target_pct": 15,
        "label": "Starting scan…",
    }

    async def progress(line: str):
        # Drive the estimate from parser stages instead of letting a timer race
        # to 96% while a remote file read is still blocked.
        _parsed_pct, label = progress_details(line)
        low = line.lower()
        stage_targets = (
            ("stage 1/4", 25),
            ("stage 2/4", 50),
            ("stage 3/4", 75),
            ("stage 4/4", 92),
            ("stage 1/3", 32),
            ("stage 2/3", 65),
            ("stage 3/3", 92),
            ("stage 1/1", 82),
            ("stage 2/2", 82),
        )
        target = next((pct for marker, pct in stage_targets if marker in low), None)
        if target is not None:
            progress_state["target_pct"] = max(
                int(progress_state["target_pct"]),
                target,
            )
        progress_state["label"] = label

    heartbeat_stop = asyncio.Event()
    async def heartbeat():
        pulse = ("·", "••", "•••")
        index = 0
        while not heartbeat_stop.is_set():
            # Keep updates responsive without flooding Telegram with edits.
            await asyncio.sleep(0.8)
            if heartbeat_stop.is_set():
                break

            # Move quickly toward the next real parser-stage milestone, but
            # never animate to a fake 96% while the file is still being read.
            # Successful completion alone is allowed to set 100%.
            shown_pct = int(progress_state["shown_pct"])
            target_pct = min(
                94,
                max(shown_pct, int(progress_state.get("target_pct", 15))),
            )
            if shown_pct < target_pct:
                step = min(4, max(1, math.ceil((target_pct - shown_pct) / 3)))
                shown_pct = min(target_pct, shown_pct + step)
                progress_state["shown_pct"] = shown_pct

            label = progress_state["label"]
            # Queue position is not shown for an active scan, so avoid an
            # unnecessary queue/database lookup on every progress refresh.
            await edit_status(
                status_message,
                status_text(
                    filename,
                    f"{label} {pulse[index % len(pulse)]}",
                    shown_pct,
                ),
                buttons=cancel_button(scan_token),
            )
            index += 1

    heartbeat_task = asyncio.create_task(heartbeat())
    try:
        async def fast_worker():
            # Use the stream-aware PyAV/FFmpeg inspector for audio/video to expose
            # codecs, duration, dimensions, frame rate, bitrate and stream counts.
            # Both inspectors keep source reads under the existing strict probe budget.
            file_obj = getattr(source_message, "file", None)
            filename = str(getattr(file_obj, "name", None) or "").lower()
            mime = str(getattr(file_obj, "mime_type", None) or "").lower()
            attributes = list(getattr(file_obj, "attributes", None) or [])
            attribute_names = {type(item).__name__.lower() for item in attributes}
            av_extensions = {
                ".mkv", ".webm", ".mp4", ".m4v", ".mov", ".avi", ".mpeg",
                ".mpg", ".ts", ".m2ts", ".mts", ".3gp", ".wmv", ".flv",
                ".ogv", ".mp3", ".flac", ".wav", ".wave", ".m4a", ".aac",
                ".opus", ".ogg", ".oga", ".wma", ".ac3", ".eac3", ".mka",
            }
            has_av_streams = (
                mime.startswith(("video/", "audio/"))
                or any("documentattributevideo" in name or "documentattributeaudio" in name for name in attribute_names)
                or any(filename.endswith(ext) for ext in av_extensions)
            )

            # Matroska/WebM store the Info and Tracks elements near the file
            # header in normal files. The bounded parser can extract these
            # without the random EOF seeks FFmpeg sometimes attempts on very
            # large Telegram files. Use that fast path first to avoid a slow
            # FFmpeg failure followed by a second full metadata pass.
            if filename.endswith((".mkv", ".webm", ".mka")):
                try:
                    fast_report, fast_bytes = await inspect_telegram_message(
                        client,
                        source_message,
                        progress=progress,
                        deep=False,
                    )
                    log.info(
                        "Fast Matroska/WebM scan complete | file=%s | sampled_bytes=%s",
                        safe_filename(source_message),
                        int(fast_bytes or 0),
                    )
                    return fast_report, int(fast_bytes or 0)
                except (ProbeBudgetExceeded, ProbeCancelled, asyncio.CancelledError):
                    raise
                except Exception as exc:
                    log.warning(
                        "Fast Matroska/WebM scan failed; trying FFmpeg fallback | file=%s | error=%s",
                        safe_filename(source_message),
                        type(exc).__name__,
                    )

            if has_av_streams:
                try:
                    player_report = await inspect_telegram_player(
                        client,
                        source_message,
                        scan_token,
                        progress=progress,
                    )
                    return player_report, int(getattr(player_report, "sampled", 0) or 0)
                except (ProbeBudgetExceeded, ProbeCancelled, asyncio.CancelledError):
                    raise
                except Exception as exc:
                    log.warning(
                        "Stream-aware metadata scan failed; trying bounded fallback | file=%s | error=%s",
                        safe_filename(source_message),
                        type(exc).__name__,
                    )

            # Non-AV media (photos, stickers, GIFs, PDFs and other documents)
            # use the file-aware parser and Telegram message attributes.
            return await inspect_telegram_message(
                client,
                source_message,
                progress=progress,
                deep=False,
            )

        report, _probe_bytes = await asyncio.wait_for(
            fast_worker(),
            timeout=SCAN_TIMEOUT_SECONDS,
        )
        progress_state["shown_pct"] = 100
        progress_state["label"] = "✅ Scan complete — report ready."
        queue_position = await _scan_queue_position(scan_token)
        await edit_status(
            status_message,
            status_text(filename, progress_state["label"], 100, queue_position),
            buttons=cancel_button(scan_token),
        )
        return report
    finally:
        heartbeat_stop.set()
        heartbeat_task.cancel()
        with suppress(asyncio.CancelledError):
            await heartbeat_task


def queue_status_text(filename: str, position: int) -> str:
    """Build the short, one-based FIFO position shown to a waiting user."""
    return (
        f"⏳ <b>QUEUE {position}</b>\n"
        f"📄 <b>{html.escape(filename)}</b>\n"
        "Waiting for the next available scan worker."
    )


def _waiting_scan_count_locked() -> int:
    return sum(token not in processing_scan_tokens for token in scan_queue_order)


async def _scan_queue_position(scan_token: str) -> int:
    """Return a scan's FIFO index; display formatting separates processing from waiters."""
    async with scan_queue_lock:
        try:
            return scan_queue_order.index(scan_token) + 1
        except ValueError:
            return 1


async def _refresh_queue_positions() -> None:
    """Number waiting scans from QUEUE 1, excluding any scans already processing."""
    global queued_processes
    async with scan_queue_refresh_lock:
        async with scan_queue_lock:
            queued_processes = _waiting_scan_count_locked()
            waiting_tokens = [
                token for token in scan_queue_order
                if token in queued_scan_statuses and token not in processing_scan_tokens
            ]
            entries = [
                (position, token, *queued_scan_statuses[token])
                for position, token in enumerate(waiting_tokens, 1)
            ]

        for position, token, status_message, filename in entries:
            with suppress(Exception):
                await edit_status(
                    status_message,
                    queue_status_text(filename, position),
                    buttons=cancel_button(token),
                )


async def _remove_queued_scan(scan_token: str) -> None:
    """Remove a cancelled waiter and shift all remaining queue numbers."""
    global queued_processes
    removed = False
    async with scan_queue_lock:
        if scan_token in scan_queue_order:
            scan_queue_order.remove(scan_token)
            removed = True
        queued_scan_statuses.pop(scan_token, None)
        processing_scan_tokens.discard(scan_token)
        queued_processes = _waiting_scan_count_locked()
        scan_queue_changed.set()
    if removed:
        await _refresh_queue_positions()


async def acquire_scan_slot(
    status_message: Any,
    scan_token: str,
    filename: str,
) -> None:
    """Acquire a worker slot in FIFO order while keeping active jobs numbered."""
    global active_processes, queued_processes
    queued = False

    try:
        while True:
            stats = runtime_resource_stats()
            ram_high = stats["ram_pct"] >= RAM_PAUSE_PCT
            egress_guard = web_bytes_sent >= WEB_EGRESS_PAUSE_BYTES

            if egress_guard:
                raise ResourceGuardPause(
                    f"Tracked web egress reached {stats['web_egress_gb']:.2f} GB. "
                    "New heavy scans are paused by the resource guard."
                )

            start_now = False
            refresh_queue = False
            async with scan_queue_lock:
                # Clearing under the queue lock prevents lost release/enqueue wake-ups.
                scan_queue_changed.clear()

                if scan_token not in scan_queue_order:
                    scan_queue_order.append(scan_token)
                    queued_scan_statuses[scan_token] = (status_message, filename)
                    queued = True
                    refresh_queue = True
                    scan_queue_changed.set()

                # Active scans keep their position at the front of the FIFO list.
                # The next unstarted entry may use a free worker slot.
                first_waiting = next(
                    (
                        token for token in scan_queue_order
                        if token not in processing_scan_tokens
                    ),
                    None,
                )
                # Resource-aware worker limit: more parallelism when memory is
                # healthy, progressively fewer workers as RAM pressure increases.
                worker_limit = adaptive_scan_limit()
                can_start = (
                    not ram_high
                    and worker_limit > 0
                    and active_processes < worker_limit
                    and first_waiting == scan_token
                )

                if can_start:
                    # No await occurs after acquiring the semaphore and marking
                    # the job active, preventing a cancellation from leaking a slot.
                    await check_semaphore.acquire()
                    processing_scan_tokens.add(scan_token)
                    queued = False
                    active_processes += 1
                    queued_processes = _waiting_scan_count_locked()
                    start_now = True
                    scan_queue_changed.set()
                else:
                    queued_processes = _waiting_scan_count_locked()

            if refresh_queue and not start_now:
                await _refresh_queue_positions()
            if start_now:
                return

            try:
                await asyncio.wait_for(scan_queue_changed.wait(), timeout=0.75)
            except asyncio.TimeoutError:
                pass

    except asyncio.CancelledError:
        if queued:
            await _remove_queued_scan(scan_token)
        raise
    except Exception:
        if queued:
            await _remove_queued_scan(scan_token)
        raise


async def release_scan_slot(scan_token: str) -> None:
    """Release a worker and only then promote the next waiting job to Queue 1."""
    global active_processes, queued_processes
    async with scan_queue_lock:
        if scan_token in scan_queue_order:
            scan_queue_order.remove(scan_token)
        queued_scan_statuses.pop(scan_token, None)
        processing_scan_tokens.discard(scan_token)
        active_processes = max(0, active_processes - 1)
        queued_processes = _waiting_scan_count_locked()
        scan_queue_changed.set()
    check_semaphore.release()
    # Do not leave the active worker held up by Telegram edits to other
    # queue messages. Keep the task referenced until all positions are refreshed.
    refresh_task = asyncio.create_task(_refresh_queue_positions())
    queue_refresh_tasks.add(refresh_task)
    refresh_task.add_done_callback(queue_refresh_tasks.discard)


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

        queue_position = await _scan_queue_position(scan_token)
        await edit_status(
            status_message,
            status_text(filename, "Starting scan…", 1, queue_position),
            buttons=cancel_button(scan_token),
        )

        report = await run_scan(
            source_message,
            status_message,
            scan_token=scan_token,
            client=client,
        )

        # Worker slots cover metadata parsing only. Follow-up tasks must not block
        # the next queued file after its scan is complete.
        await release_scan_slot(scan_token)
        slot_acquired = False

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

        # Thumbnails are optional web decoration. Generate them in the
        # background so scanning can return promptly and the next queued file
        # can use the released worker immediately.
        if getattr(source_message, "media", None):
            _schedule_preview_generation(
                client,
                source_message,
                scan_token,
                state,
                expires_at,
            )

        final_result_text = compact_scan_result(report)
        result_buttons = web_report_button(
            scan_token,
            bot_username,
            include_clone=include_clone,
        )

        try:
            await status_message.reply(
                final_result_text,
                parse_mode="html",
                buttons=result_buttons,
            )
            log.info(
                "Final scan result sent | token=%s | thumbnail_web_only=%s",
                scan_token,
                True,
            )
        except Exception:
            log.exception("Failed to send final scan result message | token=%s", scan_token)
            raise

        # Archive only after the scan is complete and its report was delivered.
        # Clone-bot media is downloaded by the clone and uploaded by the main bot.
        with suppress(Exception):
            stored = await archive_scanned_file(
                client,
                source_message,
                user_id=user_id,
                bot_username=bot_username,
                archive_client=bot,
            )
            if not stored:
                log.warning(
                    "Post-scan storage copy was not confirmed | token=%s",
                    scan_token,
                )

        with suppress(Exception):
            await status_message.delete()

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
            await release_scan_slot(scan_token)

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
        "📦 <b>MEDIA DETECTED</b>\n\n"
        f"📄 <code>{filename}</code>\n"
        "🔎 AniToon will identify the media type and show its available details.",
        parse_mode="html",
        buttons=metadata_button(token),
    )


async def cancel_user_scan(user_id: int) -> int:
    """Cancel every queued and running metadata scan belonging to a user, across all bot clients."""
    cancelled = 0
    for token, pending in list(pending_scans.items()):
        if pending.user_id == int(user_id):
            pending_scans.pop(token, None)
            cancelled += 1
    for token, task in list(active_scans.items()):
        if active_scan_users.get(token) == int(user_id) and not task.done():
            task.cancel()
            cancelled += 1
    return cancelled


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

def _storage_invite_hash(value: str) -> str | None:
    raw = (value or "").strip()
    if not raw:
        return None
    if raw.startswith("+"):
        return raw[1:].strip("/") or None
    try:
        parsed = urlsplit(raw)
    except Exception:
        return None
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").strip("/")
    if host in {"t.me", "telegram.me"} and path.startswith("+"):
        return path[1:].strip("/") or None
    return None


async def _resolve_storage_peer(client: Any) -> Any | None:
    key = id(client)
    if key in storage_peers:
        return storage_peers[key]

    configured_id = os.getenv("STORAGE_CHANNEL_ID", STORAGE_CHANNEL_ID_DEFAULT).strip()
    targets: list[Any] = []
    if configured_id:
        try:
            targets.append(int(configured_id))
        except ValueError:
            log.warning("Invalid STORAGE_CHANNEL_ID configuration")
    if STORAGE_CHANNEL:
        targets.append(STORAGE_CHANNEL)

    last_error: Exception | None = None
    for target in targets:
        try:
            # Bot accounts cannot use the invite-check API. Resolve a concrete
            # numeric channel ID when configured; otherwise try the supplied
            # entity directly (which works for public usernames and cached peers).
            peer = await asyncio.wait_for(client.get_entity(target), timeout=12)
            storage_peers[key] = peer
            return peer
        except Exception as exc:
            last_error = exc

    log.warning(
        "Storage channel unavailable | bot=%s | configured_target=%s | error=%s",
        _bot_id_for_client(client) or "main",
        STORAGE_CHANNEL,
        type(last_error).__name__ if last_error else "not_configured",
        exc_info=last_error is not None,
    )
    return None


async def archive_scanned_file(
    client: Any,
    source_message: Any,
    *,
    user_id: int | None,
    bot_username: str,
    archive_client: Any | None = None,
) -> bool:
    """Archive media using the main bot's channel access, including clone-bot media."""
    media = getattr(source_message, "media", None)
    if not media:
        return False

    archive_client = archive_client or client
    peer = await _resolve_storage_peer(archive_client)
    if peer is None:
        return False

    filename = safe_filename(source_message)
    source_name = str(bot_username).lstrip("@") or "AniToon"
    caption = (
        "📦 <b>AniToon Storage</b>\n"
        f"📄 <code>{html.escape(filename)}</code>\n"
        f"🤖 <b>@{html.escape(source_name)}</b>"
    )
    temp_path = None

    try:
        async with storage_semaphore:
            if client is archive_client:
                # Same bot account: forward the Telegram media server-side.
                await asyncio.wait_for(
                    archive_client.send_message(peer, source_message, silent=True),
                    timeout=STORAGE_SEND_TIMEOUT,
                )
            else:
                # Clone bot can read its incoming chat, but may not have access
                # to the private storage channel. Download through the clone,
                # then upload through the main bot which has channel access.
                suffix = os.path.splitext(filename)[1][:20]
                with tempfile.NamedTemporaryFile(
                    prefix="anitoon-archive-",
                    suffix=suffix,
                    delete=False,
                ) as temp_file:
                    temp_path = temp_file.name
                downloaded = await asyncio.wait_for(
                    client.download_media(source_message, file=temp_path),
                    timeout=120,
                )
                if not downloaded or not os.path.isfile(temp_path):
                    raise RuntimeError("Telegram media download returned no file")
                await asyncio.wait_for(
                    archive_client.send_file(
                        peer,
                        temp_path,
                        caption=caption,
                        parse_mode="html",
                        force_document=True,
                        silent=True,
                    ),
                    timeout=120,
                )
        log.info(
            "Media archived silently | source_bot=%s | file=%s",
            source_name,
            filename,
        )
        return True
    except Exception:
        log.warning(
            "Media archive failed | source_bot=%s | file=%s",
            source_name,
            filename,
            exc_info=True,
        )
        return False
    finally:
        if temp_path:
            with suppress(OSError):
                os.remove(temp_path)


def is_archivable_media_message(event: Any) -> bool:
    message = getattr(event, "message", None)
    if message is None:
        return False

    # Archive actual user media/files, not text commands or service messages.
    return any(
        getattr(message, attr, None) is not None
        for attr in (
            "document",
            "video",
            "audio",
            "voice",
            "photo",
            "gif",
        )
    )


def _schedule_storage_archive(
    event: Any,
    *,
    client: Any,
    bot_username: str,
    archive_client: Any | None = None,
) -> None:
    if not is_archivable_media_message(event):
        return

    sender = getattr(event, "sender_id", None)
    user_id = int(sender) if sender is not None else None

    task = asyncio.create_task(
        archive_scanned_file(
            client,
            event.message,
            user_id=user_id,
            bot_username=bot_username,
            archive_client=archive_client,
        )
    )
    storage_tasks.add(task)
    task.add_done_callback(storage_tasks.discard)


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
    if include_clone:
        commands = [
            types.BotCommand(command="start", description="Open the main menu"),
            types.BotCommand(command="help", description="Show the command guide"),
            types.BotCommand(command="about", description="About AniToon"),
            types.BotCommand(command="scan_files", description="Open Scan Files instructions"),
            types.BotCommand(command="stats", description="Open your scan statistics"),
            types.BotCommand(command="clone", description="Open Clone Manager"),
            types.BotCommand(command="addtogroup", description="Add AniToon to a group"),
            types.BotCommand(command="cancel", description="Cancel all your active scans"),
        ]
    else:
        commands = [
            types.BotCommand(command="start", description="Open the clone menu"),
            types.BotCommand(command="help", description="Show the command guide"),
            types.BotCommand(command="about", description="About AniToon"),
            types.BotCommand(command="scan_files", description="Open Scan Files instructions"),
            types.BotCommand(command="stats", description="Open your scan statistics"),
            types.BotCommand(command="clone", description="Open Clone Manager on the main bot"),
            types.BotCommand(command="addtogroup", description="Add this bot to a group"),
            types.BotCommand(command="cancel", description="Cancel all your active scans"),
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
        try:
            await handle_new_message(
                event,
                client=client,
                bot_username=bot_username,
                include_clone=include_clone,
            )
        except Exception:
            log.exception(
                "Incoming message handler failed | bot=%s | chat=%s | message=%s",
                bot_username,
                getattr(event, "chat_id", None),
                getattr(event, "id", None),
            )
            with suppress(Exception):
                await event.reply(
                    "⚠️ AniToon couldn’t process this message right now. Please try again."
                )

    async def on_callback(event):
        try:
            await handle_callback(
                event,
                client=client,
                bot_username=bot_username,
                include_clone=include_clone,
            )
        except Exception:
            log.exception(
                "Callback action failed | bot=%s | data=%s",
                bot_username,
                (getattr(event, "data", b"") or b"")[:80],
            )
            with suppress(Exception):
                await event.answer(
                    "That action hit a temporary error. Please tap again.",
                    alert=True,
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
    raw_text = (event.raw_text or "").strip()
    if not event.is_private or not raw_text or raw_text.startswith("/"):
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
        f"⚡ Active scans: <b>{active_processes}/{adaptive_scan_limit()} allowed</b> • cap {scan_worker_cap}/{MAX_SCAN_WORKERS}\n"
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



def is_checkable_message(event) -> bool:
    """Accept downloadable files/photos and supported Telegram post attachments."""
    message = getattr(event, "message", None)
    if message is None:
        return False

    if any(getattr(message, key, None) is not None for key in (
        "photo", "video", "document", "sticker", "gif", "audio",
        "voice", "video_note", "animation",
    )):
        return True

    media = getattr(message, "media", None)
    if media is None:
        return False

    # Any Telegram MessageMedia wrapper can yield useful message-level info,
    # even when it has no downloadable file bytes. Ignore the empty placeholder.
    media_type = type(media).__name__.lower()
    return media_type.startswith("messagemedia") and "empty" not in media_type


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
        start_parts = text.split(maxsplit=1)
        start_payload = start_parts[1].strip().split(maxsplit=1)[0].lower() if len(start_parts) > 1 else ""
        if start_payload in {"help", "about", "scan_files", "stats", "clones", "addtogroup", "cancel"}:
            if start_payload == "help":
                await event.reply(help_text, parse_mode="html", buttons=help_buttons())
            elif start_payload == "about":
                await event.reply(ABOUT_TEXT, parse_mode="html", buttons=about_buttons())
            elif start_payload == "scan_files":
                await event.reply(SCAN_TEXT, parse_mode="html", buttons=scan_page_buttons())
            elif start_payload == "stats":
                await render_user_stats(event, int(sender.id), edit=False)
            elif start_payload == "clones":
                if include_clone:
                    await render_clone_list(event, int(sender.id), edit=False)
                else:
                    await show_clone_redirect(event, payload="clones")
            elif start_payload == "addtogroup":
                await event.reply(
                    "➕ <b>Add AniToon to your group</b>",
                    parse_mode="html",
                    buttons=[[Button.url("➕ Add Me to Your Group", add_to_group_url(bot_username))]],
                )
            elif start_payload == "cancel":
                cancelled = await cancel_user_scan(int(sender.id))
                await event.reply(
                    f"✅ Cancelled <b>{cancelled}</b> queued/running scan job(s)." if cancelled
                    else "ℹ️ You have no queued or running metadata scans.",
                    parse_mode="html",
                )
            return
        if start_payload in {"clone", "createclone"}:
            if include_clone:
                await begin_clone_setup(event)
            else:
                await show_clone_redirect(event, payload="clones")
            return
        start_buttons = home_buttons(
            bot_username,
            include_clone=include_clone,
            user_id=getattr(sender, "id", None),
            show_privacy=bool(getattr(event, "is_private", True)),
        )
        art = _anitoon_start_art_bytes()
        if art:
            art_stream = io.BytesIO(art)
            art_stream.name = "AniToon.jpg"
            try:
                await event.client.send_file(
                    event.chat_id,
                    art_stream,
                    caption=home_text,
                    parse_mode="html",
                    buttons=start_buttons,
                    force_document=False,
                )
            except errors.ImageProcessFailedError:
                log.exception("Telegram rejected AniToon start image; falling back to text home")
                await event.reply(
                    home_text,
                    parse_mode="html",
                    buttons=start_buttons,
                )
        else:
            await event.reply(
                home_text,
                parse_mode="html",
                buttons=start_buttons,
            )
        return

    if command == "/help":
        await record_user(event)
        sender = await event.get_sender()
        await event.reply(help_text, parse_mode="html", buttons=help_buttons())
        return

    if command == "/scan_files":
        await record_user(event)
        await event.reply(SCAN_TEXT, parse_mode="html", buttons=scan_page_buttons())
        return

    if command == "/about":
        await record_user(event)
        sender = await event.get_sender()
        await event.reply(ABOUT_TEXT, parse_mode="html", buttons=about_buttons())
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
        sender = await event.get_sender()
        if include_clone:
            await render_clone_list(event, int(sender.id), edit=False)
        else:
            await show_clone_redirect(event, payload="clones")
        return

    if command == "/stats":
        await record_user(event)
        sender = await event.get_sender()
        await render_user_stats(event, int(sender.id), edit=False)
        return

    if command in {"/status", "/resources"}:
        sender = await event.get_sender()
        user_id = getattr(sender, "id", None)
        if not _owner_allowed(user_id):
            # Owner-only resource/status commands are intentionally invisible
            # to ordinary users.
            return
        await record_user(event)
        if command == "/resources":
            await render_owner_resources(event, int(user_id))
        else:
            await render_public_status(event, edit=False)
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
        sender = await event.get_sender()
        user_id = getattr(sender, "id", None)
        if user_id is None:
            await event.reply("⚠️ I could not identify your Telegram account.")
            return
        cancelled = await cancel_user_scan(int(user_id))
        if cancelled:
            await event.reply(
                f"✅ Cancelled <b>{cancelled}</b> queued/running scan job(s) across AniToon and your clone bots.",
                parse_mode="html",
            )
        else:
            await event.reply("ℹ️ You have no queued or running metadata scans.")
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

    if data.startswith("owner:workers:"):
        sender = await event.get_sender()
        user_id = getattr(sender, "id", None)
        if not _owner_allowed(user_id):
            await event.answer("Owner access only.", alert=True)
            return

        parts = data.split(":")
        new_cap = scan_worker_cap
        try:
            if len(parts) == 3 and parts[2] == "up":
                new_cap = min(MAX_SCAN_WORKERS, scan_worker_cap + 1)
            elif len(parts) == 3 and parts[2] == "down":
                new_cap = max(1, scan_worker_cap - 1)
            elif len(parts) == 4 and parts[2] == "set":
                new_cap = max(1, min(int(parts[3]), MAX_SCAN_WORKERS))
            else:
                await event.answer("Invalid worker control.", alert=True)
                return
        except ValueError:
            await event.answer("Invalid worker cap.", alert=True)
            return

        persisted = await change_scan_worker_cap(new_cap)
        if persisted:
            await event.answer(f"Worker cap saved: {scan_worker_cap}/{MAX_SCAN_WORKERS}.")
        else:
            await event.answer(f"Worker cap set: {scan_worker_cap}/{MAX_SCAN_WORKERS} (not persisted).")
        await render_owner_resources(event, int(user_id))
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

    if data.startswith("owner:dashboard:day:"):
        await event.answer()
        sender = await event.get_sender()
        user_id = getattr(sender, "id", None)
        if not _owner_allowed(user_id):
            await event.answer("Owner access only.", alert=True)
            return
        raw_date = data.split(":", 3)[3]
        try:
            chosen = datetime.strptime(raw_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            await event.answer("Invalid dashboard date.", alert=True)
            return
        today = datetime.now(timezone.utc).date()
        oldest = today - timedelta(days=6)
        if chosen.date() < oldest or chosen.date() > today:
            await event.answer("Dashboard is limited to the last 7 days.", alert=True)
            return
        await render_owner_dashboard(event, int(user_id), selected_day=chosen)
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
        sender = await event.get_sender()
        user_id = getattr(sender, "id", None)
        if user_id is None:
            await event.answer("User not found.", alert=True)
            return
        await event.answer()
        if not include_clone:
            await show_clone_redirect(event, payload="clones", edit=True)
            return
        await render_clone_list(event, int(user_id))
        return

    if data == "clone:list":
        sender = await event.get_sender()
        user_id = getattr(sender, "id", None)
        if user_id is None:
            await event.answer("User not found.", alert=True)
            return
        await event.answer()
        if not include_clone:
            await show_clone_redirect(event, payload="clones", edit=True)
            return
        await render_clone_list(event, int(user_id))
        return

    if data.startswith("clone:stats:"):
        if not include_clone:
            await event.answer()
            await show_clone_redirect(event, payload="clones", edit=True)
            return
        await event.answer()
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
        if not include_clone:
            await event.answer()
            await show_clone_redirect(event, payload="clones", edit=True)
            return
        await event.answer()
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
        if not include_clone:
            await event.answer()
            await show_clone_redirect(event, payload="clones", edit=True)
            return
        await event.answer()
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
        await event.edit(SCAN_TEXT, parse_mode="html", buttons=scan_page_buttons())
        return

    if data in {"clone:create", "home:clone"}:
        await event.answer()
        if include_clone:
            await begin_clone_setup(event)
        else:
            await show_clone_redirect(event, payload="clone", edit=True)
        return


    if data == "clone:cancel":
        await event.answer()
        sender = await event.get_sender()
        user_id = getattr(sender, "id", None)
        if user_id is not None:
            clone_setup_pending.pop(int(user_id), None)
        home_text = HOME_TEXT if include_clone else CLONE_HOME_TEXT
        await event.edit(
            home_text,
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
        await event.edit(ABOUT_TEXT, parse_mode="html", buttons=about_buttons())
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
            queue_full = (
                active_processes >= max(1, adaptive_scan_limit())
                or any(token not in processing_scan_tokens for token in scan_queue_order)
            )

        if queue_full:
            await event.answer("Your scan is queued. Its QUEUE number updates automatically.")
        else:
            await event.answer("Metadata scan starting…")

        status_message = await event.get_message()
        await edit_status(
            status_message,
            "⏳ <b>QUEUE — REGISTERING SCAN</b>\n"
            "📄 <b>Waiting for queue position…</b>\n\n"
            "<code>[▰▱▱▱▱▱▱▱▱▱] 10%</code>\n"
            "Preparing your scan. Queue position will appear shortly.",
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
        ("instagram", "Instagram", "@AniToonHQ", "https://www.instagram.com/AniToonHQ"),
        ("youtube", "YouTube", "AniToon HQ", "https://www.youtube.com/channel/UC5LrPauKQX6PkO8mLd-DxEg"),
    ]

    def card(icon: str, name: str, url: str) -> str:
        images = {
            "Movies": "https://images.unsplash.com/photo-1489599849927-2ee91cede3ba?auto=format&fit=crop&w=1200&q=82",
            "All Animes": "https://commons.wikimedia.org/wiki/Special:FilePath/Mahuri.jpg",
            "Dual Content": "https://commons.wikimedia.org/wiki/Special:FilePath/Luffy.jpg",
            "Manga": "https://commons.wikimedia.org/wiki/Special:FilePath/Manga%20%281%29.jpg",
            "One Piece": "https://commons.wikimedia.org/wiki/Special:FilePath/Luffy.jpg",
            "Jujutsu Kaisen": "https://commons.wikimedia.org/wiki/Special:FilePath/Sketch%20of%20gojo%20satoru.jpg",
            "Naruto Shippuden": "https://commons.wikimedia.org/wiki/Special:FilePath/Naruto%20%2820858980350%29.jpg",
            "Doraemon": "https://commons.wikimedia.org/wiki/Special:FilePath/Doraemon%20at%20National%20Museum%20of%20Singapore..jpg",
            "Shin-Chan": "https://commons.wikimedia.org/wiki/Special:FilePath/Shinchan.jpg",
            "Beyblade": "https://commons.wikimedia.org/wiki/Special:FilePath/Beyblade.jpg",
            "Pokemon": "https://commons.wikimedia.org/wiki/Special:FilePath/Pokémon%20%288941002724%29.jpg",
        }
        image = images.get(name, "")
        return (
            f'<a class="channel channel-picture" style="--channel-image:url(\"{html.escape(image)}\")" '
            f'data-channel="{html.escape(name)}" href="{html.escape(url)}" target="_blank" rel="noopener noreferrer">'
            f'<span class="channel-picture-shade"></span>'
            f'<span class="channel-picture-content"><span class="channel-name">{html.escape(name)}</span>'
            f'<small>{("Anime" if name in {"All Animes","Dual Content","One Piece","Jujutsu Kaisen","Naruto Shippuden"} else "Cartoon" if name in {"Doraemon","Shin-Chan","Beyblade","Pokemon"} else "Manga" if name=="Manga" else "Movies")}</small></span>'
            f'<span class="channel-arrow">↗</span>'
            f'</a>'
        )

    current_html = "".join(card(*item) for item in channels)
    completed_html = "".join(card(*item) for item in completed)

    social_logos = {
        "instagram": (
            '<span class="social-icon social-instagram" aria-hidden="true">'
            '<svg viewBox="0 0 24 24" role="img"><defs>'
            '<linearGradient id="igGradient" x1="0" y1="1" x2="1" y2="0">'
            '<stop offset="0%" stop-color="#FEDA75"/><stop offset="35%" stop-color="#FA7E1E"/>'
            '<stop offset="62%" stop-color="#D62976"/><stop offset="82%" stop-color="#962FBF"/>'
            '<stop offset="100%" stop-color="#4F5BD5"/>'
            '</linearGradient></defs><rect x="2.2" y="2.2" width="19.6" height="19.6" rx="5.4" fill="url(#igGradient)"/>'
            '<rect x="7" y="7" width="10" height="10" rx="3.2" fill="none" stroke="#fff" stroke-width="1.8"/>'
            '<circle cx="12" cy="12" r="2.7" fill="none" stroke="#fff" stroke-width="1.8"/>'
            '<circle cx="17.2" cy="6.9" r="1.15" fill="#fff"/></svg></span>'
        ),
        "youtube": (
            '<span class="social-icon social-youtube" aria-hidden="true">'
            '<svg viewBox="0 0 24 24" role="img"><rect x="2" y="5.2" width="20" height="13.6" rx="4.1" fill="#FF0000"/>'
            '<path d="M10.1 8.6 16 12l-5.9 3.4Z" fill="#fff"/></svg></span>'
        ),
    }
    social_html = "".join(
        f'<a class="social-channel social-{icon}" href="{html.escape(url)}" target="_blank" rel="noopener noreferrer">'
        f'{social_logos.get(icon, "")}'
        f'<span class="social-main"><strong>{html.escape(name)}</strong><small>{html.escape(handle)}</small></span>'
        f'<span class="channel-arrow">↗</span>'
        f'</a>'
        for icon, name, handle, url in socials
    )

    command_items = [
        ("/help", "Help and command guide", "help", "📖"),
        ("/about", "About AniToon", "about", "ℹ️"),
        ("/scan_files", "How to scan files", "scan_files", "🔎"),
        ("/stats", "Your scan statistics", "stats", "📊"),
        ("/clone", "Open Clone Manager", "clones", "🤖"),
        ("/addtogroup", "Add the bot to a group", "addtogroup", "➕"),
    ]
    bot_link = f"https://t.me/{html.escape(BOT_USERNAME, quote=True)}"
    command_html = "".join(
        f'<a class="command-card" href="{bot_link}?start={payload}" target="_blank" rel="noopener noreferrer">'
        f'<span class="command-icon" aria-hidden="true">{icon}</span>'
        f'<span class="command-copy"><b>{html.escape(command)}</b><small>{html.escape(description)}</small></span>'
        f'<span class="command-arrow" aria-hidden="true">↗</span></a>'
        for command, description, payload, icon in command_items
    )

    media_guide_html = """
    <section class="section">
      <div class="section-head">
        <div><small>SUPPORTED TELEGRAM MEDIA</small><h2>🧾 What AniToon checks</h2></div>
      </div>
      <div class="media-guide-wrap">
        <table class="media-guide">
          <thead><tr><th>Media / format</th><th>What AniToon checks</th><th>How to use it</th></tr></thead>
          <tbody>
            <tr><td>📷 Photos & images</td><td>Image dimensions, file size, format and available image metadata.</td><td>Send the photo/image, tap <b>Scan File Info</b>, then open the web report.</td></tr>
            <tr><td>🎬 Videos & movies</td><td>Duration, dimensions, frame rate, codecs, container and available video/audio tracks.</td><td>Send the video or video file and scan it. Detailed codec data depends on what can be read.</td></tr>
            <tr><td>🎧 Music & audio files</td><td>Duration, audio codec, channels, sample rate, bitrate and tags when available.</td><td>Send an audio file or music document, then open the report.</td></tr>
            <tr><td>🎙️ Voice messages</td><td>Available audio properties, duration, file size and encoding details.</td><td>Send/forward the voice message to the bot and scan it.</td></tr>
            <tr><td>⭕ Video notes</td><td>Available video duration, dimensions and stream details.</td><td>Send the round video message and use Scan File Info.</td></tr>
            <tr><td>🌀 GIFs & animations</td><td>Animation/video properties and file details exposed by Telegram.</td><td>Send the GIF/animation as a Telegram attachment, then scan it.</td></tr>
            <tr><td>🌟 Stickers & custom emoji</td><td>Sticker format (such as static, animated or video-based), dimensions and available file attributes.</td><td>Send the sticker to the bot and scan the message. Some custom-emoji details may be limited.</td></tr>
            <tr><td>📄 PDFs & office documents</td><td>File name, size, MIME type and document properties supported by the file inspector; PDFs can include page and document metadata.</td><td>Send the document as an attachment and open its report.</td></tr>
            <tr><td>🗜️ ZIPs, archives & other files</td><td>Filename, extension, size, MIME type and general metadata available without unpacking unsupported contents.</td><td>Send as a document. AniToon does not promise to inspect every file inside an archive.</td></tr>
            <tr><td>💬 Subtitle files</td><td>Subtitle stream details when embedded in media, plus basic file metadata for separate subtitle files.</td><td>Send the subtitle file or a media file containing subtitle tracks.</td></tr>
            <tr><td>📍 Other Telegram attachments</td><td>Forwards and supported attachment types may expose captions, sender/post context, links or general metadata.</td><td>Send/forward the message. Unsupported message types may only show limited details.</td></tr>
          </tbody>
        </table>
      </div>
      <p style="padding:0 16px 14px;margin:0;color:var(--muted);font-size:11px">AniToon reports information available from Telegram and the file itself. Results vary by format, file size and embedded metadata; it cannot guarantee every field for every file.</p>
    </section>
    """
    
    report_embed = ""
    if report_token:
        safe_token = html.escape(report_token)
        report_embed = f"""
        <section class="section report-section">
          <div class="section-head">
            <div>
              <small>LIVE REPORT</small>
              <h2>🔬 File Metadata</h2>
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
<meta name="description" content="AniToon Media Info — anime channels and Telegram file metadata tools.">
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
.command-grid{{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:9px;padding:13px}}
.command-card{{display:flex;align-items:center;gap:10px;min-height:70px;padding:12px;border:1px solid var(--line);border-radius:15px;color:var(--text);text-decoration:none;background:linear-gradient(145deg,rgba(255,255,255,.045),rgba(255,255,255,.012));transition:.2s ease}}
.command-card:hover{{transform:translateY(-3px);border-color:rgba(154,140,255,.34);box-shadow:0 14px 30px rgba(0,0,0,.2)}}
.command-icon{{display:grid;place-items:center;width:36px;height:36px;flex:0 0 auto;border-radius:11px;background:rgba(154,140,255,.10);border:1px solid rgba(154,140,255,.16);font-size:18px}}
.command-copy{{display:flex;flex-direction:column;gap:2px;min-width:0}}
.command-copy b{{font-size:12px;overflow-wrap:anywhere}}
.command-copy small{{font-size:10px;color:var(--muted);line-height:1.35}}
.command-arrow{{margin-left:auto;color:var(--b);font-size:16px}}
.media-guide-wrap{{overflow-x:auto;padding:13px}}
.media-guide{{width:100%;border-collapse:collapse;min-width:640px;text-align:left}}
.media-guide th,.media-guide td{{padding:11px 12px;border-bottom:1px solid var(--line);vertical-align:top}}
.media-guide th{{color:var(--b);font-size:10px;letter-spacing:.06em;text-transform:uppercase;background:rgba(255,255,255,.035)}}
.media-guide td{{font-size:12px;color:var(--text)}}
.media-guide td:first-child{{font-weight:850;white-space:nowrap}}
.media-guide td:last-child{{color:var(--muted)}}
.channel{{display:flex;align-items:center;gap:12px;min-height:58px;padding:11px 12px;border:1px solid var(--line);border-radius:16px;color:var(--text);text-decoration:none;background:linear-gradient(145deg,rgba(255,255,255,.035),rgba(255,255,255,.012));transition:.20s ease}}
.channel:hover{{transform:translateY(-4px);border-color:rgba(255,255,255,.30);box-shadow:0 18px 38px rgba(0,0,0,.30)}}
.start-hero{{animation:startReveal .8s cubic-bezier(.2,1,.2,1) both}}
.channel-picture{{position:relative;min-height:112px;overflow:hidden;isolation:isolate;background:#090b16 center/cover no-repeat; background-image:var(--channel-image);border-color:rgba(255,255,255,.12)}}
.channel-picture-shade{{position:absolute;inset:0;background:linear-gradient(180deg,rgba(0,0,0,.08),rgba(0,0,0,.12) 35%,rgba(3,4,11,.84) 100%),linear-gradient(90deg,rgba(4,5,12,.58),transparent 65%);transition:.3s ease}}
.channel-picture-content{{position:relative;z-index:2;display:flex;flex-direction:column;justify-content:flex-end;align-items:flex-start;gap:2px;min-height:112px;width:100%;padding:14px}}
.channel-picture-content .channel-name{{font-size:15px;line-height:1.1;text-shadow:0 2px 12px rgba(0,0,0,.75)}}
.channel-picture-content small{{font-size:8px;font-weight:900;letter-spacing:.12em;text-transform:uppercase;color:rgba(255,255,255,.68)}}
.channel-picture .channel-arrow{{position:absolute;top:10px;right:12px;z-index:3;color:#fff;text-shadow:0 2px 12px rgba(0,0,0,.65)}}
.channel-picture:hover{{background-size:110%;transform:translateY(-4px)}}
.channel-picture:hover .channel-picture-shade{{background:linear-gradient(180deg,rgba(0,0,0,.02),rgba(0,0,0,.12) 30%,rgba(3,4,11,.72) 100%),linear-gradient(90deg,rgba(4,5,12,.42),transparent 65%)}}
.channel-picture::after{{content:"";position:absolute;inset:0;z-index:1;background:linear-gradient(110deg,transparent 25%,rgba(255,255,255,.12) 46%,transparent 62%);background-size:220% 100%;animation:pictureSheen 6.5s ease-in-out infinite;pointer-events:none}}
@keyframes startReveal{{from{{opacity:0;transform:translateY(14px)}}to{{opacity:1;transform:none}}}}
@keyframes artFloat{{0%,100%{{transform:translateY(0)}}50%{{transform:translateY(-5px)}}}}
@keyframes pictureSheen{{0%,18%{{background-position:-120% 0}}65%,100%{{background-position:120% 0}}}}
.channel-icon{{width:36px;height:36px;display:grid;place-items:center;flex:0 0 auto;border-radius:12px;background:rgba(154,140,255,.08);border:1px solid rgba(154,140,255,.13);font-size:18px}}
.channel-name{{flex:1;min-width:0;font-weight:800}}
.channel-arrow{{color:var(--muted);font-size:18px}}
.social-channel{{display:flex;align-items:center;gap:12px;min-height:72px;padding:12px 13px;border:1px solid var(--line);border-radius:16px;color:var(--text);text-decoration:none;background:linear-gradient(145deg,rgba(255,255,255,.045),rgba(255,255,255,.012));transition:.20s ease}}
.social-channel:hover{{transform:translateY(-3px);border-color:rgba(94,231,255,.30);box-shadow:0 14px 30px rgba(0,0,0,.22)}}
.social-icon{{width:42px;height:42px;display:grid;place-items:center;flex:0 0 auto;border-radius:13px;border:1px solid rgba(255,255,255,.13);overflow:hidden;box-shadow:0 7px 18px rgba(0,0,0,.20)}} .social-icon svg{{width:100%;height:100%;display:block}} .social-icon.social-instagram{{background:linear-gradient(145deg,#ffdc80,#fcaf45 28%,#e1306c 62%,#833ab4 82%,#5851db)}} .social-icon.social-youtube{{background:#ff0000}} .social-instagram .social-icon{{background:transparent}}
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
@media(max-width:700px){{.list,.command-grid{{grid-template-columns:1fr}}.hero{{padding:24px 18px;border-radius:22px}}.footer{{flex-direction:column;align-items:flex-start}}.report-section iframe{{height:2050px}}}}
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

  <header class="hero start-hero">
    <span class="eyebrow"><span class="dot"></span> Media tools</span>
    <h1>⛩ AniToon</h1>
    <div class="lead">A lightweight hub for Telegram file reports, useful media tools and anime communities.</div>
    <div class="cta-row">
      <a class="cta primary" href="https://t.me/AniToon_1Bot" target="_blank" rel="noopener noreferrer">🤖 Open AniToon Bot</a>
      <a class="cta" href="https://t.me/Anitoon_group" target="_blank" rel="noopener noreferrer">👥 Community</a>
    </div>
  </header>

  <section class="section">
    <div class="section-head">
      <div><small>BOT SHORTCUTS</small><h2>⌨️ Commands & Tools</h2></div>
      <a class="mini-link" href="https://t.me/{BOT_USERNAME}" target="_blank" rel="noopener noreferrer">Open Bot ↗</a>
    </div>
    <div class="command-grid">{command_html}</div>
  </section>

  {media_guide_html}

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
    <span><b>AniToon</b> • Media tools</span>
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
    report = _coerce_report(report)
    is_pdf = (
        str(report.ext or "").lower() == ".pdf"
        or str(report.detected or "").lower().startswith("pdf")
        or str(report.media_kind or "").lower() == "pdf"
    )
    filename = html.escape(report.filename or "Telegram media file")

    audio = list(report.audio.get("tracks", []) or [])[:MAX_REPORT_TRACKS]
    video = list(report.video.get("tracks", []) or [])[:MAX_REPORT_TRACKS]
    subtitles = list(report.subtitles or [])[:MAX_REPORT_TRACKS]

    def esc(value: Any) -> str:
        return html.escape(str(value))

    def pretty_value(value: Any) -> str:
        if isinstance(value, bool):
            return "Yes" if value else "No"
        text = str(value)
        low = text.strip().lower()
        if low == "yes":
            return "Yes"
        if low == "no":
            return "No"
        return text

    def human_size(value: Any) -> str:
        try:
            size = float(value)
        except Exception:
            return "—"
        units = ("B", "KB", "MB", "GB", "TB")
        i = 0
        while size >= 1024 and i < len(units) - 1:
            size /= 1024
            i += 1
        return f"{size:.1f} {units[i]}" if i else f"{int(size)} {units[i]}"

    def icon_for(kind: str) -> str:
        return {"Video": "🎬", "Audio": "🎧", "Subtitle": "💬"}.get(kind, "◈")

    media_info = dict(report.media_info) if isinstance(getattr(report, "media_info", None), dict) else {}
    if is_pdf:
        pdf_container = report.container if isinstance(report.container, dict) else {}
        pdf_fields = {
            "pdf_version": pdf_container.get("pdf_version"),
            "pdf_pages": pdf_container.get("pages"),
            "pdf_page_size": pdf_container.get("page_size_name") or pdf_container.get("page_size_points"),
            "pdf_title": pdf_container.get("title"),
            "pdf_author": pdf_container.get("author"),
            "pdf_subject": pdf_container.get("subject"),
            "pdf_keywords": pdf_container.get("keywords"),
            "pdf_creator": pdf_container.get("creator"),
            "pdf_producer": pdf_container.get("producer"),
            "pdf_creation_date": pdf_container.get("creation_date"),
            "pdf_modified_date": pdf_container.get("modified_date"),
            "pdf_encrypted": pdf_container.get("encrypted"),
            "pdf_tagged": pdf_container.get("tagged"),
            "pdf_linearized": pdf_container.get("linearized"),
            "pdf_forms": pdf_container.get("forms"),
            "pdf_javascript": pdf_container.get("javascript"),
            "pdf_embedded_files": pdf_container.get("embedded_files"),
            "pdf_fonts": pdf_container.get("fonts"),
        }
        for key, value in pdf_fields.items():
            if value not in (None, "", []):
                media_info.setdefault(key, value)

    media_label_overrides = {
        "category": "Media category",
        "detected_format": "Detected format",
        "telegram_media_type": "Telegram media type",
        "file_extension": "File extension",
        "declared_mime_type": "MIME type",
        "file_size_bytes": "File size (bytes)",
        "image_pixel_dimensions": "Image pixels",
        "image_format": "Image format",
        "image_color_mode": "Color mode",
        "image_frame_count": "Frame count",
        "image_is_animated": "Animated",
        "image_frame_duration_ms": "Frame duration (ms)",
        "image_has_transparency": "Transparency",
        "image_progressive": "Progressive image",
        "image_exif_summary": "Safe EXIF summary",
        "sticker_alt_text": "Sticker emoji / alt text",
        "sticker_set": "Sticker set",
        "sticker_is_mask": "Mask sticker",
        "sticker_is_animated": "Animated sticker",
        "video_width": "Video width",
        "video_height": "Video height",
        "video_duration_seconds": "Video duration (seconds)",
        "supports_streaming": "Supports streaming",
        "video_is_round_note": "Round video note",
        "video_is_silent": "Silent video",
        "audio_title": "Audio title",
        "audio_performer": "Performer / artist",
        "audio_duration_seconds": "Audio duration (seconds)",
        "audio_is_voice": "Voice message",
        "audio_is_music": "Music / track",
        "audio_has_waveform": "Voice waveform present",
        "pdf_version": "PDF version",
        "pdf_pages": "Pages",
        "pdf_page_size": "Page size",
        "pdf_encrypted": "Encrypted",
        "pdf_tagged": "Tagged document",
        "pdf_linearized": "Linearized",
        "pdf_forms": "Forms",
        "pdf_javascript": "JavaScript",
        "pdf_embedded_files": "Embedded files",
        "pdf_fonts": "Fonts detected",
        "pdf_title": "Title",
        "pdf_author": "Author",
        "pdf_subject": "Subject",
        "pdf_keywords": "Keywords",
        "pdf_creator": "Creator",
        "pdf_producer": "Producer",
        "pdf_creation_date": "Creation date",
        "pdf_modified_date": "Modified date",
        "caption_preview": "Caption preview",
        "caption_characters": "Caption characters",
        "telegram_message_id": "Telegram message ID",
        "message_date_utc": "Message date (UTC)",
        "post_author": "Post author",
        "views": "Views",
        "forwards": "Forwards",
        "replies": "Replies",
        "grouped_media_album": "Part of a media album",
        "is_channel_post": "Channel post",
        "forwarded_from_name": "Forwarded from",
        "forwarded_post_number": "Original post number",
        "forwarded_date_utc": "Original post date (UTC)",
        "game_title": "Game title",
        "game_description": "Game description",
        "invoice_title": "Invoice title",
        "invoice_currency": "Invoice currency",
        "invoice_total_amount": "Invoice total amount",
        "custom_emoji_alt_text": "Custom emoji",
        "webpage_title": "Web page title",
        "webpage_site_name": "Web page site",
        "webpage_url": "Web page URL",
        "webpage_type": "Web page type",
        "webpage_description": "Web page description",
        "poll_question": "Poll question",
        "poll_options": "Poll options",
        "poll_option_count": "Poll option count",
        "poll_is_quiz": "Quiz poll",
        "poll_multiple_answers": "Multiple poll answers",
        "poll_voter_count": "Voter count",
        "location_coordinates_present": "Location attached",
        "venue_title": "Venue title",
        "venue_provider": "Venue provider",
        "venue_address_present": "Venue address present",
        "contact_name": "Contact name",
        "contact_phone_present": "Contact phone present",
        "dice_value": "Dice value",
    }
    def media_group_for(key: str, value: Any) -> str:
        low_key = key.lower()
        category = str(media_info.get("category") or report.media_kind or "").lower()
        filename_low = str(report.filename or "").lower()
        is_gif_animation = (
            "gif" in category or "animation" in category or filename_low.endswith(".gif")
            or str(media_info.get("image_format") or "").lower() == "gif"
            or str(media_info.get("image_is_animated") or "").lower() in {"yes", "true", "1"}
        )
        if low_key.startswith("sticker_"):
            return "Sticker"
        if low_key.startswith("pdf_"):
            if low_key in {"pdf_encrypted", "pdf_tagged", "pdf_linearized", "pdf_forms", "pdf_javascript", "pdf_embedded_files"}:
                return "PDF security"
            return "PDF document"
        if low_key.startswith("audio_"):
            return "Audio & music"
        if low_key.startswith("video_"):
            return "Video & animation"
        if low_key.startswith("image_"):
            if is_gif_animation and any(part in low_key for part in ("frame", "animated", "format")):
                return "GIF & animation"
            return "Image details"
        if low_key.startswith("webpage_"):
            return "Web page preview"
        if low_key.startswith("poll_"):
            return "Poll & quiz"
        if low_key.startswith(("location_", "venue_")):
            return "Location & venue"
        if low_key.startswith("contact_"):
            return "Contact card"
        if low_key.startswith("game_"):
            return "Game details"
        if low_key.startswith("invoice_"):
            return "Invoice details"
        if low_key.startswith("custom_emoji"):
            return "Emoji details"
        if low_key.startswith(("caption_", "telegram_", "message_", "post_", "forwarded_", "views", "forwards", "replies", "grouped_media", "is_channel_")):
            return "Telegram message & post"
        return "File & container metadata"

    media_groups: dict[str, list[tuple[str, Any]]] = {}
    omit_from_report = {
        # Already visible in the hero, summary cards, or stream cards.
        "category", "detected_format", "file_extension", "declared_mime_type",
        "file_size_bytes", "archive_directory_preview", "image_format",
        "image_pixel_dimensions", "video_width", "video_height", "dimensions",
        "video_duration_seconds", "audio_duration_seconds", "caption_characters",
        "has_caption", "is_document", "telegram_media_type", "telegram_message_id",
        "telegram_filename_attribute", "pdf_creator", "pdf_producer", "pdf_fonts",
        "pdf_linearized", "image_color_mode", "image_progressive", "webpage_type",
        # Keep meaningful caption/author/views/forwards and forwarding provenance,
        # but hide Telegram implementation IDs and simple presence flags.
        "is_channel_post", "grouped_media_album", "contact_phone_present",
        "venue_address_present", "location_coordinates_present",
        "archive_directory_preview",
    }
    # Negative capability flags produce clutter (e.g. "Animated: No" on every
    # still image). Positive flags remain visible; file/security essentials stay.
    omit_false_flags = {
        "image_is_animated", "image_has_transparency", "sticker_is_mask",
        "sticker_is_animated", "video_is_round_note", "video_is_silent",
        "audio_is_voice", "audio_is_music", "audio_has_waveform",
        "pdf_tagged", "pdf_forms", "pdf_javascript", "pdf_embedded_files",
        "poll_is_quiz", "poll_multiple_answers",
    }
    if is_pdf:
        # Page count already appears in the PDF overview card.
        omit_from_report.add("pdf_pages")
    for key, value in media_info.items():
        if key in omit_from_report or value in (None, "", []):
            continue
        if key in omit_false_flags and str(value).strip().lower() in {"no", "false", "0", "none"}:
            continue
        group_name = media_group_for(str(key), value)
        media_groups.setdefault(group_name, []).append(
            (media_label_overrides.get(key, str(key).replace("_", " ").title()), value)
        )

    group_icons = {
        "GIF & animation": "🎞️", "Image details": "🖼️", "Sticker": "🌟",
        "PDF document": "📄", "PDF security": "🔐", "Audio & music": "🎧", "Video & animation": "🎬",
        "Web page preview": "🌐", "Poll & quiz": "📊", "Location & venue": "📍",
        "Contact card": "👤", "Game details": "🎮", "Invoice details": "🧾",
        "Emoji details": "✨", "Telegram message & post": "📨",
        "File & container metadata": "🧩",
    }
    media_detail_sections_list: list[str] = []
    media_detail_nav: list[str] = []
    for group_name, fields in media_groups.items():
        section_id = "media-" + re.sub(r"[^a-z0-9]+", "-", group_name.lower()).strip("-")
        cards = "".join(
            f'<div class="tech"><small>{esc(label)}</small><b>{esc(pretty_value(value))}</b></div>'
            for label, value in fields
        )
        media_detail_sections_list.append(
            f'<details id="{section_id}" class="section media-type-section" open>'
            f'<summary class="section-head"><h2>{group_icons.get(group_name, "🧩")} {esc(group_name)}</h2>'
            f'<span class="pill">{len(fields)} details <span class="chev">⌄</span></span></summary>'
            f'<div class="section-body"><div class="tech-grid">{cards}</div></div></details>'
        )
        media_detail_nav.append(f'<a href="#{section_id}">{esc(group_name)}</a>')
    media_details_sections = "".join(media_detail_sections_list)

    # ZIPs get a dedicated directory listing; contents are never extracted.
    archive_entries = [
        item for item in (getattr(report, "archive_entries", []) or [])
        if isinstance(item, dict)
    ][:MAX_REPORT_ARCHIVE_ENTRIES]
    is_zip_archive = (
        str(report.ext or "").lower() == ".zip"
        or "zip archive" in str(report.detected or "").lower()
    )
    archive_contents_section = ""
    if is_zip_archive:
        try:
            archive_entry_count = max(
                len(archive_entries),
                int(media_info.get("archive_entry_count") or len(archive_entries)),
            )
        except (TypeError, ValueError):
            archive_entry_count = len(archive_entries)

        try:
            uncompressed_bytes = int(media_info.get("archive_uncompressed_bytes") or 0)
        except (TypeError, ValueError):
            uncompressed_bytes = 0
        try:
            compressed_bytes = int(media_info.get("archive_compressed_bytes") or 0)
        except (TypeError, ValueError):
            compressed_bytes = 0

        archive_stats = []
        if "archive_uncompressed_bytes" in media_info:
            archive_stats.append(("Original size", human_size(uncompressed_bytes)))
        if "archive_compressed_bytes" in media_info:
            archive_stats.append(("Compressed size", human_size(compressed_bytes)))
        if uncompressed_bytes > 0 and compressed_bytes < uncompressed_bytes:
            archive_stats.append((
                "Space saved",
                f"{(1 - compressed_bytes / uncompressed_bytes) * 100:.1f}%",
            ))
        encrypted_flag = str(
            media_info.get("archive_contains_encrypted_members") or ""
        ).strip().lower()
        if encrypted_flag in {"yes", "true", "1"}:
            archive_stats.append(("Password-protected entries", "Yes"))

        archive_stat_cards = "".join(
            f'<div class="tech"><small>{esc(label)}</small><b>{esc(value)}</b></div>'
            for label, value in archive_stats
        )

        archive_rows: list[str] = []
        for entry in archive_entries:
            try:
                entry_size = int(entry.get("size") or 0)
            except (TypeError, ValueError):
                entry_size = 0
            try:
                entry_compressed = int(entry.get("compressed_size") or 0)
            except (TypeError, ValueError):
                entry_compressed = 0
            is_directory = bool(entry.get("is_dir"))
            file_type = "Folder" if is_directory else str(entry.get("file_type") or "File")
            audio_names = entry.get("audio_names") or []
            subtitle_names = entry.get("subtitle_names") or []
            if isinstance(audio_names, str):
                audio_names = [audio_names]
            if isinstance(subtitle_names, str):
                subtitle_names = [subtitle_names]
            audio_html = "<br>".join(esc(name) for name in audio_names if name) or "—"
            subtitle_html = "<br>".join(esc(name) for name in subtitle_names if name) or "—"
            archive_rows.append(
                '<tr>'
                f'<td class="archive-name"><code>{esc(entry.get("name") or "Unnamed entry")}</code></td>'
                f'<td>{esc(file_type)}</td>'
                f'<td>{esc(human_size(entry_size))}</td>'
                f'<td>{esc(human_size(entry_compressed))}</td>'
                f'<td class="archive-track-names">{audio_html}</td>'
                f'<td class="archive-track-names">{subtitle_html}</td>'
                f'<td>{esc(entry.get("modified_at") or "—")}</td>'
                '</tr>'
            )
        if not archive_rows:
            archive_rows.append(
                '<tr><td colspan="7">No readable ZIP directory was found in the bounded scan. '
                'The archive may be damaged or too large for the current metadata scan.</td></tr>'
            )
        if archive_entry_count > len(archive_entries):
            archive_note = (
                f"Showing the first {len(archive_entries)} of {archive_entry_count} entries. "
            )
        else:
            archive_note = f"Showing {len(archive_entries)} entries. "
        archive_note += "Embedded audio/subtitle names are shown when the archive format and readable file prefix expose them."
        archive_contents_section = (
            '<details id="archive-contents" class="section" open>'
            '<summary class="section-head"><h2>📦 ZIP contents</h2>'
            f'<span class="pill">{archive_entry_count} entries <span class="chev">⌄</span></span></summary>'
            '<div class="section-body">'
            f'<div class="tech-grid">{archive_stat_cards}</div>'
            '<p class="archive-note">' + esc(archive_note) + '</p>'
            '<div class="archive-table-wrap"><table class="archive-table">'
            '<thead><tr><th>Name / path</th><th>Type</th><th>Size</th><th>Compressed</th>'
            '<th>Audio track names</th><th>Subtitle names</th><th>Modified</th></tr></thead>'
            f'<tbody>{"".join(archive_rows)}</tbody></table></div>'
            '</div></details>'
        )

    def track_cards(items: list[dict[str, Any]], kind: str) -> str:
        if not items:
            return (
                f'<div class="empty-state">'
                f'<span class="empty-icon">{icon_for(kind)}</span>'
                f'<div><strong>0 {esc(kind.lower())} tracks</strong>'
                f'<p>This report contains 0 detected {esc(kind.lower())} streams.</p></div>'
                f'</div>'
            )

        cards: list[str] = []
        for index, track in enumerate(items, 1):
            name = track.get("name") or track.get("display_name") or f"{kind} Track {index}"
            language = track.get("language_name") or track.get("language")
            codec = track.get("codec_name") or track.get("codec")
            details: list[tuple[str, Any]] = []

            priority_details = [
                ("Language", (track.get("language_name") or track.get("language")) if kind in {"Audio", "Subtitle"} else None),
                ("Codec", track.get("codec_name") or track.get("codec")),
                ("Resolution / pixels", track.get("dimensions")),
                ("Display size", track.get("display_dimensions")),
                ("Frame rate", track.get("frame_rate")),
                ("Scan type", track.get("scan_type")),
                ("Pixel format", track.get("pixel_format")),
                ("Profile", track.get("profile")),
                ("Level", track.get("level")),
                ("Channel layout", track.get("layout")),
                ("Channels", track.get("channels")),
                ("Sample rate", track.get("sample_rate")),
                ("Duration", track.get("duration")),
                ("Bit depth", track.get("bit_depth")),
                ("Bitrate", track.get("bitrate")),
                ("Stereo mode", track.get("stereo_mode")),
                ("Format", track.get("subtitle_format") or track.get("format")),
            ]
            details.extend(
                (label, pretty_value(value))
                for label, value in priority_details
                if value is not None and value != "" and value != []
            )
            known_keys = {
                "language","language_name","codec","codec_name","dimensions","display_dimensions",
                "frame_rate","scan_type","pixel_format","profile","level","channels","layout",
                "sample_rate","bit_depth","bitrate","default","enabled","forced","original",
                "commentary","hearing_impaired","visual_impaired","stereo_mode","codec_delay",
                "seek_preroll","subtitle_format","format","track_uid","duration"
            }
            for key, value in track.items():
                if key in known_keys or key in {"type","track","name","display_name","name_source"}:
                    continue
                if kind == "Video" and key in {"language", "language_name"}:
                    continue
                if value is not None and value != "" and value != []:
                    details.append((key.replace("_", " ").title(), pretty_value(value)))
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
                <article class="track-card" style="--track-index:{index}">
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

    runtime = report.container.get("runtime") or "N/A"
    container_name = report.detected or "Detected media"
    mime = report.mime or "application/octet-stream"
    size_text = human_size(report.size)
    sampled_text = human_size(report.sampled)

    first_video = video[0] if video else {}
    quality = (
        first_video.get("dimensions")
        or media_info.get("image_pixel_dimensions")
        or media_info.get("dimensions")
        or "—"
    )
    primary_codec = first_video.get("codec_name") or first_video.get("codec") or "—"
    bitrate = report.container.get("average_bitrate") or "—"
    title = report.container.get("title")

    # Only summarize streams that were actually detected. Other file types get
    # the small, type-specific overview below instead of irrelevant zero-count tracks.
    media_category = str(media_info.get("category") or report.media_kind or "File")
    has_stream_report = bool(video or audio or subtitles)
    if has_stream_report:
        stream_summary_items: list[tuple[str, Any]] = []
        # File size and format already appear in the hero; keep only stream-specific facts.
        if video:
            stream_summary_items.append(("Video tracks", len(video)))
        if audio:
            stream_summary_items.append(("Audio tracks", len(audio)))
        if subtitles:
            stream_summary_items.append(("Subtitle tracks", len(subtitles)))
        if quality not in (None, "", "—"):
            stream_summary_items.append(("Primary quality / pixels", quality))
        if runtime not in (None, "", "N/A", "—"):
            stream_summary_items.append(("Runtime", runtime))
        summary_cards = "".join(
            f'<div class="stat"><b>{esc(value)}</b><span>{esc(label)}</span></div>'
            for label, value in stream_summary_items
        )
    else:
        extra_label, extra_value = (
            ("Frames", media_info.get("image_frame_count")) if media_info.get("image_frame_count") not in (None, "")
            else ("PDF pages", media_info.get("pdf_pages")) if media_info.get("pdf_pages") not in (None, "")
            else ("Poll options", media_info.get("poll_option_count")) if media_info.get("poll_option_count") not in (None, "")
            else ("", None)
        )
        # Category, format and size are already clear in the hero.
        summary_items = [
            ("Dimensions / pixels", media_info.get("image_pixel_dimensions") or media_info.get("dimensions") or "—"),
            (extra_label, extra_value),
        ]
        summary_cards = "".join(
            f'<div class="stat"><b>{esc(value)}</b><span>{esc(label)}</span></div>'
            for label, value in summary_items
            if value not in (None, "", "—")
        )

    # The preview is the automatic Telegram thumbnail generated from the file.
    # Keep exactly one thumbnail in the web report.
    previews = list(getattr(report, "previews", []) or [])[:1]
    hero_src = (
        "data:image/jpeg;base64," + str(previews[0].get("data") or "")
        if previews and previews[0].get("data")
        else ""
    )
    hero_alt = "Automatic file thumbnail" if previews and previews[0].get("data") else ""
    hero_label = "🎞️ Auto thumbnail from file" if previews and previews[0].get("data") else ""

    report_id = esc((report_token or "local")[:14])

    def stream_section(
        items: list[dict[str, Any]],
        kind: str,
        section_id: str,
        filter_key: str,
        placeholder: str,
    ) -> str:
        if not items:
            return ""
        title_text = f"{icon_for(kind)} {kind if kind != 'Subtitle' else 'Subtitles'}"
        count_text = f"{len(items)} track{'s' if len(items) != 1 else ''}"
        return (
            f'<details id="{section_id}" class="section" open>'
            f'<summary class="section-head"><h2>{title_text}</h2>'
            f'<span class="pill">{count_text} <span class="chev">⌄</span></span></summary>'
            f'<div class="section-body"><div class="toolbar">'
            f'<input class="search" data-filter="{filter_key}" placeholder="{placeholder}"></div>'
            f'<div data-track-list="{filter_key}">{track_cards(items, kind)}</div>'
            f'</div></details>'
        )

    video_section = stream_section(video, "Video", "video-section", "video", "Filter video tracks…")
    audio_section = stream_section(audio, "Audio", "audio-section", "audio", "Filter audio by language, codec, name…")
    subtitle_section = stream_section(subtitles, "Subtitle", "subs-section", "subtitle", "Filter subtitle tracks…")

    nav_links = ['<a href="#overview">Overview</a>'] + media_detail_nav
    if archive_contents_section:
        nav_links.append('<a href="#archive-contents">ZIP contents</a>')
    if video_section:
        nav_links.append('<a href="#video-section">Video</a>')
    if audio_section:
        nav_links.append('<a href="#audio-section">Audio</a>')
    if subtitle_section:
        nav_links.append('<a href="#subs-section">Subtitles</a>')

    technical_items = [
        # Hero/summary already contain type, size, runtime, and the primary format.
        # Retain only a useful detail not already shown elsewhere.
        ("Average bitrate", bitrate if bitrate != "—" else None),
    ]
    technical_cards = "".join(
        f'<div class="tech"><small>{esc(label)}</small><b>{esc(value)}</b></div>'
        for label, value in technical_items
        if value not in (None, "", "—")
    )
    technical_section = (
        '<details id="technical-section" class="section" open>'
        '<summary class="section-head"><h2>⚙️ Technical</h2>'
        '<span class="pill">File details <span class="chev">⌄</span></span></summary>'
        f'<div class="section-body"><div class="tech-grid">{technical_cards}</div></div></details>'
        if technical_cards else ""
    )
    if technical_section:
        nav_links.append('<a href="#technical-section">Technical</a>')
    quick_nav = '<nav class="quick" aria-label="Quick navigation">' + "".join(nav_links) + '</nav>'

    language_pills = []
    if audio:
        language_pills.extend(
            f'<span class="lang-pill">🎧 {esc(name)}</span>'
            for name in _language_names_for_report(report, "audio")[:12]
        )
    if subtitles:
        language_pills.extend(
            f'<span class="lang-pill">💬 {esc(name)}</span>'
            for name in _language_names_for_report(report, "subtitle")[:12]
        )
    language_ribbon = (
        '<div class="language-ribbon"><span class="language-label">LANGUAGES</span>'
        + "".join(language_pills) + "</div>"
        if language_pills else ""
    )

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
  --accent:#a78bfa;--cyan:#67e8f9;--pink:#f472b6;--violet:#8b5cf6;--good:#7cf4b0;--shadow:0 28px 80px rgba(0,0,0,.38);
}}
*{{box-sizing:border-box}}
html{{scroll-behavior:smooth}}
body{{margin:0;min-height:100vh;background:radial-gradient(circle at 12% 8%,rgba(139,92,246,.16),transparent 28%),radial-gradient(circle at 88% 18%,rgba(103,232,249,.12),transparent 25%),radial-gradient(circle at 50% 100%,rgba(244,114,182,.10),transparent 32%),var(--bg);color:var(--text);font:14px/1.55 Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;overflow-x:hidden}}
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
.hero{{position:relative;display:grid;grid-template-columns:minmax(0,1.2fr) minmax(320px,420px);gap:18px;overflow:hidden;padding:20px;border:1px solid var(--line);border-radius:28px;background:linear-gradient(145deg,rgba(17,20,46,.88),rgba(7,9,22,.68));box-shadow:var(--shadow);backdrop-filter:blur(22px)}}
.hero-copy{{min-width:0;padding:5px 2px}}
.eyebrow{{display:inline-flex;align-items:center;gap:7px;padding:6px 9px;border:1px solid rgba(124,244,176,.15);border-radius:999px;background:rgba(124,244,176,.06);color:var(--good);font-size:9px;font-weight:950;letter-spacing:.13em;text-transform:uppercase}}
.eyebrow i{{width:7px;height:7px;border-radius:50%;background:var(--good);box-shadow:0 0 14px rgba(124,244,176,.70);animation:pulse 1.8s ease-in-out infinite}}
h1{{margin:13px 0 7px;font-size:clamp(28px,4.8vw,48px);line-height:1.02;letter-spacing:-.045em}}
.file{{max-width:850px;color:#cbd0e4;font-size:13px;overflow-wrap:anywhere}}
.hero-pills{{display:flex;gap:7px;flex-wrap:wrap;margin-top:13px}}
.pill{{display:inline-flex;align-items:center;gap:6px;padding:7px 9px;border:1px solid var(--line);border-radius:999px;background:rgba(255,255,255,.035);color:var(--muted);font-size:10px}}
.countdown{{color:var(--cyan);font-weight:950}}
.hero-media{{position:relative;width:min(100%,400px);height:230px;min-height:0;max-height:230px;border-radius:20px;overflow:hidden;border:1px solid rgba(155,140,255,.34);background:linear-gradient(145deg,rgba(155,140,255,.12),rgba(94,231,255,.055));box-shadow:0 18px 46px rgba(0,0,0,.25),0 0 34px rgba(155,140,255,.10);transform:translateZ(0);animation:thumbFloat 6.5s ease-in-out infinite}}
.hero-media::before{{content:"";position:absolute;inset:-2px;border-radius:22px;padding:1px;background:conic-gradient(from 0deg,rgba(155,140,255,.0),rgba(155,140,255,.72),rgba(94,231,255,.72),rgba(124,244,176,.35),rgba(155,140,255,.0));-webkit-mask:linear-gradient(#000 0 0) content-box,linear-gradient(#000 0 0);-webkit-mask-composite:xor;mask-composite:exclude;animation:thumbRing 7s linear infinite;pointer-events:none;z-index:3}}
.hero-media::after{{content:"";position:absolute;inset:0;background:linear-gradient(115deg,transparent 18%,rgba(255,255,255,.08) 43%,transparent 59%),linear-gradient(180deg,transparent 45%,rgba(0,0,0,.48));background-size:220% 100%,100% 100%;background-position:-120% 0,0 0;background-repeat:no-repeat;animation:thumbSheen 6s ease-in-out infinite;pointer-events:none;z-index:2}}
.hero-media img{{display:block;width:100%;height:100%;max-height:none;min-height:0;object-fit:contain;object-position:center;background:rgba(0,0,0,.12);image-rendering:auto;transform:scale(1.005);transition:transform .55s cubic-bezier(.2,1,.2,1),filter .55s ease}}
.hero-media:hover{{animation-play-state:paused;box-shadow:0 22px 54px rgba(0,0,0,.30),0 0 44px rgba(94,231,255,.14)}}
.hero-media:hover img{{transform:scale(1.02);filter:saturate(1.04) contrast(1.02)}}
.media-label{{position:absolute;left:11px;bottom:10px;z-index:4;padding:6px 8px;border-radius:9px;background:rgba(5,6,17,.66);border:1px solid rgba(255,255,255,.12);backdrop-filter:blur(10px);font-size:10px;font-weight:900}}
.summary{{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:9px;margin-top:12px}}
.language-ribbon{{display:flex;gap:6px;flex-wrap:wrap;align-items:center;margin-top:10px;padding:9px 10px;border:1px solid var(--line);border-radius:14px;background:rgba(255,255,255,.025)}}
.language-label{{font-size:9px;font-weight:950;letter-spacing:.12em;color:var(--muted);margin-right:2px}}
.lang-pill{{display:inline-flex;align-items:center;gap:5px;padding:5px 7px;border-radius:999px;border:1px solid rgba(155,140,255,.18);background:linear-gradient(135deg,rgba(155,140,255,.10),rgba(94,231,255,.05));font-size:9px;font-weight:850;animation:badgeFloat 4s ease-in-out infinite alternate}}

.stat{{position:relative;overflow:hidden;padding:12px;border:1px solid var(--line);border-radius:15px;background:linear-gradient(145deg,rgba(255,255,255,.045),rgba(255,255,255,.018));transition:.25s ease;animation:cardIn .65s cubic-bezier(.2,1,.2,1) both}}\n.media-type-section{{animation:cardIn .55s cubic-bezier(.2,1,.2,1) both}}\n@media(prefers-reduced-motion:reduce){{*,*::before,*::after{{animation-duration:.01ms!important;animation-iteration-count:1!important;scroll-behavior:auto!important;transition-duration:.01ms!important}}}}
.stat::before{{content:"";position:absolute;inset:-40%;background:conic-gradient(from 180deg,transparent,rgba(155,140,255,.14),rgba(94,231,255,.12),transparent 55%);animation:statSpin 7s linear infinite;pointer-events:none}}
.stat b,.stat span{{position:relative;z-index:1}}
.stat:nth-child(1){{animation-delay:.05s}}.stat:nth-child(2){{animation-delay:.10s}}.stat:nth-child(3){{animation-delay:.15s}}.stat:nth-child(4){{animation-delay:.20s}}.stat:nth-child(5){{animation-delay:.25s}}
.stat:hover{{transform:translateY(-3px) scale(1.012);border-color:var(--line2);box-shadow:0 14px 32px rgba(0,0,0,.16)}}
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
.track-card{{position:relative;overflow:hidden;display:flex;gap:12px;padding:13px;border:1px solid var(--line);border-radius:16px;background:linear-gradient(145deg,rgba(255,255,255,.032),rgba(255,255,255,.012));margin-bottom:8px;transition:.22s ease;animation:cardIn .55s cubic-bezier(.2,1,.2,1) both;animation-delay:min(900ms,calc(var(--track-index) * 12ms));will-change:transform,opacity}}
.track-card::after{{content:"";position:absolute;inset:-30%;background:radial-gradient(circle at 0 0,rgba(155,140,255,.10),transparent 36%);opacity:0;transition:.25s ease;pointer-events:none}}
.track-card:last-child{{margin-bottom:0}}
.track-card:hover{{transform:translateY(-2px);border-color:rgba(155,140,255,.28);box-shadow:0 16px 34px rgba(0,0,0,.15)}}
.track-card:hover::after{{opacity:1}}
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
.tech-grid{{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px}}
.tech{{padding:12px;border:1px solid var(--line);border-radius:14px;background:rgba(255,255,255,.028)}}
.tech small{{display:block;color:var(--muted);font-size:9px;margin-bottom:4px}}
.tech b{{display:block;font-size:11px;overflow-wrap:anywhere}}
.home-button{{position:relative;display:inline-flex;align-items:center;gap:5px;padding:8px 12px;border:1px solid rgba(155,140,255,.30);border-radius:999px;color:#f7f7fb;text-decoration:none;font-size:11px;font-weight:950;background:linear-gradient(135deg,rgba(155,140,255,.16),rgba(94,231,255,.08));box-shadow:0 6px 20px rgba(0,0,0,.14);overflow:hidden;transition:transform .22s ease,border-color .22s ease,box-shadow .22s ease}}
.bottom-actions>.home-button{{width:100%;min-height:62px;justify-content:center;font-size:14px;letter-spacing:.02em}}
.home-button::before{{content:"";position:absolute;inset:0;background:linear-gradient(110deg,transparent 25%,rgba(255,255,255,.20) 48%,transparent 70%);transform:translateX(-125%);animation:homeSheen 4.5s ease-in-out infinite;pointer-events:none}}
.home-button:hover{{transform:translateY(-2px) scale(1.02);border-color:rgba(94,231,255,.42);box-shadow:0 10px 28px rgba(94,231,255,.13)}}
.home-button span{{position:relative;z-index:1}}
.home-button{{isolation:isolate}}
@keyframes homeSheen{{0%,18%{{transform:translateX(-125%)}}62%,100%{{transform:translateX(125%)}}}}
.footer{{display:flex;justify-content:space-between;gap:10px;margin-top:15px;padding:0 3px;color:#777e9a;font-size:9px}}
.footer strong{{color:#bdc2d8}}
html[data-theme="light"]{{color-scheme:light;--bg:#eef2ff;--panel:rgba(255,255,255,.80);--line:rgba(37,45,80,.12);--line2:rgba(103,87,232,.27);--text:#18203b;--muted:#5d6788;--accent:#6757e8;--cyan:#0b7ea0;--good:#128b52;--shadow:0 24px 65px rgba(44,55,100,.14)}}
html[data-theme="light"] body::before{{background:linear-gradient(180deg,rgba(247,249,255,.90),rgba(228,234,255,.88))}}
html[data-theme="light"] .anime-wallpaper{{opacity:.12}}
html[data-theme="light"] .nav,html[data-theme="light"] .quick,html[data-theme="light"] .hero,html[data-theme="light"] .section{{background:rgba(255,255,255,.78)}}
html[data-theme="light"] .file{{color:#4e5878}}
html[data-theme="light"] .controls{{background:rgba(255,255,255,.72)}}
@keyframes wall{{from{{transform:scale(1.04)}}to{{transform:scale(1.09)}}}}
@keyframes scan{{0%{{background-position:0%}}100%{{background-position:200%}}}}
@keyframes pulse{{0%,100%{{transform:scale(.86);opacity:.78}}50%{{transform:scale(1.16);opacity:1}}}}
@keyframes statSpin{{to{{transform:rotate(360deg)}}}}
@keyframes thumbRing{{to{{transform:rotate(360deg)}}}}
@keyframes thumbSheen{{0%,18%{{background-position:-120% 0,0 0}}62%,100%{{background-position:120% 0,0 0}}}}
@keyframes thumbFloat{{0%,100%{{transform:translate3d(0,0,0)}}50%{{transform:translate3d(0,-3px,0)}}}}
@keyframes cardIn{{from{{opacity:0;transform:translateY(12px) scale(.985)}}to{{opacity:1;transform:none}}}}
@keyframes shimmer{{0%,58%,100%{{transform:translateX(-120%)}}78%{{transform:translateX(120%)}}}}
@media(max-width:900px){{.hero{{grid-template-columns:1fr}}.hero-media{{width:100%;height:220px;min-height:220px;max-height:220px}} .hero-media img{{min-height:0;max-height:none}}.summary{{grid-template-columns:repeat(3,minmax(0,1fr))}}.tech-grid{{grid-template-columns:repeat(2,minmax(0,1fr))}}}}
.archive-note{{color:var(--muted);font-size:12px;margin:12px 0}}
.archive-table-wrap{{width:100%;overflow-x:auto;margin-top:12px;border:1px solid var(--line);border-radius:12px}}
.archive-table{{width:100%;min-width:840px;border-collapse:collapse;font-size:12px;background:rgba(0,0,0,.10)}}
.archive-track-names{{min-width:180px;max-width:280px;white-space:normal;overflow-wrap:anywhere}}
.archive-table th,.archive-table td{{padding:10px 11px;border-bottom:1px solid var(--line);text-align:left;vertical-align:top}}
.archive-table th{{color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:.06em;background:rgba(255,255,255,.035);white-space:nowrap}}
.archive-table td{{color:var(--text);overflow-wrap:anywhere}}
.archive-table .archive-name{{max-width:340px}}
.archive-table code{{white-space:pre-wrap;overflow-wrap:anywhere;font-size:11px;color:var(--cyan)}}
@media(max-width:620px){{.wrap{{padding:10px 9px 30px}}.nav{{top:6px}}.brand{{font-size:9px}}.hero{{padding:15px;border-radius:22px}}.hero-media{{width:100%;height:200px;min-height:200px;max-height:200px}} .hero-media img{{min-height:0;max-height:none}}h1{{font-size:29px}}.summary{{grid-template-columns:repeat(2,minmax(0,1fr))}}.spec-grid,.tech-grid{{grid-template-columns:1fr}}.track-heading{{flex-direction:column}}.badges{{justify-content:flex-start}}.quick{{top:64px;overflow:auto;flex-wrap:nowrap}}.quick a{{white-space:nowrap}}.footer{{flex-direction:column;align-items:flex-start}}}}
@media(prefers-reduced-motion:reduce){{*,*::before,*::after{{animation:none!important;transition:none!important;scroll-behavior:auto!important}}}}
@media print{{
  @page{{margin:12mm}}
  html,body{{background:#fff!important;color:#111!important}}
  .anime-wallpaper,.bg-grid,.topline,.nav,.quick,.footer{{display:none!important}}
  .wrap{{max-width:none!important;margin:0!important;padding:0!important}}
  .hero,.section,.stat,.tech,.track-card,.spec{{background:#fff!important;color:#111!important;box-shadow:none!important;backdrop-filter:none!important;animation:none!important;border-color:#ccc!important;break-inside:avoid;page-break-inside:avoid}}
  .section:not([open])>.section-body{{display:block!important}}
  .section-head{{background:#fff!important;border-color:#ccc!important}}
  .file,.pill,.stat span,.tech small,.spec span,.track-heading p,.empty{{color:#333!important}}
  .track-heading h3,.track-heading p,.spec strong,.tech b,.stat b{{color:#111!important}}
  .hero-media{{animation:none!important}}
}}
</style>
</head>
<body>
<div class="anime-wallpaper"></div><div class="bg-grid"></div><div class="topline"></div>
<div class="wrap">
  <nav class="nav">
    <div class="brand">AniToon • File Reports</div>
    <div class="nav-tools">
      <a class="home-button" href="/" aria-label="Go to AniToon home">⌂ Home <span>↗</span></a>
      <button class="home-button" type="button" id="print-report" title="Print or save this report as PDF"><span>🖨️ Save PDF</span></button>
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
      <span class="eyebrow"><i></i> @anitoon_1bot</span>
      <h1>File Report</h1>
      <div class="file">{filename}</div>
      {f'<div class="hero-pills"><span class="pill">🎞️ {esc(title)}</span></div>' if title else ''}
      <div class="hero-pills">
        <span class="pill">📦 {esc(container_name)}</span>
        <span class="pill">📏 {esc(size_text)}</span>
        <span class="pill">⏱️ <span id="countdown" class="countdown">--:--</span></span>
      </div>
    </div>
    {f'<div class="hero-media" id="hero-media"><img src="{hero_src}" alt="{hero_alt}" loading="eager"><span class="media-label">{hero_label}</span></div>' if hero_src else ''}
  </header>

  <div class="summary" id="overview">
    {summary_cards}
  </div>
  {language_ribbon}

  {media_details_sections}
  {archive_contents_section}

  {quick_nav}
  {video_section}
  {audio_section}
  {subtitle_section}
  {technical_section}

  <div class="bottom-actions">
    <a class="home-button bottom-home" href="/" aria-label="Go to AniToon home">⌂ Home <span>↗</span></a>
  </div>

  <div class="footer"><span><strong>AniToon</strong> • File Reports</span></div>
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
  const printBtn = document.getElementById("print-report");
  printBtn?.addEventListener("click", () => window.print());
}})();
</script>
</body>
</html>"""
    return document.encode("utf-8")


def minimal_web_report_page(
    report: Report,
    report_token: str | None = None,
    expires_at: datetime | None = None,
) -> bytes:
    """Readable, resilient HTML fallback used if the advanced report renderer fails."""
    safe = _coerce_report(report)
    filename = html.escape(safe.filename or "Telegram media file")
    kind_text = str(safe.media_kind or safe.detected or "Media file")
    detected_text = str(safe.detected or safe.ext or "Unknown format")
    kind = html.escape(kind_text)
    detected = html.escape(detected_text)
    show_kind = kind_text.strip().lower() != detected_text.strip().lower()
    size_text = html.escape(human(safe.size))
    runtime_value = str(safe.container.get("runtime") or "").strip()
    runtime_footer = (
        f'<span>Runtime: {html.escape(runtime_value)}</span>'
        if runtime_value and runtime_value.lower() not in {"not available", "n/a", "—"}
        else ""
    )
    video_count = len(safe.video.get("tracks", []))
    audio_count = len(safe.audio.get("tracks", []))
    subtitle_count = len(safe.subtitles)

    # Keep only useful, non-zero summary counts. Media type/format and file name
    # already appear in the hero, so the fallback does not repeat them below.
    stat_items = [("File size", size_text)]
    if video_count:
        stat_items.append(("Video tracks", str(video_count)))
    if audio_count:
        stat_items.append(("Audio tracks", str(audio_count)))
    if subtitle_count:
        stat_items.append(("Subtitles", str(subtitle_count)))
    stat_cards = "".join(
        f'<div class="stat"><span>{html.escape(label)}</span><strong>{html.escape(value)}</strong></div>'
        for label, value in stat_items
    )

    sections = []
    if safe.media_info:
        section_items = [("media", "🧩 Media-specific details")]
    else:
        section_items = []
    section_items.extend([
        ("video", "🎬 Video tracks"),
        ("audio", "🎧 Audio tracks"),
        ("subs", "💬 Subtitle tracks"),
        ("technical", "⚙️ Technical details"),
    ])
    for section, title in section_items:
        has_content = (
            section == "media" and bool(safe.media_info)
            or section == "video" and bool(safe.video.get("tracks"))
            or section == "audio" and bool(safe.audio.get("tracks") or safe.audio.get("sample_codecs"))
            or section == "subs" and bool(safe.subtitles)
            or section == "technical" and bool(
                safe.container.get("average_bitrate") or safe.container.get("title")
            )
        )
        if not has_content:
            continue
        try:
            content = web_section(safe, section)
            if not content.strip():
                continue
            sections.append(
                f'<section class="panel"><h2>{html.escape(title)}</h2>'
                f'<div class="section-content">{content}</div></section>'
            )
        except Exception:
            log.debug("Fallback report section render failed | section=%s", section, exc_info=True)

    # Keep ZIP fallback reports useful as well if the advanced renderer errors.
    is_zip = (
        str(safe.ext or "").lower() == ".zip"
        or "zip archive" in str(safe.detected or "").lower()
    )
    if is_zip:
        archive_entries = [
            item for item in (safe.archive_entries or [])
            if isinstance(item, dict)
        ][:MAX_REPORT_ARCHIVE_ENTRIES]
        try:
            archive_count = max(
                len(archive_entries),
                int((safe.media_info or {}).get("archive_entry_count") or len(archive_entries)),
            )
        except (TypeError, ValueError):
            archive_count = len(archive_entries)

        def archive_names_html(value: Any) -> str:
            names = value if isinstance(value, list) else [value] if value else []
            return "<br>".join(
                html.escape(str(name)) for name in names if name not in (None, "")
            ) or "—"

        archive_rows = []
        for entry in archive_entries:
            try:
                entry_size = int(entry.get("size") or 0)
            except (TypeError, ValueError):
                entry_size = 0
            try:
                compressed_size = int(entry.get("compressed_size") or 0)
            except (TypeError, ValueError):
                compressed_size = 0
            archive_rows.append(
                "<tr>"
                f'<td class="archive-name">{html.escape(str(entry.get("name") or "Unnamed entry"))}</td>'
                f'<td>{html.escape("Folder" if entry.get("is_dir") else str(entry.get("file_type") or "File"))}</td>'
                f'<td>{html.escape(human(entry_size))}</td>'
                f'<td>{html.escape(human(compressed_size))}</td>'
                f'<td>{archive_names_html(entry.get("audio_names"))}</td>'
                f'<td>{archive_names_html(entry.get("subtitle_names"))}</td>'
                f'<td>{html.escape(str(entry.get("modified_at") or "—"))}</td>'
                "</tr>"
            )
        if not archive_rows:
            archive_rows.append(
                '<tr><td colspan="7">ZIP directory details are not readable within the current scan limits.</td></tr>'
            )
        archive_note = (
            f"Showing {len(archive_entries)} of {archive_count} entries. "
            if archive_count > len(archive_entries)
            else f"{len(archive_entries)} entries. "
        ) + "Track names are shown when the archive metadata exposes them."
        archive_html = (
            '<section class="panel" id="archive-contents">'
            f'<h2>📦 ZIP contents <span class="pill">{archive_count} entries</span></h2>'
            f'<p class="archive-note">{html.escape(archive_note)}</p>'
            '<div class="archive-table-wrap"><table class="archive-table">'
            '<thead><tr><th>Name / path</th><th>Type</th><th>Size</th><th>Compressed</th>'
            '<th>Audio track names</th><th>Subtitle names</th><th>Modified</th></tr></thead>'
            f'<tbody>{"".join(archive_rows)}</tbody></table></div></section>'
        )
        sections.append(archive_html)

    expiry_text = (
        expires_at.strftime("%d %b %Y • %H:%M UTC")
        if isinstance(expires_at, datetime)
        else "Link expiry unavailable"
    )
    report_id = html.escape((report_token or "local")[:16])
    document = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#080918">
<title>AniToon • {filename}</title>
<style>
:root{{color-scheme:dark;--bg:#070918;--panel:rgba(16,20,43,.82);--line:rgba(255,255,255,.11);--text:#f7f8ff;--muted:#aab2d0;--accent:#b5a4ff;--cyan:#83eafa}}
*{{box-sizing:border-box}}
body{{margin:0;min-height:100vh;padding:22px 14px 42px;background:radial-gradient(circle at 10% 0,rgba(151,119,255,.19),transparent 32%),radial-gradient(circle at 95% 18%,rgba(83,216,245,.12),transparent 28%),var(--bg);color:var(--text);font:14px/1.65 Inter,ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif}}
main{{width:min(980px,100%);margin:0 auto}}
.brand{{font-size:11px;font-weight:900;letter-spacing:.14em;text-transform:uppercase;color:var(--cyan)}}
.hero{{padding:24px;border:1px solid var(--line);border-radius:22px;background:linear-gradient(135deg,rgba(23,27,58,.93),rgba(9,12,29,.88));box-shadow:0 22px 70px rgba(0,0,0,.27)}}
h1{{font-size:clamp(27px,5vw,42px);line-height:1.08;letter-spacing:-.04em;margin:10px 0}}
.file{{color:var(--muted);overflow-wrap:anywhere;margin-top:8px}}
.pills{{display:flex;flex-wrap:wrap;gap:7px;margin-top:15px}}
.pill{{padding:6px 9px;border:1px solid var(--line);border-radius:999px;background:rgba(255,255,255,.035);font-size:11px;color:var(--muted)}}
.stats{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:9px;margin-top:17px}}
.stat{{min-width:0;padding:12px;border:1px solid var(--line);border-radius:14px;background:rgba(255,255,255,.035)}}
.stat span{{display:block;font-size:10px;color:var(--muted);margin-bottom:4px}}
.stat strong{{display:block;font-size:15px;overflow-wrap:anywhere}}
.panel{{margin-top:13px;padding:19px;border:1px solid var(--line);border-radius:18px;background:var(--panel);box-shadow:0 15px 40px rgba(0,0,0,.14);overflow-wrap:anywhere}}
h2{{font-size:16px;margin:0 0 12px;letter-spacing:-.01em}}
.section-content{{color:#d7dcf2;line-height:1.85}}
.section-content br+br{{content:"";display:block;margin-top:5px}}
.archive-note{{color:var(--muted);font-size:12px;margin:10px 0}}
.archive-table-wrap{{width:100%;overflow-x:auto;border:1px solid var(--line);border-radius:12px}}
.archive-table{{width:100%;min-width:820px;border-collapse:collapse;font-size:12px}}
.archive-table th,.archive-table td{{padding:9px 10px;border-bottom:1px solid var(--line);text-align:left;vertical-align:top;overflow-wrap:anywhere}}
.archive-table th{{color:var(--muted);font-size:10px;text-transform:uppercase;white-space:nowrap}}
.archive-table td{{color:var(--text)}}
.archive-table code{{overflow-wrap:anywhere}}
.footer{{margin:17px 3px 0;color:#818aa9;font-size:11px;display:flex;flex-wrap:wrap;justify-content:space-between;gap:8px}}
@media(max-width:620px){{body{{padding:12px 10px 28px}}.hero,.panel{{padding:16px;border-radius:17px}}.stats{{grid-template-columns:repeat(2,minmax(0,1fr))}}.stat strong{{font-size:14px}}}}
@media(prefers-reduced-motion:reduce){{*,*::before,*::after{{scroll-behavior:auto!important;animation:none!important;transition:none!important}}}}
</style>
</head>
<body><main>
<header class="hero">
  <div class="brand">⛩ AniToon • Media report</div>
  <h1>File information</h1>
  <div class="file"><strong>{filename}</strong></div>
  <div class="pills">{f'<span class="pill">{kind}</span>' if show_kind else ''}<span class="pill">{detected}</span><span class="pill">HTML report</span></div>
  <div class="stats">{stat_cards}</div>
</header>
{''.join(sections)}
<footer class="footer"><span>Report expires: {html.escape(expiry_text)}</span><span>Report ID: {report_id}</span>{runtime_footer}</footer>
</main></body></html>"""
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
                    "preview_tasks": len(preview_tasks),
                    "max_active_scans": scan_worker_cap,
                    "worker_hard_limit": MAX_SCAN_WORKERS,
                    "adaptive_worker_limit": adaptive_scan_limit(),
                    "ram_auto_control": True,
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
                    candidates = list(getattr(state.report, "previews", []) or [])
                    preview_items = [
                        item for item in candidates
                        if isinstance(item, dict) and str(item.get("data") or "").strip()
                    ][:1]
                else:
                    try:
                        stored_payload = await load_web_report(token)
                    except Exception:
                        stored_payload = None
                    if stored_payload:
                        report_payload = stored_payload.get("report") if isinstance(stored_payload, dict) else {}
                        candidates = list((report_payload or {}).get("previews") or [])
                        preview_items = [
                            item for item in candidates
                            if isinstance(item, dict) and str(item.get("data") or "").strip()
                        ][:1]
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

                # A report URL is a standalone page. Do not redirect it through
                # the home page; this keeps the Telegram button reliable on all
                # browsers and makes the report route independent of embed mode.
                state = web_states.get(token)

                if not state:
                    try:
                        stored_report = await load_web_report(token)
                    except Exception:
                        stored_report = None
                    if stored_report:
                        try:
                            stored_payload = stored_report if isinstance(stored_report, dict) else {"report": stored_report}
                            restored_report = _coerce_report(stored_payload.get("report") or {})
                            stored_expiry = stored_payload.get("expires_at")
                            if isinstance(stored_expiry, str):
                                stored_expiry = datetime.fromisoformat(stored_expiry.replace("Z", "+00:00"))
                            if isinstance(stored_expiry, datetime):
                                if stored_expiry.tzinfo is None:
                                    stored_expiry = stored_expiry.replace(tzinfo=timezone.utc)
                                else:
                                    stored_expiry = stored_expiry.astimezone(timezone.utc)
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
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="theme-color" content="#050611">
<title>AniToon • Link Expired</title>
<style>
:root{color-scheme:dark}
*{box-sizing:border-box}
body{margin:0;min-height:100vh;display:grid;place-items:center;padding:24px;background:
radial-gradient(circle at 20% 10%,rgba(154,140,255,.18),transparent 38%),
radial-gradient(circle at 85% 85%,rgba(94,231,255,.12),transparent 40%),
#050611;color:#f7f7fb;font:15px/1.6 system-ui,-apple-system,"Segoe UI",sans-serif}
.card{width:min(560px,100%);padding:32px;border:1px solid rgba(255,255,255,.1);border-radius:24px;background:rgba(11,14,31,.82);box-shadow:0 30px 90px rgba(0,0,0,.38);text-align:center}
.icon{font-size:48px;margin-bottom:8px}
h1{margin:0 0 10px;font-size:30px}
p{margin:8px 0;color:#aeb5ce}
strong{color:#fff}
</style>
</head>
<body>
<main class="card">
<div class="icon">⏳</div>
<h1>Link expired</h1>
<p>This file report link has expired.</p>
<p><strong>Send the file to the bot again</strong> to create a new report and get a new link.</p>
</main>
</body>
</html>""".encode("utf-8")
                    head = b"Content-Type: text/html; charset=utf-8\r\n"
                    code = b"410 Gone"
                else:
                    try:
                        body = web_page(state.report, token, state.expires_at)
                    except Exception:
                        log.exception(
                            "Web report render failed; using safe fallback | token=%s",
                            token,
                        )
                        body = minimal_web_report_page(
                            state.report,
                            token,
                            state.expires_at,
                        )
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
            with suppress(Exception):
                body = (
                    b"<!doctype html><html><head><meta charset=\"utf-8\"><title>"
                    b"AniToon Web Error</title></head><body style=\"font-family:system-ui;padding:32px\">"
                    b"<h2>AniToon Web</h2><p>The requested page could not be rendered.</p>"
                    b"</body></html>"
                )
                writer.write(
                    b"HTTP/1.1 500 Internal Server Error\r\n"
                    b"Content-Type: text/html; charset=utf-8\r\n"
                    + f"Content-Length: {len(body)}\r\n".encode("ascii")
                    + b"Cache-Control: no-store\r\nConnection: close\r\n\r\n"
                    + body
                )
                await writer.drain()
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
    global scan_worker_cap, scan_worker_cap_persisted, scan_worker_cap_source
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
                log.info("Starting main Telegram bot connection")
                # Telethon can wait on a data-center migration. Bound the
                # whole startup operation and restart the client if it stalls.
                await asyncio.wait_for(
                    bot.start(bot_token=BOT_TOKEN),
                    timeout=45,
                )
                if not bot.is_connected():
                    raise ConnectionError("Telegram client did not stay connected after startup")
                break
            except errors.FloodWaitError as exc:
                wait_seconds = max(1, int(exc.seconds) + 5)
                log.warning(
                    "Telegram authorization rate-limited; waiting %s seconds before retry",
                    wait_seconds,
                )
                with suppress(Exception):
                    await bot.disconnect()
                await asyncio.sleep(wait_seconds)
            except TimeoutError:
                log.exception(
                    "Telegram startup timed out after 45 seconds; resetting connection and retrying"
                )
                with suppress(Exception):
                    await bot.disconnect()
                await asyncio.sleep(5)
            except Exception:
                log.exception(
                    "Telegram startup failed; resetting connection and retrying in 10 seconds"
                )
                with suppress(Exception):
                    await bot.disconnect()
                await asyncio.sleep(10)

        try:
            mongo_ok = await ensure_mongodb()
            log.info(
                "MongoDB startup check | configured=%s | connected=%s",
                mongodb_is_configured(),
                mongo_ok,
            )
            if mongo_ok:
                saved_worker_cap = await load_scan_worker_cap()
                if saved_worker_cap is not None:
                    scan_worker_cap = max(1, min(int(saved_worker_cap), MAX_SCAN_WORKERS))
                    scan_worker_cap_persisted = True
                    scan_worker_cap_source = "MongoDB"
                    log.info("Loaded saved scan worker cap | cap=%s/%s", scan_worker_cap, MAX_SCAN_WORKERS)
                from mongo_store import purge_expired_web_reports
                await purge_expired_web_reports()
            try:
                await _resolve_storage_peer(bot)
            except Exception:
                log.warning("Storage channel startup check failed", exc_info=True)
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
            scan_worker_cap,
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
        for task in list(storage_tasks):
            task.cancel()
        for task in list(storage_tasks):
            with suppress(asyncio.CancelledError):
                await task
        storage_tasks.clear()
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
