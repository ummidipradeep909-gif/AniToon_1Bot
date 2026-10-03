from __future__ import annotations

import asyncio
import html
import json
import logging
import os
import secrets
import time
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlsplit
from typing import Any

from dotenv import load_dotenv
from telethon import Button, TelegramClient, errors, events

from file_inspector import Report, format_report, format_section
from media_probe import (
    ProbeBudgetExceeded,
    ProbeCancelled,
    cancel_probe,
    get_probe,
    inspect_telegram_player,
    purge_probes,
)

load_dotenv()

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"].strip()
BOT_TOKEN = os.environ["BOT_TOKEN"].strip()

FILE_CHECKER_PRIVATE_ONLY = (
    os.getenv("FILE_CHECKER_PRIVATE_ONLY", "0").strip().lower()
    not in {"0", "false", "no", "off"}
)
MAX_CONCURRENT_CHECKS = max(1, min(int(os.getenv("MAX_CONCURRENT_CHECKS", "2")), 4))
SCAN_TIMEOUT_SECONDS = max(30, min(int(os.getenv("SCAN_TIMEOUT_SECONDS", "300")), 300))
REPORT_LINK_TTL_SECONDS = 5 * 60
PENDING_SCAN_TTL_SECONDS = 10 * 60
MAX_STORED_RESULTS = 100
PUBLIC_WEB_URL = (
    os.getenv("PUBLIC_WEB_URL", "https://anitoons-1bot-oa44.onrender.com")
    .strip()
    .rstrip("/")
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("anitoons-file-checker")

bot = TelegramClient("file_checker_bot", API_ID, API_HASH)
bot.flood_sleep_threshold = 15 * 60
check_semaphore = asyncio.Semaphore(MAX_CONCURRENT_CHECKS)
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
    busy: bool = False


scan_states: dict[tuple[int, int], ScanState] = {}
web_states: dict[str, ScanState] = {}
pending_scans: dict[str, PendingScan] = {}
active_scans: dict[str, asyncio.Task] = {}

HELP_TEXT = (
    "🔬 <b>AniToons File Intelligence</b>\n\n"
    "Send a video or Telegram document and I will inspect its media metadata.\n\n"
    "<b>Audio</b> — track name, language, codec, channels, sample rate and flags when stored.\n"
    "<b>Subtitles</b> — track name, language, codec, default/forced and accessibility flags when stored.\n"
    "<b>Video</b> — codec, resolution and track metadata when available.\n"
    "<b>Container</b> — runtime, title and estimated average bitrate.\n\n"
    "🌐 After the scan, I send one button that opens the complete file-information page in your browser.\n\n"
    "🛡️ <b>Large-file safety:</b> the scanner uses bounded byte-range reads and does not intentionally download the complete large file."
)


def main_buttons():
    return [
        [
            Button.inline("📖 Help", b"home:help"),
            Button.inline("📊 Status", b"home:status"),
        ],
        [Button.inline("🛡️ Scan policy", b"home:policy")],
    ]


def web_report_button(token: str):
    return [[Button.url("🌐 Open File Info", f"{PUBLIC_WEB_URL}/report/{token}")]]


def metadata_button(token: str):
    return [[Button.inline("📥 Download Metadata", f"scan:{token}".encode("ascii"))]]


def cancel_button(token: str):
    return [[Button.inline("❌ Cancel Scan", f"cancel:{token}".encode("ascii"))]]


def purge_pending_scans() -> None:
    now = time.monotonic()
    expired = [
        token
        for token, pending in pending_scans.items()
        if now - pending.created_at > PENDING_SCAN_TTL_SECONDS
    ]
    for token in expired:
        pending_scans.pop(token, None)


def compact_scan_result(report: Report) -> str:
    audio_tracks = report.audio.get("tracks", [])
    subtitle_tracks = report.subtitles or []

    def names(items):
        values = []
        for item in items:
            name = (
                item.get("name")
                or item.get("display_name")
                or item.get("language_name")
                or item.get("codec_name")
                or "Unnamed track"
            )
            values.append(str(name))
        return values

    audio_names = names(audio_tracks) if isinstance(audio_tracks, list) else []
    subtitle_names = names(subtitle_tracks)

    lines = ["✅ <b>FILE SCAN COMPLETE</b>", ""]
    lines.append(f"📄 <b>{html.escape(report.filename)}</b>")

    if report.container.get("runtime"):
        lines.append(f"⏱ Runtime: <b>{html.escape(report.container['runtime'])}</b>")

    lines.append(
        f"🔊 Audio: <b>{len(audio_tracks) if isinstance(audio_tracks, list) else 0}</b>"
        + (f" — {html.escape(', '.join(audio_names))}" if audio_names else " — names not detected")
    )
    lines.append(
        f"💬 Subtitles: <b>{len(subtitle_tracks)}</b>"
        + (f" — {html.escape(', '.join(subtitle_names))}" if subtitle_names else " — names not detected")
    )
    lines += ["", "🌐 <b>Open the complete file information in your browser.</b>"]
    return "\n".join(lines)


def is_checkable_message(event) -> bool:
    message = event.message
    if not message or not getattr(message, "media", None):
        return False
    if not getattr(message, "file", None) and not getattr(message, "photo", None):
        return False
    if FILE_CHECKER_PRIVATE_ONLY and not event.is_private:
        return False
    return True


def safe_filename(message: Any) -> str:
    file_obj = getattr(message, "file", None)
    name = getattr(file_obj, "name", None)
    if name:
        return str(name)
    ext = getattr(file_obj, "ext", None)
    if ext:
        return f"telegram_file{ext}"
    if getattr(message, "photo", None):
        return "telegram_photo"
    return "telegram_file"


def state_key(message: Any) -> tuple[int, int] | None:
    chat_id = getattr(message, "chat_id", None)
    msg_id = getattr(message, "id", None)
    if chat_id is None or msg_id is None:
        return None
    return int(chat_id), int(msg_id)


def cache_state(report_message: Any, source_message: Any, report: Report) -> ScanState | None:
    key = state_key(report_message)
    if key is None:
        return None

    state = scan_states.get(key)
    token = state.web_token if state else secrets.token_urlsafe(18)

    if state is None:
        state = ScanState(
            source_message=source_message,
            report=report,
            created_at=time.monotonic(),
            web_token=token,
        )
    else:
        state.source_message = source_message
        state.report = report
        state.created_at = time.monotonic()

    scan_states[key] = state
    web_states[token] = state
    _purge_states()
    return state


def _purge_states() -> None:
    purge_pending_scans()
    now = time.monotonic()
    expired_keys = [
        key for key, state in scan_states.items()
        if now - state.created_at > REPORT_LINK_TTL_SECONDS
    ]
    for key in expired_keys:
        state = scan_states.pop(key, None)
        if state:
            web_states.pop(state.web_token, None)

    expired_tokens = [
        token for token, state in web_states.items()
        if now - state.created_at > STATE_TTL_SECONDS
    ]
    for token in expired_tokens:
        web_states.pop(token, None)

    if len(web_states) > MAX_STORED_RESULTS:
        ordered = sorted(web_states.items(), key=lambda item: item[1].created_at)
        for token, state in ordered[: len(web_states) - MAX_STORED_RESULTS]:
            web_states.pop(token, None)
            for key, current in list(scan_states.items()):
                if current is state:
                    scan_states.pop(key, None)


def clip(text: str, limit: int = 3900) -> str:
    return text if len(text) <= limit else text[: limit - 40] + "\n\n…message shortened."


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
) -> Report:
    filename = safe_filename(source_message)
    port = int(os.getenv("PORT", "10000"))

    async def progress(line: str):
        await edit_status(
            status_message,
            status_text(filename, line),
            buttons=cancel_button(scan_token),
        )

    return await asyncio.wait_for(
        inspect_telegram_player(
            bot,
            source_message,
            scan_token,
            progress=progress,
            budget=int(os.getenv("FILE_DEEP_PROBE_BYTES", "8388608")),
            port=port,
        ),
        timeout=SCAN_TIMEOUT_SECONDS,
    )


async def analyze_source(
    source_message: Any,
    status_message: Any,
    scan_token: str,
) -> None:
    global checks_total, checks_ok, checks_failed
    checks_total += 1
    filename = safe_filename(source_message)

    try:
        async with check_semaphore:
            report = await run_scan(
                source_message,
                status_message,
                scan_token=scan_token,
            )

            state = cache_state(status_message, source_message, report)
            if state is None:
                raise RuntimeError("Could not create web report link")

            # Reuse the private random scan token for the browser report URL.
            state.web_token = scan_token
            web_states[scan_token] = state

            result = compact_scan_result(report)
            await edit_status(
                status_message,
                result,
                buttons=web_report_button(scan_token),
            )
            checks_ok += 1

    except asyncio.CancelledError:
        checks_failed += 1
        await cancel_probe(scan_token)
        await edit_status(
            status_message,
            "❌ <b>Metadata scan cancelled.</b>",
            buttons=main_buttons(),
        )
        raise

    except ProbeBudgetExceeded:
        checks_failed += 1
        await edit_status(
            status_message,
            "🛑 <b>Safe scan limit reached.</b>\n\n"
            "The player engine stopped before downloading the complete file.",
            buttons=main_buttons(),
        )

    except ProbeCancelled:
        checks_failed += 1
        await edit_status(
            status_message,
            "❌ <b>Metadata scan cancelled.</b>",
            buttons=main_buttons(),
        )

    except asyncio.TimeoutError:
        checks_failed += 1
        await edit_status(
            status_message,
            "⏰ <b>Metadata scan reached the 5-minute limit.</b>",
            buttons=main_buttons(),
        )

    except errors.FloodWaitError as exc:
        checks_failed += 1
        await edit_status(
            status_message,
            f"⏳ Telegram temporarily rate-limited this scan for {int(exc.seconds)} seconds.",
            buttons=main_buttons(),
        )

    except Exception as exc:
        checks_failed += 1
        log.exception("File metadata scan failed for %s", filename)
        await edit_status(
            status_message,
            "❌ <b>Metadata scan failed.</b>\n\n"
            f"<code>{html.escape(type(exc).__name__)}</code>",
            buttons=main_buttons(),
        )

    finally:
        await cancel_probe(scan_token) if scan_token in active_scans else None
        active_scans.pop(scan_token, None)


async def analyze(event) -> None:
    purge_pending_scans()

    token = secrets.token_urlsafe(18)
    pending_scans[token] = PendingScan(
        source_message=event.message,
        created_at=time.monotonic(),
    )

    await event.reply(
        "📥 <b>Download Metadata</b>",
        parse_mode="html",
        buttons=metadata_button(token),
    )


async def handle_new_message(event):
    text = (event.raw_text or "").strip().lower()
    if text in {"/start", "/help"}:
        await event.reply(
            HELP_TEXT,
            parse_mode="html",
            buttons=main_buttons(),
        )
        return

    if not is_checkable_message(event):
        return

    await analyze(event)


async def handle_callback(event):
    purge_pending_scans()
    data = (event.data or b"").decode("ascii", "ignore")

    if data == "home:help":
        await event.answer()
        await event.edit(
            HELP_TEXT,
            parse_mode="html",
            buttons=main_buttons(),
        )
        return

    if data == "home:policy":
        await event.answer()
        await event.edit(
            "🛡️ <b>SCAN POLICY</b>\n\n"
            "• Sending a file does not start a scan.\n"
            "• Scan starts only after <b>📥 Download Metadata</b> is pressed.\n"
            "• Uses targeted Telegram byte-range reads.\n"
            "• Searches Matroska metadata for real audio/subtitle TrackEntry records.\n"
            "• Never intentionally downloads the complete large file.\n"
            "• Scan limit: 5 minutes.\n"
            "• Browser report link stays valid for 5 minutes or until the service restarts.",
            parse_mode="html",
            buttons=[[Button.inline("⬅️ Back", b"home:back")]],
        )
        return

    if data == "home:status":
        uptime = datetime.now(timezone.utc) - started_at
        await event.answer()
        await event.edit(
            "📊 <b>BOT STATUS</b>\n\n"
            f"🟢 Telegram: <b>{'connected' if bot.is_connected() else 'disconnected'}</b>\n"
            f"⏱ Uptime: <code>{str(uptime).split('.')[0]}</code>\n"
            f"📦 Scans: <code>{checks_total}</code>\n"
            f"✅ Successful: <code>{checks_ok}</code>\n"
            f"❌ Failed: <code>{checks_failed}</code>",
            parse_mode="html",
            buttons=[[Button.inline("⬅️ Back", b"home:back")]],
        )
        return

    if data == "home:back":
        await event.answer()
        await event.edit(
            HELP_TEXT,
            parse_mode="html",
            buttons=main_buttons(),
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

        await event.answer("Metadata scan started…")

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
            )
        )
        active_scans[token] = task
        return

    if data.startswith("cancel:"):
        token = data[7:].strip()
        task = active_scans.get(token)

        if not task:
            await event.answer("This scan is no longer running.", alert=True)
            return

        await event.answer("Cancelling scan…")
        task.cancel()
        return

    await event.answer()


async def handle_ping(event):
    text = (event.raw_text or "").strip().lower()
    if text not in {"/ping", "/status"}:
        return

    uptime = datetime.now(timezone.utc) - started_at
    await event.reply(
        "🟢 <b>AniToons File Intelligence is online</b>\n"
        f"⏱ Uptime: <code>{str(uptime).split('.')[0]}</code>\n"
        f"📦 Scans: <code>{checks_total}</code>\n"
        f"✅ Successful: <code>{checks_ok}</code>\n"
        f"❌ Failed: <code>{checks_failed}</code>",
        parse_mode="html",
        buttons=main_buttons(),
    )


def web_section(report: Report, section: str) -> str:
    return format_section(report, section).replace("\n", "<br>")


def web_page(report: Report) -> bytes:
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
  background:
    radial-gradient(900px 400px at 15% -10%, rgba(56,189,248,.13), transparent 60%),
    radial-gradient(800px 380px at 100% 0%, rgba(168,85,247,.10), transparent 60%),
    var(--bg);
  color: var(--text);
  font: 14px/1.6 system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
}}
.wrap {{ max-width: 1080px; margin:auto; padding:20px 14px 50px; }}
.hero {{
  padding:24px;
  border:1px solid var(--border);
  border-radius:24px;
  background:linear-gradient(135deg,rgba(15,23,42,.96),rgba(17,24,39,.84));
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
@media(max-width:720px) {{
  .summary {{ grid-template-columns:repeat(2,minmax(0,1fr)); }}
  .grid {{ grid-template-columns:1fr; }}
  .badges {{ justify-content:flex-start; }}
}}
</style>
</head>
<body>
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
  const el = document.getElementById("countdown");
  const tick = () => {{
    const m = Math.floor(left / 60);
    const s = left % 60;
    if (el) el.textContent = String(m).padStart(2, "0") + ":" + String(s).padStart(2, "0");
    if (left > 0) {{ left -= 1; setTimeout(tick, 1000); }}
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
        try:
            raw = await reader.read(4096)
            first = raw.split(b"\r\n", 1)[0].decode("latin1", "replace")
            parts = first.split(" ", 2)
            target = parts[1] if len(parts) > 1 else "/"
            path = urlsplit(target).path
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

                    if range_value.lower().startswith("bytes="):
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
                    "checks_total": checks_total,
                    "checks_ok": checks_ok,
                    "checks_failed": checks_failed,
                    "web_reports": len(web_states),
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
                state = web_states.get(token)

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
                    body = web_page(state.report)
                    head = b"Content-Type: text/html; charset=utf-8\r\n"
                    code = b"200 OK"

            elif path == "/":
                body = (
                    b"AniToons File Intelligence Bot is running. "
                    b"Use /health or open a scan report link from Telegram."
                )
                head = b"Content-Type: text/plain; charset=utf-8\r\n"
                code = b"200 OK"

            else:
                body = b"Not found"
                head = b"Content-Type: text/plain; charset=utf-8\r\n"
                code = b"404 Not Found"

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
    bot.add_event_handler(
        handle_new_message,
        events.NewMessage(incoming=True),
    )
    bot.add_event_handler(
        handle_callback,
        events.CallbackQuery,
    )
    bot.add_event_handler(
        handle_ping,
        events.NewMessage(incoming=True),
    )

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

        me = await bot.get_me()
        username = getattr(me, "username", "unknown")

        log.info(
            "Telegram bot online as @%s | private_only=%s | concurrency=%s | scan_timeout=%ss | web=%s",
            username,
            FILE_CHECKER_PRIVATE_ONLY,
            MAX_CONCURRENT_CHECKS,
            SCAN_TIMEOUT_SECONDS,
            PUBLIC_WEB_URL,
        )

        await bot.run_until_disconnected()

    finally:
        health.close()
        await health.wait_closed()
        with suppress(Exception):
            await bot.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
