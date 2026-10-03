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

from file_inspector import Report, format_report, format_section, inspect_telegram_message

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
STATE_TTL_SECONDS = 60 * 60
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
        if now - pending.created_at > STATE_TTL_SECONDS
    ]
    for token in expired:
        pending_scans.pop(token, None)


def compact_scan_result(report: Report) -> str:
    audio_tracks = report.audio.get("tracks", [])
    subtitle_tracks = report.subtitles or []

    def names(items):
        values = []
        for item in items:
            name = item.get("name") or item.get("display_name") or item.get("language_name") or item.get("codec_name")
            if name and name not in values:
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
        if now - state.created_at > STATE_TTL_SECONDS
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
    return (
        "🔬 <b>AniToons File Intelligence</b>\n\n"
        f"📄 <code>{html.escape(filename[:120])}</code>\n"
        f"{line}\n\n"
        "🛡️ Bounded byte-range scan • no complete large-file download"
    )


async def run_scan(
    source_message: Any,
    status_message: Any,
    *,
    deep: bool,
    scan_token: str | None = None,
) -> Report:
    filename = safe_filename(source_message)

    async def progress(line: str):
        await edit_status(
            status_message,
            status_text(filename, line),
            buttons=cancel_button(scan_token) if scan_token else None,
        )

    report, sampled = await asyncio.wait_for(
        inspect_telegram_message(
            bot,
            source_message,
            progress=progress,
            deep=deep,
        ),
        timeout=SCAN_TIMEOUT_SECONDS,
    )
    report.notes.insert(
        0,
        f"Read approximately {sampled / 1024 / 1024:.2f} MiB across targeted ranges.",
    )
    return report


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
                deep=True,
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
        await edit_status(
            status_message,
            "❌ <b>Metadata scan cancelled.</b>",
            buttons=main_buttons(),
        )
        raise

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
            "• Browser report expires after about 1 hour or a service restart.",
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
    summary = web_section(report, "summary")
    audio = web_section(report, "audio")
    subs = web_section(report, "subs")
    video = web_section(report, "video")
    technical = web_section(report, "technical")

    filename = html.escape(report.filename)
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    document = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>AniToons File Info — {filename}</title>
<style>
:root {{
  color-scheme: dark;
  font-family: Inter, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}}
body {{
  margin: 0;
  background: #0f172a;
  color: #e5e7eb;
}}
.wrap {{
  max-width: 980px;
  margin: 0 auto;
  padding: 24px 16px 48px;
}}
.header {{
  background: #111827;
  border: 1px solid #334155;
  border-radius: 18px;
  padding: 22px;
  margin-bottom: 16px;
}}
h1 {{ margin: 0 0 8px; font-size: 24px; }}
.meta {{ color: #94a3b8; font-size: 13px; }}
.card {{
  background: #111827;
  border: 1px solid #334155;
  border-radius: 16px;
  padding: 18px;
  margin-top: 14px;
  overflow-wrap: anywhere;
}}
.card h2 {{
  margin: 0 0 12px;
  font-size: 18px;
}}
.info {{
  line-height: 1.65;
  font-size: 14px;
}}
.note {{
  margin-top: 16px;
  background: #1e293b;
  border-radius: 12px;
  padding: 12px;
  color: #cbd5e1;
  font-size: 13px;
}}
</style>
</head>
<body>
<div class="wrap">
  <div class="header">
    <h1>🔬 AniToons File Intelligence</h1>
    <div class="meta">{filename}</div>
    <div class="meta">Generated {generated} • bounded partial scan</div>
  </div>

  <section class="card"><h2>📋 Summary</h2><div class="info">{summary}</div></section>
  <section class="card"><h2>🔊 Audio</h2><div class="info">{audio}</div></section>
  <section class="card"><h2>💬 Subtitles</h2><div class="info">{subs}</div></section>
  <section class="card"><h2>🎬 Video</h2><div class="info">{video}</div></section>
  <section class="card"><h2>⚙️ Technical</h2><div class="info">{technical}</div></section>

  <div class="note">
    🛡️ This page contains scan metadata only. The scanner uses bounded Telegram byte-range reads and does not intentionally download the complete large file.
    Browser report links expire after approximately 1 hour or when the service restarts.
  </div>
</div>
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
