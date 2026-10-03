from __future__ import annotations

import asyncio
import html
import json
import logging
import os
import time
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
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
    busy: bool = False


scan_states: dict[tuple[int, int], ScanState] = {}

HELP_TEXT = (
    "🔬 <b>AniToons File Intelligence</b>\n\n"
    "Send a video or Telegram document and I will inspect its media metadata.\n\n"
    "<b>Audio</b> — track name, language, codec, channels, sample rate, default/original/commentary flags when stored.\n"
    "<b>Subtitles</b> — track name, language, codec, default/forced/SDH-style flags when stored.\n"
    "<b>Video</b> — codec, resolution and track metadata when available.\n"
    "<b>Container</b> — runtime, title and estimated average bitrate.\n\n"
    "🛡️ <b>Large-file safety:</b> the scanner uses bounded byte-range reads. It never intentionally downloads the complete large file.\n\n"
    "Some containers place important metadata outside the sampled ranges, so undetected tracks are reported as <i>not detected</i>, not as proof of absence."
)


def main_buttons():
    return [
        [Button.inline("📖 Help", b"home:help"), Button.inline("📊 Status", b"home:status")],
        [Button.inline("🛡️ Scan policy", b"home:policy")],
    ]


def report_buttons():
    return [
        [Button.inline("🔊 Audio", b"view:audio"), Button.inline("💬 Subtitles", b"view:subs")],
        [Button.inline("🎬 Video", b"view:video"), Button.inline("⚙️ Technical", b"view:technical")],
        [Button.inline("🔎 Deeper re-scan", b"scan:deep"), Button.inline("🏠 Summary", b"view:summary")],
    ]


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


def cache_state(report_message: Any, source_message: Any, report: Report) -> None:
    key = state_key(report_message)
    if key is None:
        return
    scan_states[key] = ScanState(source_message, report, time.monotonic())
    _purge_states()


def _purge_states() -> None:
    now = time.monotonic()
    expired = [k for k, v in scan_states.items() if now - v.created_at > STATE_TTL_SECONDS]
    for key in expired:
        scan_states.pop(key, None)
    if len(scan_states) > MAX_STORED_RESULTS:
        ordered = sorted(scan_states.items(), key=lambda kv: kv[1].created_at)
        for key, _ in ordered[: len(scan_states) - MAX_STORED_RESULTS]:
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


async def run_scan(source_message: Any, status_message: Any, *, deep: bool) -> Report:
    filename = safe_filename(source_message)

    async def progress(line: str):
        await edit_status(status_message, status_text(filename, line))

    report, sampled = await asyncio.wait_for(
        inspect_telegram_message(bot, source_message, progress=progress, deep=deep),
        timeout=SCAN_TIMEOUT_SECONDS,
    )
    report.notes.insert(0, f"Read approximately {sampled / 1024 / 1024:.2f} MiB across targeted ranges.")
    return report


async def analyze(event) -> None:
    global checks_total, checks_ok, checks_failed
    checks_total += 1

    filename = safe_filename(event.message)
    status = await event.reply(
        status_text(filename, "⏳ Starting advanced scan…"),
        parse_mode="html",
    )

    async with check_semaphore:
        try:
            report = await run_scan(event.message, status, deep=True)
            result = clip(format_report(report))
            await edit_status(status, result, buttons=report_buttons())
            cache_state(status, event.message, report)
            checks_ok += 1
        except asyncio.TimeoutError:
            checks_failed += 1
            await edit_status(
                status,
                "⏰ <b>Scan time limit reached.</b>\n\n"
                "The scanner stopped safely before attempting a full-file download.\n"
                "Use <b>Deeper re-scan</b> only when the saved metadata windows were not enough.",
                buttons=main_buttons(),
            )
        except errors.FloodWaitError as exc:
            checks_failed += 1
            await edit_status(
                status,
                f"⏳ Telegram temporarily rate-limited this scan. Try again in {int(exc.seconds)} seconds.",
                buttons=main_buttons(),
            )
        except Exception as exc:
            checks_failed += 1
            log.exception("File analysis failed for %s", filename)
            await edit_status(
                status,
                "❌ <b>Could not inspect this file.</b>\n\n"
                f"<code>{html.escape(type(exc).__name__)}</code>\n\n"
                "The scanner did not perform a full-file download.",
                buttons=main_buttons(),
            )


async def handle_new_message(event):
    text = (event.raw_text or "").strip().lower()
    if text in {"/start", "/help"}:
        await event.reply(HELP_TEXT, parse_mode="html", buttons=main_buttons())
        return
    if not is_checkable_message(event):
        return
    await analyze(event)


async def handle_callback(event):
    _purge_states()
    data = (event.data or b"").decode("utf-8", "ignore")

    if data == "home:help":
        await event.answer()
        await event.edit(HELP_TEXT, parse_mode="html", buttons=main_buttons())
        return

    if data == "home:policy":
        await event.answer()
        await event.edit(
            "🛡️ <b>SCAN POLICY</b>\n\n"
            "• Reads only bounded byte ranges from Telegram.\n"
            "• Uses Matroska Info/Tracks indexes when available.\n"
            "• May probe a few sparse ranges if track metadata is not found.\n"
            "• Never intentionally downloads the complete large file.\n"
            "• A 5-minute hard timeout stops an unusually slow/deep scan.",
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
            f"❌ Failed: <code>{checks_failed}</code>\n"
            f"⚙️ Concurrent scans: <code>{MAX_CONCURRENT_CHECKS}</code>",
            parse_mode="html",
            buttons=[[Button.inline("⬅️ Back", b"home:back")]],
        )
        return

    if data == "home:back":
        await event.answer()
        await event.edit(HELP_TEXT, parse_mode="html", buttons=main_buttons())
        return

    source_message = await event.get_message()
    key = state_key(source_message)
    state = scan_states.get(key) if key else None
    if state is None:
        await event.answer("This scan result expired. Send the file again.", alert=True)
        return

    if data.startswith("view:"):
        section = data.split(":", 1)[1]
        await event.answer()
        text = format_section(state.report, section)
        if state.report.container.get("runtime") and section in {"audio", "subs", "video"}:
            text += f"\n\n⏱ Container runtime: <b>{state.report.container['runtime']}</b>"
        await event.edit(clip(text), parse_mode="html", buttons=report_buttons())
        return

    if data == "scan:deep":
        if state.busy:
            await event.answer("A scan is already running for this file.", alert=True)
            return
        state.busy = True
        await event.answer("Deeper metadata scan started…")
        try:
            async with check_semaphore:
                report = await run_scan(state.source_message, source_message, deep=True)
            state.report = report
            state.created_at = time.monotonic()
            await event.edit(clip(format_report(report)), buttons=report_buttons())
        except asyncio.TimeoutError:
            await event.edit(
                "⏰ <b>Deep scan reached the 5-minute limit.</b>\n\n"
                "No full-file download was performed.",
                parse_mode="html",
                buttons=report_buttons(),
            )
        except errors.FloodWaitError as exc:
            await event.edit(
                f"⏳ Telegram rate-limited the deeper scan for {int(exc.seconds)} seconds.",
                buttons=report_buttons(),
            )
        except Exception as exc:
            log.exception("Deep scan failed")
            await event.edit(
                f"❌ Deep scan failed: <code>{html.escape(type(exc).__name__)}</code>",
                parse_mode="html",
                buttons=report_buttons(),
            )
        finally:
            state.busy = False


async def handle_ping(event):
    text = (event.raw_text or "").strip().lower()
    if text not in {"/ping", "/status"}:
        return
    uptime = datetime.now(timezone.utc) - started_at
    await event.reply(
        "🟢 <b>AniToons File Intelligence is online</b>\n"
        f"⏱ Uptime: <code>{str(uptime).split('.')[0]}</code>\n"
        f"📦 Checks: <code>{checks_total}</code>\n"
        f"✅ Successful: <code>{checks_ok}</code>\n"
        f"❌ Failed: <code>{checks_failed}</code>",
        parse_mode="html",
        buttons=main_buttons(),
    )


async def health_server():
    port = int(os.getenv("PORT", "10000"))

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            raw = await reader.read(4096)
            first = raw.split(b"\r\n", 1)[0].decode("latin1", "replace")
            parts = first.split(" ")
            path = parts[1] if len(parts) > 1 else "/"

            if path == "/health":
                payload = {
                    "status": "ok",
                    "service": "anitoons-file-intelligence",
                    "telegram_connected": bot.is_connected(),
                    "checks_total": checks_total,
                    "checks_ok": checks_ok,
                    "checks_failed": checks_failed,
                    "uptime_seconds": int((datetime.now(timezone.utc) - started_at).total_seconds()),
                }
                body = json.dumps(payload, separators=(",", ":")).encode()
                head = b"Content-Type: application/json\r\n"
                code = b"200 OK"
            elif path == "/":
                body = b"AniToons File Intelligence Bot is running. Use /health for monitoring."
                head = b"Content-Type: text/plain; charset=utf-8\r\n"
                code = b"200 OK"
            else:
                body = b"Not found"
                head = b"Content-Type: text/plain; charset=utf-8\r\n"
                code = b"404 Not Found"

            writer.write(
                b"HTTP/1.1 " + code + b"\r\n" + head +
                f"Content-Length: {len(body)}\r\n".encode() +
                b"Cache-Control: no-store\r\nConnection: close\r\n\r\n" + body
            )
            await writer.drain()
        except Exception:
            log.exception("Health request failed")
        finally:
            writer.close()
            with suppress(Exception):
                await writer.wait_closed()

    server = await asyncio.start_server(handler, "0.0.0.0", port)
    log.info("Health endpoint listening on 0.0.0.0:%s", port)
    return server


async def main():
    bot.add_event_handler(handle_new_message, events.NewMessage(incoming=True))
    bot.add_event_handler(handle_callback, events.CallbackQuery)
    bot.add_event_handler(handle_ping, events.NewMessage(incoming=True))

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
            "Telegram bot online as @%s | private_only=%s | concurrency=%s | scan_timeout=%ss",
            username,
            FILE_CHECKER_PRIVATE_ONLY,
            MAX_CONCURRENT_CHECKS,
            SCAN_TIMEOUT_SECONDS,
        )
        await bot.run_until_disconnected()
    finally:
        health.close()
        await health.wait_closed()
        with suppress(Exception):
            await bot.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
