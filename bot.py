from __future__ import annotations

import asyncio
import json
import logging
import os
from contextlib import suppress
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv
from telethon import TelegramClient, events, errors

from file_inspector import format_report, inspect_telegram_message

load_dotenv()

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"].strip()
BOT_TOKEN = os.environ["BOT_TOKEN"].strip()

FILE_CHECKER_PRIVATE_ONLY = (
    os.getenv("FILE_CHECKER_PRIVATE_ONLY", "0").strip().lower()
    not in {"0", "false", "no", "off"}
)
MAX_CONCURRENT_CHECKS = max(
    1, min(int(os.getenv("MAX_CONCURRENT_CHECKS", "3")), 8)
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("anitoons-file-checker")

bot = TelegramClient("file_checker_bot", API_ID, API_HASH)
check_semaphore = asyncio.Semaphore(MAX_CONCURRENT_CHECKS)
started_at = datetime.now(timezone.utc)
checks_total = 0
checks_ok = 0
checks_failed = 0

HELP_TEXT = (
    "🔬 <b>AniToons File Intelligence Bot</b>\n\n"
    "Send me almost any Telegram file and I’ll inspect it.\n\n"
    "<b>Checks can include</b>\n"
    "🎬 Video — resolution, duration, container, codecs/tracks\n"
    "🔊 Audio — duration, tags, sample rate/channels when present\n"
    "💬 Subtitles — SRT, VTT, ASS/SSA, TTML, SAMI and embedded tracks\n"
    "🖼 Images — format and dimensions\n"
    "📦 Archives/documents — file signatures and basic structure\n"
    "🧩 Other files — MIME/type/signature detection\n\n"
    "<b>Download policy</b>\n"
    "For large files I read only a small sample from the beginning "
    "(default 2 MiB). The complete large file is not downloaded or saved.\n\n"
    "<i>Some containers store important indexes near the end, so a "
    "beginning-only check cannot always reveal every track.</i>"
)

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

async def edit_status(message, text: str) -> None:
    with suppress(Exception):
        await message.edit(text)

async def analyze(event) -> None:
    global checks_total, checks_ok, checks_failed
    checks_total += 1

    filename = safe_filename(event.message)
    status = await event.reply(
        "🧠 <b>Analyzing file…</b>\n"
        f"📄 <code>{filename[:100]}</code>\n"
        "⏱ Reading only a small beginning sample — no full-file download."
    )

    async with check_semaphore:
        try:
            report, sample = await inspect_telegram_message(bot, event.message)
            result = format_report(report)

            footer = (
                "\n\n⚡ <b>Lightweight scan</b>"
                f" • {sample.data.__len__() / 1024 / 1024:.2f} MiB sampled"
            )
            result += footer

            if len(result) > 3900:
                result = result[:3820] + "\n\n…report shortened."

            await edit_status(status, result)
            checks_ok += 1
        except errors.FloodWaitError as exc:
            checks_failed += 1
            await edit_status(
                status,
                f"⏳ Telegram rate limit. Please try again after {exc.seconds} seconds.",
            )
        except Exception as exc:
            checks_failed += 1
            log.exception("File analysis failed for %s", filename)
            await edit_status(
                status,
                "❌ <b>Could not inspect this file.</b>\n"
                f"<code>{type(exc).__name__}</code>\n\n"
                "The file itself was not downloaded in full.",
            )

async def handle_new_message(event):
    if (event.raw_text or "").strip().lower() in {"/start", "/help"}:
        await event.reply(HELP_TEXT, parse_mode="html")
        return
    if not is_checkable_message(event):
        return
    await analyze(event)

async def handle_ping(event):
    text = (event.raw_text or "").strip().lower()
    if text in {"/ping", "/status"}:
        uptime = datetime.now(timezone.utc) - started_at
        await event.reply(
            "🟢 <b>AniToons File Intelligence is online</b>\n"
            f"⏱ Uptime: <code>{str(uptime).split('.')[0]}</code>\n"
            f"📦 Checks: <code>{checks_total}</code>\n"
            f"✅ Successful: <code>{checks_ok}</code>\n"
            f"❌ Failed: <code>{checks_failed}</code>",
            parse_mode="html",
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
                    "uptime_seconds": int(
                        (datetime.now(timezone.utc) - started_at).total_seconds()
                    ),
                }
                body = json.dumps(payload, separators=(",", ":")).encode()
                head = b"Content-Type: application/json\r\n"
                code = b"200 OK"
            elif path == "/":
                body = (
                    b"AniToons File Intelligence Bot is running. "
                    b"Use /health for monitoring."
                )
                head = b"Content-Type: text/plain; charset=utf-8\r\n"
                code = b"200 OK"
            else:
                body = b"Not found"
                head = b"Content-Type: text/plain; charset=utf-8\r\n"
                code = b"404 Not Found"

            response = (
                b"HTTP/1.1 " + code + b"\r\n"
                + head
                + f"Content-Length: {len(body)}\r\n".encode()
                + b"Cache-Control: no-store\r\n"
                + b"Connection: close\r\n\r\n"
                + body
            )
            writer.write(response)
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
    bot.add_event_handler(handle_ping, events.NewMessage(incoming=True))

    health = await health_server()
    try:
        await bot.start(bot_token=BOT_TOKEN)
        me = await bot.get_me()
        username = getattr(me, "username", "unknown")
        log.info(
            "Telegram bot online as @%s | private_only=%s | concurrency=%s",
            username,
            FILE_CHECKER_PRIVATE_ONLY,
            MAX_CONCURRENT_CHECKS,
        )
        await bot.run_until_disconnected()
    finally:
        health.close()
        await health.wait_closed()
        with suppress(Exception):
            await bot.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
