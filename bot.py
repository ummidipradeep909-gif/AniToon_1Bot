from __future__ import annotations

import asyncio
import logging
import os

from dotenv import load_dotenv
from telethon import TelegramClient, events

from file_inspector import format_report, inspect_telegram_message

load_dotenv()

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"].strip()
BOT_TOKEN = os.environ["BOT_TOKEN"].strip()

FILE_CHECKER_PRIVATE_ONLY = (
    os.getenv("FILE_CHECKER_PRIVATE_ONLY", "1").strip().lower()
    not in {"0", "false", "no", "off"}
)
FILE_PROBE_BYTES = os.getenv("FILE_PROBE_BYTES", "2097152")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("file-checker")

bot = TelegramClient("file_checker_bot", API_ID, API_HASH)

def is_checkable_message(event) -> bool:
    message = event.message
    if not message or not getattr(message, "media", None):
        return False
    if not getattr(message, "file", None) and not getattr(message, "photo", None):
        return False
    if FILE_CHECKER_PRIVATE_ONLY and not event.is_private:
        return False
    return True

def help_text() -> str:
    return (
        "🔎 File Info Bot\n\n"
        "Send me an audio file, video, subtitle, image, document, archive, "
        "PDF, or another file.\n\n"
        f"I read only a small sample from the beginning (default: {FILE_PROBE_BYTES} bytes) "
        "plus Telegram-provided metadata. I do not download the complete file.\n\n"
        "Large-file metadata can be incomplete when important information is stored "
        "near the end of a container."
    )

async def file_check_handler(event):
    if not is_checkable_message(event):
        return

    status = await event.reply(
        "🔎 Checking file…\n"
        "Reading only a small sample from the beginning. No full-file download."
    )

    try:
        report, _sample = await inspect_telegram_message(bot, event.message)
        result = format_report(report)
        if len(result) > 3900:
            result = result[:3850] + "\n…report trimmed to fit Telegram."
        try:
            await status.edit(result)
        except Exception:
            await event.reply(result)
    except Exception as exc:
        log.exception("File check failed")
        error = f"❌ File check failed: {type(exc).__name__}: {exc}"
        try:
            await status.edit(error)
        except Exception:
            await event.reply(error)

async def health_server():
    port = int(os.getenv("PORT", "10000"))

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            request = await reader.read(4096)
            first_line = request.split(b"\r\n", 1)[0].decode("latin1", "replace")
            path = first_line.split(" ")[1] if len(first_line.split(" ")) > 1 else "/"
            if path == "/health":
                body = b'{"status":"ok","service":"telegram-file-checker"}'
                response = (
                    b"HTTP/1.1 200 OK\r\n"
                    b"Content-Type: application/json\r\n"
                    b"Cache-Control: no-store\r\n"
                    + f"Content-Length: {len(body)}\r\n".encode()
                    + b"Connection: close\r\n\r\n"
                    + body
                )
            elif path == "/":
                body = b"Telegram File Checker is running."
                response = (
                    b"HTTP/1.1 200 OK\r\n"
                    b"Content-Type: text/plain; charset=utf-8\r\n"
                    + f"Content-Length: {len(body)}\r\n".encode()
                    + b"Connection: close\r\n\r\n"
                    + body
                )
            else:
                body = b"Not found"
                response = (
                    b"HTTP/1.1 404 Not Found\r\n"
                    b"Content-Type: text/plain; charset=utf-8\r\n"
                    + f"Content-Length: {len(body)}\r\n".encode()
                    + b"Connection: close\r\n\r\n"
                    + body
                )
            writer.write(response)
            await writer.drain()
        except Exception:
            log.exception("Health server request failed")
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    server = await asyncio.start_server(handle, "0.0.0.0", port)
    log.info("Health server listening on 0.0.0.0:%s", port)
    return server

async def main():
    bot.add_event_handler(file_check_handler, events.NewMessage(incoming=True))
    health = await health_server()
    await bot.start(bot_token=BOT_TOKEN)
    me = await bot.get_me()
    username = getattr(me, "username", "unknown")
    log.info(
        "Telegram file checker online as @%s | private_only=%s | probe=%s",
        username,
        FILE_CHECKER_PRIVATE_ONLY,
        FILE_PROBE_BYTES,
    )
    try:
        await bot.run_until_disconnected()
    finally:
        health.close()
        await health.wait_closed()
        await bot.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
