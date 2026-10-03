from __future__ import annotations

import asyncio
import os

from telethon import events

import bot as base_bot
from file_inspector import format_report, inspect_telegram_message

FILE_CHECKER_ENABLED = os.getenv("FILE_CHECKER_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
FILE_CHECKER_OWNER_ONLY = os.getenv("FILE_CHECKER_OWNER_ONLY", "0").strip().lower() in {"1", "true", "yes", "on"}
FILE_CHECKER_PRIVATE_ONLY = os.getenv("FILE_CHECKER_PRIVATE_ONLY", "1").strip().lower() in {"1", "true", "yes", "on"}

def is_checkable_message(event) -> bool:
    message = event.message
    if not message or not getattr(message, "media", None):
        return False
    if not getattr(message, "file", None) and not getattr(message, "photo", None):
        return False
    if FILE_CHECKER_PRIVATE_ONLY and not event.is_private:
        return False
    if FILE_CHECKER_OWNER_ONLY and event.sender_id != base_bot.OWNER_ID:
        return False
    return True

async def file_check_handler(event):
    if not FILE_CHECKER_ENABLED or not is_checkable_message(event):
        return
    status = await event.reply(
        "🔎 Checking file metadata…\n"
        "Only a small sample from the beginning will be read; the full file is not downloaded."
    )
    try:
        report, _sample = await inspect_telegram_message(base_bot.bot, event.message)
        result = format_report(report)
        if len(result) > 3900:
            result = result[:3850] + "\n…report trimmed to fit Telegram."
        try:
            await status.edit(result)
        except Exception:
            await event.reply(result)
    except Exception as exc:
        base_bot.log.exception("File check failed")
        try:
            await status.edit(f"❌ File check failed: {type(exc).__name__}: {exc}")
        except Exception:
            await event.reply(f"❌ File check failed: {type(exc).__name__}: {exc}")

async def main():
    base_bot.bot.add_event_handler(file_check_handler, events.NewMessage(incoming=True))
    base_bot.log.info(
        "File checker enabled=%s private_only=%s owner_only=%s probe_bytes=%s",
        FILE_CHECKER_ENABLED,
        FILE_CHECKER_PRIVATE_ONLY,
        FILE_CHECKER_OWNER_ONLY,
        os.getenv("FILE_PROBE_BYTES", "2097152"),
    )
    await base_bot.main()

if __name__ == "__main__":
    asyncio.run(main())
