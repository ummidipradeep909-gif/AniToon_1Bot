from __future__ import annotations

import asyncio
import logging
import os
import random

from dotenv import load_dotenv
from telethon import TelegramClient, events, functions, types, errors
from telethon.sessions import StringSession

load_dotenv()

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"].strip()
BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
OWNER_ID = int(os.environ["OWNER_ID"])
USER_SESSION = os.getenv("USER_SESSION", "").strip()
MONGODB = os.getenv("MONGODB", "").strip()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("anitoons")

bot = TelegramClient("bot_memory", API_ID, API_HASH)

# USER_SESSION is optional. When supplied, a Telegram user account is used
# for channel joining/reactions. Without it, the bot account itself is used.
user = (
    TelegramClient(StringSession(USER_SESSION), API_ID, API_HASH)
    if USER_SESSION
    else None
)
worker = user or bot

REACTIONS = ["❤️", "🔥", "👍"]


def owner_only(event) -> bool:
    return bool(event.is_private and event.sender_id == OWNER_ID)


async def react(event):
    if not event.message or not getattr(event.message, "post", False):
        return

    chat = await event.get_chat()
    if not isinstance(chat, types.Channel) or getattr(chat, "megagroup", False):
        return

    reaction = random.choice(REACTIONS)

    try:
        await worker(
            functions.messages.SendReactionRequest(
                peer=chat,
                msg_id=event.message.id,
                reaction=[types.ReactionEmoji(emoticon=reaction)],
            )
        )
        log.info(
            "Reacted %s in %s to message %s",
            reaction,
            chat.title,
            event.message.id,
        )
    except errors.RPCError as exc:
        log.warning("Reaction failed: %s", exc)


@bot.on(events.NewMessage(incoming=True))
async def commands(event):
    if not owner_only(event):
        return

    text = (event.raw_text or "").strip()

    if text == "/start":
        await event.reply(
            "AniToons_1Bot is online.\n\n"
            "/status - check bot/worker status\n"
            "/join <@channel> - join using USER_SESSION\n"
            "/help - show commands"
        )

    elif text == "/help":
        await event.reply(
            "/start\n"
            "/status\n"
            "/join <@channel>\n"
            "/help"
        )

    elif text == "/status":
        bot_me = await bot.get_me()
        worker_me = await worker.get_me()
        mode = "user session" if user else "bot session"
        await event.reply(
            f"Bot: @{getattr(bot_me, 'username', 'unknown')}\n"
            f"Worker: @{getattr(worker_me, 'username', None) or 'no_username'}\n"
            f"Mode: {mode}\n"
            f"MongoDB configured: {'yes' if MONGODB else 'no'}"
        )

    elif text.startswith("/join "):
        if not user:
            await event.reply(
                "USER_SESSION is not configured. A bot account cannot join "
                "channels by itself. Add the bot to the target channel as an "
                "administrator, or configure USER_SESSION."
            )
            return

        ref = text.split(maxsplit=1)[1].strip()

        try:
            entity = await user.get_entity(ref)

            if not isinstance(entity, types.Channel):
                await event.reply("That target is not a Telegram channel.")
                return

            await user(functions.channels.JoinChannelRequest(channel=entity))
            await event.reply(f"Joined: {entity.title}")

        except errors.UserAlreadyParticipantError:
            await event.reply("Already joined.")
        except errors.RPCError as exc:
            await event.reply(f"Telegram error: {exc}")


@worker.on(events.NewMessage(incoming=True))
async def new_channel_post(event):
    try:
        await react(event)
    except Exception:
        log.exception("Unhandled reaction error")


async def main():
    await bot.start(bot_token=BOT_TOKEN)

    if user:
        await user.connect()

        if not await user.is_user_authorized():
            raise RuntimeError(
                "USER_SESSION is invalid or not authorized."
            )

        log.info("Bot and user worker sessions connected.")

        await asyncio.gather(
            bot.run_until_disconnected(),
            user.run_until_disconnected(),
        )
    else:
        log.info(
            "Bot session connected. USER_SESSION is not set; "
            "running in bot-only mode."
        )
        await bot.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
