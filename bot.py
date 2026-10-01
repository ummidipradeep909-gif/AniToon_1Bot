from __future__ import annotations

import logging
import os
import random
import asyncio

from dotenv import load_dotenv
from telethon import TelegramClient, events, functions, types, errors
from telethon.sessions import StringSession

load_dotenv()

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"].strip()
BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
OWNER_ID = int(os.environ["OWNER_ID"])
USER_SESSION = os.environ["USER_SESSION"].strip()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("anitoons")

bot = TelegramClient("bot_memory", API_ID, API_HASH)
user = TelegramClient(StringSession(USER_SESSION), API_ID, API_HASH)

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
        await user(
            functions.messages.SendReactionRequest(
                peer=chat,
                msg_id=event.message.id,
                reaction=[types.ReactionEmoji(emoticon=reaction)],
            )
        )
        log.info("Reacted %s in %s to message %s", reaction, chat.title, event.message.id)
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
            "/status - check both sessions\n"
            "/join <@channel> - join a public channel\n"
            "/help - show commands"
        )
    elif text == "/help":
        await event.reply(
            "/start\n/status\n/join <@channel>\n/help"
        )
    elif text == "/status":
        bot_me = await bot.get_me()
        user_me = await user.get_me()
        await event.reply(
            f"Bot: @{getattr(bot_me, 'username', 'unknown')}\n"
            f"Worker: @{getattr(user_me, 'username', None) or 'no_username'}"
        )
    elif text.startswith("/join "):
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

@user.on(events.NewMessage(incoming=True))
async def new_channel_post(event):
    try:
        await react(event)
    except Exception:
        log.exception("Unhandled reaction error")

async def main():
    await bot.start(bot_token=BOT_TOKEN)
    await user.connect()

    if not await user.is_user_authorized():
        raise RuntimeError("USER_SESSION is invalid or not authorized.")

    log.info("Bot and worker sessions connected.")
    await asyncio.gather(
        bot.run_until_disconnected(),
        user.run_until_disconnected(),
    )

if __name__ == "__main__":
    asyncio.run(main())
