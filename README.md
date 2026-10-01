# AniToons_1Bot

Render-ready Telegram channel join/reaction worker.

## Render
This repository is intended to run as a Render background worker.

Build command:
pip install -r requirements.txt

Start command:
python bot.py

Required environment variables:
API_ID
API_HASH
MONGODB
OWNER_ID
BOT_TOKEN
USER_SESSION

The bot token and Telegram user session are secrets and must not be committed.


## Button dashboard
Send /panel in the bot private chat. The dashboard provides inline controls for channels, reactions, delays, testing, logs, and global start/stop.

## Render variables
API_ID, API_HASH, MONGODB, OWNER_ID and BOT_TOKEN are required for the normal setup. USER_SESSION is optional; when present it enables a Telegram user-account worker for channel joining and reactions. Without USER_SESSION, the bot can still react in channels where the bot account is a member and Telegram permits the operation.
