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
