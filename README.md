# AniToons_1Bot

Render-ready Telegram channel join/reaction worker with a lightweight file checker.

## Render

Build command:

\`\`\`text
pip install -r requirements.txt
\`\`\`

Start command:

\`\`\`text
python file_bot.py
\`\`\`

Required environment variables:

\`API_ID\`, \`API_HASH\`, \`BOT_TOKEN\`, \`OWNER_ID\`, \`MONGODB\` and optionally \`USER_SESSION\` as used by the existing worker.

The bot token and Telegram user session are secrets and must not be committed.

## Button dashboard

Send \`/panel\` in the bot private chat. The existing dashboard provides channel, reaction, delay, test, log and global start/stop controls.

## Lightweight file checker

Send a file, audio, video, subtitle, image, archive, PDF or other document to the bot. The checker uses Telegram metadata plus only a small prefix sample from the beginning of the remote file. By default it reads at most **2 MiB** and does not save the complete file to disk.

See [README_FILE_CHECKER.md](README_FILE_CHECKER.md) for supported checks and limitations.

## File-checker environment variables

\`FILE_CHECKER_ENABLED=1\` enables the checker.

\`FILE_CHECKER_PRIVATE_ONLY=1\` (default) limits checks to private chats. Set it to \`0\` for groups/channels.

\`FILE_CHECKER_OWNER_ONLY=1\` restricts file checks to \`OWNER_ID\`.

\`FILE_PROBE_BYTES=2097152\` controls the maximum prefix sample (64 KiB to 4 MiB).

\`FILE_PROBE_CHUNK_BYTES=262144\` controls the Telegram request/chunk size (64 KiB to 512 KiB).
