# AniToons_1Bot — Telegram File Info Bot

This project now does **one job only**: receive a Telegram file and send file information back.

## What the bot checks

Audio, video, subtitle, image, document, archive, PDF and other file types.

The report can include:

- filename, MIME type, detected type and total size
- Telegram audio/video metadata
- audio header information when available
- subtitle formats such as SRT, VTT, ASS/SSA, TTML and SAMI
- embedded Matroska/WebM and MP4 track hints when those atoms/elements are inside the sample
- basic image dimensions
- common file signatures
- a SHA-256 hash of the sampled bytes

## No full-file download

For large files the bot samples only the beginning of the remote file.

Default maximum sample: **2 MiB**.

Maximum configurable sample: **4 MiB**.

The sample is kept in memory and is not saved as a complete downloaded file. A file that is smaller than the sample limit may necessarily be read completely.

A beginning-only sample cannot guarantee metadata stored near the end of some containers.

## Telegram usage

Open the bot and send a file.

Use:

- \`/start\`
- \`/help\`

The bot replies with the inspection report. No channel management, reactions, MongoDB, SQLite, user session or other bot features are used.

## Render

The service is configured as a Render **web service** because UptimeRobot needs an HTTP endpoint.

Required environment variables:

- \`API_ID\`
- \`API_HASH\`
- \`BOT_TOKEN\`

Optional:

- \`FILE_CHECKER_PRIVATE_ONLY=1\` limits checks to private chats.
- \`FILE_PROBE_BYTES=2097152\` sets the prefix sample size (64 KiB–4 MiB).
- \`FILE_PROBE_CHUNK_BYTES=262144\` sets the request chunk size (64 KiB–512 KiB).

Render exposes the service URL after deployment.

### UptimeRobot

Create an **HTTP(s) monitor** using:

\`https://<your-render-service-url>/health\`

The endpoint returns HTTP 200 with a small JSON response while the service is running.

Do not use the Telegram \`t.me\` bot link as the UptimeRobot monitor URL.

## Local run

\`\`\`bash
pip install -r requirements.txt
python file_bot.py
\`\`\`
