# Lightweight File Checker

This add-on integrates with \`AniToons_1Bot\` and checks Telegram files without downloading the complete file.

## What it does

Send a file, audio, video, subtitle, image, archive, PDF, or other document to the bot. The bot uses Telegram metadata plus a small **prefix sample** read with Telethon \`iter_download()\`.

Default sample: **2 MiB from the beginning**. The sample is kept in memory only and discarded after the report.

It can report:

- filename, total size, MIME type and detected type
- Telegram audio/video metadata such as duration, artist/title, resolution and streaming support when Telegram provides it
- audio header information for MP3/ID3, FLAC, WAV and Ogg/Opus samples
- subtitle detection for SRT, VTT, ASS/SSA, TTML, SAMI and MicroDVD files
- embedded Matroska/WebM track metadata when the Tracks element is inside the initial sample
- embedded MP4 subtitle/text/audio handler hints when metadata is inside the initial sample
- basic PNG/JPEG dimensions
- common archive/document/image signatures
- SHA-256 of the sampled bytes (not the complete file)

## Important limitation

A prefix-only scan cannot guarantee complete embedded-stream information for every container. Some MP4/MOV files put important metadata near the end, and archive indexes commonly live at the end. In those cases the bot reports that the information was not present in the sampled beginning instead of downloading the full file.

For very small files whose total size is below the sample limit, Telegram may necessarily send the entire small file because there is nothing more to sample.

The checker does not execute or unpack the uploaded file.

## Configuration

\`FILE_CHECKER_ENABLED=1\` enables the checker.

\`FILE_CHECKER_PRIVATE_ONLY=1\` (default) limits checks to files sent in private chats. Set it to \`0\` for groups/channels.

\`FILE_CHECKER_OWNER_ONLY=1\` restricts checks to \`OWNER_ID\`.

\`FILE_PROBE_BYTES=2097152\` controls the maximum prefix sample (64 KiB to 4 MiB).

\`FILE_PROBE_CHUNK_BYTES=262144\` controls the Telegram request/chunk size (64 KiB to 512 KiB).

## Running

Use:

\`\`\`bash
python file_bot.py
\`\`\`

The existing channel/reaction functionality from \`bot.py\` is still used. \`file_bot.py\` adds the lightweight file-check handler and then starts the existing bot application.
