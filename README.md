# AniToon_1Bot — Telegram File Inspector

AniToon_1Bot inspects supported Telegram media and document uploads and creates a browser-based report with the metadata it can safely read.

## How to use the bot

1. Send a supported photo, video, video note, audio or voice message, PDF, ZIP archive, or another document.
2. Press **Scan File** beneath the upload. Uploading a file does not start a scan automatically.
3. Follow the queue and progress message while AniToon inspects the file.
4. When the scan completes, press **View File Info** to open the browser report.

For video uploads, AniToon also displays the total runtime with the upload when Telegram provides that duration.

## Main features

- Inspect photos, videos, video notes, audio, voice messages, PDFs, ZIP archives and supported document uploads.
- Type-specific browser reports for available image, video, audio, subtitle, PDF and technical metadata.
- ZIP archive directory listings, file sizes and timestamps, plus available audio/subtitle track names without extracting archive contents to disk.
- Shared FIFO scan queue with visible queue positions, progress updates, cancellation and resource safeguards.
- Clone bot manager and owner dashboard for user, clone, queue, performance and service status.

Stickers, GIFs/animations and ordinary link previews are not listed as supported scan inputs in the current bot workflow.

Research commands, scheduled topic reports and video-discovery commands are not part of the bot's user-facing feature set.

## Configuration

Required environment variables:

- `API_ID`
- `API_HASH`
- `BOT_TOKEN`

Optional:

- `MONGODB_URI` — persistence for scan/user/clone records and dashboard analytics.
- `CLONE_TOKEN_ENCRYPTION_KEY` — encryption key for stored clone bot tokens.
- `MAX_CONCURRENT_CHECKS` — owner-configurable upper bound for simultaneous scans, from 1 to 5 (Render defaults to 5). A CPU- and RAM-aware controller dynamically limits worker starts to reduce contention on small instances: it respects the container CPU quota as well as RAM thresholds, and pauses at 75% RAM usage. The owner can change the cap from the Performance & RAM dashboard, and MongoDB saves that selection across restarts.
- `SCAN_TIMEOUT_SECONDS` — per-scan timeout.

Never commit bot tokens, API credentials or database credentials to source control.

## Web reports and limits

Report links expire after **10 minutes**. File inspection reports the metadata available from Telegram and the file; it is not a full malware scan, full media decode, or safety guarantee. Large files and archive members are inspected with bounded reads, and ZIP entries are listed without extracting files to disk. Some metadata may be unavailable depending on the format and what can be read.

The Render service exposes `/health` for service monitoring. Resource guards limit scan concurrency and probe work, but they cannot guarantee that hosting quotas will never be reached.

## Run locally

```bash
pip install -r requirements.txt
python bot.py
```
