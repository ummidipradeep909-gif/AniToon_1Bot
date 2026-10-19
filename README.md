# AniToon_1Bot — Telegram Media Inspector

AniToon inspects supported Telegram media and documents, then provides a clean browser report with the metadata it can safely obtain using bounded reads.

## Main features

- Inspect photos, videos, animations, stickers, audio, PDFs, documents and supported post attachments.
- Clean, type-specific web reports with useful media metadata and less repeated technical noise.
- ZIP archive contents, file sizes and timestamps, plus available audio/subtitle track names without extracting archive contents to disk.
- Shared FIFO scan queue with visible processing/queue positions, cancellation and resource safeguards.
- Clone bot manager and owner dashboard for user, clone, queue, performance and service status.

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

Report links expire after **10 minutes**. File inspection reports available metadata; it is not a full malware scan, full media decode, or safety guarantee. Large files and archive members are inspected with bounded reads, and ZIP entries are listed without extracting files to disk.

The Render service exposes `/health` for service monitoring. Resource guards limit scan concurrency and probe work, but they cannot guarantee that hosting quotas will never be reached.

## Run locally

```bash
pip install -r requirements.txt
python bot.py
```
