# AniToon_1Bot — Media Intelligence

AniToon is a lightweight Telegram media-inspection bot with web reports, optional Gemini-powered web research, YouTube discovery, and opt-in scheduled topic reports.

## Current features

- Inspect supported Telegram media and documents; return technical metadata and bounded-sample reports.
- View web reports with existing report-expiry and resource safeguards.
- `/intel <topic>` — ask Gemini for a concise search-grounded report with source links, emerging trends, cautious public-reaction analysis, claim checks and uncertainty notes when GEMINI_API_KEY is configured. Without a key or during Gemini errors, it falls back to a keyless headline/source digest from public RSS feeds (not AI analysis).
- \`/videos <topic>\` — find YouTube videos with titles, channel names, upload dates, descriptions and direct links.
- \`/shorts <topic>\` — search short-duration YouTube videos. YouTube's public Data API filter returns videos under four minutes; it does not confirm that each result is actually a YouTube Short.
- \`/watch <topic>\` — opt in to a scheduled Gemini report every 12 hours by default.
- \`/watches\` — list your scheduled topics.
- \`/unwatch <exact topic>\` — stop a scheduled topic.
- Creator support is linked from the bot menu, help/about screens and website: [@AniToon_0Bot](https://t.me/AniToon_0Bot).

## Configure Gemini and YouTube

The core bot still starts without these optional keys. The added search commands need API access:

1. Create a Gemini API key in [Google AI Studio](https://aistudio.google.com/app/apikey).
2. Create a Google Cloud API key and enable the YouTube Data API v3 for video metadata/search. If one Google key is allowed to access both APIs, you may use the same key for both variables.
3. In Render → \`AniToons_1Bot\` → Environment, set:
   - \`GEMINI_API_KEY\` — enables \`/intel\` and scheduled reports.
   - \`GEMINI_MODEL\` — optional model override; default \`gemini-3.5-flash-lite\`.
   - \`YOUTUBE_API_KEY\` — enables individual YouTube results; without this, \`/videos\` and \`/shorts\` provide a topic-specific YouTube search page instead.
   - \`YOUTUBE_REGION_CODE\` — optional two-letter region; defaults to \`IN\`.
   - \`INTEL_WATCH_INTERVAL_HOURS\` — optional scheduled interval from 6 to 24 hours; defaults to \`12\`.
4. Redeploy the Render service after setting the keys.

Keys are read from environment variables; they are not stored in source control. Google API usage limits and possible charges depend on your account, APIs and model. Grounding helps cite sources but does not guarantee correctness or exhaustive search coverage.

## Scheduled reports and storage

Scheduled watches are private-chat subscriptions; use `/watch` in a private chat with the bot. The first report runs after the configured interval. When Gemini is configured, reports prefer Gemini; otherwise (or during a Gemini failure), they send a lightweight public RSS headline/source digest. For persistence across restarts, the existing MongoDB connection must be configured and reachable via `MONGODB_URI` (or an existing supported alias). Watches are capped at two per user and twenty globally to limit background API calls.

Reports distinguish sourced facts from inferences when the provider returns grounding metadata. Search coverage and available pages may be incomplete. Sentiment summaries are directional observations from retrieved sources, not representative polling. No YouTube media is downloaded or re-hosted.

## Lower-memory defaults

The Render Blueprint sets conservative scan defaults:

- One concurrent scan.
- 2 MiB initial media sample and 4 MiB deep sample.
- 128 KiB media-probe chunks.
- 90-second scan timeout.

Existing resource and web-egress guards remain in place. These reduce workload and network transfer; they cannot guarantee that the Render monthly quota will never be reached.

## Render free-tier limits

Render documents **750 free instance hours per workspace per calendar month**. The allowance is shared by free web services in the workspace, and using all included hours suspends those free web services until the next month. An always-on Telegram bot consumes runtime hours; lowering memory or downloaded MB does not make those hours free. Check the workspace's current usage in the Render Dashboard before keeping multiple free services awake.

The service is a Render web service with a \`/health\` endpoint. Use \`https://<your-render-service-url>/health\` for an HTTP uptime monitor. A monitor sending requests frequently can keep the free web service awake, which also consumes instance hours.

## Run locally

Required existing variables: \`API_ID\`, \`API_HASH\`, \`BOT_TOKEN\`. Optional features use the keys listed above.

\`\`\`bash
pip install -r requirements.txt
python bot.py
\`\`\`

Run the lightweight helper tests with:

\`\`\`bash
python -m unittest test_intelligence.py -v
\`\`\`
