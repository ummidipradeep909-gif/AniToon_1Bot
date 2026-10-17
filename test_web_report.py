"""Regression tests for the advanced HTML media report renderer."""

import os
from datetime import datetime, timedelta, timezone

# bot.py requires these at import time; no Telegram connection is started by importing it.
os.environ.setdefault("API_ID", "12345")
os.environ.setdefault("API_HASH", "0123456789abcdef0123456789abcdef")
os.environ.setdefault("BOT_TOKEN", "123456:TEST_TOKEN")

import bot
from file_inspector import Report


def test_video_report_uses_advanced_renderer_and_preserves_track_details():
    report = Report(
        filename="sample.mkv",
        size=3 * 1024**3,
        mime="video/x-matroska",
        ext=".mkv",
        detected="Matroska/WebM container",
        media_kind="Video",
        sampled=5 * 1024 * 1024,
        video={
            "tracks": [
                {
                    "type": "video",
                    "track": "1",
                    "name": "Video Track 1",
                    "codec_name": "H.264/AVC",
                    "dimensions": "1920 × 1080",
                    "frame_rate": "23.976 fps",
                    "profile": "High",
                }
            ]
        },
        audio={
            "tracks": [
                {
                    "type": "audio",
                    "track": "2",
                    "name": "Japanese",
                    "language": "jpn",
                    "language_name": "Japanese",
                    "codec_name": "AAC",
                    "channels": "2",
                    "sample_rate": "48000 Hz",
                },
                {
                    "type": "audio",
                    "track": "3",
                    "name": "English",
                    "language": "eng",
                    "language_name": "English",
                    "codec_name": "AAC",
                    "channels": "2",
                    "sample_rate": "48000 Hz",
                },
            ]
        },
        subtitles=[
            {
                "type": "subtitle",
                "track": "4",
                "name": "English subtitles",
                "language": "eng",
                "language_name": "English",
                "format": "SubRip",
            }
        ],
        container={
            "runtime": "02:22:53",
            "average_bitrate": "3.68 Mbps",
        },
        media_info={
            "category": "Video",
            "file_extension": ".mkv",
            "detected_format": "Matroska/WebM container",
        },
    )

    page = bot.web_page(
        report,
        "regression-test-token",
        datetime.now(timezone.utc) + timedelta(hours=1),
    ).decode("utf-8")

    # The advanced renderer uses track cards and grouped media metadata.
    # The old fallback report doesn't include these rich track-card components.
    assert '<article class="track-card"' in page
    assert "H.264/AVC" in page
    assert "Japanese" in page
    assert "English subtitles" in page
    assert "File &amp; container metadata" in page
