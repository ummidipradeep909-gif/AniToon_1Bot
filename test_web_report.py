"""Regression tests for the advanced HTML media report renderer."""

import os
from datetime import datetime, timedelta, timezone

# bot.py requires these at import time; no Telegram connection is started by importing it.
os.environ.setdefault("API_ID", "12345")
os.environ.setdefault("API_HASH", "0123456789abcdef0123456789abcdef")
os.environ.setdefault("BOT_TOKEN", "123456:TEST_TOKEN")

import bot
from file_inspector import Report, format_report, format_section


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
    assert "Detected format" not in page



def test_pdf_report_uses_the_shared_file_report_layout_and_clean_pdf_fields():
    report = Report(
        filename="course-notes.pdf",
        size=42 * 1024,
        mime="application/pdf",
        ext=".pdf",
        detected="PDF document",
        media_kind="PDF",
        container={
            "pdf_version": "1.7",
            "pages": "12",
            "page_size_name": "A4",
            "title": "Course Notes",
            "author": "Polytechnic Department",
            "subject": "Chemistry",
            "encrypted": "no",
            "tagged": "yes",
            "linearized": "no",
            "forms": "no",
            "javascript": "no",
            "embedded_files": "no",
            "fonts": "4",
        },
        media_info={
            "category": "PDF",
            "detected_format": "PDF document",
            "file_extension": ".pdf",
            "declared_mime_type": "application/pdf",
            "file_size_bytes": 43008,
            "pdf_pages": "12",
            "telegram_message_id": "98765",
            "views": "12345",
        },
    )

    page = bot.web_page(
        report,
        "pdf-regression-token",
        datetime.now(timezone.utc) + timedelta(hours=1),
    ).decode("utf-8")

    # PDF pages now share the same nav, hero, summary grid and theme controls.
    assert '<div class="brand">AniToon • File Reports</div>' in page
    assert '<div class="hero-copy">' in page
    assert 'id="overview"' in page
    assert 'id="theme-toggle"' in page
    assert "Course Notes" in page
    assert "Polytechnic Department" in page
    assert "PDF security" in page
    assert "Tagged document" in page

    # Keep useful Telegram source/post context while presenting friendly labels.
    assert "telegram_message_id" not in page
    assert "Telegram message ID" in page
    assert "98765" in page
    assert "Views" in page
    assert "12345" in page
    assert "Runtime source" not in page


def test_queue_positions_shift_only_after_the_active_scan_finishes(monkeypatch):
    import asyncio

    old_order = list(bot.scan_queue_order)
    old_statuses = dict(bot.queued_scan_statuses)
    old_processing = set(bot.processing_scan_tokens)
    old_count = bot.queued_processes
    calls = []
    status_one, status_two, status_three = object(), object(), object()

    async def fake_edit_status(message, text, buttons=None):
        calls.append((message, text, buttons))

    monkeypatch.setattr(bot, "edit_status", fake_edit_status)
    monkeypatch.setattr(bot, "cancel_button", lambda token: f"cancel:{token}")

    try:
        bot.scan_queue_order[:] = ["token-1", "token-2", "token-3"]
        bot.queued_scan_statuses.clear()
        bot.queued_scan_statuses.update({
            "token-1": (status_one, "first.mkv"),
            "token-2": (status_two, "second.pdf"),
            "token-3": (status_three, "third.zip"),
        })
        # Active scans are not assigned queue numbers. The waiting scans start
        # at QUEUE 1 and retain consecutive positions while the active scan runs.
        bot.processing_scan_tokens.clear()
        bot.processing_scan_tokens.add("token-1")
        bot.queued_processes = 2

        async def exercise_positions():
            await bot._refresh_queue_positions()
            assert len(calls) == 2
            assert calls[0][0] is status_two and "QUEUE 1" in calls[0][1]
            assert calls[1][0] is status_three and "QUEUE 2" in calls[1][1]
            assert "0%" not in calls[0][1] and "0%" not in calls[1][1]

            # Completing the active scan does not count it as a waiting queue item.
            await bot._remove_queued_scan("token-1")

        asyncio.run(exercise_positions())

        shifted = calls[-2:]
        assert shifted[0][0] is status_two and "QUEUE 1" in shifted[0][1]
        assert shifted[1][0] is status_three and "QUEUE 2" in shifted[1][1]
        assert bot.queued_processes == 2
    finally:
        bot.scan_queue_order[:] = old_order
        bot.queued_scan_statuses.clear()
        bot.queued_scan_statuses.update(old_statuses)
        bot.processing_scan_tokens.clear()
        bot.processing_scan_tokens.update(old_processing)
        bot.queued_processes = old_count
        bot.scan_queue_changed.clear()


def test_processing_status_is_separate_from_waiting_queue_numbers():
    first = bot.status_text("first.mkv", "Starting scan", 1, 1)
    second = bot.status_text("second.pdf", "Starting scan", 1, 2)

    assert "<b>PROCESSING</b>" in first
    assert "<b>PROCESSING</b>" in second
    assert "QUEUE" not in first
    assert "QUEUE" not in second
    assert "first.mkv" in first
    assert "second.pdf" in second


def test_user_report_hides_probe_internals_but_keeps_scan_caveats():
    report = Report(
        filename="large-video.mkv",
        size=20 * 1024 * 1024,
        mime="video/x-matroska",
        ext=".mkv",
        detected="Matroska/WebM container",
        media_kind="Video",
        sampled=4 * 1024 * 1024,
        probe_ranges=["initial metadata: +0 B (4.00 MiB)"],
        notes=[
            "Combined metadata sources: custom container parser + PyAV/FFmpeg.",
            "Partial scan: source processing was bounded below 5 MB.",
        ],
    )

    summary = format_report(report)
    technical = format_section(report, "technical")

    assert "Sampled:" not in summary
    assert "targeted range" not in summary
    assert "Combined metadata sources" not in summary
    assert "Partial scan" in summary
    assert "Sampled:" not in technical
    assert "Ranges:" not in technical
    assert "Probe map" not in technical
    assert "initial metadata" not in technical
