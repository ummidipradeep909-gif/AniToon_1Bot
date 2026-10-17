"""Regression tests for the advanced HTML media report renderer."""

import io
import os
import zipfile
from types import SimpleNamespace
from datetime import datetime, timedelta, timezone

# bot.py requires these at import time; no Telegram connection is started by importing it.
os.environ.setdefault("API_ID", "12345")
os.environ.setdefault("API_HASH", "0123456789abcdef0123456789abcdef")
os.environ.setdefault("BOT_TOKEN", "123456:TEST_TOKEN")

import bot
import file_inspector
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

    # Keep reader-relevant post context but omit implementation IDs.
    assert "telegram_message_id" not in page
    assert "Telegram message ID" not in page
    assert "98765" not in page
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
    status_one, status_two, status_three, status_four, status_five = (object() for _ in range(5))

    async def fake_edit_status(message, text, buttons=None):
        calls.append((message, text, buttons))

    monkeypatch.setattr(bot, "edit_status", fake_edit_status)
    monkeypatch.setattr(bot, "cancel_button", lambda token: f"cancel:{token}")

    try:
        bot.scan_queue_order[:] = ["token-1", "token-2", "token-3", "token-4", "token-5"]
        bot.queued_scan_statuses.clear()
        bot.queued_scan_statuses.update({
            "token-1": (status_one, "first.mkv"),
            "token-2": (status_two, "second.pdf"),
            "token-3": (status_three, "third.zip"),
            "token-4": (status_four, "fourth.mp4"),
            "token-5": (status_five, "fifth.pdf"),
        })
        # The first file keeps processing; all later scans wait as QUEUE 1, 2, 3, 4.
        bot.processing_scan_tokens.clear()
        bot.processing_scan_tokens.add("token-1")
        bot.queued_processes = 4

        async def exercise_positions():
            await bot._refresh_queue_positions()
            assert len(calls) == 4
            expected = [
                (status_two, "QUEUE 1", "second.pdf"),
                (status_three, "QUEUE 2", "third.zip"),
                (status_four, "QUEUE 3", "fourth.mp4"),
                (status_five, "QUEUE 4", "fifth.pdf"),
            ]
            for (message, queue_label, filename), call in zip(expected, calls):
                assert call[0] is message
                assert queue_label in call[1]
                assert filename in call[1]
                assert "0%" not in call[1]

            # When the active scan ends, waiting scans remain numbered from QUEUE 1.
            await bot._remove_queued_scan("token-1")

        asyncio.run(exercise_positions())

        shifted = calls[-4:]
        assert ["QUEUE 1", "QUEUE 2", "QUEUE 3", "QUEUE 4"] == [
            next(label for label in ("QUEUE 1", "QUEUE 2", "QUEUE 3", "QUEUE 4") if label in call[1])
            for call in shifted
        ]
        assert bot.queued_processes == 4
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



def test_adaptive_scan_limit_scales_down_with_ram_pressure(monkeypatch):
    original_limit = bot.MAX_CONCURRENT_CHECKS
    monkeypatch.setattr(bot, "MAX_CONCURRENT_CHECKS", 3)

    for ram_pct, expected in ((20.0, 3), (50.0, 2), (65.0, 1), (80.0, 0)):
        monkeypatch.setattr(
            bot,
            "runtime_resource_stats",
            lambda ram_pct=ram_pct: {"ram_pct": ram_pct},
        )
        assert bot.adaptive_scan_limit() == expected

    monkeypatch.setattr(bot, "MAX_CONCURRENT_CHECKS", original_limit)


def test_owner_home_uses_admin_studio_label():
    source = open(bot.__file__, "r", encoding="utf-8").read()
    assert "🛠️ Admin Studio" in source
    assert "ANITOON • ADMIN STUDIO" in source
    assert "Performance & RAM" in source


def test_zip_report_lists_member_details_without_extracting_contents(monkeypatch):
    archive_buffer = io.BytesIO()
    with zipfile.ZipFile(archive_buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("Episodes/", b"")
        archive.writestr("Episodes/episode-01.mkv", b"sample video data " * 20)
        archive.writestr("Audio/Japanese.flac", b"fake audio bytes")
        archive.writestr("Subtitles/English.srt", b"1\\n00:00:01,000 --> 00:00:02,000\\nHello\\n")
        archive.writestr("README.txt", b"Archive description")

    raw_zip = archive_buffer.getvalue()
    source_file = SimpleNamespace(
        name="anime-pack.zip",
        size=len(raw_zip),
        mime_type="application/zip",
        attributes=[],
    )
    message = SimpleNamespace(file=source_file, media=SimpleNamespace())
    report = Report(
        filename="anime-pack.zip",
        size=len(raw_zip),
        mime="application/zip",
        ext=".zip",
        detected="ZIP archive",
        media_kind="Archive",
    )

    def fake_nested_report(nested_message, pieces):
        return Report(
            filename=nested_message.file.name,
            size=nested_message.file.size,
            mime=nested_message.file.mime_type,
            ext=".mkv",
            detected="Matroska/WebM container",
            media_kind="Video",
            audio={
                "tracks": [{
                    "type": "audio",
                    "name": "Japanese Main Track",
                    "language_name": "Japanese",
                    "codec_name": "AAC",
                }]
            },
            subtitles=[{
                "type": "subtitles",
                "name": "English Signs",
                "language_name": "English",
                "subtitle_format": "ASS",
            }],
        )

    monkeypatch.setattr(file_inspector, "_report", fake_nested_report)

    # The archive parser inspects ZIP directory data and bounded media prefixes;
    # it never extracts archive member contents to disk.
    file_inspector._populate_telegram_media_info(message, report, raw_zip)

    assert report.media_info["archive_entry_count"] == "5"
    assert len(report.archive_entries) == 5
    assert report.archive_entries[0]["name"] == "Episodes/"
    assert report.archive_entries[0]["is_dir"] is True
    episode = next(item for item in report.archive_entries if item["name"].endswith(".mkv"))
    assert episode["size"] > 0
    assert episode["compression"] == "Deflate"
    assert "Japanese Main Track (Japanese · AAC)" in episode["audio_names"]
    assert "English Signs (English · ASS)" in episode["subtitle_names"]

    page = bot.web_page(
        report,
        "zip-regression-token",
        datetime.now(timezone.utc) + timedelta(hours=1),
    ).decode("utf-8")

    assert "ZIP contents" in page
    assert "Episodes/episode-01.mkv" in page
    assert "Audio/Japanese.flac" in page
    assert "Subtitles/English.srt" in page
    assert "Japanese Main Track (Japanese · AAC)" in page
    assert "English Signs (English · ASS)" in page
    assert "Original size" in page
    assert "Compressed size" in page
    assert "Audio track names" in page
    assert "Subtitle names" in page
    assert "CRC-32" not in page
    assert "Password-protected entries" not in page
    assert "Embedded audio/subtitle names are shown" in page

    fallback = bot.minimal_web_report_page(
        report,
        "zip-fallback-token",
        datetime.now(timezone.utc) + timedelta(hours=1),
    ).decode("utf-8")
    assert "ZIP contents" in fallback
    assert "Japanese Main Track (Japanese · AAC)" in fallback
    assert "English Signs (English · ASS)" in fallback
    assert "CRC-32" not in fallback


def test_file_report_hides_redundant_and_internal_metadata():
    report = Report(
        filename="sample.jpg",
        size=4096,
        mime="image/jpeg",
        ext=".jpg",
        detected="JPEG image",
        media_kind="Image",
        media_info={
            "category": "Image",
            "detected_format": "JPEG image",
            "file_extension": "JPG",
            "declared_mime_type": "image/jpeg",
            "file_size_bytes": "4096",
            "image_format": "JPEG",
            "image_pixel_dimensions": "800 × 600",
            "telegram_media_type": "MessageMediaDocument",
            "telegram_message_id": "98765",
            "caption_characters": "130",
            "has_caption": "Yes",
            "image_is_animated": "No",
            "image_has_transparency": "No",
            "post_author": "AniToon Channel",
            "views": "100",
        },
    )

    page = bot.web_page(
        report,
        "clean-report-test",
        datetime.now(timezone.utc) + timedelta(hours=1),
    ).decode("utf-8")

    assert "AniToon Channel" in page
    assert "100" in page
    assert "image/jpeg" not in page
    assert "MessageMediaDocument" not in page
    assert "98765" not in page
    assert "Caption characters" not in page
    assert "Image is animated" not in page
    assert "Transparency" not in page

    fallback = bot.minimal_web_report_page(
        report,
        "clean-fallback-test",
        datetime.now(timezone.utc) + timedelta(hours=1),
    ).decode("utf-8")
    assert "MessageMediaDocument" not in fallback
    assert "98765" not in fallback
    assert "Caption length" not in fallback
    assert "image/jpeg" not in fallback


def test_large_zip_reads_directory_from_tail_within_probe_budget(monkeypatch):
    import asyncio

    archive_buffer = io.BytesIO()
    padded_data = bytes(range(251)) * 22_000
    with zipfile.ZipFile(archive_buffer, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr("media/episode-01.mkv", b"small fake media header")
        archive.writestr("subs/English.srt", b"1\\n00:00:01,000 --> 00:00:02,000\\nHello\\n")
        archive.writestr("padding.bin", padded_data)
    raw_zip = archive_buffer.getvalue()
    assert len(raw_zip) > file_inspector.MAX_SAFE_SOURCE_BYTES

    class FakeMedia:
        data = raw_zip

    class FakeClient:
        async def iter_download(
            self, media, offset=0, limit=0, chunk_size=0, request_size=0, file_size=None
        ):
            yield media.data[offset:offset + limit]

    source_file = SimpleNamespace(
        name="large-pack.zip",
        size=len(raw_zip),
        mime_type="application/zip",
        attributes=[],
    )
    message = SimpleNamespace(file=source_file, media=FakeMedia())

    def fake_nested_report(nested_message, pieces):
        return Report(
            filename=nested_message.file.name,
            size=nested_message.file.size,
            mime=nested_message.file.mime_type,
            ext=".mkv",
            detected="Matroska/WebM container",
            media_kind="Video",
            audio={"tracks": [{
                "type": "audio",
                "name": "Japanese Main Track",
                "language_name": "Japanese",
                "codec_name": "AAC",
            }]},
            subtitles=[{
                "type": "subtitles",
                "name": "English Signs",
                "language_name": "English",
                "subtitle_format": "ASS",
            }],
        )

    monkeypatch.setattr(file_inspector, "_report", fake_nested_report)
    report, bytes_read = asyncio.run(
        file_inspector.inspect_telegram_message(FakeClient(), message)
    )

    assert report.detected == "ZIP archive"
    assert report.media_info["archive_entry_count"] == "3"
    assert len(report.archive_entries) == 3
    episode = next(entry for entry in report.archive_entries if entry["name"].endswith(".mkv"))
    assert "Japanese Main Track (Japanese · AAC)" in episode["audio_names"]
    assert "English Signs (English · ASS)" in episode["subtitle_names"]
    assert bytes_read < file_inspector.MAX_SAFE_SOURCE_BYTES
    assert bytes_read < len(raw_zip) // 2

