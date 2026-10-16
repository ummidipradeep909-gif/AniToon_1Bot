from __future__ import annotations

import io
import zipfile
from datetime import datetime, timezone
from types import SimpleNamespace

from PIL import Image

from file_inspector import (
    ProbePiece,
    Report,
    _metadata_only_report,
    _report,
    magic,
)


def make_message(name="sample.bin", mime=None, size=None, attrs=None, **overrides):
    file_obj = SimpleNamespace(
        name=name,
        mime_type=mime,
        size=size,
        attributes=attrs or [],
        width=None,
        height=None,
    )
    values = dict(
        file=file_obj,
        media=SimpleNamespace(),
        photo=None,
        raw_text=None,
        message=None,
        id=42,
        date=datetime(2026, 10, 16, tzinfo=timezone.utc),
        post_author=None,
        views=None,
        forwards=None,
        replies=None,
        grouped_id=None,
        post=False,
        fwd_from=None,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


def build_report(message, data):
    piece = ProbePiece(offset=0, data=bytes(data), label="test data")
    return _report(message, [piece])


def test_png_dimensions_and_image_specific_details():
    output = io.BytesIO()
    Image.new("RGB", (640, 360), (24, 90, 180)).save(output, format="PNG")
    data = output.getvalue()

    report = build_report(make_message("cover.png", "image/png", len(data)), data)

    assert report.detected == "PNG image"
    assert report.media_info["category"] == "Image"
    assert report.media_info["image_pixel_dimensions"] == "640 × 360"
    assert report.media_info["image_format"] == "PNG"
    assert report.media_info["image_frame_count"] == "1"


def test_image_signatures_cover_jpeg_gif_webp_and_avif():
    assert magic(b"\xff\xd8\xff" + b"data", "picture.jpg", "image/jpeg")[0] == "JPEG image"
    assert magic(b"GIF89a" + b"data", "animation.gif", "image/gif")[0] == "GIF image"
    assert magic(b"RIFF" + (4).to_bytes(4, "little") + b"WEBP" + b"data", "picture.webp", "image/webp")[0] == "WebP image"
    assert magic(b"\x00\x00\x00\x18ftypavif" + b"data", "picture.avif", "image/avif")[0] == "AVIF image"


def test_sticker_attributes_are_not_mislabeled_as_video():
    StickerAttribute = type("DocumentAttributeSticker", (), {})
    sticker = StickerAttribute()
    sticker.alt = "🎉"
    sticker.stickerset = SimpleNamespace(short_name="party_pack", title="Party Pack")
    sticker.mask = False
    raw = b"RIFF" + (4).to_bytes(4, "little") + b"WEBP" + b"data"
    message = make_message(
        "sticker.webp",
        "image/webp",
        len(raw),
        attrs=[sticker],
    )
    message.file.width = 512
    message.file.height = 512

    report = build_report(message, raw)

    assert report.media_info["category"] == "Sticker"
    assert report.media_info["sticker_alt_text"] == "🎉"
    assert report.media_info["sticker_set"] == "party_pack"
    assert "tracks" not in report.video


def test_pdf_has_pdf_specific_details():
    data = b"%PDF-1.7\n1 0 obj <</Type /Page>> endobj\n%%EOF"
    report = build_report(make_message("sample.pdf", "application/pdf", len(data)), data)

    assert report.detected == "PDF document"
    assert report.media_info["category"] == "PDF document"
    assert report.media_info["pdf_version"] == "1.7"
    assert report.media_info["pdf_pages"] == "1"


def test_zip_based_office_document_reports_directory_and_title():
    output = io.BytesIO()
    core_xml = (
        b'<?xml version="1.0"?>'
        b'<cp:coreProperties xmlns:cp="urn:core" xmlns:dc="urn:dc">'
        b"<dc:title>Unit Test Document</dc:title>"
        b"<dc:creator>Test Author</dc:creator>"
        b"<cp:lastModifiedBy>Editor</cp:lastModifiedBy>"
        b"</cp:coreProperties>"
    )
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("docProps/core.xml", core_xml)
        archive.writestr("word/document.xml", "<document/>")
    data = output.getvalue()

    report = build_report(make_message("example.docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document", len(data)), data)

    assert report.detected == "Microsoft Word document"
    assert report.media_info["category"] == "Office document"
    assert report.media_info["archive_entry_count"] == "2"
    assert report.media_info["document_title"] == "Unit Test Document"
    assert report.media_info["document_creator"] == "Test Author"


def test_audio_streaminfo_is_extracted_from_flac():
    streaminfo = bytearray(34)
    packed = (48000 << 44) | (1 << 41) | (15 << 36) | 48000
    streaminfo[10:18] = packed.to_bytes(8, "big")
    data = b"fLaC" + b"\x00" + len(streaminfo).to_bytes(3, "big") + bytes(streaminfo)
    report = build_report(make_message("audio.flac", "audio/flac", len(data)), data)

    assert report.detected == "FLAC audio"
    assert report.audio["sample_rate"] == "48000 Hz"
    assert report.audio["channels"] == "2"
    assert report.audio["bits_per_sample"] == "16"


def test_metadata_only_poll_preserves_question_and_options():
    PollAnswer = lambda text: SimpleNamespace(text=text)
    poll = SimpleNamespace(
        question=SimpleNamespace(text="Which format do you use?"),
        answers=[PollAnswer("PNG"), PollAnswer("JPEG")],
        quiz=False,
        multiple_choice=True,
    )
    media = type("MessageMediaPoll", (), {})()
    media.poll = poll
    media.results = SimpleNamespace(total_voters=12)
    message = make_message(
        name="Telegram poll",
        media=media,
        file=None,
    )

    report = _metadata_only_report(message)

    assert report.media_info["category"] == "Poll"
    assert report.media_info["poll_question"] == "Which format do you use?"
    assert report.media_info["poll_options"] == "PNG | JPEG"
    assert report.media_info["poll_voter_count"] == "12"


def test_large_file_is_marked_as_partial_without_full_download():
    data = b"hello" * 100
    report = build_report(
        make_message("large.bin", "application/octet-stream", 100_000_000),
        data,
    )

    assert any("Partial scan" in note for note in report.notes)
    assert any("Metadata outside the probed ranges" in note for note in report.notes)
