from types import SimpleNamespace

from file_inspector import build_report

def msg(name="sample.bin", mime=None, size=None, attrs=None):
    file_obj = SimpleNamespace(
        name=name,
        mime_type=mime,
        size=size,
        attributes=attrs or [],
    )
    return SimpleNamespace(
        file=file_obj,
        media=object(),
        photo=None,
        raw_text=None,
    )

def test_png_dimensions():
    data = bytes.fromhex(
        "89504E470D0A1A0A0000000D4948445200000280000001680806000000"
    )
    report = build_report(msg("cover.png", "image/png", len(data)), data, 2_000_000)
    assert report.detected_type == "PNG image"
    assert report.image["dimensions"] == "640 × 360"

def test_flac_streaminfo():
    streaminfo = bytearray(34)
    packed = (48000 << 44) | (1 << 41) | (15 << 36) | 48000
    streaminfo[10:18] = packed.to_bytes(8, "big")
    block = b"\x00" + len(streaminfo).to_bytes(3, "big") + bytes(streaminfo)
    data = b"fLaC" + block
    report = build_report(msg("audio.flac", "audio/flac", len(data)), data, 2_000_000)
    assert report.detected_type == "FLAC audio"
    assert report.audio["sample_rate"] == "48000 Hz"
    assert report.audio["channels"] == "2"
    assert report.audio["bits_per_sample"] == "16"

def test_srt_detection():
    data = (
        b"1\n00:00:01,000 --> 00:00:03,000\nHello world\n\n"
        b"2\n00:00:04,000 --> 00:00:05,000\nAgain\n"
    )
    report = build_report(
        msg("episode.en.srt", "application/x-subrip", len(data)),
        data,
        2_000_000,
    )
    assert any(x["format"].startswith("SubRip") for x in report.subtitles)

def test_large_file_is_marked_partial():
    data = b"hello" * 100
    report = build_report(
        msg("large.bin", "application/octet-stream", 100_000_000),
        data,
        2_000_000,
    )
    assert report.partial_only is True
    assert any("No full-file download" in note for note in report.notes)

def test_mkv_track_metadata():
    def vint(value):
        return bytes([0x80 | value])

    def elem(eid, payload):
        return eid + vint(len(payload)) + payload

    audio = elem(
        b"\xAE",
        elem(b"\x83", b"\x02")
        + elem(b"\x86", b"A_AAC")
        + elem(b"\x22\xB5\x9C", b"eng")
        + elem(b"\x53\x6E", b"Japanese"),
    )
    subtitle = elem(
        b"\xAE",
        elem(b"\x83", b"\x11")
        + elem(b"\x86", b"S_TEXT/ASS")
        + elem(b"\x22\xB5\x9C", b"eng"),
    )
    data = b"\x1A\x45\xDF\xA3" + elem(b"\x16\x54\xAE\x6B", audio + subtitle)
    report = build_report(
        msg("episode.mkv", "video/x-matroska", 100_000_000),
        data,
        2_000_000,
    )
    assert "A_AAC" in report.audio.get("embedded_tracks", "")
    assert any(x.get("codec") == "S_TEXT/ASS" for x in report.subtitles)
