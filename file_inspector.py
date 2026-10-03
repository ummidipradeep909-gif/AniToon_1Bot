from __future__ import annotations

import hashlib
import io
import os
import re
from dataclasses import dataclass, field
from typing import Any, Iterable

MAX_PROBE = 4 * 1024 * 1024
DEFAULT_PROBE = 2 * 1024 * 1024
DEFAULT_CHUNK = 256 * 1024

SUB_EXT = {".srt":"SubRip (SRT)", ".vtt":"WebVTT", ".ass":"ASS", ".ssa":"SSA", ".sub":"SUB", ".ttml":"TTML", ".dfxp":"DFXP/TTML", ".smi":"SAMI", ".sami":"SAMI", ".sbv":"SBV", ".stl":"EBU STL", ".idx":"VobSub IDX"}
AUDIO_EXT = {".mp3":"MP3", ".flac":"FLAC", ".wav":"WAV", ".wave":"WAV", ".ogg":"Ogg", ".oga":"Ogg", ".opus":"Opus", ".m4a":"M4A", ".aac":"AAC", ".ac3":"AC-3", ".eac3":"E-AC-3", ".mka":"Matroska audio"}

def _env_int(name: str, default: int, lo: int, hi: int) -> int:
    try:
        value = int(os.getenv(name, ""))
    except ValueError:
        return default
    return max(lo, min(hi, value))

def probe_bytes() -> int:
    return _env_int("FILE_PROBE_BYTES", DEFAULT_PROBE, 64 * 1024, MAX_PROBE)

def probe_chunk() -> int:
    return _env_int("FILE_PROBE_CHUNK_BYTES", DEFAULT_CHUNK, 64 * 1024, 512 * 1024)

@dataclass(slots=True)
class Sample:
    data: bytes
    requested: int
    chunks: int
    error: str | None = None

@dataclass(slots=True)
class Report:
    filename: str = "unknown"
    size: int | None = None
    mime: str | None = None
    ext: str = ""
    detected: str = "Unknown"
    media_kind: str = "File"
    sampled: int = 0
    sample_hash: str = ""
    partial_only: bool = True
    notes: list[str] = field(default_factory=list)
    video: dict[str, str] = field(default_factory=dict)
    audio: dict[str, str] = field(default_factory=dict)
    subtitles: list[dict[str, str]] = field(default_factory=list)
    image: dict[str, str] = field(default_factory=dict)
    container: dict[str, str] = field(default_factory=dict)

    @property
    def detected_type(self) -> str:
        return self.detected

async def sample_telegram_media(
    client: Any,
    media: Any,
    total_size: int | None,
    max_bytes: int | None = None,
) -> Sample:
    wanted = max_bytes or probe_bytes()
    wanted = max(64 * 1024, min(wanted, MAX_PROBE))
    chunk = min(probe_chunk(), wanted)
    limit = (wanted + chunk - 1) // chunk
    out, count = io.BytesIO(), 0
    try:
        async for part in client.iter_download(
            media,
            offset=0,
            limit=limit,
            chunk_size=chunk,
            request_size=chunk,
            file_size=total_size,
        ):
            count += 1
            remain = wanted - out.tell()
            if remain <= 0:
                break
            out.write(bytes(part[:remain]))
            if out.tell() >= wanted:
                break
        return Sample(out.getvalue(), wanted, count)
    except Exception as exc:
        return Sample(
            out.getvalue(),
            wanted,
            count,
            f"{type(exc).__name__}: {exc}",
        )

def _ext(name: str) -> str:
    i = (name or "").rfind(".")
    return name[i:].lower() if i > 0 else ""

def human(n: int | None) -> str:
    if n is None:
        return "unknown"
    x, units = float(n), ("B", "KiB", "MiB", "GiB", "TiB")
    for unit in units:
        if x < 1024 or unit == units[-1]:
            return f"{int(x)} B" if unit == "B" else f"{x:.2f} {unit}"
        x /= 1024
    return f"{n} B"

def magic(data: bytes, name: str, mime: str | None) -> tuple[str, str]:
    h = data[:64]
    if h.startswith(b"%PDF-"):
        return "PDF document", "pdf"
    if h.startswith((b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08")):
        return "ZIP/ZIP-based archive", "zip"
    if h.startswith(b"Rar!\x1a\x07\x00"):
        return "RAR archive", "rar"
    if h.startswith(b"Rar!\x1a\x07\x01\x00"):
        return "RAR5 archive", "rar5"
    if h.startswith(b"7z\xbc\xaf\x27\x1c"):
        return "7-Zip archive", "7z"
    if h.startswith(b"\x1f\x8b"):
        return "GZip compressed data", "gzip"
    if h.startswith(b"BZh"):
        return "BZip2 compressed data", "bzip2"
    if h.startswith(b"\xfd7zXZ\x00"):
        return "XZ compressed data", "xz"
    if h.startswith(b"\x89PNG\r\n\x1a\n"):
        return "PNG image", "png"
    if h.startswith(b"\xff\xd8\xff"):
        return "JPEG image", "jpeg"
    if h[:6] in (b"GIF87a", b"GIF89a"):
        return "GIF image", "gif"
    if h.startswith(b"RIFF") and len(h) >= 12 and h[8:12] == b"WEBP":
        return "WebP image", "webp"
    if h.startswith(b"RIFF") and len(h) >= 12 and h[8:12] == b"WAVE":
        return "WAV audio", "wav"
    if h.startswith(b"RIFF") and len(h) >= 12 and h[8:12] == b"AVI ":
        return "AVI video", "avi"
    if h.startswith((b"II*\x00", b"MM\x00*")):
        return "TIFF image", "tiff"
    if h.startswith(b"\x00\x00\x01\x00"):
        return "ICO image", "ico"
    if h.startswith(b"fLaC"):
        return "FLAC audio", "flac"
    if h.startswith(b"OggS"):
        return "Ogg container", "ogg"
    if h.startswith(b"ID3"):
        return "MP3 audio (ID3)", "mp3"
    if len(h) >= 2 and h[0] == 0xFF and h[1] & 0xE0 == 0xE0:
        return "MPEG audio stream", "mpeg-audio"
    if h.startswith(b"\x1aE\xdf\xa3"):
        return "Matroska/WebM container", "mkv"
    i = data[:4096].find(b"ftyp")
    if i >= 4 and i + 8 <= len(data):
        brand = data[i + 4:i + 8].decode("latin1", "replace").strip("\x00")
        return f"ISO-BMFF media ({brand or 'ftyp'})", "mp4"

    txt = data[:8192].decode("utf-8", "replace").lstrip("\ufeff \t\r\n")
    if txt.startswith("WEBVTT"):
        return "WebVTT subtitle", "vtt"
    if "[Events]" in txt and re.search(r"^\s*\[Script Info\]", txt, re.I | re.M):
        return "ASS/SSA subtitle", "ass"
    if re.search(r"<tt(?:\s|>)", txt, re.I):
        return "TTML/XML subtitle", "ttml"
    if re.search(r"<SAMI(?:\s|>)", txt, re.I):
        return "SAMI subtitle", "smi"
    if re.search(r"\d{2}:\d{2}:\d{2}[,.]\d{3}\s*-->\s*", txt):
        return "SubRip subtitle", "srt"
    if re.search(r"\{\d+\}\{\d+\}", txt[:4096]):
        return "MicroDVD subtitle", "sub"
    printable = sum(
        c in "\r\n\t" or 32 <= ord(c) <= 126 or ord(c) >= 160 for c in txt
    ) / max(1, len(txt))
    if (mime and mime.startswith("text/")) or printable > 0.93:
        return "Text-like document", "text"
    return mime or "Binary file", "binary"

def png_info(data: bytes, report: Report) -> None:
    if len(data) >= 26:
        report.image.update(
            dimensions=f"{int.from_bytes(data[16:20], 'big')} × {int.from_bytes(data[20:24], 'big')}",
            bit_depth=str(data[24]),
            color_type=str(data[25]),
        )

def jpeg_info(data: bytes, report: Report) -> None:
    i = 2
    while i + 9 < len(data):
        if data[i] != 0xFF:
            i += 1
            continue
        marker = data[i + 1]
        i += 2
        if marker in (0xD8, 0xD9) or 0xD0 <= marker <= 0xD7:
            continue
        if i + 2 > len(data):
            break
        size = int.from_bytes(data[i:i + 2], "big")
        if size < 2 or i + size > len(data):
            break
        if marker in {
            0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7,
            0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF,
        } and size >= 7:
            report.image["dimensions"] = (
                f"{int.from_bytes(data[i+5:i+7], 'big')} × "
                f"{int.from_bytes(data[i+3:i+5], 'big')}"
            )
            return
        i += size

def id3_text(payload: bytes) -> str:
    if not payload:
        return ""
    encoding = payload[0]
    codec = {0: "latin1", 1: "utf-16", 2: "utf-16-be", 3: "utf-8"}.get(
        encoding, "utf-8"
    )
    return payload[1:].decode(codec, "replace").strip("\x00")

def mp3_info(data: bytes, report: Report) -> None:
    if not data.startswith(b"ID3") or len(data) < 10:
        return
    size = ((data[6] & 127) << 21) | ((data[7] & 127) << 14) | ((data[8] & 127) << 7) | (data[9] & 127)
    end, pos = min(len(data), 10 + size), 10
    tags = {"TIT2": "title", "TPE1": "artist", "TALB": "album", "TRCK": "track", "TCON": "genre", "TDRC": "date"}
    while pos + 10 <= end:
        frame_id = data[pos:pos+4].decode("latin1", "replace")
        frame_size = int.from_bytes(data[pos+4:pos+8], "big")
        if not frame_id.strip("\x00") or pos + 10 + frame_size > end:
            break
        if frame_id in tags and frame_size:
            report.audio[tags[frame_id]] = id3_text(data[pos+10:pos+10+frame_size])
        pos += 10 + frame_size

def flac_info(data: bytes, report: Report) -> None:
    pos = 4
    while pos + 4 <= len(data):
        last = bool(data[pos] & 0x80)
        block_type = data[pos] & 0x7F
        size = int.from_bytes(data[pos+1:pos+4], "big")
        start, end = pos + 4, pos + 4 + size
        if end > len(data):
            break
        if block_type == 0 and size >= 34:
            x = data[start:start+34]
            packed = int.from_bytes(x[10:18], "big")
            report.audio.update(
                sample_rate=f"{packed >> 44} Hz",
                channels=str(((packed >> 41) & 7) + 1),
                bits_per_sample=str(((packed >> 36) & 31) + 1),
                total_samples=f"{packed & ((1 << 36) - 1):,}",
            )
        if last:
            break
        pos = end

def wav_info(data: bytes, report: Report) -> None:
    pos = 12
    while pos + 8 <= len(data) and pos < 1024 * 1024:
        chunk_id = data[pos:pos+4]
        size = int.from_bytes(data[pos+4:pos+8], "little")
        start, end = pos + 8, pos + 8 + size
        if end > len(data):
            break
        if chunk_id == b"fmt " and size >= 16:
            report.audio.update(
                format_code=str(int.from_bytes(data[start:start+2], "little")),
                channels=str(int.from_bytes(data[start+2:start+4], "little")),
                sample_rate=f"{int.from_bytes(data[start+4:start+8], 'little')} Hz",
                bits_per_sample=str(int.from_bytes(data[start+14:start+16], "little")),
            )
            return
        pos = end + (size & 1)

def ogg_info(data: bytes, report: Report) -> None:
    pos = data.find(b"OpusHead")
    if pos >= 0 and pos + 16 <= len(data):
        report.audio.update(
            codec="Opus",
            channels=str(data[pos+9]),
            input_sample_rate=f"{int.from_bytes(data[pos+12:pos+16], 'little')} Hz",
        )
        return
    pos = data.find(b"vorbis")
    if pos >= 0 and pos + 16 <= len(data):
        report.audio.update(
            codec="Vorbis",
            channels=str(data[pos+11]),
            sample_rate=f"{int.from_bytes(data[pos+12:pos+16], 'little')} Hz",
        )

def mp4_info(data: bytes, report: Report) -> None:
    handlers = []
    for match in re.finditer(b"hdlr", data[:2 * 1024 * 1024]):
        pos = match.start()
        if pos + 12 > len(data):
            continue
        handler = data[pos+8:pos+12].decode("latin1", "replace")
        if handler in {"soun", "vide", "subt", "text", "clcp", "sbtl"} and handler not in handlers:
            handlers.append(handler)
    if handlers:
        report.container["handlers_in_sample"] = ", ".join(handlers)
        if any(x in {"subt", "text", "clcp", "sbtl"} for x in handlers):
            report.subtitles.append(
                {"format": "Embedded MP4 subtitle/text track", "source": "initial sample"}
            )
    audio_codes = [
        x.decode("latin1") for x in (b"mp4a", b"ac-3", b"ec-3", b"Opus", b"alaw", b"ulaw")
        if x in data[:2 * 1024 * 1024]
    ]
    if audio_codes:
        report.audio["sample_codecs"] = ", ".join(dict.fromkeys(audio_codes))
    subtitle_codes = [
        x.decode("latin1") for x in (b"tx3g", b"wvtt", b"stpp", b"c608", b"c708")
        if x in data[:2 * 1024 * 1024]
    ]
    if subtitle_codes:
        report.subtitles.append(
            {
                "format": "Embedded subtitle sample entries",
                "codecs": ", ".join(dict.fromkeys(subtitle_codes)),
                "source": "initial sample",
            }
        )

def _vint(data: bytes, pos: int) -> tuple[int, int] | None:
    if pos >= len(data):
        return None
    first = data[pos]
    mask, width = 0x80, 1
    while width <= 8 and not (first & mask):
        mask >>= 1
        width += 1
    if width > 8 or pos + width > len(data):
        return None
    value = first & (mask - 1)
    for i in range(1, width):
        value = (value << 8) | data[pos + i]
    return value, width

def _ebml_children(data: bytes, start: int, end: int) -> Iterable[tuple[int, int, int]]:
    pos = start
    while pos + 2 <= end:
        first, mask, id_len = data[pos], 0x80, 1
        while id_len <= 8 and not (first & mask):
            mask >>= 1
            id_len += 1
        if id_len > 8 or pos + id_len >= end:
            return
        element_id = int.from_bytes(data[pos:pos+id_len], "big")
        size_info = _vint(data, pos + id_len)
        if not size_info:
            return
        size, size_len = size_info
        start_data = pos + id_len + size_len
        end_data = end if size == (1 << (7 * size_len)) - 1 else min(end, start_data + size)
        if end_data < start_data:
            return
        yield element_id, start_data, end_data
        if end_data <= pos:
            return
        pos = end_data

def mkv_info(data: bytes, report: Report) -> None:
    marker = b"\x16\x54\xAE\x6B"
    pos, found = data.find(marker), 0
    while pos >= 0:
        size_info = _vint(data, pos + 4)
        if not size_info:
            break
        size, size_len = size_info
        start, end = pos + 4 + size_len, min(len(data), pos + 4 + size_len + size)
        for element_id, track_start, track_end in _ebml_children(data, start, end):
            if element_id != 0xAE:
                continue
            track_type = codec = language = title = None
            for child_id, child_start, child_end in _ebml_children(data, track_start, track_end):
                payload = data[child_start:child_end]
                if child_id == 0x83 and payload:
                    raw = int.from_bytes(payload, "big")
                    track_type = {1: "video", 2: "audio", 17: "subtitles"}.get(raw, str(raw))
                elif child_id == 0x86:
                    codec = payload.decode("utf-8", "replace").strip("\x00")
                elif child_id == 0x22B59C:
                    language = payload.decode("ascii", "replace").strip("\x00")
                elif child_id == 0x536E:
                    title = payload.decode("utf-8", "replace").strip("\x00")
            found += 1
            item = {"track": str(found), "type": track_type or "unknown"}
            if codec:
                item["codec"] = codec
            if language:
                item["language"] = language
            if title:
                item["name"] = title
            if item["type"] == "subtitles":
                report.subtitles.append(item)
            elif item["type"] == "audio":
                report.audio.setdefault("embedded_tracks", "")
                report.audio["embedded_tracks"] += (
                    ", " if report.audio["embedded_tracks"] else ""
                ) + " / ".join(item.values())
            elif item["type"] == "video":
                report.video.setdefault("embedded_tracks", "")
                report.video["embedded_tracks"] += (
                    ", " if report.video["embedded_tracks"] else ""
                ) + " / ".join(item.values())
        pos = data.find(marker, end)

def attrs(message: Any, report: Report) -> None:
    file_obj = getattr(message, "file", None)
    for attr in getattr(file_obj, "attributes", []) or []:
        cls = type(attr).__name__
        if "Audio" in cls:
            report.media_kind = "Audio"
            if getattr(attr, "duration", None) is not None:
                report.audio["duration"] = f"{int(attr.duration)} s"
            if getattr(attr, "performer", None):
                report.audio["artist"] = str(attr.performer)
            if getattr(attr, "title", None):
                report.audio["title"] = str(attr.title)
        elif "Video" in cls:
            report.media_kind = "Video"
            width, height = getattr(attr, "w", None), getattr(attr, "h", None)
            if width and height:
                report.video["dimensions"] = f"{width} × {height}"
            if getattr(attr, "duration", None) is not None:
                report.video["duration"] = f"{int(attr.duration)} s"
            if getattr(attr, "supports_streaming", False):
                report.video["streaming"] = "yes"
        elif "Sticker" in cls:
            report.media_kind = "Sticker"
    if getattr(message, "photo", None):
        report.media_kind = "Photo"

def build_report(
    message: Any,
    sample: bytes,
    sample_requested: int,
    sample_error: str | None = None,
) -> Report:
    file_obj = getattr(message, "file", None)
    filename = str(getattr(file_obj, "name", None) or "unknown")
    size = getattr(file_obj, "size", None)
    mime = getattr(file_obj, "mime_type", None)
    report = Report(
        filename=filename,
        size=int(size) if isinstance(size, int) else None,
        mime=str(mime) if mime else None,
        ext=_ext(filename),
        sampled=len(sample),
        sample_hash=hashlib.sha256(sample).hexdigest() if sample else "",
    )
    attrs(message, report)
    report.detected, kind = magic(sample, filename, mime)

    if kind == "png":
        png_info(sample, report)
    elif kind == "jpeg":
        jpeg_info(sample, report)
    elif kind == "flac":
        flac_info(sample, report)
    elif kind == "wav":
        wav_info(sample, report)
    elif kind == "ogg":
        ogg_info(sample, report)
    elif kind in {"mp3", "mpeg-audio"}:
        mp3_info(sample, report)
    elif kind == "mp4":
        mp4_info(sample, report)
    elif kind == "mkv":
        mkv_info(sample, report)

    if report.ext in AUDIO_EXT and not report.audio:
        report.audio["format_hint"] = AUDIO_EXT[report.ext]
    if report.ext in SUB_EXT:
        report.subtitles.append({"format": SUB_EXT[report.ext], "source": "filename extension"})

    text = sample[:512 * 1024].decode("utf-8", "replace")
    if report.ext == ".vtt" or text.lstrip("\ufeff").startswith("WEBVTT"):
        report.subtitles.append({"format": "WebVTT", "source": "content"})
    elif report.ext in {".ass", ".ssa"} or "[Events]" in text:
        report.subtitles.append({"format": "ASS/SSA", "source": "content"})
    elif report.ext in {".ttml", ".dfxp"} or re.search(r"<tt(?:\s|>)", text, re.I):
        report.subtitles.append({"format": "TTML/XML", "source": "content"})
    elif report.ext in {".smi", ".sami"} or re.search(r"<SAMI(?:\s|>)", text, re.I):
        report.subtitles.append({"format": "SAMI", "source": "content"})
    elif report.ext == ".srt" or re.search(r"\d{2}:\d{2}:\d{2}[,.]\d{3}\s*-->\s*", text):
        report.subtitles.append({"format": "SubRip (SRT)", "source": "content"})

    unique, seen = [], set()
    for item in report.subtitles:
        key = tuple(sorted(item.items()))
        if key not in seen:
            seen.add(key)
            unique.append(item)
    report.subtitles = unique

    if sample_error:
        report.notes.append(f"Partial read error: {sample_error}")
    if report.size is not None and report.size <= sample_requested and report.size == len(sample):
        report.partial_only = False
        report.notes.append("The file is smaller than the sample limit, so the complete small file was read.")
    else:
        report.notes.append("No full-file download was performed; this report uses Telegram metadata plus the initial sample only.")
    if report.size is not None and len(sample) < report.size:
        report.notes.append("Some embedded metadata may be outside the sampled beginning.")
    return report

def lines_dict(values: dict[str, str]) -> list[str]:
    return [f"{key.replace('_', ' ').title()}: {value}" for key, value in values.items()]

def format_report(report: Report) -> str:
    percent = f" ({report.sampled / report.size * 100:.3f}%)" if report.size else ""
    out = [
        "🔎 LIGHTWEIGHT FILE CHECK",
        "",
        f"Name: {report.filename}",
        f"Type: {report.detected}",
        f"Media: {report.media_kind}",
        f"MIME: {report.mime or 'unknown'}",
        f"Size: {human(report.size)}",
        f"Read: {human(report.sampled)} from the beginning{percent}",
    ]
    if report.sample_hash:
        out.append(f"Sample SHA-256: {report.sample_hash[:32]}…")
    if report.video:
        out += ["", "🎬 Video"] + lines_dict(report.video)
    if report.audio:
        out += ["", "🔊 Audio"] + lines_dict(report.audio)
    elif report.media_kind == "Video" or (report.mime and report.mime.startswith("video/")):
        out += ["", "🔊 Audio", "Not detected in the sampled metadata."]
    if report.subtitles:
        out += [
            "",
            "💬 Subtitles",
            *[
                " • " + " | ".join(f"{k}: {v}" for k, v in item.items())
                for item in report.subtitles
            ],
        ]
    elif report.media_kind == "Video" or (report.mime and report.mime.startswith("video/")):
        out += [
            "",
            "💬 Subtitles",
            "Not detected in the filename or sampled metadata.",
        ]
    if report.image:
        out += ["", "🖼 Image"] + lines_dict(report.image)
    if report.container:
        out += ["", "📦 Container"] + lines_dict(report.container)
    if report.notes:
        out += ["", "ℹ️ Notes"] + [" • " + note for note in report.notes]
    return "\n".join(out)

async def inspect_telegram_message(client: Any, message: Any):
    file_obj = getattr(message, "file", None)
    sample = await sample_telegram_media(
        client,
        getattr(message, "media", None),
        getattr(file_obj, "size", None),
    )
    return build_report(message, sample.data, sample.requested, sample.error), sample
