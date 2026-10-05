from __future__ import annotations

import asyncio
import base64
import html
import io
import logging
import re
import time
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any

import av

from file_inspector import inspect_telegram_message

from file_inspector import Report

log = logging.getLogger("anitoons-media-probe")

LANG_NAMES = {
    "eng":"English","en":"English","jpn":"Japanese","ja":"Japanese","hin":"Hindi","hi":"Hindi",
    "tel":"Telugu","te":"Telugu","tam":"Tamil","ta":"Tamil","mal":"Malayalam","ml":"Malayalam",
    "kan":"Kannada","kn":"Kannada","mar":"Marathi","mr":"Marathi","ben":"Bengali","bn":"Bengali",
    "guj":"Gujarati","gu":"Gujarati","pan":"Punjabi","pa":"Punjabi","urd":"Urdu","ur":"Urdu",
    "nep":"Nepali","ne":"Nepali","sin":"Sinhala","si":"Sinhala","asm":"Assamese","as":"Assamese",
    "ori":"Odia","ory":"Odia","odia":"Odia","san":"Sanskrit","sa":"Sanskrit",
    "kor":"Korean","ko":"Korean","zho":"Chinese","chi":"Chinese","zh":"Chinese",
    "vie":"Vietnamese","vi":"Vietnamese","tha":"Thai","th":"Thai","ind":"Indonesian","id":"Indonesian",
    "msa":"Malay","may":"Malay","ms":"Malay","fil":"Filipino","tl":"Filipino",
    "spa":"Spanish","es":"Spanish","fra":"French","fre":"French","fr":"French",
    "deu":"German","ger":"German","de":"German","ita":"Italian","it":"Italian","rus":"Russian","ru":"Russian",
    "ara":"Arabic","ar":"Arabic","fas":"Persian","per":"Persian","fa":"Persian",
    "por":"Portuguese","pt":"Portuguese","nld":"Dutch","dut":"Dutch","nl":"Dutch",
    "pol":"Polish","pl":"Polish","tur":"Turkish","tr":"Turkish","ell":"Greek","gre":"Greek","el":"Greek",
    "heb":"Hebrew","he":"Hebrew","ukr":"Ukrainian","uk":"Ukrainian","ron":"Romanian","rum":"Romanian","ro":"Romanian",
    "hun":"Hungarian","hu":"Hungarian","ces":"Czech","cze":"Czech","cs":"Czech","slk":"Slovak","slo":"Slovak","sk":"Slovak",
    "hrv":"Croatian","hr":"Croatian","srp":"Serbian","sr":"Serbian","slv":"Slovenian","sl":"Slovenian",
    "bul":"Bulgarian","bg":"Bulgarian","swe":"Swedish","sv":"Swedish","dan":"Danish","da":"Danish",
    "fin":"Finnish","fi":"Finnish","nor":"Norwegian","no":"Norwegian","isl":"Icelandic","ice":"Icelandic","is":"Icelandic",
    "est":"Estonian","et":"Estonian","lav":"Latvian","lv":"Latvian","lit":"Lithuanian","lt":"Lithuanian",
    "kat":"Georgian","ka":"Georgian","aze":"Azerbaijani","az":"Azerbaijani","kaz":"Kazakh","kk":"Kazakh",
    "uzb":"Uzbek","uz":"Uzbek","mong":"Mongolian","mn":"Mongolian","khm":"Khmer","km":"Khmer",
    "mya":"Burmese","bur":"Burmese","my":"Burmese","lao":"Lao","lo":"Lao",
    "swa":"Swahili","sw":"Swahili","amh":"Amharic","am":"Amharic","afr":"Afrikaans","af":"Afrikaans",
    "zul":"Zulu","zu":"Zulu","som":"Somali","so":"Somali","und":"Undetermined",
}

CODEC_NAMES = {
    "aac": "AAC", "ac3": "AC-3", "eac3": "E-AC-3", "opus": "Opus",
    "vorbis": "Vorbis", "mp3": "MP3", "flac": "FLAC", "truehd": "TrueHD",
    "dts": "DTS", "dca": "DTS", "alac": "ALAC", "pcm_s16le": "PCM S16LE",
    "pcm_s24le": "PCM S24LE", "pcm_s32le": "PCM S32LE",
    "h264": "H.264/AVC", "avc": "H.264/AVC", "hevc": "H.265/HEVC",
    "h265": "H.265/HEVC", "av1": "AV1", "vp8": "VP8", "vp9": "VP9",
    "mpeg2video": "MPEG-2 Video", "mpeg4": "MPEG-4 Video", "vc1": "VC-1",
    "ass": "ASS", "ssa": "SSA", "subrip": "SubRip", "webvtt": "WebVTT",
    "hdmv_pgs_subtitle": "PGS", "dvd_subtitle": "VobSub",
}

DEFAULT_RANGE_CHUNK = 512 * 1024
# Source-media probing is capped well below 4 MB.
DEFAULT_BUDGET = 2_560 * 1024
MAX_PROBE_BUDGET = 2_560 * 1024
RANGE_TOKEN_TTL = 10 * 60
MAX_TRACKS_PER_KIND = 100
PREVIEW_RATIOS = (0.50,)
PREVIEW_BUDGET = 256 * 1024
RANGE_IO_CONCURRENCY = 4
range_io_semaphore = asyncio.Semaphore(RANGE_IO_CONCURRENCY)


class ProbeBudgetExceeded(RuntimeError):
    pass


class ProbeCancelled(RuntimeError):
    pass


@dataclass
class RangeProbeSession:
    token: str
    client: Any
    media: Any
    total: int | None
    budget: int = DEFAULT_BUDGET
    chunk_size: int = DEFAULT_RANGE_CHUNK
    fetched_bytes: int = 0
    created_at: float = field(default_factory=time.monotonic)
    cancelled: bool = False
    chunks: dict[int, bytes] = field(default_factory=dict)
    ranges: list[tuple[int, int]] = field(default_factory=list)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def expired(self) -> bool:
        return time.monotonic() - self.created_at > RANGE_TOKEN_TTL

    async def cancel(self) -> None:
        async with self.lock:
            self.cancelled = True

    async def _fetch_chunk(self, offset: int) -> bytes:
        if self.cancelled:
            raise ProbeCancelled("scan cancelled")
        cached = self.chunks.get(offset)
        if cached is not None:
            return cached

        request_size = min(self.chunk_size, max(1, (self.total or self.chunk_size) - offset))
        request_size = max(4096, request_size - (request_size % 4096))
        if request_size <= 0:
            return b""
        if self.fetched_bytes + request_size > self.budget:
            raise ProbeBudgetExceeded("metadata byte-range budget reached")

        data = b""
        async with range_io_semaphore:
            async for part in self.client.iter_download(
                self.media,
                offset=offset,
                limit=request_size,
                chunk_size=request_size,
                request_size=request_size,
                file_size=self.total,
            ):
                data = bytes(part)
                break

        if not data:
            return b""

        self.chunks[offset] = data
        self.fetched_bytes += len(data)
        self.ranges.append((offset, len(data)))
        return data

    async def read(self, start: int, length: int) -> bytes:
        if self.cancelled:
            raise ProbeCancelled("scan cancelled")
        if length <= 0:
            return b""

        if self.total is not None:
            if start >= self.total:
                return b""
            length = min(length, self.total - start)

        first = (start // self.chunk_size) * self.chunk_size
        last = ((start + length - 1) // self.chunk_size) * self.chunk_size

        pieces = []
        async with self.lock:
            offset = first
            while offset <= last:
                data = await self._fetch_chunk(offset)
                if not data:
                    break
                pieces.append((offset, data))
                offset += self.chunk_size

        out = bytearray()
        wanted_end = start + length
        for offset, data in pieces:
            left = max(start, offset)
            right = min(wanted_end, offset + len(data))
            if right > left:
                out.extend(data[left - offset:right - offset])
        return bytes(out)



class TelegramSeekableFile(io.RawIOBase):
    """
    File-like object for FFmpeg/PyAV.

    FFmpeg is allowed to seek anywhere, but every read is converted into a
    bounded Telegram byte-range request. No local complete file is created.
    The asyncio event loop performs Telegram I/O while FFmpeg runs in a worker
    thread.
    """
    def __init__(self, session: RangeProbeSession, loop: asyncio.AbstractEventLoop):
        self.session = session
        self.loop = loop
        self.position = 0
        self.closed_flag = False

    def readable(self):
        return True

    def seekable(self):
        return True

    def writable(self):
        return False

    def tell(self):
        return self.position

    def seek(self, offset, whence=io.SEEK_SET):
        if self.closed_flag:
            raise ValueError("I/O operation on closed media")
        total = self.session.total

        if whence == io.SEEK_SET:
            target = offset
        elif whence == io.SEEK_CUR:
            target = self.position + offset
        elif whence == io.SEEK_END:
            if total is None:
                raise OSError("SEEK_END requires known Telegram file size")
            target = total + offset
        else:
            raise ValueError("invalid whence")

        if target < 0:
            raise OSError("negative seek position")

        self.position = target
        return self.position

    def _read_sync(self, size):
        future = asyncio.run_coroutine_threadsafe(
            self.session.read(self.position, size),
            self.loop,
        )
        try:
            data = future.result(timeout=45)
        except Exception as exc:
            raise OSError(str(exc)) from exc
        self.position += len(data)
        return data

    def read(self, size=-1):
        if self.closed_flag:
            raise ValueError("I/O operation on closed media")
        if size is None or size < 0:
            if self.session.total is None:
                raise OSError("unbounded read is not allowed")
            size = self.session.total - self.position
        if size <= 0:
            return b""
        return self._read_sync(size)

    def readinto(self, buffer):
        data = self.read(len(buffer))
        n = len(data)
        buffer[:n] = data
        return n

    def close(self):
        self.closed_flag = True
        return super().close()


probe_sessions: dict[str, RangeProbeSession] = {}


def register_probe(token: str, client: Any, media: Any, total: int | None, budget: int = DEFAULT_BUDGET) -> RangeProbeSession:
    session = RangeProbeSession(
        token=token,
        client=client,
        media=media,
        total=total,
        budget=budget,
    )
    probe_sessions[token] = session
    return session


def get_probe(token: str) -> RangeProbeSession | None:
    session = probe_sessions.get(token)
    if session and not session.expired():
        return session
    probe_sessions.pop(token, None)
    return None


async def cancel_probe(token: str) -> None:
    session = get_probe(token)
    if session:
        await session.cancel()


def remove_probe(token: str) -> None:
    probe_sessions.pop(token, None)


def purge_probes() -> None:
    now = time.monotonic()
    for token, session in list(probe_sessions.items()):
        if session.cancelled or now - session.created_at > RANGE_TOKEN_TTL:
            probe_sessions.pop(token, None)


def _tag(metadata: Any, *names: str) -> str | None:
    if metadata is None:
        return None
    try:
        items = dict(metadata)
    except (TypeError, ValueError):
        return None
    lowered = {str(k).lower(): v for k, v in items.items()}
    for name in names:
        value = lowered.get(name.lower())
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


_LANGUAGE_TEXT_HINTS = (
    (re.compile(r"\b(?:english|eng|en)\b", re.I), "English"),
    (re.compile(r"\b(?:japanese|jpn|ja)\b", re.I), "Japanese"),
    (re.compile(r"\b(?:telugu|tel|te)\b", re.I), "Telugu"),
    (re.compile(r"\b(?:hindi|hin|hi)\b", re.I), "Hindi"),
    (re.compile(r"\b(?:tamil|tam|ta)\b", re.I), "Tamil"),
    (re.compile(r"\b(?:malayalam|mal|ml)\b", re.I), "Malayalam"),
    (re.compile(r"\b(?:kannada|kan|kn)\b", re.I), "Kannada"),
    (re.compile(r"\b(?:korean|kor|ko)\b", re.I), "Korean"),
    (re.compile(r"\b(?:chinese|mandarin|zho|chi|zh)\b", re.I), "Chinese"),
    (re.compile(r"\b(?:arabic|ara|ar)\b", re.I), "Arabic"),
    (re.compile(r"\b(?:spanish|spa|es)\b", re.I), "Spanish"),
    (re.compile(r"\b(?:french|fra|fre|fr)\b", re.I), "French"),
    (re.compile(r"\b(?:german|deu|ger|de)\b", re.I), "German"),
    (re.compile(r"\b(?:russian|rus|ru)\b", re.I), "Russian"),
    (re.compile(r"\b(?:portuguese|por|pt)\b", re.I), "Portuguese"),
    (re.compile(r"\b(?:marathi|mar|mr)\b", re.I), "Marathi"),
    (re.compile(r"\b(?:bengali|ben|bn)\b", re.I), "Bengali"),
    (re.compile(r"\b(?:gujarati|guj|gu)\b", re.I), "Gujarati"),
    (re.compile(r"\b(?:punjabi|pan|pa)\b", re.I), "Punjabi"),
    (re.compile(r"\b(?:urdu|urd|ur)\b", re.I), "Urdu"),
    (re.compile(r"\b(?:nepali|nep|ne)\b", re.I), "Nepali"),
    (re.compile(r"\b(?:sinhala|sin|si)\b", re.I), "Sinhala"),
)

def _language_from_text(text: str | None) -> str | None:
    value = (text or "").strip()
    if not value:
        return None
    for pattern, canonical in _LANGUAGE_TEXT_HINTS:
        if pattern.search(value):
            return canonical
    return None

def _language_name(code: str | None) -> str | None:
    if not code:
        return None
    value = code.strip().lower().replace("_", "-")
    if not value or value in {"und", "unknown", "unk", "zxx", "none"}:
        return None
    name = LANG_NAMES.get(value) or LANG_NAMES.get(value.split("-")[0])
    if name and name.lower() != "undetermined":
        return name
    return None


def _codec_name(stream: Any) -> str:
    candidates = []
    try:
        candidates.append(str(stream.codec_context.name or "").strip())
    except Exception:
        pass
    try:
        candidates.append(str(stream.codec_context.codec.name or "").strip())
    except Exception:
        pass
    try:
        candidates.append(str(stream.codec_context.codec.long_name or "").strip())
    except Exception:
        pass

    placeholders = {"", "unknown", "unk", "undefined", "und", "none"}
    for value in candidates:
        if not value or value.strip().lower() in placeholders:
            continue
        short = value.lower()
        return CODEC_NAMES.get(short) or value

    stream_type = str(getattr(stream, "type", "") or "").lower()
    return {
        "audio": "Audio",
        "subtitle": "Subtitle",
        "video": "Video",
    }.get(stream_type, "Media")

def _flag(disposition: Any, name: str) -> str:
    try:
        return "yes" if bool(getattr(disposition, name)) else "no"
    except Exception:
        try:
            return "yes" if bool(disposition & getattr(type(disposition), name.upper())) else "no"
        except Exception:
            return "no"


def _stream_track(stream: Any) -> dict[str, Any]:
    metadata = stream.metadata
    language = _tag(metadata, "language", "language_ietf", "languagebcp47")
    language_name = _language_name(language)

    title = _tag(
        metadata,
        "title",
        "track_name",
        "name",
        "handler_name",
    )
    codec_name = _codec_name(stream)

    clean_title = title.strip() if title else None
    if clean_title and clean_title.lower() in {
        "unknown", "und", "undefined", "audio", "video", "subtitle", "track"
    }:
        clean_title = None

    title_language = _language_from_text(clean_title)
    if title_language:
        language_name = title_language


    kind = str(getattr(stream, "type", "media") or "media").lower()
    base_type = {
        "audio": "Audio",
        "video": "Video",
        "subtitle": "Subtitle",
    }.get(kind, "Media")
    track_no = getattr(stream, "index", None)
    track_label = f" {int(track_no) + 1}" if isinstance(track_no, int) and track_no >= 0 else ""

    if clean_title:
        display_name = clean_title
        display_source = "embedded track title"
    elif language_name and codec_name not in {"Audio", "Video", "Subtitle", "Media"}:
        display_name = f"{language_name} {codec_name} {base_type} Track{track_label}"
        display_source = "language + codec metadata"
    elif language_name:
        display_name = f"{language_name} {base_type} Track{track_label}"
        display_source = "language metadata"
    elif codec_name not in {"Audio", "Video", "Subtitle", "Media"}:
        display_name = f"{codec_name} {base_type} Track{track_label}"
        display_source = "codec metadata"
    else:
        display_name = f"{base_type} Track{track_label}".strip()
        display_source = "stream type fallback"

    track = {
        "type": "subtitle" if kind == "subtitle" else kind,
        "track": str(int(getattr(stream, "index", -1)) + 1) if isinstance(getattr(stream, "index", None), int) and getattr(stream, "index", -1) >= 0 else "",
        "name": display_name,
        "display_name": display_name,
        "name_source": display_source,
        "language": language,
        "language_name": language_name,
        "codec": str(getattr(stream.codec_context, "name", "") or ""),
        "codec_name": None if codec_name in {"Audio", "Video", "Subtitle", "Media"} else codec_name,
        "default": _flag(stream.disposition, "default"),
        "original": _flag(stream.disposition, "original"),
        "commentary": _flag(stream.disposition, "comment"),
        "forced": _flag(stream.disposition, "forced"),
        "hearing_impaired": _flag(stream.disposition, "hearing_impaired"),
        "visual_impaired": _flag(stream.disposition, "visual_impaired"),
    }

    if kind == "audio":
        channels = getattr(stream.codec_context, "channels", None)
        sample_rate = getattr(stream.codec_context, "sample_rate", None)
        if channels: track["channels"] = str(channels)
        if sample_rate: track["sample_rate"] = f"{float(sample_rate)/1000:.1f} kHz"
        bitrate = getattr(stream, "bit_rate", None)
        if bitrate: track["bitrate"] = f"{float(bitrate)/1000:.0f} kb/s"
        try:
            layout = stream.layout.name
        except Exception:
            layout = None
        if layout: track["layout"] = str(layout)

    elif kind == "video":
        width = getattr(stream, "width", None)
        height = getattr(stream, "height", None)
        if width and height: track["dimensions"] = f"{int(width)} × {int(height)}"
        pix_fmt = getattr(stream, "pix_fmt", None)
        if pix_fmt: track["pixel_format"] = str(pix_fmt)
        profile = getattr(stream, "profile", None)
        if profile: track["profile"] = str(profile)
        level = getattr(stream.codec_context, "level", None)
        if level not in (None, -99, -1):
            track["level"] = str(level)
        fps = getattr(stream, "average_rate", None)
        if fps:
            try: track["frame_rate"] = f"{float(fps):.3f} fps"
            except Exception: pass

    elif kind == "subtitle":
        codec = str(getattr(stream.codec_context, "name", "") or "")
        if codec and codec.lower() not in {"unknown", "und"}:
            track["subtitle_format"] = CODEC_NAMES.get(codec.lower(), codec)

    for key in ("title", "language", "language_ietf", "handler_name", "comment"):
        value = _tag(metadata, key)
        if value: track[f"tag_{key}"] = value

    return {k: v for k, v in track.items() if v not in (None, "")}

def _container_runtime(container: Any) -> str | None:
    duration = getattr(container, "duration", None)
    if duration is None:
        return None
    seconds = float(duration) / 1_000_000.0
    total = max(0, int(round(seconds)))
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def _build_report(message: Any, container: Any, session: RangeProbeSession) -> Report:
    f = getattr(message, "file", None)
    filename = str(getattr(f, "name", None) or "Telegram media file")
    size = getattr(f, "size", None)
    mime = getattr(f, "mime_type", None)

    fmt = getattr(container, "format", None)
    format_name = str(getattr(fmt, "name", "") or "").strip()
    format_long = str(getattr(fmt, "long_name", "") or "").strip()
    detected = format_long or format_name or "Detected media"

    report = Report(
        filename=filename,
        size=int(size) if isinstance(size, int) else session.total,
        mime=str(mime) if mime else None,
        ext="",
        detected=detected,
        media_kind="Video" if any(s.type == "video" for s in container.streams) else (
            "Audio" if any(s.type == "audio" for s in container.streams) else "File"
        ),
        sampled=session.fetched_bytes,
    )

    runtime = _container_runtime(container)
    if runtime:
        report.container["runtime"] = runtime
        report.container["runtime_source"] = "PyAV/FFmpeg container metadata"

    title = _tag(container.metadata, "title")
    if title: report.container["title"] = title

    bit_rate = getattr(container, "bit_rate", None)
    if bit_rate:
        report.container["average_bitrate"] = f"{float(bit_rate)/1_000_000:.2f} Mbps"

    for stream in container.streams:
        track = _stream_track(stream)
        if stream.type == "audio":
            bucket = report.audio.setdefault("tracks", [])
            if len(bucket) < MAX_TRACKS_PER_KIND:
                bucket.append(track)
        elif stream.type == "video":
            bucket = report.video.setdefault("tracks", [])
            if len(bucket) < MAX_TRACKS_PER_KIND:
                bucket.append(track)
        elif stream.type in {"subtitle", "subtitles"}:
            if len(report.subtitles) < MAX_TRACKS_PER_KIND:
                report.subtitles.append(track)

    report.probe_ranges = [
        f"PyAV range: +{off} B ({size} B)"
        for off, size in session.ranges
    ]
    report.notes = [
        f"Player-engine scan: PyAV {av.__version__} with bundled FFmpeg.",
        f"Telegram ranges fetched: {session.fetched_bytes / 1024 / 1024:.2f} MiB.",
        "No complete local copy was created.",
    ]
    return report


def _track_key(track: dict[str, Any]) -> tuple[str, str, str, str]:
    kind = str(track.get("type") or "").lower()
    if kind == "subtitles": kind = "subtitle"
    return (
        kind,
        str(track.get("track") or ""),
        str(track.get("language") or "").lower(),
        str(track.get("codec_name") or track.get("codec") or "").lower(),
    )


def _useful_name(value: Any, kind: str) -> bool:
    if not value: return False
    text = str(value).strip()
    return text.lower() not in {
        "", "unknown", "unk", "undefined", "und", "audio", "video", "subtitle", "track"
    }


def _merge_tracks(primary: list[dict[str, Any]], secondary: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    index: dict[tuple[str, str, str, str], int] = {}

    for track in list(primary) + list(secondary):
        item = dict(track)
        key = _track_key(item)
        if key in index:
            current = merged[index[key]]
            for field, value in item.items():
                if value in (None, ""): continue
                if field in {"name", "display_name"}:
                    if _useful_name(value, key[0]) and not _useful_name(current.get(field), key[0]):
                        current[field] = value
                    continue
                if current.get(field) in (None, ""):
                    current[field] = value
            if _useful_name(item.get("name"), key[0]) and not _useful_name(current.get("name"), key[0]):
                current["name"] = item["name"]
                current["display_name"] = item.get("display_name") or item["name"]
        else:
            index[key] = len(merged)
            merged.append(item)
    return merged[:MAX_TRACKS_PER_KIND]


def _merge_reports(primary: Report, secondary: Report) -> Report:
    # Merge the custom container parser and PyAV/FFmpeg so one source can fill
    # gaps left by the other without replacing useful metadata already found.
    primary.video["tracks"] = _merge_tracks(
        list(primary.video.get("tracks", []) or []),
        list(secondary.video.get("tracks", []) or []),
    )
    primary.audio["tracks"] = _merge_tracks(
        list(primary.audio.get("tracks", []) or []),
        list(secondary.audio.get("tracks", []) or []),
    )
    primary.subtitles = _merge_tracks(
        list(primary.subtitles or []),
        list(secondary.subtitles or []),
    )
    primary.video["tracks"] = primary.video["tracks"][:MAX_TRACKS_PER_KIND]
    primary.audio["tracks"] = primary.audio["tracks"][:MAX_TRACKS_PER_KIND]
    primary.subtitles = primary.subtitles[:MAX_TRACKS_PER_KIND]

    for source in (secondary.video, secondary.audio):
        for key, value in source.items():
            if key == "tracks" or value in (None, "", []): continue
            if key not in primary.video and source is secondary.video:
                primary.video[key] = value
            if key not in primary.audio and source is secondary.audio:
                primary.audio[key] = value

    for key, value in secondary.container.items():
        if value not in (None, ""): primary.container.setdefault(key, value)

    if (not primary.detected or primary.detected.lower() in {"unknown", "detected media"}) and secondary.detected:
        primary.detected = secondary.detected
    if not primary.mime and secondary.mime: primary.mime = secondary.mime
    primary.media_kind = secondary.media_kind if secondary.media_kind != "File" else primary.media_kind
    primary.sampled = int(primary.sampled or 0) + int(secondary.sampled or 0)
    if not primary.previews and secondary.previews:
        primary.previews = list(secondary.previews)

    seen=set(primary.probe_ranges)
    primary.probe_ranges += [x for x in secondary.probe_ranges if x not in seen]
    seen_notes=set(primary.notes)
    primary.notes += [x for x in secondary.notes if x not in seen_notes]
    primary.notes.insert(0, "Combined metadata sources: custom container parser + PyAV/FFmpeg.")
    return primary


def _extract_video_previews(container: Any, stream: Any) -> list[dict[str, Any]]:
    previews: list[dict[str, Any]] = []
    duration = None
    with suppress(Exception):
        if stream.duration is not None and stream.time_base is not None:
            duration = float(stream.duration * stream.time_base)
    if not duration or duration <= 0:
        with suppress(Exception):
            if container.duration is not None:
                duration = float(container.duration / av.time_base)
    if not duration or duration <= 0:
        return previews

    for ratio in PREVIEW_RATIOS:
        target_seconds = max(0.0, min(duration - 0.05, duration * float(ratio)))
        try:
            if stream.time_base is not None:
                target_pts = int(target_seconds / float(stream.time_base))
                container.seek(target_pts, stream=stream, any_frame=False, backward=True)
            else:
                container.seek(int(target_seconds * av.time_base), any_frame=False, backward=True)

            selected = None
            for frame in container.decode(stream):
                selected = frame
                break
            if selected is None:
                continue

            image = selected.to_image()
            image.thumbnail((640, 360))
            output = io.BytesIO()
            image.save(output, format="JPEG", quality=78, optimize=True)
            previews.append({
                "ratio": int(ratio * 100),
                "seconds": target_seconds,
                "data": base64.b64encode(output.getvalue()).decode("ascii"),
                "mime": "image/jpeg",
            })
        except Exception:
            log.exception("Preview extraction failed | ratio=%.2f", ratio)
    return previews

PREVIEW_CONCURRENCY = 1
preview_semaphore = asyncio.Semaphore(PREVIEW_CONCURRENCY)

async def generate_video_previews(
    client: Any,
    message: Any,
    token: str,
    *,
    budget: int = 256 * 1024,
    timeout: int = 5,
) -> list[dict[str, Any]]:
    """Return one Telegram thumbnail only; never download/seek the source video."""
    async with preview_semaphore:
        try:
            f = getattr(message, "file", None)
            media = getattr(message, "media", None)
            if not media:
                return []

            # Telegram's thumbnail is already a small generated image. We use it
            # directly and intentionally do not fall back to reading the video.
            thumb = await asyncio.wait_for(
                client.download_media(message, file=bytes, thumb=0),
                timeout=timeout,
            )
            if not thumb:
                log.info("No Telegram thumbnail available | token=%s", token)
                return []

            raw = bytes(thumb)
            if not raw or len(raw) > PREVIEW_BUDGET:
                log.info(
                    "Telegram thumbnail rejected | token=%s | bytes=%s",
                    token,
                    len(raw),
                )
                return []

            return [{
                "ratio": 0,
                "seconds": 0,
                "data": base64.b64encode(raw).decode("ascii"),
                "mime": "image/jpeg",
                "label": "Telegram thumbnail",
            }]
        except (asyncio.TimeoutError, asyncio.CancelledError):
            log.info("Telegram thumbnail preview unavailable | token=%s", token)
            return []
        except Exception:
            log.exception("Telegram thumbnail preview failed | token=%s", token)
            return []

def _open_with_ffmpeg(reader: TelegramSeekableFile, format_hint: str | None = None) -> Any:
    options = {
        "probesize": str(4 * 1024 * 1024),
        "analyzeduration": "5000000",
        "fflags": "+genpts",
        "scan_all_pmts": "1",
    }
    kwargs = {
        "mode": "r",
        "options": options,
        "buffer_size": 1024 * 1024,
    }
    if format_hint:
        kwargs["format"] = format_hint
    container = av.open(reader, **kwargs)
    _ = list(container.streams)
    return container


async def inspect_telegram_player(
    client: Any,
    message: Any,
    token: str,
    *,
    progress: Any = None,
    budget: int = DEFAULT_BUDGET,
    port: int = 10000,
) -> Report:
    f = getattr(message, "file", None)
    media = getattr(message, "media", None)
    if not media:
        raise ValueError("Message has no media")

    total = getattr(f, "size", None)
    name = str(getattr(f, "name", None) or "")
    mime = str(getattr(f, "mime_type", None) or "").lower()
    is_mkv = name.lower().endswith((".mkv", ".webm")) or "matroska" in mime
    effective_budget = max(2 * 1024 * 1024, min(int(budget or DEFAULT_BUDGET), MAX_PROBE_BUDGET))

    async def say(value: str):
        if progress:
            await progress(value)

    custom_report: Report | None = None
    if is_mkv:
        try:
            await say("🧭 Stage 1/4 • inspecting container indexes and track metadata…")
            custom_report, _ = await inspect_telegram_message(
                client,
                message,
                progress=progress,
                deep=False,
            )
        except (ProbeBudgetExceeded, ProbeCancelled):
            raise
        except Exception as exc:
            await say(f"🔄 Stage 2/4 • switching metadata source ({type(exc).__name__})…")

    await say("🎬 Stage 2/4 • opening the seekable FFmpeg player engine…")
    session = register_probe(token, client, media, total, budget=effective_budget)
    loop = asyncio.get_running_loop()
    reader = TelegramSeekableFile(session, loop)

    format_hint = None
    lower_name = name.lower()
    if lower_name.endswith(".mkv") or "matroska" in mime:
        format_hint = "matroska"
    elif lower_name.endswith(".webm") or "webm" in mime:
        format_hint = "webm"
    elif lower_name.endswith((".mp4", ".m4v", ".mov", ".m4a")) or "mp4" in mime:
        format_hint = "mov,mp4,m4a,3gp,3g2,mj2"
    elif lower_name.endswith((".ts", ".m2ts", ".mts")) or "mpegts" in mime:
        format_hint = "mpegts"

    try:
        container = None
        try:
            container = await asyncio.to_thread(
                _open_with_ffmpeg,
                reader,
                format_hint,
            )
            await say("🔎 Stage 3/4 • reading video, audio and subtitle streams…")
            player_report = _build_report(message, container, session)
        finally:
            if container is not None:
                container.close()

        await say("🧩 Stage 4/4 • merging every available metadata source…")
        if custom_report is not None:
            return _merge_reports(custom_report, player_report)
        return player_report

    except (ProbeBudgetExceeded, ProbeCancelled):
        raise
    except Exception as exc:
        if isinstance(exc, av.error.ExitError):
            # Never run a second multi-range parser after the main player probe.
            # For MKV, the container parser already populated custom_report.
            if custom_report is not None:
                return custom_report
            try:
                fallback, _ = await inspect_telegram_message(
                    client,
                    message,
                    progress=progress,
                    deep=False,
                )
                fallback.notes.insert(
                    0,
                    "FFmpeg could not expose all streams; returned the bounded metadata scan.",
                )
                return fallback
            except Exception:
                raise
        raise
    finally:
        reader.close()
        remove_probe(token)
