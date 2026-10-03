from __future__ import annotations

import asyncio
import html
import time
from dataclasses import dataclass, field
from typing import Any

import av

from file_inspector import Report

LANG_NAMES = {
    "eng": "English", "en": "English", "jpn": "Japanese", "ja": "Japanese",
    "hin": "Hindi", "hi": "Hindi", "tel": "Telugu", "te": "Telugu",
    "tam": "Tamil", "ta": "Tamil", "mal": "Malayalam", "ml": "Malayalam",
    "kan": "Kannada", "kn": "Kannada", "kor": "Korean", "ko": "Korean",
    "zho": "Chinese", "chi": "Chinese", "zh": "Chinese", "spa": "Spanish",
    "es": "Spanish", "fra": "French", "fre": "French", "fr": "French",
    "deu": "German", "ger": "German", "de": "German", "ita": "Italian",
    "it": "Italian", "rus": "Russian", "ru": "Russian", "ara": "Arabic",
    "ar": "Arabic", "por": "Portuguese", "pt": "Portuguese",
    "und": "Undetermined",
}

CODEC_NAMES = {
    "aac": "AAC", "ac3": "AC-3", "eac3": "E-AC-3", "opus": "Opus",
    "vorbis": "Vorbis", "mp3": "MP3", "flac": "FLAC", "truehd": "TrueHD",
    "dts": "DTS", "dca": "DTS", "alac": "ALAC",
    "h264": "H.264/AVC", "hevc": "H.265/HEVC", "av1": "AV1", "vp9": "VP9",
    "ass": "ASS", "ssa": "SSA", "subrip": "SubRip", "webvtt": "WebVTT",
    "hdmv_pgs_subtitle": "PGS", "dvd_subtitle": "VobSub",
}

DEFAULT_RANGE_CHUNK = 256 * 1024
DEFAULT_BUDGET = 8 * 1024 * 1024
RANGE_TOKEN_TTL = 10 * 60


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
        async for part in self.client.iter_download(
            self.media,
            offset=offset,
            limit=1,
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


def _language_name(code: str | None) -> str | None:
    if not code:
        return None
    value = code.strip().lower().replace("_", "-")
    return LANG_NAMES.get(value) or LANG_NAMES.get(value.split("-")[0])


def _codec_name(stream: Any) -> str:
    try:
        short = str(stream.codec_context.name or "").lower()
    except Exception:
        short = ""
    try:
        long_name = str(stream.codec_context.codec.long_name or "")
    except Exception:
        long_name = ""
    return CODEC_NAMES.get(short) or long_name or short or "Unknown"


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

    # Player-like label priority: explicit track title first, then language,
    # then codec. This mirrors the metadata concepts used by media players.
    display_name = title or language_name or codec_name

    track = {
        "type": str(stream.type),
        "track": str(getattr(stream, "index", "")),
        "name": display_name,
        "display_name": display_name,
        "name_source": (
            "embedded track title" if title
            else "embedded language" if language_name
            else "FFmpeg codec name"
        ),
        "language": language,
        "language_name": language_name,
        "codec": str(getattr(stream.codec_context, "name", "") or ""),
        "codec_name": codec_name,
        "default": _flag(stream.disposition, "default"),
        "original": _flag(stream.disposition, "original"),
        "commentary": _flag(stream.disposition, "comment"),
        "forced": _flag(stream.disposition, "forced"),
        "hearing_impaired": _flag(stream.disposition, "hearing_impaired"),
        "visual_impaired": _flag(stream.disposition, "visual_impaired"),
    }

    if stream.type == "audio":
        channels = getattr(stream.codec_context, "channels", None)
        sample_rate = getattr(stream.codec_context, "sample_rate", None)
        if channels:
            track["channels"] = str(channels)
        if sample_rate:
            track["sample_rate"] = f"{float(sample_rate)/1000:.1f} kHz"
        bitrate = getattr(stream, "bit_rate", None)
        if bitrate:
            track["bitrate"] = f"{float(bitrate)/1000:.0f} kb/s"
        try:
            layout = stream.layout.name
        except Exception:
            layout = None
        if layout:
            track["layout"] = str(layout)

    elif stream.type == "video":
        width = getattr(stream, "width", None)
        height = getattr(stream, "height", None)
        if width and height:
            track["dimensions"] = f"{int(width)} × {int(height)}"
        pix_fmt = getattr(stream, "pix_fmt", None)
        if pix_fmt:
            track["pixel_format"] = str(pix_fmt)
        profile = getattr(stream, "profile", None)
        if profile:
            track["profile"] = str(profile)

    elif stream.type == "subtitle":
        codec = str(getattr(stream.codec_context, "name", "") or "")
        if codec:
            track["subtitle_format"] = CODEC_NAMES.get(codec.lower(), codec)

    # Keep useful FFmpeg tags for the browser page.
    for key in ("title", "language", "language_ietf", "handler_name", "comment"):
        value = _tag(metadata, key)
        if value:
            track[f"tag_{key}"] = value

    return track


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
    filename = str(getattr(f, "name", None) or "telegram_file")
    size = getattr(f, "size", None)
    mime = getattr(f, "mime_type", None)

    format_name = str(getattr(container.format, "name", "") or "unknown")
    format_long = str(getattr(container.format, "long_name", "") or format_name)

    report = Report(
        filename=filename,
        size=int(size) if isinstance(size, int) else session.total,
        mime=str(mime) if mime else None,
        ext="",
        detected=f"{format_long} ({format_name})",
        media_kind="Video" if any(s.type == "video" for s in container.streams) else "File",
        sampled=session.fetched_bytes,
    )

    runtime = _container_runtime(container)
    if runtime:
        report.container["runtime"] = runtime
        report.container["runtime_source"] = "FFmpeg/PyAV container metadata"

    title = _tag(container.metadata, "title")
    if title:
        report.container["title"] = title

    bit_rate = getattr(container, "bit_rate", None)
    if bit_rate:
        report.container["average_bitrate"] = f"{float(bit_rate)/1_000_000:.2f} Mbps"

    for stream in container.streams:
        track = _stream_track(stream)
        if stream.type == "audio":
            report.audio.setdefault("tracks", []).append(track)
        elif stream.type == "video":
            report.video.setdefault("tracks", []).append(track)
        elif stream.type == "subtitle":
            report.subtitles.append(track)

    report.probe_ranges = [
        f"FFmpeg range: +{off} B ({size} B)"
        for off, size in session.ranges
    ]

    report.notes = [
        f"Player-engine scan: PyAV {av.__version__} / bundled FFmpeg.",
        f"Telegram ranges fetched: {session.fetched_bytes / 1024 / 1024:.2f} MiB.",
        "No complete file copy was created.",
    ]

    return report


def _open_with_ffmpeg(url: str) -> Any:
    # FFmpeg handles Matroska/MP4/WebM/MOV/TS and many other containers.
    # Keep probing focused on metadata and stream headers.
    options = {
        "seekable": "1",
        "probesize": str(2 * 1024 * 1024),
        "analyzeduration": "3000000",
        "multiple_requests": "1",
    }

    container = av.open(
        url,
        mode="r",
        options=options,
        timeout=(8.0, 30.0),
    )
    # Touch streams so all stream headers and metadata are initialized.
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
    session = register_probe(token, client, media, total, budget=budget)
    url = f"http://127.0.0.1:{port}/probe/{token}"

    async def say(value: str):
        if progress:
            await progress(value)

    try:
        await say("🎬 Opening player-style media engine…")

        # av.open is synchronous. Run it off the Telegram event loop so the
        # local /probe endpoint can continue serving its ranged reads.
        container = await asyncio.to_thread(_open_with_ffmpeg, url)

        try:
            await say("🎵 Reading all audio and subtitle stream metadata…")
            # Accessing metadata/streams is enough; do not decode any packets.
            report = _build_report(message, container, session)
        finally:
            container.close()

        if not report.audio.get("tracks") and not report.subtitles:
            report.notes.append(
                "FFmpeg did not expose track metadata within the bounded range budget."
            )

        return report

    finally:
        remove_probe(token)
