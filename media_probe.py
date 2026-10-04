from __future__ import annotations

import asyncio
import html
import time
from dataclasses import dataclass, field
from typing import Any
import io
import asyncio

import av

from file_inspector import inspect_telegram_message

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

DEFAULT_RANGE_CHUNK = 512 * 1024
DEFAULT_BUDGET = 4 * 1024 * 1024
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


def _language_name(code: str | None) -> str | None:
    if not code:
        return None
    value = code.strip().lower().replace("_", "-")
    if value in {"und", "unknown", "unk"}:
        return None
    return LANG_NAMES.get(value) or LANG_NAMES.get(value.split("-")[0])


def _codec_name(stream: Any) -> str:
    candidates = []
    try:
        candidates.append(str(stream.codec_context.name or "").strip().lower())
    except Exception:
        pass
    try:
        candidates.append(str(stream.codec_context.codec.name or "").strip().lower())
    except Exception:
        pass
    try:
        candidates.append(str(stream.codec_context.codec.long_name or "").strip())
    except Exception:
        pass
    for value in candidates:
        if value:
            short = value.lower()
            return CODEC_NAMES.get(short) or value
    return "Audio" if getattr(stream, "type", "") == "audio" else (
        "Subtitle" if getattr(stream, "type", "") == "subtitle" else "Video"
    )


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

    # Prefer a meaningful embedded name, then language + codec.
    # A known media type should never be displayed as a bare "Unknown".
    clean_title = title.strip() if title else None
    if clean_title and clean_title.lower() in {"unknown", "und", "undefined", "audio", "track"}:
        clean_title = None
    if clean_title:
        display_name = clean_title
        display_source = "embedded track title"
    elif language_name:
        display_name = f"{language_name} • {codec_name}"
        display_source = "embedded language + codec"
    else:
        display_name = codec_name or ("Audio" if stream.type == "audio" else str(stream.type).title())
        display_source = "codec metadata" if codec_name else "stream type fallback"

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


def _open_with_ffmpeg(reader: TelegramSeekableFile, format_hint: str | None = None) -> Any:
    # PyAV accepts seekable Python file-like objects. This removes the fragile
    # HTTP-proxy behavior and lets FFmpeg issue real seek/read operations.
    options = {
        "probesize": str(768 * 1024),
        "analyzeduration": "1500000",
        "fflags": "+genpts",
    }

    kwargs = {
        "mode": "r",
        "options": options,
        "buffer_size": 256 * 1024,
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

    async def say(value: str):
        if progress:
            await progress(value)

    # For Matroska/WebM, use the targeted EBML parser first. Matroska requires
    # the first Info/Tracks metadata to be before the first Cluster or indexed
    # by an early SeekHead, so this path avoids making FFmpeg walk video data.
    if is_mkv:
        try:
            await say("🧭 Stage 1/4 • reading Matroska stream metadata…")
            report, used = await inspect_telegram_message(
                client,
                message,
                progress=progress,
                deep=True,
            )
            real_audio = report.audio.get("tracks", [])
            if real_audio:
                await say("🧩 Stage 4/4 • building the final player-style track list…")
                return report
        except Exception as exc:
            # Continue to the player engine for containers that the targeted
            # parser cannot resolve.
            await say(f"🔄 Stage 2/4 • switching to player engine ({type(exc).__name__})…")

    await say("🎬 Stage 2/4 • opening the seekable player engine…")
    session = register_probe(token, client, media, total, budget=budget)
    loop = asyncio.get_running_loop()
    reader = TelegramSeekableFile(session, loop)

    format_hint = None
    lower_name = name.lower()
    if lower_name.endswith(".mkv") or "matroska" in mime:
        format_hint = "matroska"
    elif lower_name.endswith(".webm") or "webm" in mime:
        format_hint = "webm"
    elif lower_name.endswith((".mp4", ".m4v", ".mov")) or "mp4" in mime:
        format_hint = "mov,mp4,m4a,3gp,3g2,mj2"

    try:
        container = None
        try:
            container = await asyncio.to_thread(
                _open_with_ffmpeg,
                reader,
                format_hint,
            )
            await say("🔎 Stage 3/4 • reading every exposed audio/subtitle/video stream…")
            report = _build_report(message, container, session)
        finally:
            if container is not None:
                container.close()

        await say("🧩 Stage 4/4 • assembling the complete stream list…")
        return report

    except (ProbeBudgetExceeded, ProbeCancelled) as exc:
        raise exc
    except Exception as exc:
        # Normalize FFmpeg's immediate-exit condition into a useful scanner
        # error so it is not presented as an opaque ExitError.
        if isinstance(exc, av.error.ExitError):
            try:
                fallback, _ = await inspect_telegram_message(
                    client,
                    message,
                    progress=progress,
                    deep=True,
                )
                fallback.notes.insert(
                    0,
                    "FFmpeg requested beyond the safe range budget; returned targeted container metadata instead.",
                )
                return fallback
            except Exception:
                pass
        raise
    finally:
        reader.close()
        remove_probe(token)
