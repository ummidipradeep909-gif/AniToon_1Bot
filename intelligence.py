"""Low-dependency web/media intelligence helpers for AniToon.

Uses Python's standard library for Google Gemini and YouTube Data API requests.
Keys are supplied by environment variables; no secrets are stored in source.
"""
from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timezone
from html import escape
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, quote, urlsplit
from urllib.request import Request, urlopen
from typing import Any

MAX_QUERY_CHARS = 140
MAX_VIDEO_RESULTS = 5
HTTP_TIMEOUT_SECONDS = 12
_CACHE_TTL_SECONDS = 900
_CACHE_MAX_ITEMS = 32
_INTEL_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_YOUTUBE_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}


def _cache_get(cache: dict[str, tuple[float, dict[str, Any]]], key: str) -> dict[str, Any] | None:
    entry = cache.get(key)
    if not entry:
        return None
    created, value = entry
    if time.monotonic() - created > _CACHE_TTL_SECONDS:
        cache.pop(key, None)
        return None
    return value


def _cache_put(cache: dict[str, tuple[float, dict[str, Any]]], key: str, value: dict[str, Any]) -> None:
    now = time.monotonic()
    expired = [item for item, (created, _) in cache.items() if now - created > _CACHE_TTL_SECONDS]
    for item in expired:
        cache.pop(item, None)
    while len(cache) >= _CACHE_MAX_ITEMS:
        cache.pop(next(iter(cache)))
    cache[key] = (now, value)


class IntelligenceError(RuntimeError):
    """A safe, user-displayable intelligence error."""


def _clean_query(value: str) -> str:
    query = " ".join(str(value or "").split()).strip()
    if len(query) > MAX_QUERY_CHARS:
        query = query[:MAX_QUERY_CHARS].rstrip()
    if not query:
        raise IntelligenceError("Please provide a topic or search phrase.")
    return query


def _json_request(
    url: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    api_key: str | None = None,
) -> dict[str, Any]:
    data = None
    headers = {"Accept": "application/json", "User-Agent": "AniToon-Media-Intelligence/1.0"}
    if api_key:
        # Current Gemini auth keys are sent in the x-goog-api-key header.
        # Avoid putting Gemini credentials in request URLs.
        headers["x-goog-api-key"] = api_key
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = Request(url, data=data, headers=headers, method=method)
    try:
        with urlopen(req, timeout=HTTP_TIMEOUT_SECONDS) as response:
            raw = response.read(1_500_000)
        parsed = json.loads(raw.decode("utf-8", "replace"))
        if not isinstance(parsed, dict):
            raise IntelligenceError("The provider returned an unexpected response.")
        return parsed
    except HTTPError as exc:
        # Do not include request URLs, which may contain API keys, in user-visible errors.
        if exc.code in (401, 403):
            raise IntelligenceError("The API key is invalid, disabled, or missing required API access.") from None
        if exc.code == 429:
            raise IntelligenceError("The provider quota or rate limit has been reached. Try again later.") from None
        raise IntelligenceError(f"The provider returned HTTP {exc.code}. Try again later.") from None
    except (URLError, TimeoutError, OSError):
        raise IntelligenceError("The provider could not be reached. Please try again later.") from None
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise IntelligenceError("The provider returned an unreadable response.") from None


def _safe_https_url(value: Any) -> str | None:
    url = str(value or "").strip()
    try:
        parts = urlsplit(url)
    except ValueError:
        return None
    if parts.scheme != "https" or not parts.netloc or parts.username or parts.password:
        return None
    if not (parts.hostname or "").lower():
        return None
    return url


def gemini_intelligence(topic: str) -> dict[str, Any]:
    """Return a concise, search-grounded intelligence report and only real provider citations."""
    query = _clean_query(topic)
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise IntelligenceError(
            "Gemini is not configured yet. Add GEMINI_API_KEY in Render → Environment, then redeploy."
        )
    model = os.getenv("GEMINI_MODEL", "gemini-3.5-flash-lite").strip() or "gemini-3.5-flash-lite"
    if not re.fullmatch(r"[A-Za-z0-9._-]{1,100}", model):
        raise IntelligenceError("GEMINI_MODEL contains unsupported characters.")
    cache_key = model + ":" + query.casefold()
    cached = _cache_get(_INTEL_CACHE, cache_key)
    if cached is not None:
        return cached
    prompt = (
        "Create a concise, evidence-first media intelligence brief about this topic: "
        + query
        + "\nUse Google Search grounding. Include: (1) key developments and dates, "
        "(2) emerging trend and what is driving it, (3) a cautious sentiment/public-reaction "
        "reading only when evidence supports it, (4) two or three important claims and whether "
        "the cited sources support them, (5) uncertainties, missing evidence, and what to verify next. "
        "Separate sourced facts from inference. Do not invent quotes, statistics, URLs, or sources. "
        "Do not imply search results represent the whole public. If evidence is inadequate, say so. "
        "Keep the answer under 550 words and do not create a separate bibliography; source links are "
        "attached from actual grounding metadata by the application."
    )
    endpoint = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        + quote(model, safe="")
        + ":generateContent"
    )
    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "tools": [{"google_search": {}}],
        "generationConfig": {"temperature": 0.2, "maxOutputTokens": 700},
    }
    response = _json_request(endpoint, method="POST", payload=payload, api_key=api_key)
    candidates = response.get("candidates") or []
    candidate = candidates[0] if candidates and isinstance(candidates[0], dict) else {}
    content = candidate.get("content") if isinstance(candidate, dict) else {}
    parts = content.get("parts", []) if isinstance(content, dict) else []
    text_parts = [
        str(part.get("text", "")).strip()
        for part in parts
        if isinstance(part, dict) and str(part.get("text", "")).strip()
    ]
    summary = "\n".join(text_parts).strip()
    if not summary:
        block_reason = (
            (candidate.get("finishReason") if isinstance(candidate, dict) else None)
            or response.get("promptFeedback", {}).get("blockReason")
            or "no response text"
        )
        raise IntelligenceError(f"Gemini could not create this report ({block_reason}).")

    metadata = candidate.get("groundingMetadata", {}) if isinstance(candidate, dict) else {}
    chunks = metadata.get("groundingChunks", []) if isinstance(metadata, dict) else []
    sources: list[dict[str, str]] = []
    seen: set[str] = set()
    for chunk in chunks if isinstance(chunks, list) else []:
        if not isinstance(chunk, dict):
            continue
        web = chunk.get("web", {})
        if not isinstance(web, dict):
            continue
        url = _safe_https_url(web.get("uri"))
        if not url or url in seen:
            continue
        seen.add(url)
        title = " ".join(str(web.get("title") or urlsplit(url).hostname or "Source").split())[:180]
        sources.append({"title": title, "url": url})
        if len(sources) >= 7:
            break

    search_queries = []
    raw_queries = metadata.get("webSearchQueries", []) if isinstance(metadata, dict) else []
    for item in raw_queries if isinstance(raw_queries, list) else []:
        item = " ".join(str(item).split())[:140]
        if item and item not in search_queries:
            search_queries.append(item)
        if len(search_queries) >= 5:
            break

    result = {
        "topic": query,
        "text": summary[:8500],
        "sources": sources,
        "search_queries": search_queries,
        "model": model,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    _cache_put(_INTEL_CACHE, cache_key, result)
    return result


def youtube_videos(topic: str, *, shorts: bool = False) -> dict[str, Any]:
    """Search YouTube using its official API; no media is downloaded."""
    query = _clean_query(topic)
    api_key = os.getenv("YOUTUBE_API_KEY", "").strip()
    mode_query = (query + " Shorts").strip() if shorts else query
    cache_key = ("shorts:" if shorts else "videos:") + mode_query.casefold()
    cached = _cache_get(_YOUTUBE_CACHE, cache_key)
    if cached is not None:
        return cached
    if not api_key:
        encoded = urlencode({"search_query": mode_query})
        fallback = "https://www.youtube.com/results?" + encoded
        result = {
            "topic": query,
            "shorts": shorts,
            "videos": [],
            "fallback_url": fallback,
            "configured": False,
        }
        _cache_put(_YOUTUBE_CACHE, cache_key, result)
        return result
    params = {
        "part": "snippet",
        "type": "video",
        "maxResults": str(MAX_VIDEO_RESULTS),
        "q": mode_query,
        "order": "relevance",
        "regionCode": os.getenv("YOUTUBE_REGION_CODE", "IN").strip()[:2] or "IN",
        "safeSearch": "moderate",
        "key": api_key,
    }
    if shorts:
        # YouTube's API filter means under 4 minutes; it does NOT prove a result is a Short.
        params["videoDuration"] = "short"
    data = _json_request("https://www.googleapis.com/youtube/v3/search?" + urlencode(params))
    results = []
    seen_ids: set[str] = set()
    for item in data.get("items", []) if isinstance(data.get("items"), list) else []:
        if not isinstance(item, dict):
            continue
        video_id = item.get("id", {}).get("videoId") if isinstance(item.get("id"), dict) else None
        snippet = item.get("snippet", {}) if isinstance(item.get("snippet"), dict) else {}
        if not video_id or not re.fullmatch(r"[A-Za-z0-9_-]{6,20}", str(video_id)) or video_id in seen_ids:
            continue
        seen_ids.add(str(video_id))
        url = "https://www.youtube.com/watch?v=" + str(video_id)
        title = " ".join(str(snippet.get("title") or "Untitled video").split())[:220]
        description = " ".join(str(snippet.get("description") or "").split())[:240]
        channel = " ".join(str(snippet.get("channelTitle") or "Unknown channel").split())[:100]
        published = str(snippet.get("publishedAt") or "")
        results.append({
            "title": title,
            "description": description,
            "channel": channel,
            "published_at": published[:10],
            "url": url,
            "short_candidate": bool(shorts),
        })
    result = {
        "topic": query,
        "shorts": shorts,
        "videos": results[:MAX_VIDEO_RESULTS],
        "fallback_url": None,
        "configured": True,
    }
    _cache_put(_YOUTUBE_CACHE, cache_key, result)
    return result


def intelligence_html(report: dict[str, Any]) -> str:
    """Produce Telegram-safe HTML without cutting off tags at Telegram's message limit."""
    topic = escape(str(report.get("topic") or "Topic")[:140])
    summary = escape(str(report.get("text") or "")[:1900])
    parts = [
        f"🧠 <b>Media Intelligence Report</b>\n<b>Topic:</b> {topic}\n\n",
        summary,
    ]
    sources = report.get("sources") or []
    source_fragments: list[str] = []
    for index, source in enumerate(sources[:4], start=1):
        if not isinstance(source, dict):
            continue
        url = _safe_https_url(source.get("url"))
        if not url or len(url) > 260:
            continue
        title = escape(str(source.get("title") or "Source")[:100])
        source_fragments.append(f'\n{index}. <a href="{escape(url, quote=True)}">{title}</a>')
    if source_fragments:
        parts.append("\n\n<b>Grounded sources</b>")
        for fragment in source_fragments:
            candidate = "".join(parts) + fragment
            if len(candidate) > 3500:
                break
            parts.append(fragment)
    else:
        parts.append(
            "\n\n⚠️ <i>Gemini returned no usable source links in its grounding metadata. "
            "Treat claims as unverified and try again before relying on them.</i>"
        )
    disclaimer = "\n\n<i>Search coverage is incomplete; sentiment and trend signals are not a representative poll.</i>"
    if len("".join(parts)) + len(disclaimer) <= 3800:
        parts.append(disclaimer)
    return "".join(parts)


def videos_html(result: dict[str, Any]) -> str:
    """Format a small list of link-first video results for Telegram."""
    topic = escape(str(result.get("topic") or "Search")[:140])
    is_shorts = bool(result.get("shorts"))
    heading = "YouTube Short-form Search" if is_shorts else "YouTube Video Search"
    parts = [f"🎬 <b>{heading}</b>\n<b>Topic:</b> {topic}\n\n"]
    videos = result.get("videos") or []
    if not result.get("configured"):
        url = _safe_https_url(result.get("fallback_url"))
        if url:
            parts.append(
                "YouTube Data API is not configured, so I can only provide the live search page—not verified video metadata yet.\n\n"
                f'<a href="{escape(url, quote=True)}">Open YouTube results ↗</a>\n'
                "Add <code>YOUTUBE_API_KEY</code> in Render → Environment to show individual titles, channels, upload dates and links."
            )
        return "".join(parts)
    if not videos:
        parts.append("No videos were returned for that search. Try different keywords.")
        return "".join(parts)
    for index, video in enumerate(videos[:MAX_VIDEO_RESULTS], start=1):
        if not isinstance(video, dict):
            continue
        url = _safe_https_url(video.get("url"))
        if not url:
            continue
        title = escape(str(video.get("title") or "Video")[:160])
        channel = escape(str(video.get("channel") or "Unknown channel")[:70])
        published = escape(str(video.get("published_at") or "Date not provided")[:10])
        desc = escape(str(video.get("description") or "")[:90])
        fragment = f'{index}. <a href="{escape(url, quote=True)}"><b>{title}</b></a>\n   {channel} · {published}\n'
        if desc:
            fragment += f"   {desc}\n"
        if is_shorts:
            fragment += "   <i>Short-duration search candidate; not confirmed as a YouTube Short.</i>\n"
        fragment += "\n"
        if len("".join(parts)) + len(fragment) > 3500:
            break
        parts.append(fragment)
    return "".join(parts)


