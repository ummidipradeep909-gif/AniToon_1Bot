"""Low-dependency web/media intelligence helpers for AniToon.

Uses Python's standard library for Google Gemini and YouTube Data API requests.
Keys are supplied by environment variables; no secrets are stored in source.
"""
from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from html import escape
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, quote, urlsplit
from urllib.request import Request, urlopen
from typing import Any

MAX_QUERY_CHARS = 140
MAX_VIDEO_RESULTS = 5
HTTP_TIMEOUT_SECONDS = 12


class IntelligenceError(RuntimeError):
    """A safe, user-displayable intelligence error."""


def _clean_query(value: str) -> str:
    query = " ".join(str(value or "").split()).strip()
    if len(query) > MAX_QUERY_CHARS:
        query = query[:MAX_QUERY_CHARS].rstrip()
    if not query:
        raise IntelligenceError("Please provide a topic or search phrase.")
    return query


def _json_request(url: str, *, method: str = "GET", payload: dict[str, Any] | None = None) -> dict[str, Any]:
    data = None
    headers = {"Accept": "application/json", "User-Agent": "AniToon-Media-Intelligence/1.0"}
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
    model = os.getenv("GEMINI_MODEL", "gemini-2.5-flash").strip() or "gemini-2.5-flash"
    if not re.fullmatch(r"[A-Za-z0-9._-]{1,100}", model):
        raise IntelligenceError("GEMINI_MODEL contains unsupported characters.")
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
        + ":generateContent?"
        + urlencode({"key": api_key})
    )
    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "tools": [{"google_search_retrieval": {}}],
        "generationConfig": {"temperature": 0.2, "maxOutputTokens": 700},
    }
    response = _json_request(endpoint, method="POST", payload=payload)
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

    return {
        "topic": query,
        "text": summary[:8500],
        "sources": sources,
        "search_queries": search_queries,
        "model": model,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def youtube_videos(topic: str, *, shorts: bool = False) -> dict[str, Any]:
    """Search YouTube using its official API; no media is downloaded."""
    query = _clean_query(topic)
    api_key = os.getenv("YOUTUBE_API_KEY", "").strip()
    mode_query = (query + " Shorts").strip() if shorts else query
    if not api_key:
        encoded = urlencode({"search_query": mode_query})
        fallback = "https://www.youtube.com/results?" + encoded
        return {
            "topic": query,
            "shorts": shorts,
            "videos": [],
            "fallback_url": fallback,
            "configured": False,
        }
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
    return {
        "topic": query,
        "shorts": shorts,
        "videos": results[:MAX_VIDEO_RESULTS],
        "fallback_url": None,
        "configured": True,
    }


def intelligence_html(report: dict[str, Any]) -> str:
    """Produce Telegram-safe HTML from provider text plus grounded source links."""
    topic = escape(str(report.get("topic") or "Topic")[:140])
    summary = escape(str(report.get("text") or "")[:8000])
    # Keep provider-generated markup inert, preserving its paragraphs.
    summary = summary.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "\n")
    parts = [
        f"🧠 <b>Media Intelligence Report</b>\n<b>Topic:</b> {topic}\n\n",
        summary,
    ]
    sources = report.get("sources") or []
    if sources:
        parts.append("\n\n<b>Grounded sources</b>")
        for index, source in enumerate(sources[:7], start=1):
            if not isinstance(source, dict):
                continue
            url = _safe_https_url(source.get("url"))
            if not url:
                continue
            title = escape(str(source.get("title") or "Source")[:180])
            parts.append(f'\n{index}. <a href="{escape(url, quote=True)}">{title}</a>')
    else:
        parts.append(
            "\n\n⚠️ <i>Gemini returned no source links in its grounding metadata. "
            "Treat claims as unverified and try again before relying on them.</i>"
        )
    parts.append("\n\n<i>Search coverage is incomplete; sentiment and trend signals are not a representative poll.</i>")
    rendered = "".join(parts)
    return rendered[:3900]


def videos_html(result: dict[str, Any]) -> str:
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
        return "".join(parts)[:3900]
    if not videos:
        parts.append("No videos were returned for that search. Try different keywords.")
        return "".join(parts)
    for index, video in enumerate(videos[:MAX_VIDEO_RESULTS], start=1):
        url = _safe_https_url(video.get("url"))
        if not url:
            continue
        title = escape(str(video.get("title") or "Video")[:200])
        channel = escape(str(video.get("channel") or "Unknown channel")[:100])
        published = escape(str(video.get("published_at") or "Date not provided")[:10])
        desc = escape(str(video.get("description") or "")[:160])
        parts.append(f'{index}. <a href="{escape(url, quote=True)}"><b>{title}</b></a>\n')
        parts.append(f"   {channel} · {published}\n")
        if desc:
            parts.append(f"   {desc}\n")
        if is_shorts:
            parts.append("   <i>Short-duration search candidate; the API does not confirm that it is a YouTube Short.</i>\n")
        parts.append("\n")
    return "".join(parts)[:3900]
