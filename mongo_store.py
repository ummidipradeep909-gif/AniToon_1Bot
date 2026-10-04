from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

from pymongo import MongoClient

log = logging.getLogger("anitoons-mongodb")

_client: MongoClient | None = None
_db = None
_init_lock: asyncio.Lock | None = None


def _uri() -> str:
    return os.getenv("MONGODB_URI", "").strip()


def _database_name() -> str:
    return os.getenv("MONGODB_DATABASE", "anitoons").strip() or "anitoons"


def _get_lock() -> asyncio.Lock:
    global _init_lock
    if _init_lock is None:
        _init_lock = asyncio.Lock()
    return _init_lock


def _json_clean(value: Any) -> Any:
    try:
        return json.loads(json.dumps(value, default=str))
    except Exception:
        return str(value)


async def _get_db():
    global _client, _db

    uri = _uri()
    if not uri:
        return None

    if _db is not None:
        return _db

    async with _get_lock():
        if _db is not None:
            return _db
        try:
            _client = MongoClient(
                uri,
                serverSelectionTimeoutMS=3000,
                connectTimeoutMS=3000,
            )
            await asyncio.to_thread(_client.admin.command, "ping")
            _db = _client[_database_name()]
            log.info("MongoDB connected | database=%s", _database_name())
            return _db
        except Exception:
            log.exception("MongoDB connection failed; persistence will remain unavailable")
            _client = None
            _db = None
            return None


async def record_user(event) -> None:
    db = await _get_db()
    if db is None:
        return

    try:
        sender = await event.get_sender()
        user_id = getattr(sender, "id", None)
        if user_id is None:
            return

        doc = {
            "_id": int(user_id),
            "username": getattr(sender, "username", None),
            "first_name": getattr(sender, "first_name", None),
            "last_name": getattr(sender, "last_name", None),
            "last_seen": datetime.now(timezone.utc),
            "updated_unix": int(time.time()),
        }

        await asyncio.to_thread(
            db.users.update_one,
            {"_id": int(user_id)},
            {"$set": doc, "$setOnInsert": {"created_at": datetime.now(timezone.utc)}},
            True,
        )
    except Exception:
        log.exception("Failed to store user")


async def record_scan(
    *,
    user_id: int | None,
    source_message: Any,
    report: Any,
    status: str,
) -> None:
    db = await _get_db()
    if db is None:
        return

    try:
        filename = getattr(getattr(source_message, "file", None), "name", None) or "telegram_file"
        audio = getattr(report, "audio", {}) or {}
        subtitles = getattr(report, "subtitles", {}) or {}
        video = getattr(report, "video", {}) or {}
        container = getattr(report, "container", {}) or {}

        doc = {
            "user_id": user_id,
            "chat_id": getattr(source_message, "chat_id", None),
            "message_id": getattr(source_message, "id", None),
            "filename": str(filename),
            "status": str(status),
            "created_at": datetime.now(timezone.utc),
            "container": _json_clean(container),
            "audio": _json_clean(audio),
            "video": _json_clean(video),
            "subtitles": _json_clean(subtitles),
        }

        await asyncio.to_thread(db.scans.insert_one, doc)
    except Exception:
        log.exception("Failed to store scan")


async def record_clone_request(
    *,
    user_id: int,
    clone_id: int | None,
    clone_username: str | None,
    clone_first_name: str | None,
    token: str,
) -> None:
    db = await _get_db()
    if db is None:
        log.warning("MongoDB is not configured; validated clone was not persisted")
        return

    try:
        cipher = _cipher()
        doc = {
            "user_id": int(user_id),
            "clone_id": clone_id,
            "clone_username": clone_username,
            "clone_first_name": clone_first_name,
            "status": "validated",
            "created_at": datetime.now(timezone.utc),
        }

        if cipher:
            doc["token_encrypted"] = cipher.encrypt(token.encode("utf-8")).decode("ascii")
        else:
            doc["token_persistence"] = "disabled_until_CLONE_TOKEN_ENCRYPTION_KEY_is_configured"

        await asyncio.to_thread(
            db.clones.update_one,
            {"user_id": int(user_id), "clone_id": clone_id},
            {"$set": doc},
            upsert=True,
        )
    except Exception:
        log.exception("Failed to store clone configuration")
