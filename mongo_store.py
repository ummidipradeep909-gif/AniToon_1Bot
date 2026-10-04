from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Any

from pymongo import MongoClient
from cryptography.fernet import Fernet, InvalidToken

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


def _cipher() -> Fernet | None:
    key = os.getenv("CLONE_TOKEN_ENCRYPTION_KEY", "").strip()
    if not key:
        return None
    try:
        return Fernet(key.encode("ascii"))
    except Exception:
        log.exception("Invalid CLONE_TOKEN_ENCRYPTION_KEY")
        return None


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
            await asyncio.to_thread(
                _db.scans.create_index([("user_id", 1), ("created_at", -1)])
            )
            await asyncio.to_thread(
                _db.scans.create_index([("created_at", -1)])
            )
            await asyncio.to_thread(
                _db.clones.create_index([("user_id", 1), ("status", 1), ("created_at", -1)])
            )
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
    source_bot: str | None = None,
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
            "source_bot": str(source_bot) if source_bot else None,
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
        log.warning("MongoDB is not configured; clone data will remain in memory only")
        return

    if clone_id is None:
        return

    try:
        cipher = _cipher()
        now = datetime.now(timezone.utc)
        set_fields = {
            "user_id": int(user_id),
            "clone_id": int(clone_id),
            "clone_username": clone_username,
            "clone_first_name": clone_first_name,
            "status": "online",
            "last_activity": now,
            "updated_at": now,
        }
        if cipher:
            set_fields["token_encrypted"] = cipher.encrypt(token.encode("utf-8")).decode("ascii")
        else:
            set_fields["token_persistence"] = (
                "disabled_until_CLONE_TOKEN_ENCRYPTION_KEY_is_configured"
            )

        await asyncio.to_thread(
            db.clones.update_one,
            {"user_id": int(user_id), "clone_id": int(clone_id)},
            {
                "$set": set_fields,
                "$setOnInsert": {
                    "created_at": now,
                    "messages_received": 0,
                    "scans_started": 0,
                    "scans_completed": 0,
                    "scans_failed": 0,
                    "scans_cancelled": 0,
                },
            },
            upsert=True,
        )
    except Exception:
        log.exception("Failed to store clone configuration")

async def update_clone_stats(
    *,
    clone_id: int,
    messages_received: int = 0,
    scans_started: int = 0,
    scans_completed: int = 0,
    scans_failed: int = 0,
    scans_cancelled: int = 0,
    status: str = "online",
) -> None:
    db = await _get_db()
    if db is None:
        return

    increments = {
        "messages_received": int(messages_received),
        "scans_started": int(scans_started),
        "scans_completed": int(scans_completed),
        "scans_failed": int(scans_failed),
        "scans_cancelled": int(scans_cancelled),
    }
    increments = {key: value for key, value in increments.items() if value}
    try:
        update = {
            "$set": {
                "status": status,
                "last_activity": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
            }
        }
        if increments:
            update["$inc"] = increments
        await asyncio.to_thread(
            db.clones.update_one,
            {"clone_id": int(clone_id), "status": {"$ne": "removed"}},
            update,
        )
    except Exception:
        log.exception("Failed to update clone stats")


async def list_user_clones(user_id: int) -> list[dict[str, Any]]:
    db = await _get_db()
    if db is None:
        return []

    try:
        return await asyncio.to_thread(
            lambda: list(
                db.clones.find(
                    {
                        "user_id": int(user_id),
                        "status": {"$in": ["online", "validated"]},
                    },
                    {
                        "user_id": 1,
                        "clone_id": 1,
                        "clone_username": 1,
                        "clone_first_name": 1,
                        "status": 1,
                        "created_at": 1,
                        "last_activity": 1,
                        "messages_received": 1,
                        "scans_started": 1,
                        "scans_completed": 1,
                        "scans_failed": 1,
                        "scans_cancelled": 1,
                    },
                ).sort("created_at", -1)
            )
        )
    except Exception:
        log.exception("Failed to list user's clone bots")
        return []


async def get_user_clone(user_id: int, clone_id: int) -> dict[str, Any] | None:
    db = await _get_db()
    if db is None:
        return None

    try:
        return await asyncio.to_thread(
            db.clones.find_one,
            {
                "user_id": int(user_id),
                "clone_id": int(clone_id),
                "status": {"$in": ["online", "validated"]},
            },
            {
                "user_id": 1,
                "clone_id": 1,
                "clone_username": 1,
                "clone_first_name": 1,
                "status": 1,
                "created_at": 1,
                "last_activity": 1,
                "messages_received": 1,
                "scans_started": 1,
                "scans_completed": 1,
                "scans_failed": 1,
                "scans_cancelled": 1,
            },
        )
    except Exception:
        log.exception("Failed to load clone")
        return None


async def mark_clone_removed(user_id: int, clone_id: int) -> None:
    db = await _get_db()
    if db is None:
        return

    try:
        now = datetime.now(timezone.utc)
        await asyncio.to_thread(
            db.clones.update_one,
            {"user_id": int(user_id), "clone_id": int(clone_id)},
            {
                "$set": {
                    "status": "removed",
                    "removed_at": now,
                    "updated_at": now,
                },
                "$unset": {
                    "token_encrypted": "",
                    "token_persistence": "",
                },
            },
        )
    except Exception:
        log.exception("Failed to remove clone from persistent storage")



async def load_clone_requests() -> list[dict[str, Any]]:
    """Load active clone tokens and persisted statistics for restart recovery."""
    db = await _get_db()
    if db is None:
        return []

    cipher = _cipher()
    if cipher is None:
        return []

    try:
        rows = await asyncio.to_thread(
            lambda: list(
                db.clones.find(
                    {
                        "status": {"$in": ["validated", "online"]},
                        "token_encrypted": {"$exists": True, "$ne": ""},
                    },
                    {
                        "user_id": 1,
                        "clone_id": 1,
                        "clone_username": 1,
                        "clone_first_name": 1,
                        "created_at": 1,
                        "last_activity": 1,
                        "messages_received": 1,
                        "scans_started": 1,
                        "scans_completed": 1,
                        "scans_failed": 1,
                        "scans_cancelled": 1,
                        "token_encrypted": 1,
                    },
                )
            )
        )

        restored = []
        for row in rows:
            encrypted = row.get("token_encrypted")
            if not encrypted:
                continue
            try:
                token = cipher.decrypt(str(encrypted).encode("ascii")).decode("utf-8")
            except (InvalidToken, ValueError, UnicodeDecodeError):
                log.exception(
                    "Could not decrypt saved clone token for clone_id=%s",
                    row.get("clone_id"),
                )
                continue

            restored.append(
                {
                    "user_id": row.get("user_id"),
                    "clone_id": row.get("clone_id"),
                    "clone_username": row.get("clone_username"),
                    "clone_first_name": row.get("clone_first_name"),
                    "created_at": row.get("created_at"),
                    "last_activity": row.get("last_activity"),
                    "messages_received": row.get("messages_received", 0),
                    "scans_started": row.get("scans_started", 0),
                    "scans_completed": row.get("scans_completed", 0),
                    "scans_failed": row.get("scans_failed", 0),
                    "scans_cancelled": row.get("scans_cancelled", 0),
                    "token": token,
                }
            )

        log.info("Loaded %s saved clone configuration(s)", len(restored))
        return restored
    except Exception:
        log.exception("Failed to load saved clone configurations")
        return []



async def owner_7day_summary(days: int = 7) -> dict[str, Any]:
    db = await _get_db()
    if db is None:
        return {
            "available": False,
            "users": [],
            "total_users": 0,
            "total_scans": 0,
            "completed": 0,
            "failed": 0,
            "cancelled": 0,
        }

    try:
        cutoff = datetime.now(timezone.utc) - timedelta(days=int(days))
        pipeline = [
            {"$match": {"created_at": {"$gte": cutoff}}},
            {"$group": {
                "_id": "$user_id",
                "scans": {"$sum": 1},
                "completed": {
                    "$sum": {"$cond": [{"$eq": ["$status", "completed"]}, 1, 0]}
                },
                "failed": {
                    "$sum": {"$cond": [{"$eq": ["$status", "failed"]}, 1, 0]}
                },
                "cancelled": {
                    "$sum": {"$cond": [{"$eq": ["$status", "cancelled"]}, 1, 0]}
                },
                "last_scan": {"$max": "$created_at"},
            }},
            {"$sort": {"last_scan": -1}},
        ]

        grouped = await asyncio.to_thread(lambda: list(db.scans.aggregate(pipeline)))
        total_scans = sum(int(row.get("scans", 0) or 0) for row in grouped)
        completed = sum(int(row.get("completed", 0) or 0) for row in grouped)
        failed = sum(int(row.get("failed", 0) or 0) for row in grouped)
        cancelled = sum(int(row.get("cancelled", 0) or 0) for row in grouped)

        users = []
        user_ids = [int(row["_id"]) for row in grouped if row.get("_id") is not None]
        profiles = {}
        if user_ids:
            profile_rows = await asyncio.to_thread(
                lambda: list(
                    db.users.find(
                        {"_id": {"$in": user_ids}},
                        {"username": 1, "first_name": 1, "last_name": 1, "last_seen": 1},
                    )
                )
            )
            profiles = {int(row["_id"]): row for row in profile_rows}

        for row in grouped:
            uid = row.get("_id")
            if uid is None:
                continue
            uid = int(uid)
            profile = profiles.get(uid, {})
            users.append({
                "user_id": uid,
                "username": profile.get("username"),
                "first_name": profile.get("first_name"),
                "last_name": profile.get("last_name"),
                "last_seen": profile.get("last_seen"),
                "scans": int(row.get("scans", 0) or 0),
                "completed": int(row.get("completed", 0) or 0),
                "failed": int(row.get("failed", 0) or 0),
                "cancelled": int(row.get("cancelled", 0) or 0),
                "last_scan": row.get("last_scan"),
            })

        return {
            "available": True,
            "users": users,
            "total_users": len(users),
            "total_scans": total_scans,
            "completed": completed,
            "failed": failed,
            "cancelled": cancelled,
        }
    except Exception:
        log.exception("Failed to build owner 7-day summary")
        return {
            "available": False,
            "users": [],
            "total_users": 0,
            "total_scans": 0,
            "completed": 0,
            "failed": 0,
            "cancelled": 0,
        }


async def owner_user_scans(user_id: int, days: int = 7, limit: int = 50) -> list[dict[str, Any]]:
    db = await _get_db()
    if db is None:
        return []

    try:
        cutoff = datetime.now(timezone.utc) - __import__("datetime").timedelta(days=int(days))
        return await asyncio.to_thread(
            lambda: list(
                db.scans.find(
                    {
                        "user_id": int(user_id),
                        "created_at": {"$gte": cutoff},
                    },
                    {
                        "filename": 1,
                        "status": 1,
                        "created_at": 1,
                        "chat_id": 1,
                        "message_id": 1,
                        "source_bot": 1,
                    },
                ).sort("created_at", -1).limit(int(limit))
            )
        )
    except Exception:
        log.exception("Failed to load owner user scans")
        return []


async def owner_recent_users(days: int = 7, limit: int = 100) -> list[dict[str, Any]]:
    db = await _get_db()
    if db is None:
        return []

    try:
        cutoff = datetime.now(timezone.utc) - __import__("datetime").timedelta(days=int(days))
        return await asyncio.to_thread(
            lambda: list(
                db.users.find(
                    {"last_seen": {"$gte": cutoff}},
                    {"username": 1, "first_name": 1, "last_name": 1, "last_seen": 1},
                ).sort("last_seen", -1).limit(int(limit))
            )
        )
    except Exception:
        log.exception("Failed to load owner recent users")
        return []
