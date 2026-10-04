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
    for key in ("MONGODB_URI", "MONGO_URI", "MONGODB_URL"):
        value = os.getenv(key, "").strip()
        if value:
            return value
    return ""


def mongodb_is_configured() -> bool:
    return bool(_uri())


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

            # Indexes are an optimization. The Atlas database user may be
            # intentionally restricted and not have createIndex privileges.
            index_specs = (
                ("scans.user_created", _db.scans, [("user_id", 1), ("created_at", -1)], {}),
                ("scans.created", _db.scans, [("created_at", -1)], {}),
                ("clones.user_status_created", _db.clones, [("user_id", 1), ("status", 1), ("created_at", -1)], {}),
                ("groups.bot_chat", _db.groups, [("bot_id", 1), ("chat_id", 1)], {"unique": True}),
                ("groups.onboarding", _db.groups, [("onboarding_active", 1), ("joined_at", 1)], {}),
                ("reports.expiry", _db.reports, "expires_at", {"expireAfterSeconds": 0}),
            )
            for label, collection, keys, options in index_specs:
                try:
                    await asyncio.to_thread(collection.create_index, keys, **options)
                except Exception:
                    log.warning(
                        "MongoDB index unavailable | index=%s | continuing without it",
                        label,
                        exc_info=True,
                    )

            # A successful ping is sufficient to mark MongoDB usable for
            # normal reads/writes, even when index creation is forbidden.
            log.info("MongoDB connected | database=%s", _database_name())
            return _db
        except Exception:
            log.exception("MongoDB connection failed; persistence will remain unavailable")
            _client = None
            _db = None
            return None


def mongodb_is_connected() -> bool:
    return _db is not None


async def ensure_mongodb() -> bool:
    """Connect/ping MongoDB eagerly at startup so persistence failures are visible."""
    return (await _get_db()) is not None


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
    owner_name: str | None = None,
    token: str,
) -> None:
    db = await _get_db()
    if db is None or clone_id is None:
        return

    cipher = _cipher()
    if cipher is None:
        log.error("Clone token persistence is disabled: CLONE_TOKEN_ENCRYPTION_KEY is missing/invalid")
        return

    try:
        now = datetime.now(timezone.utc)
        encrypted = cipher.encrypt(token.encode("utf-8")).decode("ascii")
        await asyncio.to_thread(
            db.clones.update_one,
            {"user_id": int(user_id), "clone_id": int(clone_id)},
            {
                "$set": {
                    "user_id": int(user_id),
                    "clone_id": int(clone_id),
                    "clone_username": clone_username,
                    "clone_first_name": clone_first_name,
                    "owner_name": owner_name,
                    "status": "online",
                    "last_activity": now,
                    "updated_at": now,
                    "token_encrypted": encrypted,
                },
                "$setOnInsert": {
                    "created_at": now,
                    "messages_received": 0,
                    "scans_started": 0,
                    "scans_completed": 0,
                    "scans_failed": 0,
                    "scans_cancelled": 0,
                },
                "$unset": {
                    "token_persistence": "",
                },
            },
            upsert=True,
        )
    except Exception:
        log.exception("Failed to persist clone configuration")


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
        update: dict[str, Any] = {
            "$set": {
                "status": str(status),
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
        log.exception("Failed to update clone statistics")


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
                        "owner_name": 1,
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
                .sort("created_at", -1)
                .limit(2)
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
                "owner_name": 1,
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
        log.exception("Failed to mark clone removed")


async def owner_clone_records() -> list[dict[str, Any]]:
    db = await _get_db()
    if db is None:
        return []
    try:
        return await asyncio.to_thread(
            lambda: list(
                db.clones.find(
                    {"status": {"$in": ["online", "validated", "offline"]}},
                    {
                        "user_id": 1,
                        "clone_id": 1,
                        "clone_username": 1,
                        "clone_first_name": 1,
                        "owner_name": 1,
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
        log.exception("Failed to load owner clone records")
        return []


async def load_clone_requests() -> list[dict[str, Any]]:
    db = await _get_db()
    if db is None:
        return []

    cipher = _cipher()
    if cipher is None:
        log.error("Cannot restore clones: CLONE_TOKEN_ENCRYPTION_KEY is missing/invalid")
        return []

    try:
        rows = await asyncio.to_thread(
            lambda: list(
                db.clones.find(
                    {
                        "status": {"$in": ["online", "validated"]},
                        "token_encrypted": {"$exists": True, "$ne": ""},
                    },
                    {
                        "user_id": 1,
                        "clone_id": 1,
                        "clone_username": 1,
                        "clone_first_name": 1,
                        "owner_name": 1,
                        "created_at": 1,
                        "last_activity": 1,
                        "messages_received": 1,
                        "scans_started": 1,
                        "scans_completed": 1,
                        "scans_failed": 1,
                        "scans_cancelled": 1,
                        "token_encrypted": 1,
                    },
                ).sort("created_at", 1)
            )
        )

        restored: list[dict[str, Any]] = []
        per_user: dict[int, int] = {}
        for row in rows:
            uid = int(row.get("user_id") or 0)
            if not uid:
                continue
            if per_user.get(uid, 0) >= 2:
                continue
            encrypted = row.get("token_encrypted")
            try:
                token = cipher.decrypt(str(encrypted).encode("ascii")).decode("utf-8")
            except (InvalidToken, ValueError, UnicodeDecodeError):
                log.error(
                    "Stored token could not be decrypted for clone_id=%s; skipping",
                    row.get("clone_id"),
                )
                continue

            per_user[uid] = per_user.get(uid, 0) + 1
            restored.append({
                "user_id": uid,
                "clone_id": row.get("clone_id"),
                "clone_username": row.get("clone_username"),
                "clone_first_name": row.get("clone_first_name"),
                "owner_name": row.get("owner_name"),
                "created_at": row.get("created_at"),
                "last_activity": row.get("last_activity"),
                "messages_received": row.get("messages_received", 0),
                "scans_started": row.get("scans_started", 0),
                "scans_completed": row.get("scans_completed", 0),
                "scans_failed": row.get("scans_failed", 0),
                "scans_cancelled": row.get("scans_cancelled", 0),
                "token": token,
            })

        return restored
    except Exception:
        log.exception("Failed to load saved clone configurations")
        return []


async def clear_clone_records() -> None:
    # Compatibility stub. Clone records must NOT be cleared on restart.
    return


async def user_scan_summary(user_id: int, days: int = 7) -> dict[str, Any]:
    db = await _get_db()
    if db is None:
        return {"available": False, "scans": 0, "completed": 0, "failed": 0, "cancelled": 0}

    try:
        cutoff = datetime.now(timezone.utc) - timedelta(days=int(days))
        rows = await asyncio.to_thread(
            lambda: list(
                db.scans.aggregate([
                    {"$match": {"user_id": int(user_id), "created_at": {"$gte": cutoff}}},
                    {"$group": {
                        "_id": None,
                        "scans": {"$sum": 1},
                        "completed": {"$sum": {"$cond": [{"$eq": ["$status", "completed"]}, 1, 0]}},
                        "failed": {"$sum": {"$cond": [{"$eq": ["$status", "failed"]}, 1, 0]}},
                        "cancelled": {"$sum": {"$cond": [{"$eq": ["$status", "cancelled"]}, 1, 0]}},
                    }},
                ])
            )
        )
        row = rows[0] if rows else {}
        return {
            "available": True,
            "scans": int(row.get("scans", 0) or 0),
            "completed": int(row.get("completed", 0) or 0),
            "failed": int(row.get("failed", 0) or 0),
            "cancelled": int(row.get("cancelled", 0) or 0),
        }
    except Exception:
        log.exception("Failed to load user scan summary")
        return {"available": False, "scans": 0, "completed": 0, "failed": 0, "cancelled": 0}


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
        grouped = await asyncio.to_thread(
            lambda: list(
                db.scans.aggregate([
                    {"$match": {"created_at": {"$gte": cutoff}}},
                    {"$group": {
                        "_id": "$user_id",
                        "scans": {"$sum": 1},
                        "completed": {"$sum": {"$cond": [{"$eq": ["$status", "completed"]}, 1, 0]}},
                        "failed": {"$sum": {"$cond": [{"$eq": ["$status", "failed"]}, 1, 0]}},
                        "cancelled": {"$sum": {"$cond": [{"$eq": ["$status", "cancelled"]}, 1, 0]}},
                        "last_scan": {"$max": "$created_at"},
                    }},
                ])
            )
        )

        scan_map = {
            int(row["_id"]): row
            for row in grouped
            if row.get("_id") is not None
        }

        profiles = await asyncio.to_thread(
            lambda: list(
                db.users.find(
                    {"last_seen": {"$gte": cutoff}},
                    {"username": 1, "first_name": 1, "last_name": 1, "last_seen": 1},
                ).sort("last_seen", -1)
            )
        )

        users = []
        seen = set()
        for profile in profiles:
            uid = int(profile["_id"])
            seen.add(uid)
            row = scan_map.get(uid, {})
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

        for row in grouped:
            if row.get("_id") is None or int(row["_id"]) in seen:
                continue
            users.append({
                "user_id": int(row["_id"]),
                "username": None,
                "first_name": None,
                "last_name": None,
                "last_seen": None,
                "scans": int(row.get("scans", 0) or 0),
                "completed": int(row.get("completed", 0) or 0),
                "failed": int(row.get("failed", 0) or 0),
                "cancelled": int(row.get("cancelled", 0) or 0),
                "last_scan": row.get("last_scan"),
            })

        total_scans = sum(int(row.get("scans", 0) or 0) for row in grouped)
        return {
            "available": True,
            "users": users,
            "total_users": len(users),
            "total_scans": total_scans,
            "completed": sum(int(row.get("completed", 0) or 0) for row in grouped),
            "failed": sum(int(row.get("failed", 0) or 0) for row in grouped),
            "cancelled": sum(int(row.get("cancelled", 0) or 0) for row in grouped),
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


async def owner_user_scans(
    user_id: int,
    days: int = 7,
    limit: int = 50,
    skip: int = 0,
) -> list[dict[str, Any]]:
    db = await _get_db()
    if db is None:
        return []

    try:
        cutoff = datetime.now(timezone.utc) - timedelta(days=int(days))
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
                ).sort("created_at", -1).skip(int(skip)).limit(int(limit))
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



async def record_group_chat(
    *,
    bot_id: int,
    bot_username: str,
    chat_id: int,
    title: str | None = None,
    clone_id: int = 0,
    joined_at: datetime | None = None,
    reset_onboarding: bool = False,
) -> dict[str, Any] | None:
    db = await _get_db()
    if db is None:
        return None

    try:
        now = datetime.now(timezone.utc)
        key = {"bot_id": int(bot_id), "chat_id": int(chat_id)}
        existing = await asyncio.to_thread(db.groups.find_one, key, {"last_onboarding_message_id": 1})
        update: dict[str, Any] = {
            "$set": {
                "bot_id": int(bot_id),
                "bot_username": str(bot_username).lstrip("@"),
                "chat_id": int(chat_id),
                "title": str(title or "Telegram group")[:200],
                "clone_id": int(clone_id or 0),
                "last_seen": now,
                "updated_at": now,
            },
            "$setOnInsert": {
                "created_at": now,
                "onboarding_active": False,
                "onboarding_day": 0,
                "last_onboarding_message_id": None,
            },
        }

        if joined_at is not None:
            update["$set"]["joined_at"] = joined_at
        if reset_onboarding:
            update["$set"].update({
                "joined_at": joined_at or now,
                "onboarding_active": True,
                "onboarding_day": 0,
                "last_onboarding_message_id": None,
            })

        await asyncio.to_thread(db.groups.update_one, key, update, True)
        return {
            **(existing or {}),
            "bot_id": int(bot_id),
            "bot_username": str(bot_username).lstrip("@"),
            "chat_id": int(chat_id),
            "title": str(title or "Telegram group")[:200],
            "clone_id": int(clone_id or 0),
            "joined_at": joined_at or ((existing or {}).get("joined_at") if existing else None),
        }
    except Exception:
        log.exception("Failed to store group chat | bot_id=%s | chat_id=%s", bot_id, chat_id)
        return None


async def list_group_chats() -> list[dict[str, Any]]:
    db = await _get_db()
    if db is None:
        return []

    try:
        return await asyncio.to_thread(
            lambda: list(
                db.groups.find(
                    {"chat_id": {"$exists": True}},
                    {
                        "bot_id": 1,
                        "bot_username": 1,
                        "chat_id": 1,
                        "title": 1,
                        "clone_id": 1,
                        "joined_at": 1,
                        "onboarding_day": 1,
                        "onboarding_active": 1,
                        "last_onboarding_message_id": 1,
                        "last_seen": 1,
                    },
                ).sort("last_seen", -1)
            )
        )
    except Exception:
        log.exception("Failed to load group chats")
        return []


async def update_group_onboarding(
    *,
    bot_id: int,
    chat_id: int,
    day: int,
    message_id: int | None = None,
    active: bool = True,
) -> None:
    db = await _get_db()
    if db is None:
        return

    try:
        now = datetime.now(timezone.utc)
        await asyncio.to_thread(
            db.groups.update_one,
            {"bot_id": int(bot_id), "chat_id": int(chat_id)},
            {
                "$set": {
                    "onboarding_day": int(day),
                    "last_onboarding_message_id": int(message_id) if message_id is not None else None,
                    "onboarding_active": bool(active),
                    "updated_at": now,
                }
            },
        )
    except Exception:
        log.exception(
            "Failed to update group onboarding | bot_id=%s | chat_id=%s",
            bot_id,
            chat_id,
        )


async def save_web_report(token: str, report_data: dict[str, Any], expires_at: datetime) -> None:
    db = await _get_db()
    if db is None:
        return
    try:
        await asyncio.to_thread(
            db.reports.update_one,
            {"_id": str(token)},
            {
                "$set": {
                    "report": _json_clean(report_data),
                    "expires_at": expires_at,
                    "updated_at": datetime.now(timezone.utc),
                },
                "$setOnInsert": {"created_at": datetime.now(timezone.utc)},
            },
            upsert=True,
        )
    except Exception:
        log.exception("Failed to persist web report")


async def load_web_report(token: str) -> dict[str, Any] | None:
    db = await _get_db()
    if db is None:
        return None
    try:
        row = await asyncio.to_thread(
            db.reports.find_one,
            {
                "_id": str(token),
                "expires_at": {"$gt": datetime.now(timezone.utc)},
            },
            {"report": 1, "expires_at": 1},
        )
        return row.get("report") if row else None
    except Exception:
        log.exception("Failed to load web report")
        return None


async def purge_expired_web_reports() -> None:
    db = await _get_db()
    if db is None:
        return
    try:
        await asyncio.to_thread(
            db.reports.delete_many,
            {"expires_at": {"$lte": datetime.now(timezone.utc)}},
        )
    except Exception:
        log.exception("Failed to purge expired web reports")
