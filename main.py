import telebot
import subprocess
import os
import zipfile
import tempfile
import shutil
from telebot import types
import time
from datetime import datetime, timedelta
import psutil
import json
import logging
import signal
import threading
import re
import sys
import atexit
import requests
import hashlib
import uuid
import secrets

# --- MongoDB imports ---
import certifi
from pymongo import MongoClient, ASCENDING
from pymongo.errors import (
    ConnectionFailure, ServerSelectionTimeoutError,
    DuplicateKeyError, PyMongoError
)

# --- Flask Keep Alive + File Serve ---
from flask import Flask, jsonify, request as flask_request, abort
from threading import Thread

app = Flask('')

# --- Logging Setup (must be first — worker infra uses logger) ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# 🗄️ MONGODB DATABASE LAYER — Merged from db.py
# All database operations directly in Host.py
# MongoDB Atlas support fully working
# ============================================================================

# ✅ FIXED: MongoDB URI — set MONGO_URI env variable on your server, OR paste correct URI below.
# How to set env var:  export MONGO_URI="mongodb+srv://USERNAME:PASSWORD@cluster.mongodb.net/?appName=AppName"
# Atlas steps to get correct URI:
#   1. Go to Atlas → your cluster → Connect → Drivers
#   2. Copy the connection string
#   3. Replace <username> and <password> with your DB user credentials (NOT your Atlas login)
#   4. Make sure the DB user exists under Security → Database Access
#   5. Make sure your server IP is whitelisted under Security → Network Access (or use 0.0.0.0/0)

MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb+srv://botuser:Botpass12345@yashhosting.cs9sj0l.mongodb.net/?appName=Yashhosting"
)
DB_NAME   = "hosting_bot"

# ✅ Startup credential sanity check — fail fast with clear message
if "YOUR_DB_USERNAME" in MONGO_URI or "YOUR_DB_PASSWORD" in MONGO_URI:
    print("=" * 60)
    print("❌ MONGO_URI is not configured!")
    print("Set the MONGO_URI environment variable on your server:")
    print('   export MONGO_URI="mongodb+srv://user:pass@cluster.mongodb.net/?appName=App"')
    print("OR edit line 48 in this file and paste your correct URI.")
    print("=" * 60)
    sys.exit(1)

# ============================================================================
# 🔌 CONNECTION MANAGER — Auto-reconnect support
# ============================================================================

_client: MongoClient | None = None
_db = None
_connect_lock = threading.Lock()


def _get_client() -> MongoClient:
    """Return existing MongoDB client or reconnect if disconnected."""
    global _client
    with _connect_lock:
        if _client is None:
            _client = _connect()
        else:
            # Ping to check if connection is alive
            try:
                _client.admin.command("ping")
            except (ConnectionFailure, ServerSelectionTimeoutError):
                logger.warning("⚠️ MongoDB disconnected. Reconnecting...")
                _client = _connect()
    return _client


def _connect() -> MongoClient:
    """Create a new MongoClient with retry logic."""
    retries = 5
    delay   = 3
    for attempt in range(1, retries + 1):
        try:
            import certifi as _certifi
            client = MongoClient(
                MONGO_URI,
                serverSelectionTimeoutMS=8000,
                connectTimeoutMS=8000,
                socketTimeoutMS=30000,
                retryWrites=True,
                retryReads=True,
                tls=True,
                tlsCAFile=_certifi.where(),
                tlsAllowInvalidCertificates=False,
            )
            # Verify connection
            client.admin.command("ping")
            logger.info(f"✅ MongoDB Atlas connected (attempt {attempt})")
            return client
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            err_str = str(e).lower()
            # ✅ Auth errors should NOT be retried — wrong credentials won't fix themselves
            if "bad auth" in err_str or "authentication failed" in err_str or "auth" in err_str:
                logger.critical(
                    "❌ MongoDB AUTHENTICATION FAILED!\n"
                    "Your username or password in MONGO_URI is wrong.\n"
                    "Fix steps:\n"
                    "  1. Go to MongoDB Atlas → Security → Database Access\n"
                    "  2. Check that the user exists and note exact username\n"
                    "  3. Edit/reset the password\n"
                    "  4. Update MONGO_URI in this script or set the MONGO_URI env variable\n"
                    "  5. Also check Network Access → 0.0.0.0/0 is whitelisted\n"
                    f"  Raw error: {e}"
                )
                raise RuntimeError(
                    "MongoDB authentication failed — wrong username/password. "
                    "Fix MONGO_URI and restart."
                ) from e
            logger.error(f"❌ MongoDB connect attempt {attempt}/{retries}: {e}")
            if attempt < retries:
                time.sleep(delay * attempt)
    raise RuntimeError("❌ Could not connect to MongoDB Atlas after multiple retries.")


def get_db():
    """Return the hosting_bot database handle."""
    global _db
    client = _get_client()
    if _db is None:
        _db = client[DB_NAME]
    return _db


# ============================================================================
# 🏗️ INIT DB — Create collections + indexes on startup
# ============================================================================

def init_db():
    """
    Called once at bot startup.
    Creates all collections and indexes automatically.
    Collections are auto-created by MongoDB on first insert,
    but indexes must be created explicitly.
    """
    try:
        db = get_db()
        logger.info("🗄️ Initializing MongoDB collections and indexes...")

        # --- users collection ---
        # Fields: user_id (unique), first_name, username, joined_at, is_active
        db.users.create_index([("user_id", ASCENDING)], unique=True)
        db.users.create_index([("is_active", ASCENDING)])

        # --- subscriptions collection ---
        # Fields: user_id (unique), expiry (datetime)
        db.subscriptions.create_index([("user_id", ASCENDING)], unique=True)
        db.subscriptions.create_index([("expiry", ASCENDING)])

        # --- user_files collection ---
        # Fields: user_id, file_name, file_type
        db.user_files.create_index(
            [("user_id", ASCENDING), ("file_name", ASCENDING)],
            unique=True
        )

        # --- admins collection ---
        # Fields: user_id (unique)
        db.admins.create_index([("user_id", ASCENDING)], unique=True)

        # --- storage_files collection ---
        # Fields: user_id, file_name, telegram_file_id, share_token (unique), ...
        db.storage_files.create_index([("share_token", ASCENDING)], unique=True)
        db.storage_files.create_index([("user_id", ASCENDING)])
        db.storage_files.create_index([("expires_at", ASCENDING)])

        # --- donate_files collection ---
        db.donate_files.create_index([("donor_user_id", ASCENDING)])

        # --- settings collection (hosting_status, bot settings etc.) ---
        db.settings.create_index([("key", ASCENDING)], unique=True)

        # --- active_users collection (simple set of user_ids) ---
        db.active_users.create_index([("user_id", ASCENDING)], unique=True)

        # --- Ensure default settings exist ---
        db.settings.update_one(
            {"key": "hosting"},
            {"$setOnInsert": {"key": "hosting", "enabled": True}},
            upsert=True
        )

        logger.info("✅ MongoDB indexes and defaults created successfully.")

    except Exception as e:
        logger.error(f"❌ MongoDB init_db error: {e}", exc_info=True)
        raise


# ============================================================================
# 👤 USER HELPER FUNCTIONS
# ============================================================================

def add_user(user_id: int, first_name: str = "", username: str = "") -> bool:
    """
    Add a new user to the users collection.
    Uses upsert — safe to call multiple times.
    Returns True if new user inserted, False if already existed.
    """
    try:
        db = get_db()
        result = db.users.update_one(
            {"user_id": user_id},
            {
                "$setOnInsert": {
                    "user_id":    user_id,
                    "first_name": first_name,
                    "username":   username,
                    "joined_at":  datetime.now().isoformat(),
                    "is_active":  True,
                }
            },
            upsert=True
        )
        # Also ensure they're in active_users
        db.active_users.update_one(
            {"user_id": user_id},
            {"$setOnInsert": {"user_id": user_id}},
            upsert=True
        )
        return result.upserted_id is not None  # True = new user
    except Exception as e:
        logger.error(f"❌ add_user error ({user_id}): {e}", exc_info=True)
        return False


def get_user(user_id: int) -> dict | None:
    """Get a single user document by user_id."""
    try:
        db = get_db()
        return db.users.find_one({"user_id": user_id}, {"_id": 0})
    except Exception as e:
        logger.error(f"❌ get_user error ({user_id}): {e}")
        return None


def update_user(user_id: int, fields: dict) -> bool:
    """Update fields of an existing user."""
    try:
        db = get_db()
        db.users.update_one({"user_id": user_id}, {"$set": fields})
        return True
    except Exception as e:
        logger.error(f"❌ update_user error ({user_id}): {e}")
        return False


def delete_user(user_id: int) -> bool:
    """Delete a user from all collections."""
    try:
        db = get_db()
        db.users.delete_one({"user_id": user_id})
        db.active_users.delete_one({"user_id": user_id})
        db.subscriptions.delete_one({"user_id": user_id})
        db.user_files.delete_many({"user_id": user_id})
        logger.info(f"🗑️ User {user_id} deleted from all collections.")
        return True
    except Exception as e:
        logger.error(f"❌ delete_user error ({user_id}): {e}")
        return False


def get_all_users() -> list[dict]:
    """Return list of all user documents."""
    try:
        db = get_db()
        return list(db.users.find({}, {"_id": 0}))
    except Exception as e:
        logger.error(f"❌ get_all_users error: {e}")
        return []


# ============================================================================
# 📋 ACTIVE USERS
# ============================================================================

def load_active_users() -> set:
    """Load all active user IDs from MongoDB into a Python set."""
    try:
        db = get_db()
        docs = db.active_users.find({}, {"user_id": 1, "_id": 0})
        return set(doc["user_id"] for doc in docs)
    except Exception as e:
        logger.error(f"❌ load_active_users error: {e}")
        return set()


def add_active_user_db(user_id: int):
    """Add user to active_users collection (upsert — no duplicates)."""
    try:
        db = get_db()
        db.active_users.update_one(
            {"user_id": user_id},
            {"$setOnInsert": {"user_id": user_id}},
            upsert=True
        )
    except Exception as e:
        logger.error(f"❌ add_active_user_db error ({user_id}): {e}")


def get_all_active_user_ids() -> list:
    """Get all active user IDs (for broadcast etc.)"""
    try:
        db = get_db()
        docs = db.active_users.find({}, {"user_id": 1, "_id": 0})
        return [doc["user_id"] for doc in docs]
    except Exception as e:
        logger.error(f"❌ get_all_active_user_ids error: {e}")
        return []


# ============================================================================
# 📁 USER FILES
# ============================================================================

def save_user_file_db(user_id: int, file_name: str, file_type: str = "py"):
    """Save or update a user's file record in MongoDB."""
    try:
        db = get_db()
        db.user_files.update_one(
            {"user_id": user_id, "file_name": file_name},
            {"$set": {
                "user_id":   user_id,
                "file_name": file_name,
                "file_type": file_type,
                "saved_at":  datetime.now().isoformat(),
            }},
            upsert=True
        )
        logger.info(f"💾 File '{file_name}' ({file_type}) saved for user {user_id}")
    except Exception as e:
        logger.error(f"❌ save_user_file_db error ({user_id}, {file_name}): {e}")


def remove_user_file_db(user_id: int, file_name: str):
    """Remove a file record from MongoDB."""
    try:
        db = get_db()
        db.user_files.delete_one({"user_id": user_id, "file_name": file_name})
        logger.info(f"🗑️ File '{file_name}' removed for user {user_id}")
    except Exception as e:
        logger.error(f"❌ remove_user_file_db error ({user_id}, {file_name}): {e}")


def load_all_user_files() -> dict:
    """
    Load all user files from MongoDB.
    Returns: {user_id: [(file_name, file_type), ...]}
    """
    try:
        db = get_db()
        docs = db.user_files.find({}, {"_id": 0})
        result = {}
        for doc in docs:
            uid  = doc["user_id"]
            fname = doc["file_name"]
            ftype = doc.get("file_type", "py")
            if uid not in result:
                result[uid] = []
            result[uid].append((fname, ftype))
        return result
    except Exception as e:
        logger.error(f"❌ load_all_user_files error: {e}")
        return {}


# ============================================================================
# 💳 SUBSCRIPTIONS
# ============================================================================

def save_subscription_db(user_id: int, expiry: datetime):
    """Save or update subscription for a user."""
    try:
        db = get_db()
        db.subscriptions.update_one(
            {"user_id": user_id},
            {"$set": {
                "user_id": user_id,
                "expiry":  expiry.isoformat(),
            }},
            upsert=True
        )
        logger.info(f"✅ Subscription saved for {user_id}, expiry={expiry.isoformat()}")
    except Exception as e:
        logger.error(f"❌ save_subscription_db error ({user_id}): {e}")


def remove_subscription_db(user_id: int):
    """Remove a subscription."""
    try:
        db = get_db()
        db.subscriptions.delete_one({"user_id": user_id})
        logger.info(f"🗑️ Subscription removed for {user_id}")
    except Exception as e:
        logger.error(f"❌ remove_subscription_db error ({user_id}): {e}")


def load_all_subscriptions() -> dict:
    """
    Load all subscriptions from MongoDB.
    Returns: {user_id: {'expiry': datetime}}
    """
    try:
        db = get_db()
        docs = db.subscriptions.find({}, {"_id": 0})
        result = {}
        for doc in docs:
            uid = doc["user_id"]
            try:
                expiry_dt = datetime.fromisoformat(doc["expiry"])
                result[uid] = {"expiry": expiry_dt}
            except Exception:
                logger.warning(f"⚠️ Bad expiry for user {uid}: {doc.get('expiry')}")
        return result
    except Exception as e:
        logger.error(f"❌ load_all_subscriptions error: {e}")
        return {}


# ============================================================================
# 🛡️ ADMINS
# ============================================================================

def add_admin_db(admin_id: int):
    """Add an admin (upsert — safe)."""
    try:
        db = get_db()
        db.admins.update_one(
            {"user_id": admin_id},
            {"$setOnInsert": {"user_id": admin_id}},
            upsert=True
        )
        logger.info(f"✅ Admin {admin_id} added to DB")
    except Exception as e:
        logger.error(f"❌ add_admin_db error ({admin_id}): {e}")


def remove_admin_db(admin_id: int) -> bool:
    """Remove an admin. Returns True if removed."""
    try:
        db = get_db()
        result = db.admins.delete_one({"user_id": admin_id})
        removed = result.deleted_count > 0
        if removed:
            logger.info(f"🗑️ Admin {admin_id} removed from DB")
        return removed
    except Exception as e:
        logger.error(f"❌ remove_admin_db error ({admin_id}): {e}")
        return False


def load_all_admins() -> set:
    """Load all admin IDs from DB."""
    try:
        db = get_db()
        docs = db.admins.find({}, {"user_id": 1, "_id": 0})
        return set(doc["user_id"] for doc in docs)
    except Exception as e:
        logger.error(f"❌ load_all_admins error: {e}")
        return set()


def ensure_owner_admin(owner_id: int, admin_id: int):
    """Make sure owner and default admin are always in admins collection."""
    add_admin_db(owner_id)
    if admin_id != owner_id:
        add_admin_db(admin_id)


# ============================================================================
# ⚙️ SETTINGS (hosting_status, etc.)
# ============================================================================

def get_setting(key: str, default=None):
    """Get a setting value by key."""
    try:
        db = get_db()
        doc = db.settings.find_one({"key": key}, {"_id": 0})
        if doc:
            return doc.get("value", doc.get("enabled", default))
        return default
    except Exception as e:
        logger.error(f"❌ get_setting error ({key}): {e}")
        return default


def set_setting(key: str, value):
    """Set a setting value by key."""
    try:
        db = get_db()
        db.settings.update_one(
            {"key": key},
            {"$set": {"key": key, "value": value}},
            upsert=True
        )
    except Exception as e:
        logger.error(f"❌ set_setting error ({key}): {e}")


def is_hosting_enabled_db() -> bool:
    """Get hosting enabled status from MongoDB."""
    try:
        db = get_db()
        doc = db.settings.find_one({"key": "hosting"}, {"_id": 0})
        if doc:
            return bool(doc.get("enabled", True))
        # Default: create and return True
        db.settings.update_one(
            {"key": "hosting"},
            {"$setOnInsert": {"key": "hosting", "enabled": True}},
            upsert=True
        )
        return True
    except Exception as e:
        logger.error(f"❌ is_hosting_enabled_db error: {e}")
        return True  # Default enabled on error


def set_hosting_status_db(enabled: bool) -> bool:
    """Update hosting status in MongoDB."""
    try:
        db = get_db()
        db.settings.update_one(
            {"key": "hosting"},
            {"$set": {"key": "hosting", "enabled": enabled}},
            upsert=True
        )
        logger.info(f"🔌 Hosting status → {'ENABLED ✅' if enabled else 'DISABLED ❌'}")
        return True
    except Exception as e:
        logger.error(f"❌ set_hosting_status_db error: {e}")
        return False


# ============================================================================
# 🗄️ STORAGE FILES (Personal file storage — Telegram file_id based)
# ============================================================================

def save_storage_file_db(user_id: int, file_name: str, telegram_file_id: str,
                          file_type: str, file_size: int, share_token: str,
                          expires_at: datetime | None = None):
    """Save a storage file record — file lives on Telegram's servers (file_id only)."""
    try:
        db = get_db()
        doc = {
            "user_id":          user_id,
            "file_name":        file_name,
            "telegram_file_id": telegram_file_id,
            "file_type":        file_type,
            "file_size":        file_size,
            "share_token":      share_token,
            "uploaded_at":      datetime.now().isoformat(),
            "expires_at":       expires_at.isoformat() if expires_at else None,
            "download_count":   0,
        }
        db.storage_files.insert_one(doc)
        logger.info(f"📦 Storage file '{file_name}' saved for user {user_id}, token={share_token}")
    except DuplicateKeyError:
        logger.error(f"❌ share_token collision for '{file_name}'")
    except Exception as e:
        logger.error(f"❌ save_storage_file_db error: {e}")


def get_user_storage_files(user_id: int) -> list:
    """
    Get all storage files for a user.
    Returns list of tuples: (storage_id, file_name, file_size, share_token, uploaded_at, expires_at)
    Uses _id as storage_id (str).
    """
    try:
        db = get_db()
        docs = db.storage_files.find(
            {"user_id": user_id},
            {"_id": 1, "file_name": 1, "file_size": 1, "share_token": 1,
             "uploaded_at": 1, "expires_at": 1}
        ).sort("uploaded_at", -1)
        result = []
        for doc in docs:
            result.append((
                str(doc["_id"]),
                doc.get("file_name", ""),
                doc.get("file_size", 0),
                doc.get("share_token", ""),
                doc.get("uploaded_at", ""),
                doc.get("expires_at"),
            ))
        return result
    except Exception as e:
        logger.error(f"❌ get_user_storage_files error ({user_id}): {e}")
        return []


def get_storage_file_by_token(token: str) -> tuple | None:
    """
    Get storage file by share_token.
    Returns: (storage_id, user_id, file_name, telegram_file_id, file_type,
               file_size, share_token, uploaded_at, expires_at)
    """
    try:
        db = get_db()
        doc = db.storage_files.find_one({"share_token": token})
        if not doc:
            return None
        return (
            str(doc["_id"]),
            doc.get("user_id"),
            doc.get("file_name", ""),
            doc.get("telegram_file_id", ""),
            doc.get("file_type", "document"),
            doc.get("file_size", 0),
            doc.get("share_token", ""),
            doc.get("uploaded_at", ""),
            doc.get("expires_at"),
        )
    except Exception as e:
        logger.error(f"❌ get_storage_file_by_token error ({token}): {e}")
        return None


def get_storage_file_by_id(storage_id: str) -> dict | None:
    """Get storage file document by its _id string."""
    try:
        from bson import ObjectId
        db = get_db()
        return db.storage_files.find_one({"_id": ObjectId(storage_id)})
    except Exception as e:
        logger.error(f"❌ get_storage_file_by_id error ({storage_id}): {e}")
        return None


def delete_storage_file_db(storage_id: str, requesting_user_id: int,
                            owner_id: int) -> tuple:
    """
    Delete a storage file record.
    Returns: (success: bool, result: str)
    """
    try:
        from bson import ObjectId
        db = get_db()
        doc = db.storage_files.find_one({"_id": ObjectId(storage_id)})
        if not doc:
            return False, "File not found"
        owner_uid = doc.get("user_id")
        fname     = doc.get("file_name", "")
        if requesting_user_id != owner_uid and requesting_user_id != owner_id:
            return False, "Permission denied"
        db.storage_files.delete_one({"_id": ObjectId(storage_id)})
        logger.info(f"🗑️ Storage file '{fname}' (id={storage_id}) deleted by user {requesting_user_id}")
        return True, fname
    except Exception as e:
        logger.error(f"❌ delete_storage_file_db error ({storage_id}): {e}", exc_info=True)
        return False, str(e)


def regenerate_storage_link_db(storage_id: str, expires_at: datetime | None,
                                new_token: str) -> bool:
    """Update share_token and expires_at for a storage file."""
    try:
        from bson import ObjectId
        db = get_db()
        db.storage_files.update_one(
            {"_id": ObjectId(storage_id)},
            {"$set": {
                "share_token": new_token,
                "expires_at":  expires_at.isoformat() if expires_at else None,
            }}
        )
        return True
    except Exception as e:
        logger.error(f"❌ regenerate_storage_link_db error ({storage_id}): {e}")
        return False


def increment_download_count_by_token(token: str):
    """Increment download_count for a storage file by share_token."""
    try:
        db = get_db()
        db.storage_files.update_one(
            {"share_token": token},
            {"$inc": {"download_count": 1}}
        )
    except Exception as e:
        logger.error(f"❌ increment_download_count_by_token error: {e}")


def increment_download_count_by_id(storage_id: str):
    """Increment download_count for a storage file by storage_id."""
    try:
        from bson import ObjectId
        db = get_db()
        db.storage_files.update_one(
            {"_id": ObjectId(storage_id)},
            {"$inc": {"download_count": 1}}
        )
    except Exception as e:
        logger.error(f"❌ increment_download_count_by_id error: {e}")


def get_all_storage_files_all_users() -> list:
    """
    Owner: get all storage files across all users.
    Returns list of tuples: (storage_id, user_id, file_name, file_size,
                              share_token, uploaded_at, expires_at)
    """
    try:
        db = get_db()
        docs = db.storage_files.find(
            {},
            {"_id": 1, "user_id": 1, "file_name": 1, "file_size": 1,
             "share_token": 1, "uploaded_at": 1, "expires_at": 1}
        ).sort([("user_id", ASCENDING), ("uploaded_at", -1)])
        result = []
        for doc in docs:
            result.append((
                str(doc["_id"]),
                doc.get("user_id"),
                doc.get("file_name", ""),
                doc.get("file_size", 0),
                doc.get("share_token", ""),
                doc.get("uploaded_at", ""),
                doc.get("expires_at"),
            ))
        return result
    except Exception as e:
        logger.error(f"❌ get_all_storage_files_all_users error: {e}")
        return []


def cleanup_expired_storage_files_db():
    """Delete expired storage file records from MongoDB."""
    try:
        db = get_db()
        now_str = datetime.now().isoformat()
        # Find docs where expires_at is not null and has expired
        result = db.storage_files.delete_many({
            "expires_at": {"$ne": None, "$lt": now_str}
        })
        if result.deleted_count:
            logger.info(f"🧹 Cleaned up {result.deleted_count} expired storage file records.")
    except Exception as e:
        logger.error(f"❌ cleanup_expired_storage_files_db error: {e}")


# ============================================================================
# 🎁 DONATE FILES
# ============================================================================

def save_donate_file_db(donor_user_id: int, donor_name: str, file_name: str,
                         telegram_file_id: str, file_size: int):
    """Save a donated file record to MongoDB."""
    try:
        db = get_db()
        doc = {
            "donor_user_id":    donor_user_id,
            "donor_name":       donor_name,
            "file_name":        file_name,
            "telegram_file_id": telegram_file_id,
            "file_size":        file_size,
            "donated_at":       datetime.now().isoformat(),
        }
        db.donate_files.insert_one(doc)
        logger.info(f"🎁 Donate file '{file_name}' saved from user {donor_user_id}")
    except Exception as e:
        logger.error(f"❌ save_donate_file_db error: {e}")


def get_all_donate_files() -> list:
    """Get all donated files."""
    try:
        db = get_db()
        docs = db.donate_files.find({})  # ✅ FIX: include _id so donate_dl_ callback works
        return list(docs)
    except Exception as e:
        logger.error(f"❌ get_all_donate_files error: {e}")
        return []


# ============================================================================
# 📊 STATS HELPERS
# ============================================================================

def count_active_users() -> int:
    """Count of all active users in DB."""
    try:
        return get_db().active_users.count_documents({})
    except Exception:
        return 0


def count_storage_files() -> int:
    """Count of all storage file records."""
    try:
        return get_db().storage_files.count_documents({})
    except Exception:
        return 0


def count_donate_files() -> int:
    """Count of all donated files."""
    try:
        return get_db().donate_files.count_documents({})
    except Exception:
        return 0


# ============================================================================
# 🔄 LOAD ALL DATA INTO MEMORY (called at startup)
# ============================================================================

def load_all_data() -> dict:
    """
    Load all persistent data from MongoDB into memory dicts.
    Returns a dict with keys:
      active_users, user_subscriptions, user_files, admin_ids
    """
    logger.info("📥 Loading all data from MongoDB into memory...")
    data = {
        "active_users":      load_active_users(),
        "user_subscriptions": load_all_subscriptions(),
        "user_files":        load_all_user_files(),
        "admin_ids":         load_all_admins(),
    }
    logger.info(
        f"✅ Data loaded — "
        f"Users: {len(data['active_users'])}, "
        f"Subs: {len(data['user_subscriptions'])}, "
        f"Files: {sum(len(v) for v in data['user_files'].values())}, "
        f"Admins: {len(data['admin_ids'])}"
    )
    return data


# ============================================================================
# 🩺 CONNECTION HEALTH CHECK (for background thread)
# ============================================================================

def ping_db() -> bool:
    """Ping MongoDB. Returns True if alive."""
    try:
        _get_client().admin.command("ping")
        return True
    except Exception:
        return False


# ============================================================================
# END MONGODB DATABASE LAYER
# ============================================================================

# ============================================================================
# ⚙️ CONFIGURATION — must be before bot init and worker infra
# ============================================================================
TOKEN = '8815106373:AAF2mRjIvCs1ZfMe1LKMn_fuLh2MeaY3lSk' # Replace with your actual token
OWNER_ID = 7613646047  # Replace with your Owner ID
ADMIN_ID = 8545044017 # Replace with your Admin ID (can be same as Owner)
YOUR_USERNAME = '@Yash_help_robot' # Replace with your Telegram username (without the @)
UPDATE_CHANNEL = 'https://t.me/CloudXSupport_channel' # Replace with your update channel link

# ✅ FORCE JOIN CONFIG
FORCE_JOIN_CHANNEL = "CloudXSupport_channel"   # Channel username (without @)
FORCE_JOIN_GROUP   = "CloudXSupport_Group"      # Group username (without @)

# Folder setup - using absolute paths
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
UPLOAD_BOTS_DIR = os.path.join(BASE_DIR, 'upload_bots')
IROTECH_DIR = os.path.join(BASE_DIR, 'inf')
# DATABASE_PATH removed — MongoDB Atlas is used via db.py
DONATE_DIR = os.path.join(BASE_DIR, 'donations')
STORAGE_DIR = os.path.join(BASE_DIR, 'storage')
GITHUB_CACHE_DIR = os.path.join(BASE_DIR, 'github_cache')

# Public base URL
PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "http://13.60.179.53:5000")

# File upload limits
FREE_USER_LIMIT = 15
SUBSCRIBED_USER_LIMIT = 50
ADMIN_LIMIT = 99999
OWNER_LIMIT = float('inf')

# ─── GitHub Repo (display link)
GITHUB_REPO_URL = os.environ.get("GITHUB_REPO_URL", 'https://github.com/YOUR_USERNAME/YOUR_REPO')

# ─── GitHub Actions Dispatch Config (from cloudhost_controller) ────────────
GITHUB_TOKEN    = os.environ.get("GITHUB_TOKEN", "")           # Personal Access Token
GITHUB_OWNER    = os.environ.get("GITHUB_OWNER", "")           # your username / org
GITHUB_REPO     = os.environ.get("GITHUB_REPO",  "")           # repo name
GITHUB_WORKFLOW = os.environ.get("GITHUB_WORKFLOW", "worker.yml")

# ─── Autoscaling Thresholds (from cloudhost_controller) ────────────────────
CPU_SPAWN_THRESHOLD = float(os.environ.get("CPU_SPAWN",  45.0))  # % → spawn new GH worker
RAM_SPAWN_THRESHOLD = float(os.environ.get("RAM_SPAWN",  45.0))
MAX_GH_SPAWNS       = int(os.environ.get("MAX_GH_SPAWNS", 5))    # max auto-dispatches/day

# ─── GitHub Spawn Counter ───────────────────────────────────────────────────
GH_SPAWNS_TODAY = 0
CONTROLLER_START_TIME = time.time()

# Per-user storage limit (in MB)
USER_STORAGE_LIMIT_MB = 500

# DDoS / Rate Limiting
RATE_LIMIT_MESSAGES = 12
RATE_LIMIT_WINDOW_SECONDS = 10
DDOS_TEMP_BAN_SECONDS = 300

# Create necessary directories
os.makedirs(UPLOAD_BOTS_DIR, exist_ok=True)
os.makedirs(IROTECH_DIR, exist_ok=True)
os.makedirs(DONATE_DIR, exist_ok=True)
os.makedirs(STORAGE_DIR, exist_ok=True)
os.makedirs(GITHUB_CACHE_DIR, exist_ok=True)

# ============================================================================
# 🤖 BOT INITIALIZATION — must happen before any @bot.* decorator
# ============================================================================
bot = telebot.TeleBot(TOKEN)

# Cache bot username at startup for deep links
BOT_USERNAME = None
try:
    _me = bot.get_me()
    BOT_USERNAME = _me.username
    print(f"✅ Bot username cached: @{BOT_USERNAME}")
except Exception as _e:
    print(f"⚠️ Could not fetch bot username: {_e}")

def get_bot_link(token):
    """Generate Telegram deep link for a storage file token"""
    if BOT_USERNAME:
        return f"https://t.me/{BOT_USERNAME}?start=dl_{token}"
    return None

# --- Data structures ---
bot_scripts = {}
user_subscriptions = {}
user_files = {}
active_users = set()
admin_ids = {ADMIN_ID, OWNER_ID}
bot_locked = False
hosting_enabled = True

# Owner Approval System
pending_approvals = {}

# Run Permission System
pending_run_requests = {}

# DDoS Protection tracking
user_message_timestamps = {}
ddos_banned_users = {}

# DB_LOCK kept for in-memory thread safety (SQLite removed, MongoDB is thread-safe)
DB_LOCK = threading.Lock()

# ============================================================================
# END EARLY CONFIG/BOT INIT
# ============================================================================

@app.route('/')
def home():
    return "🤖 Atx File Host Bot — Files served via Telegram!"

@app.route('/health')
def health():
    _bot_username = globals().get('BOT_USERNAME', 'unknown') or 'unknown'
    return jsonify({"status": "ok", "bot": _bot_username})



# ============================================================================
# ☁️ WORKER INFRASTRUCTURE — REMOVED (not needed)
# MongoDB Atlas provides persistent storage across VPS changes.
# ============================================================================

# Stubs kept so any remaining references don't crash
worker_registry = {}
worker_registry_lock = threading.Lock()
turbo_mode_active = False
task_queue = __import__('queue').PriorityQueue()
pending_tasks  = {}
completed_tasks = {}
failed_tasks    = {}
task_lock = threading.Lock()

def _get_worker_cluster_stats():
    return {"online":0,"dead":0,"total":0,"total_cores":0,"avg_cpu":0.0,
            "avg_ram":0.0,"total_tasks_done":0,"active_tasks":0,
            "total_ram_gb":0.0,"online_workers":[],"dead_workers":[]}

def _should_offload():
    return False

def _pick_worker():
    return None

def enqueue_task(task_type, payload, priority=5):
    return None

def dispatch_github_worker(task_id=None, command=None):
    return False



def run_flask():
  # Make sure to run on port provided by environment or default to 5000
  port = int(os.environ.get("PORT", 5000))
  app.run(host='0.0.0.0', port=port)

def cleanup_expired_storage_files():
    """Background thread: delete expired storage file records from MongoDB.
    Files are stored on Telegram servers (file_id), so no disk cleanup needed."""
    while True:
        try:
            time.sleep(3600)  # Run every hour
            cleanup_expired_storage_files_db()   # MongoDB via db.py
        except Exception as e:
            logger.error(f"Error in cleanup_expired_storage_files: {e}", exc_info=True)

def keep_alive():
    t = Thread(target=run_flask)
    t.daemon = True
    t.start()
    print("Flask Keep-Alive server started.")
    # Start storage expiry cleanup
    cleanup_thread = Thread(target=cleanup_expired_storage_files)
    cleanup_thread.daemon = True
    cleanup_thread.start()
    print("Storage cleanup thread started.")

# ============================================================================
# (Config, bot init, data structures moved to top — see beginning of file)
# ============================================================================

# --- Command Button Layouts (ReplyKeyboardMarkup) ---
COMMAND_BUTTONS_LAYOUT_USER_SPEC = [
    ["📢 Updates Channel", "🐙 GitHub Repo"],
    ["📤 Upload File", "📂 Check Files"],
    ["⚡ Bot Speed", "💾 Storage Info"],
    ["📊 Statistics", "🎁 Donate File"],
    ["🤖 AI Chat", "📦 Install Module"],
    ["🗄️ My Storage", "📞 Contact Owner"]
]
OWNER_COMMAND_BUTTONS_LAYOUT = [
    ["📢 Updates Channel", "🐙 GitHub Repo"],
    ["📤 Upload File", "📂 Check Files"],
    ["⚡ Bot Speed", "💾 Storage Info"],
    ["📊 Statistics", "💳 Subscriptions"],
    ["🤖 AI Chat", "📦 Install Module"],
    ["🔐 Encrypt File", "🔓 Decrypt File"],
    ["📢 Broadcast", "🔒 Lock Bot"],
    ["🟢 Running All Code", "📁 All Users Files"],
    ["👑 Owner Panel", "🎁 Donate File"],
    ["🗄️ My Storage", "🔌 Hosting Status"],
    ["⏳ Pending Files", "📞 Contact Owner"]
]
# Keep ADMIN layout as alias for Owner layout (no separate admins)
ADMIN_COMMAND_BUTTONS_LAYOUT_USER_SPEC = OWNER_COMMAND_BUTTONS_LAYOUT

# --- Database Setup (MongoDB Atlas via db.py) ---
def load_data():
    """Load all persistent data from MongoDB into in-memory structures."""
    global user_subscriptions, user_files, active_users, admin_ids
    data = load_all_data()          # <- db.py function
    active_users.update(data["active_users"])
    user_subscriptions.update(data["user_subscriptions"])
    user_files.update(data["user_files"])
    admin_ids.update(data["admin_ids"])
    logger.info(f"✅ Data loaded from MongoDB: {len(active_users)} users, "
                f"{len(user_subscriptions)} subs, {len(admin_ids)} admins.")

# Initialize MongoDB + Load Data at startup
init_db()       # <- db.py: creates collections + indexes
load_data()     # <- loads all data into memory
# --- End Database Setup ---


# --- Helper Functions ---
def get_user_folder(user_id):
    """Get or create user's folder for storing files"""
    user_folder = os.path.join(UPLOAD_BOTS_DIR, str(user_id))
    os.makedirs(user_folder, exist_ok=True)
    return user_folder

def get_user_file_limit(user_id):
    """Get the file upload limit for a user"""
    # if free_mode: return FREE_MODE_LIMIT # Removed free_mode check
    if user_id == OWNER_ID: return OWNER_LIMIT
    if user_id in admin_ids: return ADMIN_LIMIT
    if user_id in user_subscriptions and user_subscriptions[user_id]['expiry'] > datetime.now():
        return SUBSCRIBED_USER_LIMIT
    return FREE_USER_LIMIT

def get_user_file_count(user_id):
    """Get the number of files uploaded by a user"""
    return len(user_files.get(user_id, []))

def get_user_storage_bytes(user_id):
    """Get total bytes used by a user's folder"""
    user_folder = os.path.join(UPLOAD_BOTS_DIR, str(user_id))
    if not os.path.exists(user_folder):
        return 0
    total = 0
    for dirpath, _, filenames in os.walk(user_folder):
        for fname in filenames:
            fp = os.path.join(dirpath, fname)
            try:
                total += os.path.getsize(fp)
            except OSError:
                pass
    return total

def get_user_storage_mb(user_id):
    """Get storage used by user in MB (2-decimal precision)"""
    return round(get_user_storage_bytes(user_id) / (1024 * 1024), 2)

def get_total_storage_bytes():
    """Get total storage bytes used across all users"""
    if not os.path.exists(UPLOAD_BOTS_DIR):
        return 0
    total = 0
    for dirpath, _, filenames in os.walk(UPLOAD_BOTS_DIR):
        for fname in filenames:
            fp = os.path.join(dirpath, fname)
            try:
                total += os.path.getsize(fp)
            except OSError:
                pass
    return total

def is_rate_limited(user_id):
    """DDoS protection: returns True if user is sending too fast."""
    if user_id == OWNER_ID or user_id in admin_ids:
        return False  # Admins bypass rate limiting
    now = time.time()
    # Check if currently banned
    if user_id in ddos_banned_users:
        if ddos_banned_users[user_id] > now:
            return True
        else:
            del ddos_banned_users[user_id]  # Ban expired
    # Track timestamps
    if user_id not in user_message_timestamps:
        user_message_timestamps[user_id] = []
    # Remove timestamps outside window
    user_message_timestamps[user_id] = [
        t for t in user_message_timestamps[user_id]
        if now - t < RATE_LIMIT_WINDOW_SECONDS
    ]
    user_message_timestamps[user_id].append(now)
    if len(user_message_timestamps[user_id]) > RATE_LIMIT_MESSAGES:
        ddos_banned_users[user_id] = now + DDOS_TEMP_BAN_SECONDS
        logger.warning(f"🛡️ DDoS: User {user_id} rate-limited for {DDOS_TEMP_BAN_SECONDS}s")
        return True
    return False

def is_bot_running(script_owner_id, file_name): # Parameter renamed for clarity
    """Check if a bot script is currently running for a specific user"""
    script_key = f"{script_owner_id}_{file_name}" # Key uses script_owner_id
    script_info = bot_scripts.get(script_key)
    if script_info and script_info.get('process'):
        try:
            proc = psutil.Process(script_info['process'].pid)
            is_running = proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE
            if not is_running:
                logger.warning(f"Process {script_info['process'].pid} for {script_key} found in memory but not running/zombie. Cleaning up.")
                if 'log_file' in script_info and hasattr(script_info['log_file'], 'close') and not script_info['log_file'].closed:
                    try:
                        script_info['log_file'].close()
                    except Exception as log_e:
                        logger.error(f"Error closing log file during zombie cleanup {script_key}: {log_e}")
                if script_key in bot_scripts:
                    del bot_scripts[script_key]
            return is_running
        except psutil.NoSuchProcess:
            logger.warning(f"Process for {script_key} not found (NoSuchProcess). Cleaning up.")
            if 'log_file' in script_info and hasattr(script_info['log_file'], 'close') and not script_info['log_file'].closed:
                try:
                     script_info['log_file'].close()
                except Exception as log_e:
                     logger.error(f"Error closing log file during cleanup of non-existent process {script_key}: {log_e}")
            if script_key in bot_scripts:
                 del bot_scripts[script_key]
            return False
        except Exception as e:
            logger.error(f"Error checking process status for {script_key}: {e}", exc_info=True)
            return False
    return False


def kill_process_tree(process_info):
    """Kill a process and all its children, ensuring log file is closed."""
    pid = None
    log_file_closed = False
    script_key = process_info.get('script_key', 'N/A')

    try:
        # ---- CLOSE LOG FILE SAFELY ----
        if 'log_file' in process_info and hasattr(process_info['log_file'], 'close') and not process_info['log_file'].closed:
            try:
                process_info['log_file'].close()
                log_file_closed = True
                logger.info(f"Closed log file for {script_key} (PID: {process_info.get('process', {}).get('pid', 'N/A')})")
            except Exception as log_e:
                logger.error(f"Error closing log file during kill for {script_key}: {log_e}")

        # ---- GET PROCESS AND PID ----
        process = process_info.get('process')
        if process and hasattr(process, 'pid'):
            pid = process.pid
            if pid:
                try:
                    parent = psutil.Process(pid)

                    # ❗ TERMUX-SAFE: Remove children() (access denied)
                    logger.warning(f"Skipping children() scan for {script_key} (PID: {pid}) due to Termux limitations.")

                    # ---- TERMINATE PARENT ----
                    try:
                        parent.terminate()
                        logger.info(f"Terminated parent process {pid} for {script_key}")

                        try:
                            parent.wait(timeout=1)
                        except psutil.TimeoutExpired:
                            logger.warning(f"Parent process {pid} for {script_key} did not terminate. Killing.")
                            parent.kill()
                            logger.info(f"Killed parent process {pid} for {script_key}")

                    except psutil.NoSuchProcess:
                        logger.warning(f"Parent process {pid} for {script_key} already gone.")

                    except Exception as e:
                        logger.error(f"Error terminating parent {pid} for {script_key}: {e}. Trying kill...")
                        try:
                            parent.kill()
                            logger.info(f"Killed parent process {pid} for {script_key}")
                        except Exception as e2:
                            logger.error(f"Failed to kill parent {pid} for {script_key}: {e2}")

                except psutil.NoSuchProcess:
                    logger.warning(f"Process {pid or 'N/A'} for {script_key} not found during kill. Already terminated?")

            else:
                logger.error(f"Process PID is None for {script_key}.")

        elif log_file_closed:
            logger.warning(f"Process object missing for {script_key}, but log file closed.")

        else:
            logger.error(f"Process object missing for {script_key}, and no log file. Cannot kill.")

    except Exception as e:
        logger.error(f"❌ Unexpected error killing process tree for PID {pid or 'N/A'} ({script_key}): {e}", exc_info=True)

# =========================================================
# ============  OWNER APPROVAL SYSTEM (PART 1)  ===========
# =========================================================

def generate_approval_id():
    """Generate a unique approval ID."""
    import random, string
    return ''.join(random.choices(string.ascii_letters + string.digits, k=16))

def send_approval_request_single(approval_id, user_id, user_name, file_name, file_ext, file_content_bytes):
    """
    Forward the ORIGINAL file to OWNER with [Approve] [Reject] buttons.
    Stores pending approval so callbacks can act on it.
    """
    try:
        caption = (
            f"📋 *New File Approval Request*\n\n"
            f"👤 User: `{user_id}` ({user_name})\n"
            f"📄 File: `{file_name}`\n"
            f"📦 Size: `{round(len(file_content_bytes)/1024, 1)} KB`\n\n"
            f"⚠️ File will NOT run until you approve."
        )
        markup = types.InlineKeyboardMarkup()
        markup.row(
            types.InlineKeyboardButton("✅ Approve", callback_data=f"approve_{approval_id}"),
            types.InlineKeyboardButton("❌ Reject", callback_data=f"reject_{approval_id}"),
            types.InlineKeyboardButton("👁 Ack", callback_data=f"ack_{approval_id}")
        )
        import io
        sent = bot.send_document(
            OWNER_ID,
            io.BytesIO(file_content_bytes),
            caption=caption,
            parse_mode='Markdown',
            visible_file_name=file_name,
            reply_markup=markup
        )
        logger.info(f"Approval request sent to OWNER for file '{file_name}' from user {user_id}. approval_id={approval_id}")
        return sent.message_id
    except Exception as e:
        logger.error(f"Failed to send approval request to owner: {e}", exc_info=True)
        return None

def send_approval_request_zip_entry(approval_id, user_id, user_name, file_name, file_content_bytes, chat_id, message_obj):
    """
    Send a single extracted ZIP entry to OWNER with Approve/Reject buttons.
    File is saved in pending_approvals — NOT run until owner approves.
    """
    import io
    file_ext = os.path.splitext(file_name)[1].lower()
    try:
        # Save in pending_approvals so approve/reject callbacks can act on it
        pending_approvals[approval_id] = {
            'user_id': user_id,
            'chat_id': chat_id,
            'file_name': file_name,
            'file_ext': file_ext,
            'file_content': file_content_bytes,
            'message_obj': message_obj,
            'is_zip_entry': True,
        }

        caption = (
            f"🗜️ *ZIP Entry — Approval Required*\n\n"
            f"👤 User: `{user_id}` ({user_name})\n"
            f"📄 File: `{file_name}`\n"
            f"📦 Size: `{round(len(file_content_bytes)/1024, 1)} KB`\n\n"
            f"⚠️ This file was extracted from a ZIP archive.\n"
            f"File will NOT be saved or run until you approve."
        )
        markup = types.InlineKeyboardMarkup()
        markup.row(
            types.InlineKeyboardButton("✅ Approve", callback_data=f"approve_{approval_id}"),
            types.InlineKeyboardButton("❌ Reject", callback_data=f"reject_{approval_id}")
        )
        bot.send_document(
            OWNER_ID,
            io.BytesIO(file_content_bytes),
            caption=caption,
            parse_mode='Markdown',
            visible_file_name=file_name,
            reply_markup=markup
        )
        logger.info(f"ZIP entry '{file_name}' sent to OWNER for approval. user={user_id}, approval_id={approval_id}")
    except Exception as e:
        logger.error(f"Failed to send ZIP entry approval to owner: {e}", exc_info=True)

def handle_approve_callback(call):
    """Owner approved a file — run it now."""
    approval_id = call.data.split('approve_', 1)[1]
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "⚠️ Owner only!", show_alert=True)
        return
    bot.answer_callback_query(call.id, "✅ Approved! Running file...")

    info = pending_approvals.pop(approval_id, None)
    if not info:
        bot.send_message(OWNER_ID, "⚠️ Approval session expired or already handled.")
        return

    user_id = info['user_id']
    chat_id = info['chat_id']
    file_name = info['file_name']
    file_ext = info['file_ext']
    file_content = info['file_content']
    message_obj = info['message_obj']

    try:
        # Edit owner's message to show approved
        try:
            bot.edit_message_caption(
                chat_id=OWNER_ID,
                message_id=call.message.message_id,
                caption=f"✅ *APPROVED*\n\n📄 `{file_name}` for user `{user_id}`",
                parse_mode='Markdown',
                reply_markup=None
            )
        except Exception:
            pass

        # Save file to user folder and run it
        user_folder = get_user_folder(user_id)
        file_path = os.path.join(user_folder, file_name)
        with open(file_path, 'wb') as f:
            f.write(file_content)
        logger.info(f"Approved file '{file_name}' saved to {file_path} for user {user_id}")

        # Notify user
        try:
            bot.send_message(chat_id, f"✅ *Your file `{file_name}` was approved by the owner!*\n\nStarting it now...", parse_mode='Markdown')
        except Exception:
            pass

        if file_ext == '.py':
            handle_py_file(file_path, user_id, user_folder, file_name, message_obj)
        elif file_ext == '.js':
            handle_js_file(file_path, user_id, user_folder, file_name, message_obj)
        # ✅ PART 3 — Auto-decrypt .enc files on approval and run if .py/.js
        elif file_ext == '.enc':
            auto_handled = try_auto_decrypt_and_run(file_content, file_name, user_id, user_folder, message_obj)
            if not auto_handled:
                try:
                    bot.send_message(chat_id,
                        f"⚠️ Could not auto-decrypt `{file_name}`.\n"
                        f"Make sure it was encrypted by this bot's 🔐 Encrypt system.",
                        parse_mode='Markdown')
                except Exception:
                    pass

    except Exception as e:
        logger.error(f"Error executing approved file '{file_name}' for user {user_id}: {e}", exc_info=True)
        try:
            bot.send_message(OWNER_ID, f"❌ Error running approved file: {e}")
        except Exception:
            pass

def handle_reject_callback(call):
    """Owner rejected a file — delete it, notify user."""
    approval_id = call.data.split('reject_', 1)[1]
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "⚠️ Owner only!", show_alert=True)
        return
    bot.answer_callback_query(call.id, "❌ Rejected!")

    info = pending_approvals.pop(approval_id, None)
    if not info:
        bot.send_message(OWNER_ID, "⚠️ Approval session expired or already handled.")
        return

    user_id = info['user_id']
    chat_id = info['chat_id']
    file_name = info['file_name']

    try:
        # Edit owner's message to show rejected
        try:
            bot.edit_message_caption(
                chat_id=OWNER_ID,
                message_id=call.message.message_id,
                caption=f"❌ *REJECTED*\n\n📄 `{file_name}` for user `{user_id}`",
                parse_mode='Markdown',
                reply_markup=None
            )
        except Exception:
            pass

        # No file was saved to disk (content was held in memory), so just notify user
        try:
            bot.send_message(
                chat_id,
                f"❌ *Your file `{file_name}` was rejected by the owner.*\n\n"
                f"The file has been deleted and will not be hosted.",
                parse_mode='Markdown'
            )
        except Exception as e:
            logger.error(f"Failed to notify user {user_id} of rejection: {e}")

        logger.info(f"File '{file_name}' from user {user_id} REJECTED by owner.")
    except Exception as e:
        logger.error(f"Error in handle_reject_callback: {e}", exc_info=True)

# =========================================================
# ============  END OWNER APPROVAL SYSTEM  ================
# =========================================================

# ============================================================
# 👁 ADMIN ACK CALLBACK — Admin acknowledges pending file
# ============================================================

@bot.callback_query_handler(func=lambda call: call.data.startswith('ack_'))
def handle_ack_callback(call):
    """Admin clicks 👁 Ack to mark they've seen the pending file."""
    approval_id = call.data.split('ack_', 1)[1]
    admin_id = call.from_user.id
    if admin_id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admins only!", show_alert=True)
        return
    bot.answer_callback_query(call.id, "👁 Acknowledged!")
    info = pending_approvals.get(approval_id)
    if not info:
        try:
            bot.edit_message_caption(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                caption="⚠️ *Already handled or expired.*",
                parse_mode='Markdown',
                reply_markup=None
            )
        except Exception:
            pass
        return
    file_name = info.get('file_name', 'unknown')
    user_id = info.get('user_id', '?')
    try:
        bot.edit_message_caption(
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            caption=(
                f"👁 *Acknowledged by Admin* `{admin_id}`\n\n"
                f"📄 File: `{file_name}`\n"
                f"👤 User: `{user_id}`\n\n"
                f"⏳ Waiting for Owner to Approve/Reject."
            ),
            parse_mode='Markdown',
            reply_markup=None
        )
    except Exception:
        pass
    # Notify owner that an admin acked
    try:
        bot.send_message(
            OWNER_ID,
            f"👁 *Admin `{admin_id}` acknowledged*\n\nFile: `{file_name}` from user `{user_id}`\nWaiting for your Approve/Reject.",
            parse_mode='Markdown'
        )
    except Exception:
        pass



# --- Automatic Package Installation & Script Running ---

def attempt_install_pip(module_name, message):
    package_name = TELEGRAM_MODULES.get(module_name.lower(), module_name) 
    if package_name is None: 
        logger.info(f"Module '{module_name}' is core. Skipping pip install.")
        return False 
    try:
        bot.reply_to(message, f"🐍 Module `{module_name}` not found. Installing `{package_name}`...", parse_mode='Markdown')
        command = [sys.executable, '-m', 'pip', 'install', package_name]
        logger.info(f"Running install: {' '.join(command)}")
        result = subprocess.run(command, capture_output=True, text=True, check=False, encoding='utf-8', errors='ignore')
        if result.returncode == 0:
            logger.info(f"Installed {package_name}. Output:\n{result.stdout}")
            bot.reply_to(message, f"✅ Package `{package_name}` (for `{module_name}`) installed.", parse_mode='Markdown')
            return True
        else:
            error_msg = f"❌ Failed to install `{package_name}` for `{module_name}`.\nLog:\n```\n{result.stderr or result.stdout}\n```"
            logger.error(error_msg)
            if len(error_msg) > 4000: error_msg = error_msg[:4000] + "\n... (Log truncated)"
            bot.reply_to(message, error_msg, parse_mode='Markdown')
            return False
    except Exception as e:
        error_msg = f"❌ Error installing `{package_name}`: {str(e)}"
        logger.error(error_msg, exc_info=True)
        bot.reply_to(message, error_msg)
        return False

def attempt_install_npm(module_name, user_folder, message):
    try:
        bot.reply_to(message, f"🟠 Node package `{module_name}` not found. Installing locally...", parse_mode='Markdown')
        command = ['npm', 'install', module_name]
        logger.info(f"Running npm install: {' '.join(command)} in {user_folder}")
        result = subprocess.run(command, capture_output=True, text=True, check=False, cwd=user_folder, encoding='utf-8', errors='ignore')
        if result.returncode == 0:
            logger.info(f"Installed {module_name}. Output:\n{result.stdout}")
            bot.reply_to(message, f"✅ Node package `{module_name}` installed locally.", parse_mode='Markdown')
            return True
        else:
            error_msg = f"❌ Failed to install Node package `{module_name}`.\nLog:\n```\n{result.stderr or result.stdout}\n```"
            logger.error(error_msg)
            if len(error_msg) > 4000: error_msg = error_msg[:4000] + "\n... (Log truncated)"
            bot.reply_to(message, error_msg, parse_mode='Markdown')
            return False
    except FileNotFoundError:
         error_msg = "❌ Error: 'npm' not found. Ensure Node.js/npm are installed and in PATH."
         logger.error(error_msg)
         bot.reply_to(message, error_msg)
         return False
    except Exception as e:
        error_msg = f"❌ Error installing Node package `{module_name}`: {str(e)}"
        logger.error(error_msg, exc_info=True)
        bot.reply_to(message, error_msg)
        return False

def run_script(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt=1):
    """Run Python script. script_owner_id is used for the script_key. message_obj_for_reply is for sending feedback."""
    max_attempts = 2 
    if attempt > max_attempts:
        bot.reply_to(message_obj_for_reply, f"❌ Failed to run '{file_name}' after {max_attempts} attempts. Check logs.")
        return

    script_key = f"{script_owner_id}_{file_name}"
    logger.info(f"Attempt {attempt} to run Python script: {script_path} (Key: {script_key}) for user {script_owner_id}")

    try:
        if not os.path.exists(script_path):
             bot.reply_to(message_obj_for_reply, f"❌ Error: Script '{file_name}' not found at '{script_path}'!")
             logger.error(f"Script not found: {script_path} for user {script_owner_id}")
             if script_owner_id in user_files:
                 user_files[script_owner_id] = [f for f in user_files.get(script_owner_id, []) if f[0] != file_name]
             remove_user_file_db(script_owner_id, file_name)
             return

        if attempt == 1:
            check_command = [sys.executable, script_path]
            logger.info(f"Running Python pre-check: {' '.join(check_command)}")
            check_proc = None
            try:
                check_proc = subprocess.Popen(check_command, cwd=user_folder, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', errors='ignore')
                stdout, stderr = check_proc.communicate(timeout=5)
                return_code = check_proc.returncode
                logger.info(f"Python Pre-check early. RC: {return_code}. Stderr: {stderr[:200]}...")
                if return_code != 0 and stderr:
                    match_py = re.search(r"ModuleNotFoundError: No module named '(.+?)'", stderr)
                    if match_py:
                        module_name = match_py.group(1).strip().strip("'\"")
                        logger.info(f"Detected missing Python module: {module_name}")
                        if attempt_install_pip(module_name, message_obj_for_reply):
                            logger.info(f"Install OK for {module_name}. Retrying run_script...")
                            bot.reply_to(message_obj_for_reply, f"🔄 Install successful. Retrying '{file_name}'...")
                            time.sleep(2)
                            threading.Thread(target=run_script, args=(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt + 1)).start()
                            return
                        else:
                            bot.reply_to(message_obj_for_reply, f"❌ Install failed. Cannot run '{file_name}'.")
                            return
                    else:
                         error_summary = stderr[:500]
                         # Strip markdown special chars to avoid Telegram parse error
                         safe_error = error_summary.replace('`', "'").replace('*', '').replace('_', '').replace('[', '').replace(']', '')
                         bot.reply_to(message_obj_for_reply, f"❌ Error in script pre-check for '{file_name}':\n{safe_error}\n\nFix the script.")
                         return
            except subprocess.TimeoutExpired:
                logger.info("Python Pre-check timed out (>5s), imports likely OK. Killing check process.")
                if check_proc and check_proc.poll() is None: check_proc.kill(); check_proc.communicate()
                logger.info("Python Check process killed. Proceeding to long run.")
            except FileNotFoundError:
                 logger.error(f"Python interpreter not found: {sys.executable}")
                 bot.reply_to(message_obj_for_reply, f"❌ Error: Python interpreter not found.")
                 return
            except Exception as e:
                 logger.error(f"Error in Python pre-check for {script_key}: {e}", exc_info=True)
                 safe_e = str(e).replace('`', "'").replace('*', '').replace('_', '')
                 bot.reply_to(message_obj_for_reply, f"❌ Unexpected error in script pre-check for '{file_name}': {safe_e}")
                 return
            finally:
                 if check_proc and check_proc.poll() is None:
                     logger.warning(f"Python Check process {check_proc.pid} still running. Killing.")
                     check_proc.kill(); check_proc.communicate()

        logger.info(f"Starting long-running Python process for {script_key}")
        log_file_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        log_file = None; process = None
        try: log_file = open(log_file_path, 'w', encoding='utf-8', errors='ignore')
        except Exception as e:
             logger.error(f"Failed to open log file '{log_file_path}' for {script_key}: {e}", exc_info=True)
             bot.reply_to(message_obj_for_reply, f"❌ Failed to open log file '{log_file_path}': {e}")
             return
        try:
            startupinfo = None; creationflags = 0
            if os.name == 'nt':
                 startupinfo = subprocess.STARTUPINFO(); startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                 startupinfo.wShowWindow = subprocess.SW_HIDE
            process = subprocess.Popen(
                [sys.executable, script_path], cwd=user_folder, stdout=log_file, stderr=log_file,
                stdin=subprocess.PIPE, startupinfo=startupinfo, creationflags=creationflags,
                encoding='utf-8', errors='ignore'
            )
            logger.info(f"Started Python process {process.pid} for {script_key}")
            bot_scripts[script_key] = {
                'process': process, 'log_file': log_file, 'file_name': file_name,
                'chat_id': message_obj_for_reply.chat.id, # Chat ID for potential future direct replies from script, defaults to admin/triggering user
                'script_owner_id': script_owner_id, # Actual owner of the script
                'start_time': datetime.now(), 'user_folder': user_folder, 'type': 'py', 'script_key': script_key
            }
            bot.reply_to(message_obj_for_reply, f"✅ Python script '{file_name}' started! (PID: {process.pid}) (For User: {script_owner_id})")
        except FileNotFoundError:
             logger.error(f"Python interpreter {sys.executable} not found for long run {script_key}")
             bot.reply_to(message_obj_for_reply, f"❌ Error: Python interpreter '{sys.executable}' not found.")
             if log_file and not log_file.closed: log_file.close()
             if script_key in bot_scripts: del bot_scripts[script_key]
        except Exception as e:
            if log_file and not log_file.closed: log_file.close()
            error_msg = f"❌ Error starting Python script '{file_name}': {str(e)}"
            logger.error(error_msg, exc_info=True)
            bot.reply_to(message_obj_for_reply, error_msg)
            if process and process.poll() is None:
                 logger.warning(f"Killing potentially started Python process {process.pid} for {script_key}")
                 kill_process_tree({'process': process, 'log_file': log_file, 'script_key': script_key})
            if script_key in bot_scripts: del bot_scripts[script_key]
    except Exception as e:
        error_msg = f"❌ Unexpected error running Python script '{file_name}': {str(e)}"
        logger.error(error_msg, exc_info=True)
        bot.reply_to(message_obj_for_reply, error_msg)
        if script_key in bot_scripts:
             logger.warning(f"Cleaning up {script_key} due to error in run_script.")
             kill_process_tree(bot_scripts[script_key])
             del bot_scripts[script_key]

def run_js_script(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt=1):
    """Run JS script. script_owner_id is used for the script_key. message_obj_for_reply is for sending feedback."""
    max_attempts = 2
    if attempt > max_attempts:
        bot.reply_to(message_obj_for_reply, f"❌ Failed to run '{file_name}' after {max_attempts} attempts. Check logs.")
        return

    script_key = f"{script_owner_id}_{file_name}"
    logger.info(f"Attempt {attempt} to run JS script: {script_path} (Key: {script_key}) for user {script_owner_id}")

    try:
        if not os.path.exists(script_path):
             bot.reply_to(message_obj_for_reply, f"❌ Error: Script '{file_name}' not found at '{script_path}'!")
             logger.error(f"JS Script not found: {script_path} for user {script_owner_id}")
             if script_owner_id in user_files:
                 user_files[script_owner_id] = [f for f in user_files.get(script_owner_id, []) if f[0] != file_name]
             remove_user_file_db(script_owner_id, file_name)
             return

        if attempt == 1:
            check_command = ['node', script_path]
            logger.info(f"Running JS pre-check: {' '.join(check_command)}")
            check_proc = None
            try:
                check_proc = subprocess.Popen(check_command, cwd=user_folder, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', errors='ignore')
                stdout, stderr = check_proc.communicate(timeout=5)
                return_code = check_proc.returncode
                logger.info(f"JS Pre-check early. RC: {return_code}. Stderr: {stderr[:200]}...")
                if return_code != 0 and stderr:
                    match_js = re.search(r"Cannot find module '(.+?)'", stderr)
                    if match_js:
                        module_name = match_js.group(1).strip().strip("'\"")
                        if not module_name.startswith('.') and not module_name.startswith('/'):
                             logger.info(f"Detected missing Node module: {module_name}")
                             if attempt_install_npm(module_name, user_folder, message_obj_for_reply):
                                 logger.info(f"NPM Install OK for {module_name}. Retrying run_js_script...")
                                 bot.reply_to(message_obj_for_reply, f"🔄 NPM Install successful. Retrying '{file_name}'...")
                                 time.sleep(2)
                                 threading.Thread(target=run_js_script, args=(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt + 1)).start()
                                 return
                             else:
                                 bot.reply_to(message_obj_for_reply, f"❌ NPM Install failed. Cannot run '{file_name}'.")
                                 return
                        else: logger.info(f"Skipping npm install for relative/core: {module_name}")
                    error_summary = stderr[:500]
                    safe_error = error_summary.replace('`', "'").replace('*', '').replace('_', '').replace('[', '').replace(']', '')
                    bot.reply_to(message_obj_for_reply, f"❌ Error in JS script pre-check for '{file_name}':\n{safe_error}\n\nFix script or install manually.")
                    return
            except subprocess.TimeoutExpired:
                logger.info("JS Pre-check timed out (>5s), imports likely OK. Killing check process.")
                if check_proc and check_proc.poll() is None: check_proc.kill(); check_proc.communicate()
                logger.info("JS Check process killed. Proceeding to long run.")
            except FileNotFoundError:
                 error_msg = "❌ Error: 'node' not found. Ensure Node.js is installed for JS files."
                 logger.error(error_msg)
                 bot.reply_to(message_obj_for_reply, error_msg)
                 return
            except Exception as e:
                 logger.error(f"Error in JS pre-check for {script_key}: {e}", exc_info=True)
                 bot.reply_to(message_obj_for_reply, f"❌ Unexpected error in JS pre-check for '{file_name}': {e}")
                 return
            finally:
                 if check_proc and check_proc.poll() is None:
                     logger.warning(f"JS Check process {check_proc.pid} still running. Killing.")
                     check_proc.kill(); check_proc.communicate()

        logger.info(f"Starting long-running JS process for {script_key}")
        log_file_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        log_file = None; process = None
        try: log_file = open(log_file_path, 'w', encoding='utf-8', errors='ignore')
        except Exception as e:
            logger.error(f"Failed to open log file '{log_file_path}' for JS script {script_key}: {e}", exc_info=True)
            bot.reply_to(message_obj_for_reply, f"❌ Failed to open log file '{log_file_path}': {e}")
            return
        try:
            startupinfo = None; creationflags = 0
            if os.name == 'nt':
                 startupinfo = subprocess.STARTUPINFO(); startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                 startupinfo.wShowWindow = subprocess.SW_HIDE
            process = subprocess.Popen(
                ['node', script_path], cwd=user_folder, stdout=log_file, stderr=log_file,
                stdin=subprocess.PIPE, startupinfo=startupinfo, creationflags=creationflags,
                encoding='utf-8', errors='ignore'
            )
            logger.info(f"Started JS process {process.pid} for {script_key}")
            bot_scripts[script_key] = {
                'process': process, 'log_file': log_file, 'file_name': file_name,
                'chat_id': message_obj_for_reply.chat.id, # Chat ID for potential future direct replies
                'script_owner_id': script_owner_id, # Actual owner of the script
                'start_time': datetime.now(), 'user_folder': user_folder, 'type': 'js', 'script_key': script_key
            }
            bot.reply_to(message_obj_for_reply, f"✅ JS script '{file_name}' started! (PID: {process.pid}) (For User: {script_owner_id})")
        except FileNotFoundError:
             error_msg = "❌ Error: 'node' not found for long run. Ensure Node.js is installed."
             logger.error(error_msg)
             if log_file and not log_file.closed: log_file.close()
             bot.reply_to(message_obj_for_reply, error_msg)
             if script_key in bot_scripts: del bot_scripts[script_key]
        except Exception as e:
            if log_file and not log_file.closed: log_file.close()
            error_msg = f"❌ Error starting JS script '{file_name}': {str(e)}"
            logger.error(error_msg, exc_info=True)
            bot.reply_to(message_obj_for_reply, error_msg)
            if process and process.poll() is None:
                 logger.warning(f"Killing potentially started JS process {process.pid} for {script_key}")
                 kill_process_tree({'process': process, 'log_file': log_file, 'script_key': script_key})
            if script_key in bot_scripts: del bot_scripts[script_key]
    except Exception as e:
        error_msg = f"❌ Unexpected error running JS script '{file_name}': {str(e)}"
        logger.error(error_msg, exc_info=True)
        bot.reply_to(message_obj_for_reply, error_msg)
        if script_key in bot_scripts:
             logger.warning(f"Cleaning up {script_key} due to error in run_js_script.")
             kill_process_tree(bot_scripts[script_key])
             del bot_scripts[script_key]

# --- Map Telegram import names to actual PyPI package names ---
TELEGRAM_MODULES = {
    # Main Bot Frameworks
    'telebot': 'pyTelegramBotAPI',
    'telegram': 'python-telegram-bot',
    'python_telegram_bot': 'python-telegram-bot',
    'aiogram': 'aiogram',
    'pyrogram': 'pyrogram',
    'telethon': 'telethon',
    'telethon.sync': 'telethon', # Handle specific imports
    'from telethon.sync import telegramclient': 'telethon', # Example

    # Additional Libraries (add more specific mappings if import name differs)
    'telepot': 'telepot',
    'pytg': 'pytg',
    'tgcrypto': 'tgcrypto',
    'telegram_upload': 'telegram-upload',
    'telegram_send': 'telegram-send',
    'telegram_text': 'telegram-text',

    # MTProto & Low-Level
    'mtproto': 'telegram-mtproto', # Example, check actual package name
    'tl': 'telethon',  # Part of Telethon, install 'telethon'

    # Utilities & Helpers (examples, verify package names)
    'telegram_utils': 'telegram-utils',
    'telegram_logger': 'telegram-logger',
    'telegram_handlers': 'python-telegram-handlers',

    # Database Integrations (examples)
    'telegram_redis': 'telegram-redis',
    'telegram_sqlalchemy': 'telegram-sqlalchemy',

    # Payment & E-commerce (examples)
    'telegram_payment': 'telegram-payment',
    'telegram_shop': 'telegram-shop-sdk',

    # Testing & Debugging (examples)
    'pytest_telegram': 'pytest-telegram',
    'telegram_debug': 'telegram-debug',

    # Scraping & Analytics (examples)
    'telegram_scraper': 'telegram-scraper',
    'telegram_analytics': 'telegram-analytics',

    # NLP & AI (examples)
    'telegram_nlp': 'telegram-nlp-toolkit',
    'telegram_ai': 'telegram-ai', # Assuming this exists

    # Web & API Integration (examples)
    'telegram_api': 'telegram-api-client',
    'telegram_web': 'telegram-web-integration',

    # Gaming & Interactive (examples)
    'telegram_games': 'telegram-games',
    'telegram_quiz': 'telegram-quiz-bot',

    # File & Media Handling (examples)
    'telegram_ffmpeg': 'telegram-ffmpeg',
    'telegram_media': 'telegram-media-utils',

    # Security & Encryption (examples)
    'telegram_2fa': 'telegram-twofa',
    'telegram_crypto': 'telegram-crypto-bot',

    # Localization & i18n (examples)
    'telegram_i18n': 'telegram-i18n',
    'telegram_translate': 'telegram-translate',

    # Common non-telegram examples
    'bs4': 'beautifulsoup4',
    'requests': 'requests',
    'pillow': 'Pillow', # Note the capitalization difference
    'cv2': 'opencv-python', # Common import name for OpenCV
    'yaml': 'PyYAML',
    'dotenv': 'python-dotenv',
    'dateutil': 'python-dateutil',
    'pandas': 'pandas',
    'numpy': 'numpy',
    'flask': 'Flask',
    'django': 'Django',
    'sqlalchemy': 'SQLAlchemy',
    'asyncio': None, # Core module, should not be installed
    'json': None,    # Core module
    'datetime': None,# Core module
    'os': None,      # Core module
    'sys': None,     # Core module
    're': None,      # Core module
    'time': None,    # Core module
    'math': None,    # Core module
    'random': None,  # Core module
    'logging': None, # Core module
    'threading': None,# Core module
    'subprocess':None,# Core module
    'zipfile':None,  # Core module
    'tempfile':None, # Core module
    'shutil':None,   # Core module
    'sqlite3':None,  # Core module
    'psutil': 'psutil',
    'atexit': None   # Core module

}
# --- End Automatic Package Installation & Script Running ---


# --- Database Operations (MongoDB Atlas via db.py) ---
# All functions delegate to db.py which handles MongoDB

def save_user_file(user_id, file_name, file_type='py'):
    """Save user file record to MongoDB + update in-memory cache."""
    save_user_file_db(user_id, file_name, file_type)     # MongoDB
    if user_id not in user_files:
        user_files[user_id] = []
    user_files[user_id] = [(fn, ft) for fn, ft in user_files[user_id] if fn != file_name]
    user_files[user_id].append((file_name, file_type))

def remove_user_file_db(user_id, file_name):
    """Remove user file record from MongoDB + update in-memory cache."""
    try:
        db = get_db()
        db.user_files.delete_one({"user_id": user_id, "file_name": file_name})
        logger.info(f"🗑️ File '{file_name}' removed for user {user_id} from MongoDB")
    except Exception as e:
        logger.error(f"❌ remove_user_file_db error ({user_id}, {file_name}): {e}")
    if user_id in user_files:
        user_files[user_id] = [f for f in user_files[user_id] if f[0] != file_name]
        if not user_files[user_id]:
            del user_files[user_id]

def add_active_user(user_id):
    """Add user to active_users set + persist to MongoDB."""
    active_users.add(user_id)
    add_active_user_db(user_id)                          # MongoDB

def save_subscription(user_id, expiry):
    """Save subscription to MongoDB + update in-memory cache."""
    save_subscription_db(user_id, expiry)                # MongoDB
    user_subscriptions[user_id] = {'expiry': expiry}

def remove_subscription_db(user_id):
    """Remove subscription from MongoDB + update in-memory cache."""
    remove_subscription_db(user_id)              # MongoDB
    user_subscriptions.pop(user_id, None)

def add_admin_db(admin_id):
    """Add admin to MongoDB + update in-memory set."""
    add_admin_db(admin_id)                       # MongoDB
    admin_ids.add(admin_id)

def remove_admin_db(admin_id):
    """Remove admin from MongoDB + update in-memory set. Returns True if removed."""
    if admin_id == OWNER_ID:
        logger.warning("Attempted to remove OWNER_ID from admins.")
        return False
    removed = remove_admin_db(admin_id)          # MongoDB
    if removed:
        admin_ids.discard(admin_id)
    return removed

# --- End Database Operations ---

# =========================================================
# ================  GITHUB REPO FEATURE  ==================
# =========================================================

def github_ask_repo(message):
    """Ask user to send GitHub repo URL for hosting"""
    user_id = message.from_user.id if hasattr(message, 'from_user') else message.chat.id
    msg = bot.send_message(
        message.chat.id if hasattr(message, 'chat') else message,
        "🐙 *GitHub Repo Host*\n\n"
        "Send me your GitHub repo URL and I will clone + host ALL files from it!\n\n"
        "📌 Format: `https://github.com/username/repo`\n\n"
        "/cancel to abort.",
        parse_mode='Markdown'
    )
    bot.register_next_step_handler(msg, github_process_repo_url)

def github_process_repo_url(message):
    """Clone the GitHub repo — send ALL files to owner for approval + AI analysis. NO auto-save/run."""
    user_id = message.from_user.id
    user_name = message.from_user.first_name or f"User_{user_id}"
    chat_id = message.chat.id

    if message.text and message.text.strip().lower() == '/cancel':
        bot.reply_to(message, "❌ Cancelled.")
        return

    repo_url = message.text.strip() if message.text else ''
    if not re.match(r'https?://github\.com/[\w\-\.]+/[\w\-\.]+', repo_url):
        bot.reply_to(message, "⚠️ Invalid GitHub URL. Must be like: `https://github.com/user/repo`\n\nSend again or /cancel.", parse_mode='Markdown')
        msg = bot.send_message(chat_id, "Send GitHub repo URL:")
        bot.register_next_step_handler(msg, github_process_repo_url)
        return

    if bot_locked and user_id not in admin_ids:
        bot.reply_to(message, "⚠️ Bot locked, cannot host files.")
        return

    if not is_hosting_enabled() and user_id != OWNER_ID:
        bot.reply_to(message, "🔴 *Hosting is currently disabled!*", parse_mode='Markdown')
        return

    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)

    status_msg = bot.reply_to(message, f"⏳ Cloning repo `{repo_url}`...\nThis may take a moment.", parse_mode='Markdown')

    repo_name = repo_url.rstrip('/').split('/')[-1].replace('.git', '')
    clone_target = os.path.join(GITHUB_CACHE_DIR, f"{user_id}_{repo_name}_{int(time.time())}")

    try:
        result = subprocess.run(
            ['git', 'clone', '--depth=1', repo_url, clone_target],
            capture_output=True, text=True, timeout=60
        )
        if result.returncode != 0:
            bot.edit_message_text(
                f"❌ Failed to clone repo!\n```\n{result.stderr[:500]}\n```",
                chat_id, status_msg.message_id, parse_mode='Markdown'
            )
            return

        # Find all .py and .js files
        found_files = []
        skipped_files = []

        for root, dirs, files in os.walk(clone_target):
            dirs[:] = [d for d in dirs if not d.startswith('.')]
            for fname in files:
                ext = os.path.splitext(fname)[1].lower()
                if ext not in ['.py', '.js']:
                    continue
                if current_files + len(found_files) >= file_limit:
                    skipped_files.append(fname)
                    continue
                if user_id != OWNER_ID:
                    used_mb = get_user_storage_mb(user_id)
                    if used_mb >= USER_STORAGE_LIMIT_MB:
                        skipped_files.append(fname)
                        continue
                full_path = os.path.join(root, fname)
                try:
                    with open(full_path, 'rb') as _f:
                        content = _f.read()
                    found_files.append((fname, ext.lstrip('.'), content))
                except Exception as _e:
                    logger.error(f"GitHub read file error {fname}: {_e}")

        # Cleanup cloned dir — files already read into memory
        try: shutil.rmtree(clone_target)
        except Exception: pass

        if not found_files:
            bot.edit_message_text(
                "⚠️ No `.py` or `.js` files found in the repo, or file/storage limit reached.",
                chat_id, status_msg.message_id
            )
            return

        # ✅ ALL FILES GO TO OWNER APPROVAL — nothing saved/run yet
        bot.edit_message_text(
            f"✅ *Repo cloned!*\n\n"
            f"📁 Repo: `{repo_url}`\n"
            f"📄 Files found: `{len(found_files)}`\n\n"
            f"⏳ *Sending each file to Owner for approval...*\n"
            f"No file will be saved or run until Owner approves.",
            chat_id, status_msg.message_id, parse_mode='Markdown'
        )

        # Notify owner — summary first
        try:
            file_list_str = "\n".join([f"  • `{fn}` ({ft})" for fn, ft, _ in found_files[:20]])
            if len(found_files) > 20:
                file_list_str += f"\n  ... aur {len(found_files) - 20} files"
            bot.send_message(
                OWNER_ID,
                f"🐙 *GitHub Repo Upload Request*\n\n"
                f"👤 User: `{user_name}` (ID: `{user_id}`)\n"
                f"🔗 Repo: `{repo_url}`\n"
                f"📄 Files: `{len(found_files)}` (skipped: `{len(skipped_files)}`)\n\n"
                f"📂 File list:\n{file_list_str}\n\n"
                f"⚠️ Har file ke liye *Approve/Reject* buttons aayenge below:",
                parse_mode='Markdown'
            )
        except Exception as _e:
            logger.error(f"GitHub owner summary error: {_e}")

        # Send each file to owner for approval with AI analysis
        approved_count = 0
        for fname, ftype, content in found_files:
            try:
                approval_id = generate_approval_id()
                file_ext = f".{ftype}"

                # Save in pending_approvals
                pending_approvals[approval_id] = {
                    'user_id': user_id,
                    'chat_id': chat_id,
                    'file_name': fname,
                    'file_ext': file_ext,
                    'file_content': content,
                    'message_obj': message,
                    'is_github': True,
                    'repo_url': repo_url,
                }

                # Quick AI keyword scan for this file
                dangerous_keywords = [b'os.system', b'subprocess', b'eval(', b'exec(',
                                      b'__import__', b'socket', b'shutil.rmtree',
                                      b'open(/etc', b'open(/root', b'requests.get', b'requests.post']
                found_kw = [kw.decode() for kw in dangerous_keywords if kw in content]
                if found_kw:
                    risk_str = f"⚠️ Suspicious: " + ", ".join([f"`{k}`" for k in found_kw])
                else:
                    risk_str = "✅ No obvious dangerous keywords"

                import io as _io
                caption = (
                    f"🐙 *GitHub File — Approval Required*\n\n"
                    f"👤 User: `{user_name}` (ID: `{user_id}`)\n"
                    f"🔗 Repo: `{repo_url}`\n"
                    f"📄 File: `{fname}`\n"
                    f"📦 Size: `{round(len(content)/1024, 1)} KB`\n\n"
                    f"🔍 *Quick Scan:* {risk_str}\n\n"
                    f"⚠️ File will NOT be saved or run until you approve."
                )
                markup = types.InlineKeyboardMarkup()
                markup.row(
                    types.InlineKeyboardButton("✅ Approve", callback_data=f"approve_{approval_id}"),
                    types.InlineKeyboardButton("❌ Reject", callback_data=f"reject_{approval_id}")
                )
                bot.send_document(
                    OWNER_ID,
                    _io.BytesIO(content),
                    caption=caption,
                    parse_mode='Markdown',
                    visible_file_name=fname,
                    reply_markup=markup
                )

                # Full AI analysis in background thread
                def _run_ai(c=content, fn=fname, uid=user_id, un=user_name):
                    ai_analyze_file(c, fn, uid, un)
                threading.Thread(target=_run_ai, daemon=True).start()

                approved_count += 1
                time.sleep(0.4)  # Avoid flood
            except Exception as _e:
                logger.error(f"GitHub approval send error for {fname}: {_e}", exc_info=True)

        # Final user message
        try:
            bot.send_message(
                chat_id,
                f"📬 *{approved_count} file(s) sent for Owner approval!*\n\n"
                f"Repo: `{repo_url}`\n\n"
                f"Owner approve karne ke baad hi files save aur run hongi.\n"
                f"Thoda wait karo.",
                parse_mode='Markdown'
            )
        except Exception: pass

    except subprocess.TimeoutExpired:
        try: shutil.rmtree(clone_target)
        except Exception: pass
        bot.edit_message_text("❌ Cloning timed out (60s). Try a smaller repo.", chat_id, status_msg.message_id)
    except FileNotFoundError:
        bot.edit_message_text("❌ `git` not installed on server. Cannot clone repos.", chat_id, status_msg.message_id, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"GitHub repo host error for {user_id}: {e}", exc_info=True)
        try: shutil.rmtree(clone_target)
        except Exception: pass
        bot.edit_message_text(f"❌ Error: {str(e)[:300]}", chat_id, status_msg.message_id)

def github_run_all_for_user_callback(call):
    """
    ✅ FIXED: GitHub 'Run All' — non-admin users need owner approval per file.
    Admins/Owner can run directly.
    """
    try:
        parts = call.data.split('_')  # github_run_all_{user_id}
        target_user_id = int(parts[3])
        requesting_user = call.from_user.id

        if requesting_user != target_user_id and requesting_user not in admin_ids:
            bot.answer_callback_query(call.id, "⚠️ Permission denied.", show_alert=True)
            return

        files_for_user = user_files.get(target_user_id, [])
        if not files_for_user:
            bot.answer_callback_query(call.id, "⚠️ No files found to run.", show_alert=True)
            return

        user_folder = get_user_folder(target_user_id)
        user_name = call.from_user.first_name or f"User_{requesting_user}"
        chat_id = call.message.chat.id

        # ✅ Admins/Owner — seedha run kar sakte hain
        if requesting_user in admin_ids:
            bot.answer_callback_query(call.id, "⏳ Starting all files (Admin)...")
            started = 0
            for fname, ftype in files_for_user:
                if is_bot_running(target_user_id, fname):
                    continue
                fpath = os.path.join(user_folder, fname)
                if not os.path.exists(fpath):
                    continue
                if ftype == 'py':
                    threading.Thread(target=run_script, args=(fpath, target_user_id, user_folder, fname, call.message)).start()
                elif ftype == 'js':
                    threading.Thread(target=run_js_script, args=(fpath, target_user_id, user_folder, fname, call.message)).start()
                started += 1
                time.sleep(0.5)
            bot.send_message(chat_id, f"▶️ Started `{started}` scripts (Admin override).", parse_mode='Markdown')
            return

        # ✅ Normal users — owner approval chahiye har file ke liye
        bot.answer_callback_query(call.id, "⏳ Run request sent to Owner for approval!")
        bot.send_message(
            chat_id,
            f"⏳ *Run Permission Requested!*\n\n"
            f"Tumhare saare GitHub files run karne ke liye Owner ka approve chahiye.\n"
            f"Owner approve karne ke baad hi chalenge.",
            parse_mode='Markdown'
        )

        # Owner ko ek ek file ke liye Allow/Deny bhejo
        sent_count = 0
        for fname, ftype in files_for_user:
            fpath = os.path.join(user_folder, fname)
            if is_bot_running(target_user_id, fname):
                continue
            if not os.path.exists(fpath):
                continue

            run_req_id = generate_approval_id()
            pending_run_requests[run_req_id] = {
                'user_id': requesting_user,
                'chat_id': chat_id,
                'script_owner_id': target_user_id,
                'file_name': fname,
                'file_path': fpath,
                'call_message': call.message,
                'timestamp': datetime.now().isoformat()
            }

            # Quick scan
            risk_str = "✅ No obvious issues."
            try:
                with open(fpath, 'rb') as _f:
                    _bytes = _f.read()
                dangerous_kw = [b'os.system', b'subprocess', b'eval(', b'exec(', b'socket', b'shutil.rmtree']
                found_kw = [kw.decode() for kw in dangerous_kw if kw in _bytes]
                if found_kw:
                    risk_str = "⚠️ Suspicious: " + ", ".join([f"`{k}`" for k in found_kw])
                # AI analysis in background
                def _run_ai(c=_bytes, fn=fname, uid=target_user_id, un=user_name):
                    ai_analyze_file(c, fn, uid, un)
                threading.Thread(target=_run_ai, daemon=True).start()
            except Exception as _e:
                risk_str = f"Scan error: {_e}"

            run_markup = types.InlineKeyboardMarkup()
            run_markup.row(
                types.InlineKeyboardButton("▶️ Allow Run", callback_data=f"runallow_{run_req_id}"),
                types.InlineKeyboardButton("🚫 Deny Run", callback_data=f"rundeny_{run_req_id}")
            )
            try:
                bot.send_message(
                    OWNER_ID,
                    f"▶️ *GitHub Run Request*\n\n"
                    f"👤 User: `{user_name}` (ID: `{requesting_user}`)\n"
                    f"📄 Script: `{fname}`\n"
                    f"🔍 *Quick Scan:* {risk_str}\n\n"
                    f"Allow karo toh script chalegi. Deny karo toh nahi.",
                    parse_mode='Markdown',
                    reply_markup=run_markup
                )
                sent_count += 1
                time.sleep(0.4)
            except Exception as _e:
                logger.error(f"GitHub run request send error {fname}: {_e}")

        if sent_count == 0:
            bot.send_message(chat_id, "⚠️ Koi file run ke liye available nahi (sab pehle se chal rahi hain ya missing hain).")

    except Exception as e:
        logger.error(f"github_run_all_for_user error: {e}", exc_info=True)
        bot.send_message(call.message.chat.id, "❌ Error processing run request.")


# =========================================================
# ================  DONATE FILE FEATURE  ==================
# =========================================================

# save_donate_file_db is imported from db.py (MongoDB version)
# Signature: save_donate_file_db(donor_user_id, donor_name, file_name, telegram_file_id, file_size)

# get_all_donate_files() is defined in the MongoDB section above

def get_donate_file_by_id(donate_id):
    """Get donate file info by donate_id from MongoDB."""
    try:
        from bson import ObjectId

        db = get_db()
        doc = db.donate_files.find_one({"_id": ObjectId(str(donate_id))})
        if not doc:
            return None
        return (
            str(doc["_id"]),
            doc.get("donor_user_id"),
            doc.get("donor_name",""),
            doc.get("file_name",""),
            doc.get("telegram_file_id",""),
            doc.get("file_size",0),
            doc.get("donated_at",""),
        )
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"get_donate_file_by_id error: {e}")
        return None

def delete_donate_file_db(donate_id, requesting_user_id, owner_id):
    """Delete a donated file from MongoDB."""
    try:
        from bson import ObjectId

        db = get_db()
        doc = db.donate_files.find_one({"_id": ObjectId(str(donate_id))})
        if not doc:
            return False, "File not found"
        if requesting_user_id != owner_id:
            return False, "Only the Owner can delete donated files."
        fname = doc.get("file_name","")
        db.donate_files.delete_one({"_id": ObjectId(str(donate_id))})
        return True, fname
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"delete_donate_file_db error: {e}")
        return False, str(e)

# =========================================================
# ================  MY STORAGE FEATURE  ===================
# =========================================================

# save_storage_file_db is imported from db.py (MongoDB version)
# Signature: save_storage_file_db(user_id, file_name, telegram_file_id, file_type, file_size, share_token, expires_at=None)

# get_user_storage_files imported from db.py (MongoDB version)

# get_storage_file_by_token imported from db.py (MongoDB version)

def generate_safe_token():
    """Generate a token safe for Telegram ?start= deep links (alphanumeric only, no - or _)"""
    import random, string
    chars = string.ascii_letters + string.digits
    return ''.join(random.choices(chars, k=32))

def regenerate_storage_link(storage_id, expires_at=None):
    """Regenerate share token — delegates to MongoDB via db.py"""
    new_token = generate_safe_token()
    success = regenerate_storage_link_db(str(storage_id), expires_at, new_token)
    return new_token if success else None

# get_all_storage_files_all_users imported from db.py (MongoDB version)

def delete_storage_file_wrapper(storage_id, requesting_user_id):
    """Delete storage file record from MongoDB — wrapper that supplies OWNER_ID."""
    return delete_storage_file_db(str(storage_id), requesting_user_id, OWNER_ID)

def _logic_my_storage_callback(call):
    """Show user's personal storage panel — called from inline button (uses call.from_user.id correctly)"""
    user_id = call.from_user.id
    files = get_user_storage_files(user_id)
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("📤 Store a File", callback_data='storage_upload'),
        types.InlineKeyboardButton("📋 My Stored Files", callback_data='storage_list')
    )
    if user_id == OWNER_ID:
        markup.add(types.InlineKeyboardButton("👑 All Users Storage", callback_data='storage_owner_all'))
    markup.add(types.InlineKeyboardButton("🔙 Back to Main", callback_data='back_to_main'))

    total_size_mb = 0
    if files:
        total_size_mb = round(sum(f[2] for f in files) / (1024*1024), 2)

    bot.send_message(
        call.message.chat.id,
        f"🗄️ *My Personal Storage*\n\n"
        f"📂 Stored Files: `{len(files)}`\n"
        f"💾 Total Size: `{total_size_mb} MB`\n\n"
        f"✅ Files are stored on *Telegram's servers* — no VPS disk used!\n"
        f"Share links are Telegram bot deep links. Anyone with the link gets the file directly from Telegram.\n\n"
        f"Choose an option:",
        reply_markup=markup, parse_mode='Markdown'
    )

def _logic_my_storage(message):
    """Show user's personal storage panel"""
    user_id = message.from_user.id
    files = get_user_storage_files(user_id)
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("📤 Store a File", callback_data='storage_upload'),
        types.InlineKeyboardButton("📋 My Stored Files", callback_data='storage_list')
    )
    if user_id == OWNER_ID:
        markup.add(types.InlineKeyboardButton("👑 All Users Storage", callback_data='storage_owner_all'))
    markup.add(types.InlineKeyboardButton("🔙 Back to Main", callback_data='back_to_main'))

    total_size_mb = 0
    if files:
        total_size_mb = round(sum(f[2] for f in files) / (1024*1024), 2)

    bot.reply_to(message,
        f"🗄️ *My Personal Storage*\n\n"
        f"📂 Stored Files: `{len(files)}`\n"
        f"💾 Total Size: `{total_size_mb} MB`\n\n"
        f"✅ Files are stored on *Telegram's servers* — no VPS disk used!\n"
        f"Share links are Telegram bot deep links. Anyone with the link gets the file directly from Telegram.\n\n"
        f"Choose an option:",
        reply_markup=markup, parse_mode='Markdown'
    )

def storage_upload_callback(call):
    """Start storage file upload - ask expiry first"""
    bot.answer_callback_query(call.id)
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("2 Hours", callback_data='storage_exp_2h'),
        types.InlineKeyboardButton("24 Hours", callback_data='storage_exp_24h'),
    )
    markup.add(
        types.InlineKeyboardButton("7 Days", callback_data='storage_exp_7d'),
        types.InlineKeyboardButton("30 Days", callback_data='storage_exp_30d'),
    )
    markup.add(
        types.InlineKeyboardButton("1 Year", callback_data='storage_exp_1y'),
        types.InlineKeyboardButton("♾️ Never Expire", callback_data='storage_exp_never'),
    )
    markup.add(types.InlineKeyboardButton("🔙 Back", callback_data='my_storage'))
    bot.send_message(
        call.message.chat.id,
        "📤 *Store a File*\n\n"
        "First, choose how long the download link should be valid:\n\n"
        "⚠️ After the link expires, it will become invalid.\n"
        "You can generate a new link from My Storage anytime.",
        reply_markup=markup, parse_mode='Markdown'
    )

def storage_expiry_chosen_callback(call):
    """User chose expiry duration, now ask for the file"""
    bot.answer_callback_query(call.id)
    exp_code = call.data.split('storage_exp_')[1]  # e.g. "2h", "7d", "never"
    
    exp_map = {
        '2h': ('2 Hours', timedelta(hours=2)),
        '24h': ('24 Hours', timedelta(hours=24)),
        '7d': ('7 Days', timedelta(days=7)),
        '30d': ('30 Days', timedelta(days=30)),
        '1y': ('1 Year', timedelta(days=365)),
        'never': ('Never', None),
    }
    label, delta = exp_map.get(exp_code, ('Never', None))
    expires_at = datetime.now() + delta if delta else None
    
    # Store chosen expiry in a temp dict keyed by user_id
    if not hasattr(storage_expiry_chosen_callback, '_pending'):
        storage_expiry_chosen_callback._pending = {}
    storage_expiry_chosen_callback._pending[call.from_user.id] = expires_at
    
    msg = bot.send_message(
        call.message.chat.id,
        f"✅ Link expiry set: *{label}*\n\n"
        f"📤 Now send me the file you want to store.\n"
        f"Any file type supported (photo, video, document, audio, etc.)\n"
        f"📌 Files are stored on Telegram's servers — up to 2GB per file!\n\n"
        f"/cancel to abort.",
        parse_mode='Markdown'
    )
    bot.register_next_step_handler(msg, process_storage_upload)

def process_storage_upload(message):
    """Handle the file/media sent to storage - supports ALL file types"""
    user_id = message.from_user.id
    if message.text and message.text.strip().lower() == '/cancel':
        bot.reply_to(message, "❌ Upload cancelled.")
        return

    # Get pending expiry for this user
    pending = getattr(storage_expiry_chosen_callback, '_pending', {})
    expires_at = pending.pop(user_id, None)  # None = never expire

    # Detect file type from message
    file_id = None
    file_name = None
    file_size = 0
    media_type = 'document'

    if message.document:
        file_id = message.document.file_id
        file_name = message.document.file_name or f"file_{int(time.time())}"
        file_size = message.document.file_size or 0
        media_type = 'document'
    elif message.photo:
        photo = message.photo[-1]  # Largest
        file_id = photo.file_id
        file_name = f"photo_{int(time.time())}.jpg"
        file_size = photo.file_size or 0
        media_type = 'photo'
    elif message.video:
        file_id = message.video.file_id
        file_name = message.video.file_name or f"video_{int(time.time())}.mp4"
        file_size = message.video.file_size or 0
        media_type = 'video'
    elif message.audio:
        file_id = message.audio.file_id
        file_name = message.audio.file_name or f"audio_{int(time.time())}.mp3"
        file_size = message.audio.file_size or 0
        media_type = 'audio'
    elif message.voice:
        file_id = message.voice.file_id
        file_name = f"voice_{int(time.time())}.ogg"
        file_size = message.voice.file_size or 0
        media_type = 'voice'
    elif message.video_note:
        file_id = message.video_note.file_id
        file_name = f"video_note_{int(time.time())}.mp4"
        file_size = message.video_note.file_size or 0
        media_type = 'video_note'
    elif message.sticker:
        file_id = message.sticker.file_id
        ext = ".webm" if message.sticker.is_video else ".webp"
        file_name = f"sticker_{int(time.time())}{ext}"
        file_size = message.sticker.file_size or 0
        media_type = 'sticker'
    elif message.animation:
        file_id = message.animation.file_id
        file_name = f"animation_{int(time.time())}.gif"
        file_size = message.animation.file_size or 0
        media_type = 'animation'
    else:
        bot.reply_to(message, "⚠️ Please send a file, photo, video, audio, or any media to store.\n/cancel to abort.")
        # Re-register handler
        pending[user_id] = expires_at
        if not hasattr(storage_expiry_chosen_callback, '_pending'):
            storage_expiry_chosen_callback._pending = {}
        storage_expiry_chosen_callback._pending[user_id] = expires_at
        msg = bot.send_message(message.chat.id, "Send the file/media to store:")
        bot.register_next_step_handler(msg, process_storage_upload)
        return

    max_storage_size = 2000 * 1024 * 1024  # 2GB (Telegram bot limit)
    if file_size > max_storage_size:
        bot.reply_to(message, f"⚠️ File too large (max 2GB). Your file: {round(file_size/1024/1024, 1)} MB")
        return

    status_msg = bot.reply_to(message, f"⏳ Storing `{file_name}`...", parse_mode='Markdown')
    try:
        # NO DOWNLOAD! We just save the Telegram file_id directly.
        # File stays on Telegram's servers forever (as long as the file_id is valid).
        share_token = generate_safe_token()
        save_storage_file_db(user_id, file_name, file_id, media_type, file_size, share_token, expires_at)

        # Generate Telegram deep link
        bot_link = get_bot_link(share_token)
        size_kb = round(file_size / 1024, 1)
        exp_str = expires_at.strftime('%Y-%m-%d %H:%M') if expires_at else "Never"

        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("📋 My Stored Files", callback_data='storage_list'))
        markup.add(types.InlineKeyboardButton("🔙 Back to Main", callback_data='back_to_main'))

        if bot_link:
            msg_text = (
                f"✅ *File Stored Successfully!*\n\n"
                f"📄 File: `{file_name}`\n"
                f"📦 Size: `{size_kb} KB`\n"
                f"⏰ Link expires: `{exp_str}`\n\n"
                f"🤖 *Share this Telegram link:*\n`{bot_link}`\n\n"

                f"Anyone who taps this link → bot opens → file sent directly from Telegram! 🚀\n"
                f"_(No VPS disk used — file is on Telegram's servers!)_"
            )
        else:
            msg_text = (
                f"✅ *File Stored!*\n📄 `{file_name}` ({size_kb} KB)\n"
                f"⚠️ Bot username not available. Restart bot to get share link."
            )

        bot.edit_message_text(
            msg_text,
            message.chat.id, status_msg.message_id,
            reply_markup=markup, parse_mode='Markdown'
        )
        # Notify owner
        try:
            notif = (f"🗄️ Storage upload by `{user_id}` ({message.from_user.first_name})\n"
                     f"File: `{file_name}` ({size_kb} KB) [{media_type}]\nExpires: {exp_str}")
            if bot_link:
                notif += f"\nLink: `{bot_link}`"
            bot.send_message(OWNER_ID, notif, parse_mode='Markdown')
        except Exception:
            pass

    except Exception as e:
        logger.error(f"Error in storage upload for {user_id}: {e}", exc_info=True)
        safe_e = str(e).replace('`', "'")
        try:
            bot.edit_message_text(f"❌ Error storing file: {safe_e[:200]}", message.chat.id, status_msg.message_id)
        except Exception:
            bot.reply_to(message, f"❌ Error storing file: {safe_e[:200]}")

def storage_list_callback(call):
    """Show list of user's stored files with download + regen link options"""
    bot.answer_callback_query(call.id)
    user_id = call.from_user.id
    files = get_user_storage_files(user_id)
    if not files:
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("📤 Store a File", callback_data='storage_upload'))
        markup.add(types.InlineKeyboardButton("🔙 Back", callback_data='back_to_main'))
        bot.send_message(call.message.chat.id,
            "📭 *No stored files yet!*\n\nUse the Store button to upload a file.",
            reply_markup=markup, parse_mode='Markdown')
        return

    for storage_id, fname, fsize, share_token, uploaded_at, expires_at in files:
        size_kb = round(fsize / 1024, 1)
        date_str = uploaded_at[:10] if uploaded_at else 'Unknown'

        # Only bot link - no HTTP
        bot_link = get_bot_link(share_token)

        # Check expiry
        exp_str = "Never"
        is_expired = False
        if expires_at:
            try:
                exp_dt = datetime.fromisoformat(expires_at)
                if datetime.now() > exp_dt:
                    is_expired = True
                    exp_str = "EXPIRED"
                else:
                    exp_str = exp_dt.strftime('%Y-%m-%d %H:%M')
            except Exception:
                exp_str = expires_at[:16] if expires_at else "Unknown"

        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton("⬇️ Download", callback_data=f'storage_dl_{storage_id}'),
            types.InlineKeyboardButton("🔄 New Link", callback_data=f'storage_regen_{storage_id}'),
        )
        markup.add(
            types.InlineKeyboardButton("🗑️ Delete", callback_data=f'storage_del_{storage_id}'),
        )

        status_icon = "❌ EXPIRED" if is_expired else "✅ Active"
        file_text = (
            f"📄 *{fname}*\n"
            f"📦 {size_kb} KB | 📅 {date_str}\n"
            f"⏰ Expires: {exp_str} | {status_icon}\n\n"
        )
        if bot_link and not is_expired:
            file_text += f"🤖 *Share Link:*\n`{bot_link}`"
        elif is_expired:
            file_text += "⚠️ Link expired. Tap 🔄 New Link to generate a fresh one."

        bot.send_message(call.message.chat.id, file_text, reply_markup=markup, parse_mode='Markdown')

    # Footer buttons
    footer_markup = types.InlineKeyboardMarkup()
    footer_markup.add(types.InlineKeyboardButton("📤 Store Another File", callback_data='storage_upload'))
    footer_markup.add(types.InlineKeyboardButton("🔙 Back", callback_data='back_to_main'))
    bot.send_message(call.message.chat.id, f"📂 Total: {len(files)} file(s) stored.", reply_markup=footer_markup)

def storage_delete_callback(call):
    """User deletes their own stored file"""
    bot.answer_callback_query(call.id)
    user_id = call.from_user.id
    try:
        storage_id = int(call.data.split('_')[2])
        success, result = delete_storage_file_wrapper(storage_id, user_id)
        if success:
            bot.send_message(call.message.chat.id, f"🗑️ Deleted stored file `{result}` successfully.", parse_mode='Markdown')
        else:
            bot.send_message(call.message.chat.id, f"❌ Could not delete: {result}")
    except Exception as e:
        logger.error(f"storage_delete_callback error: {e}", exc_info=True)
        bot.send_message(call.message.chat.id, "❌ Error deleting file.")

def storage_download_callback(call):
    """Send user their own stored file directly in chat using Telegram file_id"""
    bot.answer_callback_query(call.id, "⏳ Sending file...")
    user_id = call.from_user.id
    try:
        storage_id_raw = call.data.split('_')[2]
        doc = get_storage_file_by_id(storage_id_raw)  # MongoDB
        row = None
        if doc:
            row = (doc.get("user_id"), doc.get("file_name",""), doc.get("telegram_file_id",""), doc.get("file_type","document"), doc.get("file_size",0))
        if not row:
            bot.send_message(call.message.chat.id, "❌ File not found.")
            return
        owner_uid, fname, tg_file_id, ftype, fsize = row
        # Allow owner, admin, or file owner
        if user_id != owner_uid and user_id not in admin_ids:
            bot.answer_callback_query(call.id, "⚠️ Permission denied.", show_alert=True)
            return
        size_kb = round((fsize or 0) / 1024, 1)
        caption = f"📥 *Your stored file*\n📄 `{fname}`\n📦 {size_kb} KB"
        _send_file_by_type(call.message.chat.id, tg_file_id, ftype, caption)
        # Increment download count (MongoDB)
        try:
            increment_download_count_by_id(str(storage_id_raw))
        except Exception:
            pass
    except Exception as e:
        logger.error(f"storage_download_callback error: {e}", exc_info=True)
        bot.send_message(call.message.chat.id, "❌ Error sending file.")

def storage_regen_link_callback(call):
    """Regenerate a new share link for a stored file, with expiry choice"""
    bot.answer_callback_query(call.id)
    user_id = call.from_user.id
    try:
        storage_id = int(call.data.split('_')[2])
        # Verify ownership (MongoDB)
        doc = get_storage_file_by_id(str(storage_id))
        if not doc:
            bot.send_message(call.message.chat.id, "❌ File not found.")
            return
        owner_uid = doc.get("user_id")
        fname     = doc.get("file_name","")
        if user_id != owner_uid and user_id not in admin_ids:
            bot.send_message(call.message.chat.id, "⚠️ Permission denied.")
            return

        # Store pending regen storage_id
        if not hasattr(storage_regen_link_callback, '_pending'):
            storage_regen_link_callback._pending = {}
        storage_regen_link_callback._pending[user_id] = storage_id

        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton("2 Hours", callback_data='storage_regen_exp_2h'),
            types.InlineKeyboardButton("24 Hours", callback_data='storage_regen_exp_24h'),
        )
        markup.add(
            types.InlineKeyboardButton("7 Days", callback_data='storage_regen_exp_7d'),
            types.InlineKeyboardButton("30 Days", callback_data='storage_regen_exp_30d'),
        )
        markup.add(
            types.InlineKeyboardButton("1 Year", callback_data='storage_regen_exp_1y'),
            types.InlineKeyboardButton("♾️ Never", callback_data='storage_regen_exp_never'),
        )
        bot.send_message(call.message.chat.id,
            f"🔄 *Regenerate Link for* `{fname}`\n\nChoose new link expiry:",
            reply_markup=markup, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"storage_regen_link_callback error: {e}", exc_info=True)
        bot.send_message(call.message.chat.id, "❌ Error.")

def storage_regen_expiry_callback(call):
    """Process the expiry choice for link regeneration"""
    bot.answer_callback_query(call.id)
    user_id = call.from_user.id
    pending = getattr(storage_regen_link_callback, '_pending', {})
    storage_id = pending.pop(user_id, None)
    if not storage_id:
        bot.send_message(call.message.chat.id, "❌ Session expired. Start again from My Storage.")
        return

    exp_code = call.data.split('storage_regen_exp_')[1]
    exp_map = {
        '2h': ('2 Hours', timedelta(hours=2)),
        '24h': ('24 Hours', timedelta(hours=24)),
        '7d': ('7 Days', timedelta(days=7)),
        '30d': ('30 Days', timedelta(days=30)),
        '1y': ('1 Year', timedelta(days=365)),
        'never': ('Never', None),
    }
    label, delta = exp_map.get(exp_code, ('Never', None))
    expires_at = datetime.now() + delta if delta else None

    new_token = regenerate_storage_link(storage_id, expires_at)
    if not new_token:
        bot.send_message(call.message.chat.id, "❌ Failed to regenerate link.")
        return

    bot_link = get_bot_link(new_token)
    exp_str = expires_at.strftime('%Y-%m-%d %H:%M') if expires_at else "Never"

    if bot_link:
        msg_text = (
            f"✅ *New Link Generated!*\n\n"
            f"⏰ Expires: `{exp_str}`\n\n"
            f"🤖 *Share Link:*\n`{bot_link}`\n\n"
            f"Anyone who taps this link can download the file!"
        )
    else:
        msg_text = f"✅ New link generated!\n⏰ Expires: {exp_str}\n⚠️ Bot username unavailable. Restart bot."

    bot.send_message(call.message.chat.id, msg_text, parse_mode='Markdown')

def storage_owner_all_callback(call):
    """Owner sees all users' storage files"""
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "⚠️ Owner only.", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    all_files = get_all_storage_files_all_users()
    if not all_files:
        bot.send_message(call.message.chat.id, "📭 No storage files from any user.")
        return
    markup = types.InlineKeyboardMarkup(row_width=1)
    text = f"👑 *Owner: All Storage Files ({len(all_files)})*\n\n"
    for storage_id, uid, fname, fsize, share_token, uploaded_at, expires_at in all_files:
        size_kb = round(fsize / 1024, 1)
        date_str = uploaded_at[:10] if uploaded_at else '?'
        bot_link = get_bot_link(share_token)
        label = "(Owner)" if uid == OWNER_ID else ("(Admin)" if uid in admin_ids else "")
        exp_str = "Never"
        is_expired = False
        if expires_at:
            try:
                exp_dt = datetime.fromisoformat(expires_at)
                is_expired = datetime.now() > exp_dt
                exp_str = "EXPIRED" if is_expired else exp_dt.strftime('%Y-%m-%d %H:%M')
            except Exception:
                exp_str = expires_at[:16]
        link_line = f"`{bot_link}`" if (bot_link and not is_expired) else "❌ Expired"
        text += f"👤 `{uid}` {label}\n📄 `{fname}` ({size_kb}KB) [{date_str}] {'❌' if is_expired else '✅'}\n⏰ {exp_str}\n🤖 {link_line}\n\n"
        markup.add(types.InlineKeyboardButton(
            f"⬇️ DL: {fname[:25]} (uid:{uid})",
            callback_data=f'storage_dl_{storage_id}'
        ))
        markup.add(types.InlineKeyboardButton(
            f"🗑️ Del: {fname[:25]} (uid:{uid})",
            callback_data=f'storage_owner_del_{storage_id}'
        ))
    markup.add(types.InlineKeyboardButton("🔙 Back", callback_data='back_to_main'))
    if len(text) > 4000: text = text[:4000] + "\n...(truncated)"
    bot.send_message(call.message.chat.id, text, reply_markup=markup, parse_mode='Markdown')

def storage_owner_delete_callback(call):
    """Owner deletes any storage file record"""
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "⚠️ Owner only.", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    try:
        storage_id = int(call.data.split('_')[3])
        success, result = delete_storage_file_wrapper(storage_id, OWNER_ID)
        if success:
            bot.send_message(call.message.chat.id, f"🗑️ Owner deleted storage file record `{result}`.", parse_mode='Markdown')
        else:
            bot.send_message(call.message.chat.id, f"❌ Could not delete: {result}")
    except Exception as e:
        logger.error(f"storage_owner_delete_callback error: {e}", exc_info=True)
        bot.send_message(call.message.chat.id, "❌ Error deleting storage file.")

# =========================================================
# ========= OWNER PANEL: EXTENDED OVERVIEW  ===============
# =========================================================

# (owner_full_panel_callback is defined earlier with Worker Control Panel support)

# --- End Database Operations ---

# --- Menu creation (Inline and ReplyKeyboards) ---
def create_main_menu_inline(user_id):
    markup = types.InlineKeyboardMarkup(row_width=2)
    buttons = [
        types.InlineKeyboardButton('📢 Updates Channel', url=UPDATE_CHANNEL),
        types.InlineKeyboardButton('🐙 GitHub Repo', callback_data='github_repo'),
        types.InlineKeyboardButton('📤 Upload File', callback_data='upload'),
        types.InlineKeyboardButton('📂 Check Files', callback_data='check_files'),
        types.InlineKeyboardButton('⚡ Bot Speed', callback_data='speed'),
        types.InlineKeyboardButton('💾 Storage Info', callback_data='storage_info'),
        types.InlineKeyboardButton('🎁 Donate File', callback_data='donate_menu'),
        types.InlineKeyboardButton('🗄️ My Storage', callback_data='my_storage'),
        types.InlineKeyboardButton('📞 Contact Owner', url=f'https://t.me/{YOUR_USERNAME.replace("@", "")}')
    ]

    if user_id in admin_ids or user_id == OWNER_ID:
        pending_count = len(pending_approvals)
        markup.add(buttons[0], buttons[1])          # Updates, GitHub
        markup.add(buttons[2], buttons[3])          # Upload, Check Files
        markup.add(buttons[4], types.InlineKeyboardButton('💳 Subscriptions', callback_data='subscription'))
        markup.add(types.InlineKeyboardButton('📊 Statistics', callback_data='stats'),
                   types.InlineKeyboardButton('📢 Broadcast', callback_data='broadcast'))
        markup.add(types.InlineKeyboardButton('🔒 Lock Bot' if not bot_locked else '🔓 Unlock Bot',
                                              callback_data='lock_bot' if not bot_locked else 'unlock_bot'),
                   types.InlineKeyboardButton('🟢 Run All Scripts', callback_data='run_all_scripts'))
        if user_id == OWNER_ID:
            markup.add(types.InlineKeyboardButton('👑 Owner Panel', callback_data='owner_full_panel'),
                       types.InlineKeyboardButton('📁 All Users Files', callback_data='owner_all_files'))
            markup.add(types.InlineKeyboardButton(f'⏳ Pending Files ({pending_count})', callback_data='owner_pending_files'),
                       types.InlineKeyboardButton('🔌 Hosting Status', callback_data='hosting_status'))
        else:
            markup.add(types.InlineKeyboardButton('👑 Admin Panel', callback_data='admin_panel'),
                       types.InlineKeyboardButton('📁 All Users Files', callback_data='owner_all_files'))
            markup.add(types.InlineKeyboardButton(f'⏳ Pending Files ({pending_count})', callback_data='admin_pending_files'),
                       types.InlineKeyboardButton('🔌 Hosting Status', callback_data='hosting_status'))
        markup.add(buttons[5])                      # Storage Info
        markup.add(buttons[6], buttons[7])          # Donate File, My Storage
        markup.add(buttons[8])                      # Contact
    else:
        markup.add(buttons[0], buttons[1])          # Updates, GitHub
        markup.add(buttons[2], buttons[3])          # Upload, Check Files
        markup.add(buttons[4], buttons[5])          # Speed, Storage Info
        markup.add(types.InlineKeyboardButton('📊 Statistics', callback_data='stats'))
        markup.add(buttons[6], buttons[7])          # Donate File, My Storage
        markup.add(buttons[8])                      # Contact
    return markup

def create_reply_keyboard_main_menu(user_id):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    if user_id == OWNER_ID:
        layout_to_use = OWNER_COMMAND_BUTTONS_LAYOUT
    else:
        layout_to_use = COMMAND_BUTTONS_LAYOUT_USER_SPEC
    for row_buttons_text in layout_to_use:
        markup.add(*[types.KeyboardButton(text) for text in row_buttons_text])
    return markup

def create_control_buttons(script_owner_id, file_name, is_running=True): # Parameter renamed
    markup = types.InlineKeyboardMarkup(row_width=2)
    # Callbacks use script_owner_id
    if is_running:
        markup.row(
            types.InlineKeyboardButton("🔴 Stop", callback_data=f'stop_{script_owner_id}_{file_name}'),
            types.InlineKeyboardButton("🔄 Restart", callback_data=f'restart_{script_owner_id}_{file_name}')
        )
        markup.row(
            types.InlineKeyboardButton("🗑️ Delete", callback_data=f'delete_{script_owner_id}_{file_name}'),
            types.InlineKeyboardButton("📜 Logs", callback_data=f'logs_{script_owner_id}_{file_name}')
        )
    else:
        markup.row(
            types.InlineKeyboardButton("🟢 Start", callback_data=f'start_{script_owner_id}_{file_name}'),
            types.InlineKeyboardButton("🗑️ Delete", callback_data=f'delete_{script_owner_id}_{file_name}')
        )
        markup.row(
            types.InlineKeyboardButton("📜 View Logs", callback_data=f'logs_{script_owner_id}_{file_name}')
        )
    markup.add(types.InlineKeyboardButton("🔙 Back to Files", callback_data='check_files'))
    return markup

def create_admin_panel():
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.row(
        types.InlineKeyboardButton('➕ Add Admin', callback_data='add_admin'),
        types.InlineKeyboardButton('➖ Remove Admin', callback_data='remove_admin')
    )
    markup.row(types.InlineKeyboardButton('📋 List Admins', callback_data='list_admins'))
    # ✅ NEW: Hosting status button (Owner only)
    markup.row(types.InlineKeyboardButton('🔌 Hosting Status', callback_data='hosting_status'))
    markup.row(types.InlineKeyboardButton('🔙 Back to Main', callback_data='back_to_main'))
    return markup

def create_subscription_menu():
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.row(
        types.InlineKeyboardButton('➕ Add Subscription', callback_data='add_subscription'),
        types.InlineKeyboardButton('➖ Remove Subscription', callback_data='remove_subscription')
    )
    markup.row(types.InlineKeyboardButton('🔍 Check Subscription', callback_data='check_subscription'))
    markup.row(types.InlineKeyboardButton('🔙 Back to Main', callback_data='back_to_main'))
    return markup
# --- End Menu Creation ---

# --- File Handling ---
def handle_zip_file(downloaded_file_content, file_name_zip, message):
    """
    PART 1 — ZIP Support:
    Extract ZIP safely, send each .py/.js file to OWNER for review.
    No file is run automatically.
    """
    user_id = message.from_user.id
    user_name = message.from_user.first_name or f"User_{user_id}"
    user_folder = get_user_folder(user_id)
    temp_dir = None
    try:
        temp_dir = tempfile.mkdtemp(prefix=f"user_{user_id}_zip_")
        logger.info(f"Temp dir for zip: {temp_dir}")
        zip_path = os.path.join(temp_dir, file_name_zip)
        with open(zip_path, 'wb') as new_file:
            new_file.write(downloaded_file_content)

        # Safe extraction with path traversal protection
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for member in zip_ref.infolist():
                member_path = os.path.abspath(os.path.join(temp_dir, member.filename))
                if not member_path.startswith(os.path.abspath(temp_dir)):
                    raise zipfile.BadZipFile(f"Zip has unsafe path: {member.filename}")
            zip_ref.extractall(temp_dir)
            logger.info(f"Extracted zip to {temp_dir}")

        # Find all .py and .js files recursively
        py_js_files = []
        for root, dirs, files in os.walk(temp_dir):
            dirs[:] = [d for d in dirs if not d.startswith('.')]
            for f in files:
                ext = os.path.splitext(f)[1].lower()
                if ext in ['.py', '.js']:
                    rel_root = os.path.relpath(root, temp_dir)
                    rel_path = f if rel_root == '.' else os.path.join(rel_root, f)
                    full_path = os.path.join(root, f)
                    py_js_files.append((rel_path, ext, full_path))

        if not py_js_files:
            bot.reply_to(message, "❌ No `.py` or `.js` files found inside the ZIP archive!")
            return

        bot.reply_to(
            message,
            f"🗜️ *ZIP received!*\n\n"
            f"📄 Found `{len(py_js_files)}` script(s) inside.\n\n"
            f"⏳ Each file has been sent to the *Owner* for approval.\n"
            f"No files will be saved or run automatically — the owner must *Approve* each file individually.",
            parse_mode='Markdown'
        )

        # Notify owner about the ZIP upload
        try:
            bot.send_message(
                OWNER_ID,
                f"🗜️ *ZIP uploaded by user*\n\n"
                f"👤 User: `{user_id}` ({user_name})\n"
                f"📁 ZIP: `{file_name_zip}`\n"
                f"📄 Scripts found: `{len(py_js_files)}`\n\n"
                f"Each file is being sent for your *Approval* below — use ✅ Approve or ❌ Reject:",
                parse_mode='Markdown'
            )
        except Exception as e:
            logger.error(f"Failed to notify owner about ZIP: {e}")

        # Send each file to owner for approval (with Approve/Reject buttons)
        chat_id = message.chat.id
        for rel_path, ext, full_path in py_js_files:
            try:
                with open(full_path, 'rb') as f:
                    content = f.read()
                fname_only = os.path.basename(rel_path)
                send_approval_request_zip_entry(
                    approval_id=generate_approval_id(),
                    user_id=user_id,
                    user_name=user_name,
                    file_name=fname_only,
                    file_content_bytes=content,
                    chat_id=chat_id,
                    message_obj=message
                )
                time.sleep(0.3)
            except Exception as e:
                logger.error(f"Error sending ZIP entry '{rel_path}' to owner: {e}", exc_info=True)

        logger.info(f"ZIP from user {user_id}: {len(py_js_files)} files sent to owner for approval.")

    except zipfile.BadZipFile as e:
        logger.error(f"Bad zip file from {user_id}: {e}")
        bot.reply_to(message, f"❌ Error: Invalid/corrupted ZIP. {e}")
    except Exception as e:
        logger.error(f"❌ Error processing zip for {user_id}: {e}", exc_info=True)
        bot.reply_to(message, f"❌ Error processing zip: {str(e)}")
    finally:
        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
                logger.info(f"Cleaned temp dir: {temp_dir}")
            except Exception as e:
                logger.error(f"Failed to clean temp dir {temp_dir}: {e}", exc_info=True)

def handle_js_file(file_path, script_owner_id, user_folder, file_name, message):
    try:
        save_user_file(script_owner_id, file_name, 'js')
        threading.Thread(target=run_js_script, args=(file_path, script_owner_id, user_folder, file_name, message)).start()
    except Exception as e:
        logger.error(f"❌ Error processing JS file {file_name} for {script_owner_id}: {e}", exc_info=True)
        bot.reply_to(message, f"❌ Error processing JS file: {str(e)}")

def handle_py_file(file_path, script_owner_id, user_folder, file_name, message):
    try:
        save_user_file(script_owner_id, file_name, 'py')
        threading.Thread(target=run_script, args=(file_path, script_owner_id, user_folder, file_name, message)).start()
    except Exception as e:
        logger.error(f"❌ Error processing Python file {file_name} for {script_owner_id}: {e}", exc_info=True)
        bot.reply_to(message, f"❌ Error processing Python file: {str(e)}")
# --- End File Handling ---


# --- Logic Functions (called by commands and text handlers) ---
# ══════════════════════════════════════════════
# ✅ FORCE JOIN SYSTEM
# ══════════════════════════════════════════════

def is_user_joined(user_id):
    """Check if user has joined BOTH channel and group. Returns True/False."""
    joined_channel = False
    joined_group   = False
    try:
        member = bot.get_chat_member(f"@{FORCE_JOIN_CHANNEL}", user_id)
        if member.status in ("member", "administrator", "creator"):
            joined_channel = True
    except Exception as e:
        logger.warning(f"Force join channel check error for {user_id}: {e}")

    try:
        member = bot.get_chat_member(f"@{FORCE_JOIN_GROUP}", user_id)
        if member.status in ("member", "administrator", "creator"):
            joined_group = True
    except Exception as e:
        logger.warning(f"Force join group check error for {user_id}: {e}")

    return joined_channel and joined_group


def send_force_join_message(chat_id):
    """Send join prompt with inline buttons."""
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(
        types.InlineKeyboardButton("📢 Join Channel", url=f"https://t.me/{FORCE_JOIN_CHANNEL}"),
        types.InlineKeyboardButton("👥 Join Group",   url=f"https://t.me/{FORCE_JOIN_GROUP}"),
        types.InlineKeyboardButton("✅ Verify",        callback_data="check_join")
    )
    bot.send_message(
        chat_id,
        "🔒 *Access Restricted!*\n\n"
        "You must join our channel and group to use this bot.\n\n"
        "1️⃣ Join the channel below\n"
        "2️⃣ Join the group below\n"
        "3️⃣ Tap *✅ Verify* to unlock the bot\n\n"
        "━━━━━━━━━━━━━━━━━━━━",
        reply_markup=markup,
        parse_mode='Markdown'
    )


@bot.callback_query_handler(func=lambda call: call.data == "check_join")
def check_join_callback(call):
    """Verify button handler — check membership and unlock bot."""
    user_id = call.from_user.id
    if is_user_joined(user_id):
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception:
            pass
        bot.send_message(
            call.message.chat.id,
            "✅ *Verified! Welcome!*\n\n"
            "You have successfully joined.\n"
            "Bot is now unlocked for you! 🎉\n\n"
            "Type /start to begin.",
            parse_mode='Markdown'
        )
    else:
        bot.answer_callback_query(
            call.id,
            "❌ You haven't joined yet!\nPlease join BOTH channel and group first.",
            show_alert=True
        )


# ══════════════════════════════════════════════

def _logic_send_welcome(message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    user_name = message.from_user.first_name
    user_username = message.from_user.username

    logger.info(f"Welcome request from user_id: {user_id}, username: @{user_username}")

    if bot_locked and user_id not in admin_ids:
        bot.send_message(chat_id, "⚠️ Bot locked by admin. Try later.")
        return

    # ✅ FORCE JOIN CHECK — admins/owner bypass karte hain
    if user_id not in admin_ids and user_id != OWNER_ID:
        if not is_user_joined(user_id):
            send_force_join_message(chat_id)
            return

    user_bio = "Could not fetch bio"; photo_file_id = None
    try: user_bio = bot.get_chat(user_id).bio or "No bio"
    except Exception: pass
    try:
        user_profile_photos = bot.get_user_profile_photos(user_id, limit=1)
        if user_profile_photos.photos: photo_file_id = user_profile_photos.photos[0][-1].file_id
    except Exception: pass

    if user_id not in active_users:
        add_active_user(user_id)
        try:
            owner_notification = (f"🎉 New user!\n👤 Name: {user_name}\n✳️ User: @{user_username or 'N/A'}\n"
                                  f"🆔 ID: `{user_id}`\n📝 Bio: {user_bio}")
            bot.send_message(OWNER_ID, owner_notification, parse_mode='Markdown')
            if photo_file_id: bot.send_photo(OWNER_ID, photo_file_id, caption=f"Pic of new user {user_id}")
        except Exception as e: logger.error(f"⚠️ Failed to notify owner about new user {user_id}: {e}")

    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
    expiry_info = ""
    if user_id == OWNER_ID: user_status = "👑 Owner"
    elif user_id in admin_ids: user_status = "🛡️ Admin"
    elif user_id in user_subscriptions:
        expiry_date = user_subscriptions[user_id].get('expiry')
        if expiry_date and expiry_date > datetime.now():
            user_status = "⭐ Premium"; days_left = (expiry_date - datetime.now()).days
            expiry_info = f"\n⏳ Subscription expires in: {days_left} days"
        else: user_status = "🆓 Free User (Expired Sub)"; remove_subscription_db(user_id) # Clean up expired
    else: user_status = "🆓 Free User"

    welcome_msg_text = (f"〽️ Welcome, {user_name}!\n\n🆔 Your User ID: `{user_id}`\n"
                        f"✳️ Username: `@{user_username or 'Not set'}`\n"
                        f"🔰 Your Status: {user_status}{expiry_info}\n"
                        f"📁 Files Uploaded: {current_files} / {limit_str}\n\n"
                        f"🤖 Host & run Python (`.py`) or JS (`.js`) scripts.\n"
                        f"   Upload single scripts or `.zip` archives.\n\n"
                        f"👇 Use buttons or type commands.")
    main_reply_markup = create_reply_keyboard_main_menu(user_id)
    try:
        if photo_file_id: bot.send_photo(chat_id, photo_file_id)
        bot.send_message(chat_id, welcome_msg_text, reply_markup=main_reply_markup, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error sending welcome to {user_id}: {e}", exc_info=True)
        try: bot.send_message(chat_id, welcome_msg_text, reply_markup=main_reply_markup, parse_mode='Markdown') # Fallback without photo
        except Exception as fallback_e: logger.error(f"Fallback send_message failed for {user_id}: {fallback_e}")

def _logic_updates_channel(message):
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton('📢 Updates Channel', url=UPDATE_CHANNEL))
    bot.reply_to(message, "Visit our Updates Channel:", reply_markup=markup)

def _logic_upload_file(message):
    user_id = message.from_user.id
    if bot_locked and user_id not in admin_ids:
        bot.reply_to(message, "⚠️ Bot locked by admin, cannot accept files.")
        return

    # Removed free_mode check, relies on get_user_file_limit and FREE_USER_LIMIT
    # Users need to be admin or subscribed to upload if FREE_USER_LIMIT is 0
    # For now, FREE_USER_LIMIT > 0, so free users can upload up to that limit.
    # If we want to restrict free users entirely, set FREE_USER_LIMIT to 0.
    # For this implementation, free users get FREE_USER_LIMIT.

    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    if current_files >= file_limit:
        limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
        bot.reply_to(message, f"⚠️ File limit ({current_files}/{limit_str}) reached. Delete files first.")
        return
    bot.reply_to(message, "📤 Send your Python (`.py`), JS (`.js`), or ZIP (`.zip`) file.")

def _logic_check_files(message):
    user_id = message.from_user.id
    # chat_id = message.chat.id # user_id will be used as script_owner_id for buttons
    user_files_list = user_files.get(user_id, [])
    if not user_files_list:
        bot.reply_to(message, "📂 Your files:\n\n(No files uploaded yet)")
        return
    markup = types.InlineKeyboardMarkup(row_width=1)
    for file_name, file_type in sorted(user_files_list):
        is_running = is_bot_running(user_id, file_name) # Use user_id for checking status
        status_icon = "🟢 Running" if is_running else "🔴 Stopped"
        btn_text = f"{file_name} ({file_type}) - {status_icon}"
        # Callback data includes user_id as script_owner_id
        markup.add(types.InlineKeyboardButton(btn_text, callback_data=f'file_{user_id}_{file_name}'))
    bot.reply_to(message, "📂 Your files:\nClick to manage.", reply_markup=markup, parse_mode='Markdown')

def _logic_bot_speed(message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    start_time_ping = time.time()
    wait_msg = bot.reply_to(message, "🏃 Testing speed...")
    try:
        bot.send_chat_action(chat_id, 'typing')
        response_time = round((time.time() - start_time_ping) * 1000, 2)
        status = "🔓 Unlocked" if not bot_locked else "🔒 Locked"
        if user_id == OWNER_ID: user_level = "👑 Owner"
        elif user_id in admin_ids: user_level = "🛡️ Admin"
        elif user_id in user_subscriptions and user_subscriptions[user_id].get('expiry', datetime.min) > datetime.now(): user_level = "⭐ Premium"
        else: user_level = "🆓 Free User"
        # VPS/System info
        cpu_pct    = psutil.cpu_percent(interval=0.5)
        cpu_count  = psutil.cpu_count(logical=True)
        ram        = psutil.virtual_memory()
        disk       = psutil.disk_usage('/')
        boot_time  = psutil.boot_time()
        uptime_sec = int(time.time() - boot_time)
        uptime_str = f"{uptime_sec//86400}d {(uptime_sec%86400)//3600}h {(uptime_sec%3600)//60}m"
        ram_used   = round(ram.used / (1024**3), 2)
        ram_total  = round(ram.total / (1024**3), 2)
        ram_pct    = ram.percent
        disk_used  = round(disk.used / (1024**3), 2)
        disk_total = round(disk.total / (1024**3), 2)
        disk_pct   = disk.percent
        # Bot-level stats
        running_bots = sum(1 for k, v in list(bot_scripts.items()) if is_bot_running(int(k.split('_',1)[0]), v['file_name']))
        total_files = sum(len(f) for f in user_files.values())
        bot_storage_mb = round(get_total_storage_bytes() / (1024*1024), 2)
        # ✅ Worker Cluster Stats
        wcs = _get_worker_cluster_stats()
        worker_section = (
            f"\n━━━━━━ ☁️ Worker Cluster ━━━━━━\n"
            f"🟢 Online Workers : `{wcs['online']}` / `{wcs['total']}`\n"
            f"🔢 Total Cores    : `{wcs['total_cores']}`\n"
            f"⚡ Avg CPU        : `{wcs['avg_cpu']}%`\n"
            f"🧠 Avg RAM        : `{wcs['avg_ram']}%`\n"
            f"📦 Active Tasks   : `{wcs['active_tasks']}`\n"
            f"✅ Total Tasks Done: `{wcs['total_tasks_done']}`\n"
            f"☠️ Dead Workers   : `{wcs['dead']}`"
        ) if wcs['total'] > 0 else "\n━━━━━━ ☁️ Worker Cluster ━━━━━━\n⚠️ No workers connected yet."
        speed_msg = (
            f"⚡ *Bot Speed & VPS Stats*\n\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"⏱️ API Response: `{response_time} ms`\n"
            f"🚦 Bot Status: {status}\n"
            f"👤 Your Level: {user_level}\n\n"
            f"━━━━━━━ 🖥️ VPS Info ━━━━━━━\n"
            f"🔄 CPU Usage: `{cpu_pct}%` ({cpu_count} cores)\n"
            f"🧠 RAM: `{ram_used} / {ram_total} GB` ({ram_pct}%)\n"
            f"💽 Disk: `{disk_used} / {disk_total} GB` ({disk_pct}%)\n"
            f"⏰ Uptime: `{uptime_str}`\n"
            f"{worker_section}\n\n"
            f"━━━━━━ 🤖 Bot Stats ━━━━━━\n"
            f"👥 Total Users: `{len(active_users)}`\n"
            f"🟢 Running Bots: `{running_bots}`\n"
            f"📂 Total Files: `{total_files}`\n"
            f"💾 Bot Storage Used: `{bot_storage_mb} MB`"
        )
        bot.edit_message_text(speed_msg, chat_id, wait_msg.message_id, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error during speed test (cmd): {e}", exc_info=True)
        bot.edit_message_text("❌ Error during speed test.", chat_id, wait_msg.message_id)

def _logic_contact_owner(message):
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton('📞 Contact Owner', url=f'https://t.me/{YOUR_USERNAME.replace("@", "")}'))
    bot.reply_to(message, "Click to contact Owner:", reply_markup=markup)

def _logic_storage_info(message):
    """Public storage info - shows all users' storage (everyone can see)."""
    try:
        lines = ["💾 *Storage Usage (All Users)*\n"]
        lines.append(f"{'User ID':<14} {'Used MB':>10} {'Files':>6}")
        lines.append("─" * 34)
        total_mb = 0.0
        sorted_users = sorted(user_files.keys())
        for uid in sorted_users:
            used_mb = get_user_storage_mb(uid)
            total_mb += used_mb
            file_count = len(user_files.get(uid, []))
            label = "(Owner)" if uid == OWNER_ID else ("(Admin)" if uid in admin_ids else "")
            lines.append(f"`{uid}` {label:<8} {used_mb:>7} MB  {file_count:>4} files")
        lines.append("─" * 34)
        lines.append(f"📦 Total Used: `{round(total_mb, 2)} MB`")
        lines.append(f"👥 Total Users: `{len(sorted_users)}`")
        lines.append(f"\n⚠️ Limit per user: `{USER_STORAGE_LIMIT_MB} MB`")
        full_msg = "\n".join(lines)
        if len(full_msg) > 4000: full_msg = full_msg[:4000] + "\n... (truncated)"
        bot.reply_to(message, full_msg, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error in storage info: {e}", exc_info=True)
        bot.reply_to(message, "❌ Error fetching storage info.")

# --- Admin Logic Functions ---
def _logic_subscriptions_panel(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    bot.reply_to(message, "💳 Subscription Management\nUse inline buttons from /start or admin command menu.", reply_markup=create_subscription_menu())

def _logic_statistics(message):
    # No admin check here, allow all users but show admin-specific info if admin
    user_id = message.from_user.id
    total_users = len(active_users)
    total_files_records = sum(len(files) for files in user_files.values())

    running_bots_count = 0
    user_running_bots = 0

    for script_key_iter, script_info_iter in list(bot_scripts.items()):
        s_owner_id, _ = script_key_iter.split('_', 1) # Extract owner_id from key
        if is_bot_running(int(s_owner_id), script_info_iter['file_name']):
            running_bots_count += 1
            if int(s_owner_id) == user_id:
                user_running_bots +=1

    stats_msg_base = (f"📊 Bot Statistics:\n\n"
                      f"👥 Total Users: {total_users}\n"
                      f"📂 Total File Records: {total_files_records}\n"
                      f"🟢 Total Active Bots: {running_bots_count}\n")

    if user_id in admin_ids:
        stats_msg_admin = (f"🔒 Bot Status: {'🔴 Locked' if bot_locked else '🟢 Unlocked'}\n"
                           # f"💰 Free Mode: {'✅ ON' if free_mode else '❌ OFF'}\n" # Removed
                           f"🤖 Your Running Bots: {user_running_bots}")
        stats_msg = stats_msg_base + stats_msg_admin
    else:
        stats_msg = stats_msg_base + f"🤖 Your Running Bots: {user_running_bots}"

    bot.reply_to(message, stats_msg)


def _logic_broadcast_init(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    msg = bot.reply_to(message, "📢 Send message to broadcast to all active users.\n/cancel to abort.")
    bot.register_next_step_handler(msg, process_broadcast_message)

def _logic_toggle_lock_bot(message):
    global bot_locked  # declared at top before any use
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    bot_locked = not bot_locked
    status = "locked" if bot_locked else "unlocked"
    logger.warning(f"Bot {status} by Admin {message.from_user.id} via command/button.")
    bot.reply_to(message, f"🔒 Bot has been {status}.")

# def _logic_toggle_free_mode(message): # Removed
#     pass

def _logic_admin_panel(message):
    user_id = message.from_user.id
    if user_id != OWNER_ID:
        bot.reply_to(message, "⚠️ Sirf Owner ke liye hai!")
        return
    try:
        total_users   = get_db().users.count_documents({})
        active_subs   = get_db().subscriptions.count_documents({"expiry": {"$gt": datetime.now().isoformat()}})
        total_files   = get_db().user_files.count_documents({})
        storage_files = count_storage_files()
        donate_files  = count_donate_files()
        running_count = len(bot_scripts)
        pending_count = len(pending_approvals)

        text = (
            f"👑 *Owner Panel*\n\n"
            f"👥 Total Users: `{total_users}`\n"
            f"💳 Active Subscriptions: `{active_subs}`\n"
            f"📁 Hosted Files: `{total_files}`\n"
            f"📦 Storage Files: `{storage_files}`\n"
            f"🎁 Donated Files: `{donate_files}`\n"
            f"⏳ Pending Approvals: `{pending_count}`\n"
            f"▶️ Running Scripts: `{running_count}`\n\n"
            f"🤖 Bot: @{bot.get_me().username}\n"
            f"🟢 Server: Running OK"
        )
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton("📁 All Users Files", callback_data="owner_all_files"),
            types.InlineKeyboardButton("📊 Statistics", callback_data="stats"),
        )
        markup.add(
            types.InlineKeyboardButton("💳 Subscriptions", callback_data="subscription"),
            types.InlineKeyboardButton("📢 Broadcast", callback_data="broadcast"),
        )
        markup.add(
            types.InlineKeyboardButton("🔒 Lock Bot" if not bot_locked else "🔓 Unlock Bot",
                                       callback_data="lock_bot" if not bot_locked else "unlock_bot"),
            types.InlineKeyboardButton("🟢 Run All Scripts", callback_data="run_all_scripts"),
        )
        markup.add(
            types.InlineKeyboardButton("🔌 Hosting Status", callback_data="hosting_status"),
            types.InlineKeyboardButton("👑 View Donated Files", callback_data="owner_donate_view"),
        )
        markup.add(
            types.InlineKeyboardButton("📦 All Storage", callback_data="storage_owner_all"),
            types.InlineKeyboardButton(f"⏳ Pending Files ({pending_count})", callback_data="owner_pending_files"),
        )
        markup.add(
            types.InlineKeyboardButton("🔐 Encrypt File", callback_data="encrypt_file"),
            types.InlineKeyboardButton("🔓 Decrypt File", callback_data="decrypt_file"),
        )
        markup.add(
            types.InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main"),
        )
        bot.reply_to(message, text, reply_markup=markup)
    except Exception as e:
        logger.error(f"_logic_admin_panel (Owner Panel) error: {e}", exc_info=True)
        bot.reply_to(message, f"❌ Error loading Owner Panel: {e}")

def _logic_all_users_files(message):
    """Owner/Admin can see and control ALL users' files."""
    user_id = message.from_user.id
    if user_id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    all_files_found = False
    for target_uid, files_list in sorted(user_files.items()):
        if not files_list: continue
        all_files_found = True
        markup = types.InlineKeyboardMarkup(row_width=1)
        for file_name, file_type in sorted(files_list):
            is_running = is_bot_running(target_uid, file_name)
            status_icon = "🟢" if is_running else "🔴"
            markup.add(types.InlineKeyboardButton(
                f"{status_icon} {file_name} ({file_type})",
                callback_data=f'file_{target_uid}_{file_name}'
            ))
        label = "(Owner)" if target_uid == OWNER_ID else ("(Admin)" if target_uid in admin_ids else "")
        used_mb = get_user_storage_mb(target_uid)
        bot.send_message(message.chat.id,
            f"👤 User `{target_uid}` {label}\n💾 Storage: `{used_mb} MB`",
            reply_markup=markup, parse_mode='Markdown')
    if not all_files_found:
        bot.reply_to(message, "📂 No files found for any user.")

def _logic_run_all_scripts(message_or_call):
    if isinstance(message_or_call, telebot.types.Message):
        admin_user_id = message_or_call.from_user.id
        admin_chat_id = message_or_call.chat.id
        reply_func = lambda text, **kwargs: bot.reply_to(message_or_call, text, **kwargs)
        admin_message_obj_for_script_runner = message_or_call
    elif isinstance(message_or_call, telebot.types.CallbackQuery):
        admin_user_id = message_or_call.from_user.id
        admin_chat_id = message_or_call.message.chat.id
        bot.answer_callback_query(message_or_call.id)
        reply_func = lambda text, **kwargs: bot.send_message(admin_chat_id, text, **kwargs)
        admin_message_obj_for_script_runner = message_or_call.message 
    else:
        logger.error("Invalid argument for _logic_run_all_scripts")
        return

    if admin_user_id not in admin_ids:
        reply_func("⚠️ Admin permissions required.")
        return

    reply_func("⏳ Starting process to run all user scripts. This may take a while...")
    logger.info(f"Admin {admin_user_id} initiated 'run all scripts' from chat {admin_chat_id}.")

    started_count = 0; attempted_users = 0; skipped_files = 0; error_files_details = []

    # Use a copy of user_files keys and values to avoid modification issues during iteration
    all_user_files_snapshot = dict(user_files)

    for target_user_id, files_for_user in all_user_files_snapshot.items():
        if not files_for_user: continue
        attempted_users += 1
        logger.info(f"Processing scripts for user {target_user_id}...")
        user_folder = get_user_folder(target_user_id)

        for file_name, file_type in files_for_user:
            # script_owner_id for key context is target_user_id
            if not is_bot_running(target_user_id, file_name):
                file_path = os.path.join(user_folder, file_name)
                if os.path.exists(file_path):
                    logger.info(f"Admin {admin_user_id} attempting to start '{file_name}' ({file_type}) for user {target_user_id}.")
                    try:
                        if file_type == 'py':
                            threading.Thread(target=run_script, args=(file_path, target_user_id, user_folder, file_name, admin_message_obj_for_script_runner)).start()
                            started_count += 1
                        elif file_type == 'js':
                            threading.Thread(target=run_js_script, args=(file_path, target_user_id, user_folder, file_name, admin_message_obj_for_script_runner)).start()
                            started_count += 1
                        else:
                            logger.warning(f"Unknown file type '{file_type}' for {file_name} (user {target_user_id}). Skipping.")
                            error_files_details.append(f"`{file_name}` (User {target_user_id}) - Unknown type")
                            skipped_files += 1
                        time.sleep(0.7) # Increased delay slightly
                    except Exception as e:
                        logger.error(f"Error queueing start for '{file_name}' (user {target_user_id}): {e}")
                        error_files_details.append(f"`{file_name}` (User {target_user_id}) - Start error")
                        skipped_files += 1
                else:
                    logger.warning(f"File '{file_name}' for user {target_user_id} not found at '{file_path}'. Skipping.")
                    error_files_details.append(f"`{file_name}` (User {target_user_id}) - File not found")
                    skipped_files += 1
            # else: logger.info(f"Script '{file_name}' for user {target_user_id} already running.")

    summary_msg = (f"✅ All Users' Scripts - Processing Complete:\n\n"
                   f"▶️ Attempted to start: {started_count} scripts.\n"
                   f"👥 Users processed: {attempted_users}.\n")
    if skipped_files > 0:
        summary_msg += f"⚠️ Skipped/Error files: {skipped_files}\n"
        if error_files_details:
             summary_msg += "Details (first 5):\n" + "\n".join([f"  - {err}" for err in error_files_details[:5]])
             if len(error_files_details) > 5: summary_msg += "\n  ... and more (check logs)."

    reply_func(summary_msg, parse_mode='Markdown')
    logger.info(f"Run all scripts finished. Admin: {admin_user_id}. Started: {started_count}. Skipped/Errors: {skipped_files}")


# --- Command Handlers & Text Handlers for ReplyKeyboard ---
@bot.message_handler(commands=['start', 'help'])
def command_send_welcome(message):
    # Check for deep link: /start dl_TOKEN
    if message.text and len(message.text.split()) > 1:
        param = message.text.split()[1]
        if param.startswith('dl_'):
            token = param[3:]
            handle_storage_deep_link(message, token)
            return
    _logic_send_welcome(message)

def handle_storage_deep_link(message, token):
    """Handle Telegram bot deep link for storage file download"""
    try:
        row = get_storage_file_by_token(token)
        if not row:
            bot.reply_to(message, "❌ This file link is invalid or has been deleted.")
            return
        storage_id, owner_uid, fname, tg_file_id, ftype, fsize, share_token, uploaded_at, expires_at = row
        # Check expiry
        if expires_at:
            try:
                exp_dt = datetime.fromisoformat(expires_at)
                if datetime.now() > exp_dt:
                    bot.reply_to(message,
                        f"⏰ *Link Expired!*\n\n"
                        f"The download link for `{fname}` has expired.\n\n"
                        f"⚠️ This file's link is no longer valid.\n"
                        f"Ask the file owner to generate a new link from their storage.",
                        parse_mode='Markdown')
                    return
            except Exception:
                pass

        size_kb = round((fsize or 0) / 1024, 1)
        exp_str = "Never"
        warn_text = ""
        if expires_at:
            try:
                exp_dt = datetime.fromisoformat(expires_at)
                exp_str = exp_dt.strftime('%Y-%m-%d %H:%M')
                diff = exp_dt - datetime.now()
                hours_left = diff.total_seconds() / 3600
                if hours_left < 3:
                    warn_text = f"\n\n⚠️ *WARNING: Link expires in {int(hours_left)}h {int((hours_left%1)*60)}m!*"
            except Exception:
                pass

        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("⬇️ Download File", callback_data=f'storage_tg_dl_{token}'))

        bot.reply_to(message,
            f"📥 *File Available for Download*\n\n"
            f"📄 File: `{fname}`\n"
            f"📦 Size: `{size_kb} KB`\n"
            f"⏰ Link expires: `{exp_str}`"
            f"{warn_text}\n\n"
            f"Tap the button below to download:",
            reply_markup=markup, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"handle_storage_deep_link error: {e}", exc_info=True)
        bot.reply_to(message, "❌ Error fetching file info.")

@bot.message_handler(commands=['status']) # Kept for direct command
def command_show_status(message): _logic_statistics(message) # Changed to call _logic_statistics


def _logic_hosting_status(message):
    """Admin/Owner: Show hosting status panel from reply keyboard button"""
    user_id = message.from_user.id
    if user_id not in admin_ids and user_id != OWNER_ID:
        bot.reply_to(message, "⚠️ Admin/Owner only!")
        return
    status = is_hosting_enabled()
    status_text = "🟢 ENABLED" if status else "🔴 DISABLED"
    markup = types.InlineKeyboardMarkup()
    if user_id == OWNER_ID:
        if status:
            markup.add(types.InlineKeyboardButton("🔴 Turn OFF", callback_data='hosting_off'))
        else:
            markup.add(types.InlineKeyboardButton("🟢 Turn ON", callback_data='hosting_on'))
    markup.add(types.InlineKeyboardButton("↩️ Back", callback_data='admin_panel'))
    bot.reply_to(message,
        f"🔌 *Hosting Status*\n\n"
        f"Current Status: {status_text}\n\n"
        f"📋 When *OFF*:\n"
        f"  • No new files can be uploaded/hosted\n"
        f"  • All bot features work normally\n"
        f"  • Existing files cannot be started\n\n"
        f"When *ON*:\n"
        f"  • Full hosting functionality enabled\n"
        f"  • Users can upload and run files\n\n"
        f"{'⚠️ Only Owner can toggle hosting status.' if user_id != OWNER_ID else ''}",
        reply_markup=markup, parse_mode='Markdown')


# ============================================================================
# 🎁 DONATE FILE FEATURE — All functions
# ============================================================================

def _logic_donate_file(message):
    """Show donate file menu."""
    user_id = message.from_user.id
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("📤 Send a Donate File", callback_data='donate_send'),
        types.InlineKeyboardButton("📥 Receive Donate Files", callback_data='donate_receive'),
    )
    if user_id == OWNER_ID:
        markup.add(types.InlineKeyboardButton("👑 View All Donated Files", callback_data='owner_donate_view'))
    markup.add(types.InlineKeyboardButton("🔙 Back", callback_data='back_to_main'))
    total = count_donate_files()
    bot.reply_to(
        message,
        f"🎁 *Donate File*\n\n"
        f"Share useful files with everyone!\n"
        f"📦 Total donated files: `{total}`\n\n"
        f"• *Send* — Upload a file for others to download\n"
        f"• *Receive* — Browse & download donated files",
        reply_markup=markup, parse_mode='Markdown'
    )


def donate_send_callback(call):
    """Ask user to send a file to donate."""
    bot.answer_callback_query(call.id)
    msg = bot.send_message(
        call.message.chat.id,
        "📤 *Send a Donate File*\n\n"
        "Send any file (document, photo, video, audio) to donate it.\n"
        "It will be available for all users to download.\n\n"
        "/cancel to abort.",
        parse_mode='Markdown'
    )
    bot.register_next_step_handler(msg, process_donate_file_upload)


def process_donate_file_upload(message):
    """Handle the donated file upload."""
    if message.text and message.text.strip().lower() == '/cancel':
        bot.reply_to(message, "❌ Cancelled.")
        return

    user_id = message.from_user.id
    donor_name = message.from_user.first_name or "Unknown"

    file_id = None
    file_name = "unknown_file"
    file_size = 0

    if message.document:
        file_id = message.document.file_id
        file_name = message.document.file_name or "document"
        file_size = message.document.file_size or 0
    elif message.photo:
        file_id = message.photo[-1].file_id
        file_name = f"photo_{int(time.time())}.jpg"
        file_size = message.photo[-1].file_size or 0
    elif message.video:
        file_id = message.video.file_id
        file_name = message.video.file_name or f"video_{int(time.time())}.mp4"
        file_size = message.video.file_size or 0
    elif message.audio:
        file_id = message.audio.file_id
        file_name = message.audio.file_name or f"audio_{int(time.time())}.mp3"
        file_size = message.audio.file_size or 0
    else:
        bot.reply_to(message, "❌ Please send a valid file (document, photo, video, or audio).")
        return

    try:
        save_donate_file_db(user_id, donor_name, file_name, file_id, file_size)
        size_kb = round(file_size / 1024, 1)
        bot.reply_to(
            message,
            f"✅ *File Donated Successfully!*\n\n"
            f"📄 File: `{file_name}`\n"
            f"💾 Size: `{size_kb} KB`\n\n"
            f"Thank you for contributing! 🎉",
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.error(f"donate upload error: {e}", exc_info=True)
        bot.reply_to(message, f"❌ Error saving donated file: {e}")


def donate_receive_callback(call):
    """Show list of donated files."""
    bot.answer_callback_query(call.id)
    files = get_all_donate_files()
    if not files:
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🔙 Back", callback_data='donate_menu'))
        bot.send_message(call.message.chat.id, "📭 No donated files yet. Be the first to donate!", reply_markup=markup)
        return

    markup = types.InlineKeyboardMarkup(row_width=1)
    for f in files[:20]:  # max 20
        donate_id = str(f.get('_id', ''))
        fname = f.get('file_name', 'file')
        donor = f.get('donor_name', 'Unknown')
        size_kb = round(f.get('file_size', 0) / 1024, 1)
        btn_label = f"📄 {fname[:30]} — {donor} ({size_kb}KB)"
        markup.add(types.InlineKeyboardButton(btn_label, callback_data=f'donate_dl_{donate_id}'))

    markup.add(types.InlineKeyboardButton("🔙 Back", callback_data='donate_menu'))
    bot.send_message(
        call.message.chat.id,
        f"📥 *Donated Files* — {len(files)} total\n\nTap a file to download:",
        reply_markup=markup, parse_mode='Markdown'
    )


def donate_download_callback(call):
    """Send the donated file to the user."""
    bot.answer_callback_query(call.id, "📥 Sending file...")
    donate_id = call.data.split('donate_dl_')[1]
    try:
        file_info = get_donate_file_by_id(donate_id)
        if not file_info:
            bot.send_message(call.message.chat.id, "❌ File not found.")
            return
        _, donor_uid, donor_name, file_name, telegram_file_id, file_size, donated_at = file_info
        size_kb = round((file_size or 0) / 1024, 1)
        caption = (
            f"🎁 *Donated File*\n"
            f"📄 `{file_name}`\n"
            f"👤 Donor: {donor_name}\n"
            f"💾 Size: {size_kb} KB"
        )
        markup = types.InlineKeyboardMarkup()
        if call.from_user.id == OWNER_ID or call.from_user.id == donor_uid:
            markup.add(types.InlineKeyboardButton("🗑 Delete", callback_data=f'donate_self_del_{donate_id}'))
        try:
            bot.send_document(call.message.chat.id, telegram_file_id, caption=caption, parse_mode='Markdown', reply_markup=markup)
        except Exception:
            bot.send_message(call.message.chat.id, "❌ Could not send file. It may have been deleted from Telegram.")
    except Exception as e:
        logger.error(f"donate_download_callback error: {e}", exc_info=True)
        bot.send_message(call.message.chat.id, f"❌ Error: {e}")


def owner_view_donate_files_callback(call):
    """Owner: view all donated files with delete option."""
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "⚠️ Owner only!", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    files = get_all_donate_files()
    if not files:
        bot.send_message(call.message.chat.id, "📭 No donated files.")
        return
    markup = types.InlineKeyboardMarkup(row_width=1)
    for f in files[:20]:
        donate_id = str(f.get('_id', ''))
        fname = f.get('file_name', 'file')
        donor = f.get('donor_name', 'Unknown')
        markup.add(types.InlineKeyboardButton(f"🗑 {fname[:25]} — {donor}", callback_data=f'owner_donate_del_{donate_id}'))
    markup.add(types.InlineKeyboardButton("🔙 Back", callback_data='donate_menu'))
    bot.send_message(
        call.message.chat.id,
        f"👑 *All Donated Files* — {len(files)} total\n\nTap to delete:",
        reply_markup=markup, parse_mode='Markdown'
    )


def owner_delete_donate_callback(call):
    """Owner: delete a donated file."""
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "⚠️ Owner only!", show_alert=True)
        return
    donate_id = call.data.split('owner_donate_del_')[1]
    success, result = delete_donate_file_db(donate_id, call.from_user.id, OWNER_ID)
    if success:
        bot.answer_callback_query(call.id, f"✅ Deleted: {result}", show_alert=True)
        try:
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        except Exception:
            pass
    else:
        bot.answer_callback_query(call.id, f"❌ {result}", show_alert=True)


def donor_self_delete_donate_callback(call):
    """Donor or Owner: delete their own donated file."""
    donate_id = call.data.split('donate_self_del_')[1]
    success, result = delete_donate_file_db(donate_id, call.from_user.id, OWNER_ID)
    if success:
        bot.answer_callback_query(call.id, f"✅ Deleted: {result}", show_alert=True)
        try:
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        except Exception:
            pass
    else:
        bot.answer_callback_query(call.id, f"❌ {result}", show_alert=True)


BUTTON_TEXT_TO_LOGIC = {
    "📢 Updates Channel": _logic_updates_channel,
    "🐙 GitHub Repo": github_ask_repo,
    "📤 Upload File": _logic_upload_file,
    "📂 Check Files": _logic_check_files,
    "⚡ Bot Speed": _logic_bot_speed,
    "💾 Storage Info": _logic_storage_info,
    "📞 Contact Owner": _logic_contact_owner,
    "📊 Statistics": _logic_statistics,
    "💳 Subscriptions": _logic_subscriptions_panel,
    "📢 Broadcast": _logic_broadcast_init,
    "🔒 Lock Bot": _logic_toggle_lock_bot,
    "🟢 Running All Code": _logic_run_all_scripts,
    "📁 All Users Files": _logic_all_users_files,
    "👑 Owner Panel": _logic_admin_panel,
    "🎁 Donate File": _logic_donate_file,
    "🗄️ My Storage": _logic_my_storage,
    "🔌 Hosting Status": _logic_hosting_status,
    # ⏳ Pending Files registered below after _logic_pending_files is defined (line ~6324)
}

@bot.message_handler(func=lambda message: message.text in BUTTON_TEXT_TO_LOGIC)
def handle_button_text(message):
    # DDoS protection
    if is_rate_limited(message.from_user.id):
        try: bot.reply_to(message, "🛡️ Slow down! Too many requests. Try again shortly.")
        except Exception: pass
        return
    logic_func = BUTTON_TEXT_TO_LOGIC.get(message.text)
    if logic_func: logic_func(message)
    else: logger.warning(f"Button text '{message.text}' matched but no logic func.")

@bot.message_handler(commands=['storage'])
def command_storage(message): _logic_storage_info(message)
@bot.message_handler(commands=['allfiles'])
def command_all_files(message): _logic_all_users_files(message)

@bot.message_handler(commands=['updateschannel'])
def command_updates_channel(message): _logic_updates_channel(message)
@bot.message_handler(commands=['uploadfile'])
def command_upload_file(message): _logic_upload_file(message)
@bot.message_handler(commands=['checkfiles'])
def command_check_files(message): _logic_check_files(message)
@bot.message_handler(commands=['botspeed'])
def command_bot_speed(message): _logic_bot_speed(message)
@bot.message_handler(commands=['contactowner'])
def command_contact_owner(message): _logic_contact_owner(message)
@bot.message_handler(commands=['subscriptions'])
def command_subscriptions(message): _logic_subscriptions_panel(message)
@bot.message_handler(commands=['statistics']) # Alias for /status
def command_statistics(message): _logic_statistics(message)
@bot.message_handler(commands=['broadcast'])
def command_broadcast(message): _logic_broadcast_init(message)
@bot.message_handler(commands=['lockbot']) 
def command_lock_bot(message): _logic_toggle_lock_bot(message)
# @bot.message_handler(commands=['freemode']) # Removed
# def command_free_mode(message): _logic_toggle_free_mode(message)
@bot.message_handler(commands=['adminpanel'])
def command_admin_panel(message): _logic_admin_panel(message)
@bot.message_handler(commands=['runningallcode']) # Added
def command_run_all_code(message): _logic_run_all_scripts(message)

@bot.message_handler(commands=['github'])
def command_github(message): github_ask_repo(message)

@bot.message_handler(commands=['donate'])
def command_donate(message): _logic_donate_file(message)

@bot.message_handler(commands=['mystorage'])
def command_my_storage(message): _logic_my_storage(message)

# ✅ PART 3 — Encrypt/Decrypt commands (Owner only)
@bot.message_handler(commands=['encrypt'])
def command_encrypt(message):
    if message.from_user.id != OWNER_ID:
        bot.reply_to(message, "⚠️ Owner only.")
        return
    handle_encrypt_flow(message)

@bot.message_handler(commands=['decrypt'])
def command_decrypt(message):
    if message.from_user.id != OWNER_ID:
        bot.reply_to(message, "⚠️ Owner only.")
        return
    handle_decrypt_flow(message)


@bot.message_handler(commands=['ping'])
def ping(message):
    start_ping_time = time.time() 
    msg = bot.reply_to(message, "Pong!")
    latency = round((time.time() - start_ping_time) * 1000, 2)
    bot.edit_message_text(f"Pong! Latency: {latency} ms", message.chat.id, msg.message_id)


# --- Document (File) Handler ---
@bot.message_handler(content_types=['document'])
def handle_file_upload_doc(message): # Renamed
    user_id = message.from_user.id
    chat_id = message.chat.id # Used for replies, script context uses user_id

    # DDoS protection
    if is_rate_limited(user_id):
        try: bot.reply_to(message, "🛡️ Slow down! You're sending too fast. Try again in a few minutes.")
        except Exception: pass
        return

    doc = message.document
    logger.info(f"Doc from {user_id}: {doc.file_name} ({doc.mime_type}), Size: {doc.file_size}")

    # ✅ NEW: Check if hosting is enabled
    if not is_hosting_enabled() and user_id != OWNER_ID:
        bot.reply_to(message, "🔴 *Hosting is currently disabled!*\n\nFile uploads are temporarily unavailable.\nPlease contact the owner.",
                    parse_mode='Markdown')
        return

    if bot_locked and user_id not in admin_ids:
        bot.reply_to(message, "⚠️ Bot locked, cannot accept files.")
        return

    # File limit check (relies on FREE_USER_LIMIT being > 0 for free users)
    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    if current_files >= file_limit:
        limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
        bot.reply_to(message, f"⚠️ File limit ({current_files}/{limit_str}) reached. Delete files via /checkfiles.")
        return

    # Storage limit check (500MB per user, except owner)
    if user_id != OWNER_ID:
        used_mb = get_user_storage_mb(user_id)
        if used_mb >= USER_STORAGE_LIMIT_MB:
            bot.reply_to(message, f"⚠️ Storage limit reached! Used: {used_mb} MB / {USER_STORAGE_LIMIT_MB} MB. Delete files first.")
            return

    file_name = doc.file_name
    if not file_name: bot.reply_to(message, "⚠️ No file name. Ensure file has a name."); return
    file_ext = os.path.splitext(file_name)[1].lower()
    if file_ext not in ['.py', '.js', '.zip', '.enc']:
        bot.reply_to(message, "⚠️ Unsupported type! Only `.py`, `.js`, `.zip`, `.enc` allowed.")
        return
    max_file_size = 20 * 1024 * 1024 # 20 MB
    if doc.file_size > max_file_size:
        bot.reply_to(message, f"⚠️ File too large (Max: {max_file_size // 1024 // 1024} MB)."); return

    try:
        try:
            bot.forward_message(OWNER_ID, chat_id, message.message_id)
            bot.send_message(OWNER_ID, f"⬆️ File '{file_name}' from {message.from_user.first_name} (`{user_id}`)", parse_mode='Markdown')
        except Exception as e: logger.error(f"Failed to forward uploaded file to OWNER_ID {OWNER_ID}: {e}")

        download_wait_msg = bot.reply_to(message, f"⏳ Downloading `{file_name}`...")
        file_info_tg_doc = bot.get_file(doc.file_id) # Renamed
        downloaded_file_content = bot.download_file(file_info_tg_doc.file_path)
        bot.edit_message_text(f"✅ Downloaded `{file_name}`. Processing...", chat_id, download_wait_msg.message_id)
        logger.info(f"Downloaded {file_name} for user {user_id}")
        user_folder = get_user_folder(user_id)

        if file_ext == '.zip':
            # ZIP: extract safely, send each file to owner for review, no auto-run
            handle_zip_file(downloaded_file_content, file_name, message)
        elif file_ext == '.enc':
            # ✅ PART 3 — .enc file: send to owner for approval (with auto-decrypt on approve)
            approval_id = generate_approval_id()
            user_name = message.from_user.first_name or f"User_{user_id}"
            pending_approvals[approval_id] = {
                'user_id': user_id,
                'chat_id': chat_id,
                'file_name': file_name,
                'file_ext': file_ext,
                'file_content': downloaded_file_content,
                'message_obj': message,
            }
            bot.edit_message_text(
                f"✅ Downloaded `{file_name}`.\n\n"
                f"🔒 *Encrypted File Detected!*\n\n"
                f"⏳ *Waiting for Owner Approval...*\n"
                f"The encrypted file has been sent to the owner for review.\n"
                f"If approved, it will be auto-decrypted and run.",
                chat_id, download_wait_msg.message_id, parse_mode='Markdown'
            )
            send_approval_request_single(
                approval_id=approval_id,
                user_id=user_id,
                user_name=user_name,
                file_name=file_name,
                file_ext=file_ext,
                file_content_bytes=downloaded_file_content
            )
            logger.info(f"Encrypted file '{file_name}' from user {user_id} sent to owner for approval. approval_id={approval_id}")
        else:
            # ✅ PART 1 — INTERCEPT: Hold file in memory, send to owner for approval
            # File is NOT saved to disk and NOT run until owner approves
            approval_id = generate_approval_id()
            user_name = message.from_user.first_name or f"User_{user_id}"

            pending_approvals[approval_id] = {
                'user_id': user_id,
                'chat_id': chat_id,
                'file_name': file_name,
                'file_ext': file_ext,
                'file_content': downloaded_file_content,  # original bytes — unchanged
                'message_obj': message,
            }

            # Notify user that file is pending approval
            bot.edit_message_text(
                f"✅ Downloaded `{file_name}`.\n\n"
                f"⏳ *Waiting for Owner Approval...*\n"
                f"Your file has been sent to the owner for review.\n"
                f"It will NOT run until the owner approves it.",
                chat_id,
                download_wait_msg.message_id,
                parse_mode='Markdown'
            )

            # Send original file to owner with Approve/Reject buttons
            send_approval_request_single(
                approval_id=approval_id,
                user_id=user_id,
                user_name=user_name,
                file_name=file_name,
                file_ext=file_ext,
                file_content_bytes=downloaded_file_content  # original, unmodified
            )

            logger.info(f"File '{file_name}' from user {user_id} sent to owner for approval. approval_id={approval_id}")
    except telebot.apihelper.ApiTelegramException as e:
         logger.error(f"Telegram API Error handling file for {user_id}: {e}", exc_info=True)
         if "file is too big" in str(e).lower():
              bot.reply_to(message, f"❌ Telegram API Error: File too large to download (~20MB limit).")
         else: bot.reply_to(message, f"❌ Telegram API Error: {str(e)}. Try later.")
    except Exception as e:
        logger.error(f"❌ General error handling file for {user_id}: {e}", exc_info=True)
        bot.reply_to(message, f"❌ Unexpected error: {str(e)}")
# --- End Document Handler ---


# --- Callback Query Handlers (for Inline Buttons) ---
@bot.callback_query_handler(func=lambda call: True) 
def handle_callbacks(call):
    user_id = call.from_user.id
    data = call.data
    logger.info(f"Callback: User={user_id}, Data='{data}'")

    if bot_locked and user_id not in admin_ids and data not in ['back_to_main', 'speed', 'stats']: # Allow stats
        bot.answer_callback_query(call.id, "⚠️ Bot locked by admin.", show_alert=True)
        return
    try:
        if data == 'upload': upload_callback(call)
        elif data == 'check_files': check_files_callback(call)
        # ✅ PART 1 — Owner Approval callbacks
        elif data.startswith('approve_'): handle_approve_callback(call)
        elif data.startswith('reject_'): handle_reject_callback(call)
        elif data == 'storage_info': storage_info_callback(call)
        elif data == 'owner_all_files': admin_required_callback(call, owner_all_files_callback)
        elif data.startswith('file_'): file_control_callback(call)
        elif data.startswith('start_'): start_bot_callback(call)
        elif data.startswith('stop_'): stop_bot_callback(call)
        elif data.startswith('restart_'): restart_bot_callback(call)
        elif data.startswith('delete_'): delete_bot_callback(call)
        elif data.startswith('logs_'): logs_bot_callback(call)
        elif data == 'speed': speed_callback(call)
        elif data == 'back_to_main': back_to_main_callback(call)
        elif data.startswith('confirm_broadcast_'): handle_confirm_broadcast(call)
        elif data == 'cancel_broadcast': handle_cancel_broadcast(call)
        # --- GitHub Repo Feature ---
        elif data == 'github_repo':
            bot.answer_callback_query(call.id)
            github_ask_repo(call.message)
        elif data.startswith('github_run_all_'): github_run_all_for_user_callback(call)
        # --- Donate File Feature ---
        elif data == 'donate_menu':
            bot.answer_callback_query(call.id)
            _logic_donate_file(call.message)
        elif data == 'donate_send': donate_send_callback(call)
        elif data == 'donate_receive': donate_receive_callback(call)
        elif data.startswith('donate_dl_'): donate_download_callback(call)
        elif data == 'owner_donate_view': owner_view_donate_files_callback(call)
        elif data.startswith('owner_donate_del_'): owner_delete_donate_callback(call)
        elif data.startswith('donate_self_del_'): donor_self_delete_donate_callback(call)
        # --- My Storage Feature ---
        elif data == 'my_storage':
            bot.answer_callback_query(call.id)
            _logic_my_storage_callback(call)
        elif data == 'storage_upload': storage_upload_callback(call)
        elif data.startswith('storage_exp_'): storage_expiry_chosen_callback(call)
        elif data == 'storage_list': storage_list_callback(call)
        elif data.startswith('storage_dl_'): storage_download_callback(call)
        elif data.startswith('storage_regen_exp_'): storage_regen_expiry_callback(call)
        elif data.startswith('storage_regen_'): storage_regen_link_callback(call)
        elif data.startswith('storage_del_'): storage_delete_callback(call)
        elif data == 'storage_owner_all': storage_owner_all_callback(call)
        elif data.startswith('storage_owner_del_'): storage_owner_delete_callback(call)
        # --- Telegram deep link download ---
        elif data.startswith('storage_tg_dl_'):
            token = data.split('storage_tg_dl_')[1]
            handle_storage_tg_download(call, token)
        # --- Admin download user bot file ---
        elif data.startswith('admin_dl_file_'):
            admin_download_user_file_callback(call)
        # --- Owner Full Panel ---
        elif data == 'owner_full_panel': owner_full_panel_callback(call)
        elif data == 'owner_pending_files': owner_pending_files_callback(call)
        # --- Admin Callbacks ---
        elif data == 'subscription': admin_required_callback(call, subscription_management_callback)
        elif data == 'stats': stats_callback(call) # No admin check here, handled in func
        elif data == 'lock_bot': admin_required_callback(call, lock_bot_callback)
        elif data == 'unlock_bot': admin_required_callback(call, unlock_bot_callback)
        elif data == 'run_all_scripts': admin_required_callback(call, run_all_scripts_callback)
        elif data == 'broadcast': admin_required_callback(call, broadcast_init_callback) 
        elif data == 'admin_panel': admin_required_callback(call, admin_panel_callback)
        elif data == 'add_admin': owner_required_callback(call, add_admin_init_callback) 
        elif data == 'remove_admin': owner_required_callback(call, remove_admin_init_callback) 
        elif data == 'list_admins': admin_required_callback(call, list_admins_callback)
        elif data == 'add_subscription': admin_required_callback(call, add_subscription_init_callback) 
        elif data == 'remove_subscription': admin_required_callback(call, remove_subscription_init_callback) 
        elif data == 'check_subscription': admin_required_callback(call, check_subscription_init_callback)
        # ✅ NEW: Hosting status (Owner only)
        elif data == 'hosting_status': admin_required_callback(call, hosting_status_callback)
        elif data == 'hosting_off': hosting_off_callback(call)
        elif data == 'hosting_on': hosting_on_callback(call)
        # ✅ Pending Files — Admin sees all, Owner has full approve/reject
        elif data == 'admin_pending_files': admin_required_callback(call, admin_pending_files_callback)
        elif data == 'owner_pending_files': owner_pending_files_callback(call)
        else:
            bot.answer_callback_query(call.id, "Unknown action.")
            logger.warning(f"Unhandled callback data: {data} from user {user_id}")
    except Exception as e:
        logger.error(f"Error handling callback '{data}' for {user_id}: {e}", exc_info=True)
        try: bot.answer_callback_query(call.id, "Error processing request.", show_alert=True)
        except Exception as e_ans: logger.error(f"Failed to answer callback after error: {e_ans}")

def admin_required_callback(call, func_to_run):
    if call.from_user.id not in admin_ids and call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    func_to_run(call) 

def owner_required_callback(call, func_to_run):
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "⚠️ Owner permissions required.", show_alert=True)
        return
    func_to_run(call)

def upload_callback(call):
    user_id = call.from_user.id
    # Removed free_mode check
    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    if current_files >= file_limit:
        limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
        bot.answer_callback_query(call.id, f"⚠️ File limit ({current_files}/{limit_str}) reached.", show_alert=True)
        return
    bot.answer_callback_query(call.id) 
    bot.send_message(call.message.chat.id, "📤 Send your Python (`.py`), JS (`.js`), or ZIP (`.zip`) file.")

def check_files_callback(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id 
    user_files_list = user_files.get(user_id, [])
    if not user_files_list:
        bot.answer_callback_query(call.id, "⚠️ No files uploaded.", show_alert=True)
        try:
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("🔙 Back to Main", callback_data='back_to_main'))
            bot.edit_message_text("📂 Your files:\n\n(No files uploaded)", chat_id, call.message.message_id, reply_markup=markup)
        except Exception as e: logger.error(f"Error editing msg for empty file list: {e}")
        return
    bot.answer_callback_query(call.id) 
    markup = types.InlineKeyboardMarkup(row_width=1) 
    for file_name, file_type in sorted(user_files_list): 
        is_running = is_bot_running(user_id, file_name) # Use user_id for status check
        status_icon = "🟢 Running" if is_running else "🔴 Stopped"
        btn_text = f"{file_name} ({file_type}) - {status_icon}"
        # Callback includes user_id as script_owner_id
        markup.add(types.InlineKeyboardButton(btn_text, callback_data=f'file_{user_id}_{file_name}'))
    markup.add(types.InlineKeyboardButton("🔙 Back to Main", callback_data='back_to_main'))
    try:
        bot.edit_message_text("📂 Your files:\nClick to manage.", chat_id, call.message.message_id, reply_markup=markup, parse_mode='Markdown')
    except telebot.apihelper.ApiTelegramException as e:
         if "message is not modified" in str(e): logger.warning("Msg not modified (files).")
         else: logger.error(f"Error editing msg for file list: {e}")
    except Exception as e: logger.error(f"Unexpected error editing msg for file list: {e}", exc_info=True)

def file_control_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id

        # Allow owner/admin to control any file, or user to control their own
        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            logger.warning(f"User {requesting_user_id} tried to access file '{file_name}' of user {script_owner_id} without permission.")
            bot.answer_callback_query(call.id, "⚠️ You can only manage your own files.", show_alert=True)
            check_files_callback(call) # Show their own files
            return

        user_files_list = user_files.get(script_owner_id, [])
        if not any(f[0] == file_name for f in user_files_list):
            logger.warning(f"File '{file_name}' not found for user {script_owner_id} during control.")
            bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True)
            # If admin was viewing, this might be confusing. For now, just show their own.
            check_files_callback(call) 
            return

        bot.answer_callback_query(call.id) 
        is_running = is_bot_running(script_owner_id, file_name)
        status_text = '🟢 Running' if is_running else '🔴 Stopped'
        file_type = next((f[1] for f in user_files_list if f[0] == file_name), '?') 
        try:
            bot.edit_message_text(
                f"⚙️ Controls for: `{file_name}` ({file_type}) of User `{script_owner_id}`\nStatus: {status_text}",
                call.message.chat.id, call.message.message_id,
                reply_markup=create_control_buttons(script_owner_id, file_name, is_running),
                parse_mode='Markdown'
            )
        except telebot.apihelper.ApiTelegramException as e:
             if "message is not modified" in str(e): logger.warning(f"Msg not modified (controls for {file_name})")
             else: raise 
    except (ValueError, IndexError) as ve:
        logger.error(f"Error parsing file control callback: {ve}. Data: '{call.data}'")
        bot.answer_callback_query(call.id, "Error: Invalid action data.", show_alert=True)
    except Exception as e:
        logger.error(f"Error in file_control_callback for data '{call.data}': {e}", exc_info=True)
        bot.answer_callback_query(call.id, "An error occurred.", show_alert=True)

def start_bot_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id
        chat_id_for_reply = call.message.chat.id # Where the admin/user gets the reply

        logger.info(f"Start request: Requester={requesting_user_id}, Owner={script_owner_id}, File='{file_name}'")

        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ Permission denied to start this script.", show_alert=True); return

        # ✅ NEW: Check if hosting is enabled (allow owner to bypass)
        if not is_hosting_enabled() and requesting_user_id != OWNER_ID:
            bot.answer_callback_query(call.id, "🔴 Hosting is disabled! Cannot start files.", show_alert=True)
            return

        # ✅ FIX 2 — RUN PERMISSION: Non-admin users need owner approval to run
        if requesting_user_id not in admin_ids:
            user_folder_check = get_user_folder(script_owner_id)
            file_path_check = os.path.join(user_folder_check, file_name)
            run_req_id = generate_approval_id()
            pending_run_requests[run_req_id] = {
                'user_id': requesting_user_id,
                'chat_id': chat_id_for_reply,
                'script_owner_id': script_owner_id,
                'file_name': file_name,
                'file_path': file_path_check,
                'call_message': call.message,
                'timestamp': datetime.now().isoformat()
            }
            bot.answer_callback_query(call.id, "⏳ Run request sent to Owner for approval!")
            # Notify user
            try:
                bot.send_message(
                    chat_id_for_reply,
                    f"⏳ *Run Permission Requested!*\n\n"
                    f"Script: `{file_name}`\n\n"
                    f"Owner ka approve aane ke baad hi script chalegi.",
                    parse_mode='Markdown'
                )
            except Exception:
                pass
            # Quick scan of script
            ai_risk = "✅ No obvious issues detected."
            try:
                if os.path.exists(file_path_check):
                    with open(file_path_check, 'rb') as _f:
                        _script_bytes = _f.read()
                    dangerous_keywords = [b'os.system', b'subprocess', b'eval(', b'exec(',
                                          b'__import__', b'socket', b'shutil.rmtree']
                    found = [kw.decode() for kw in dangerous_keywords if kw in _script_bytes]
                    if found:
                        ai_risk = f"⚠️ *Suspicious keywords found:*\n" + "\n".join([f"  • `{k}`" for k in found])
            except Exception as _e:
                ai_risk = f"Scan error: {_e}"
            # Notify owner with Allow/Deny buttons
            run_markup = types.InlineKeyboardMarkup()
            run_markup.row(
                types.InlineKeyboardButton("▶️ Allow Run", callback_data=f"runallow_{run_req_id}"),
                types.InlineKeyboardButton("🚫 Deny Run", callback_data=f"rundeny_{run_req_id}")
            )
            user_name_run = call.from_user.first_name or f"User_{requesting_user_id}"
            try:
                bot.send_message(
                    OWNER_ID,
                    f"▶️ *Run Permission Request*\n\n"
                    f"👤 User: `{user_name_run}` (ID: `{requesting_user_id}`)\n"
                    f"📄 Script: `{file_name}`\n"
                    f"🕐 Time: `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`\n\n"
                    f"🔍 *Quick Scan:*\n{ai_risk}\n\n"
                    f"Allow karo toh script chalegi. Deny karo toh nahi.",
                    parse_mode='Markdown',
                    reply_markup=run_markup
                )
            except Exception as _e:
                logger.error(f"Run permission notify owner error: {_e}")
            return  # Owner approve karega toh chalegi

        user_files_list = user_files.get(script_owner_id, [])
        file_info = next((f for f in user_files_list if f[0] == file_name), None)
        if not file_info:
            bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True); check_files_callback(call); return

        file_type = file_info[1]
        user_folder = get_user_folder(script_owner_id)
        file_path = os.path.join(user_folder, file_name)

        if not os.path.exists(file_path):
            bot.answer_callback_query(call.id, f"⚠️ Error: File `{file_name}` missing! Re-upload.", show_alert=True)
            remove_user_file_db(script_owner_id, file_name); check_files_callback(call); return

        if is_bot_running(script_owner_id, file_name):
            bot.answer_callback_query(call.id, f"⚠️ Script '{file_name}' already running.", show_alert=True)
            try: bot.edit_message_reply_markup(chat_id_for_reply, call.message.message_id, reply_markup=create_control_buttons(script_owner_id, file_name, True))
            except Exception as e: logger.error(f"Error updating buttons (already running): {e}")
            return

        bot.answer_callback_query(call.id, f"⏳ Attempting to start {file_name} for user {script_owner_id}...")

        # Pass call.message as message_obj_for_reply so feedback goes to the person who clicked
        if file_type == 'py':
            threading.Thread(target=run_script, args=(file_path, script_owner_id, user_folder, file_name, call.message)).start()
        elif file_type == 'js':
            threading.Thread(target=run_js_script, args=(file_path, script_owner_id, user_folder, file_name, call.message)).start()
        else:
             bot.send_message(chat_id_for_reply, f"❌ Error: Unknown file type '{file_type}' for '{file_name}'."); return 

        time.sleep(1.5) # Give script time to actually start or fail early
        is_now_running = is_bot_running(script_owner_id, file_name) 
        status_text = '🟢 Running' if is_now_running else '🟡 Starting (or failed, check logs/replies)'
        try:
            bot.edit_message_text(
                f"⚙️ Controls for: `{file_name}` ({file_type}) of User `{script_owner_id}`\nStatus: {status_text}",
                chat_id_for_reply, call.message.message_id,
                reply_markup=create_control_buttons(script_owner_id, file_name, is_now_running), parse_mode='Markdown'
            )
        except telebot.apihelper.ApiTelegramException as e:
             if "message is not modified" in str(e): logger.warning(f"Msg not modified after starting {file_name}")
             else: raise
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing start callback '{call.data}': {e}")
        bot.answer_callback_query(call.id, "Error: Invalid start command.", show_alert=True)
    except Exception as e:
        logger.error(f"Error in start_bot_callback for '{call.data}': {e}", exc_info=True)
        bot.answer_callback_query(call.id, "Error starting script.", show_alert=True)
        try: # Attempt to reset buttons to 'stopped' state on error
            _, script_owner_id_err_str, file_name_err = call.data.split('_', 2)
            script_owner_id_err = int(script_owner_id_err_str)
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=create_control_buttons(script_owner_id_err, file_name_err, False))
        except Exception as e_btn: logger.error(f"Failed to update buttons after start error: {e_btn}")

def stop_bot_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id
        chat_id_for_reply = call.message.chat.id

        logger.info(f"Stop request: Requester={requesting_user_id}, Owner={script_owner_id}, File='{file_name}'")
        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ Permission denied.", show_alert=True); return

        user_files_list = user_files.get(script_owner_id, [])
        file_info = next((f for f in user_files_list if f[0] == file_name), None)
        if not file_info:
            bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True); check_files_callback(call); return

        file_type = file_info[1] 
        script_key = f"{script_owner_id}_{file_name}"

        if not is_bot_running(script_owner_id, file_name): 
            bot.answer_callback_query(call.id, f"⚠️ Script '{file_name}' already stopped.", show_alert=True)
            try:
                 bot.edit_message_text(
                     f"⚙️ Controls for: `{file_name}` ({file_type}) of User `{script_owner_id}`\nStatus: 🔴 Stopped",
                     chat_id_for_reply, call.message.message_id,
                     reply_markup=create_control_buttons(script_owner_id, file_name, False), parse_mode='Markdown')
            except Exception as e: logger.error(f"Error updating buttons (already stopped): {e}")
            return

        bot.answer_callback_query(call.id, f"⏳ Stopping {file_name} for user {script_owner_id}...")
        process_info = bot_scripts.get(script_key)
        if process_info:
            kill_process_tree(process_info)
            if script_key in bot_scripts: del bot_scripts[script_key]; logger.info(f"Removed {script_key} from running after stop.")
        else: logger.warning(f"Script {script_key} running by psutil but not in bot_scripts dict.")

        try:
            bot.edit_message_text(
                f"⚙️ Controls for: `{file_name}` ({file_type}) of User `{script_owner_id}`\nStatus: 🔴 Stopped",
                chat_id_for_reply, call.message.message_id,
                reply_markup=create_control_buttons(script_owner_id, file_name, False), parse_mode='Markdown'
            )
        except telebot.apihelper.ApiTelegramException as e:
             if "message is not modified" in str(e): logger.warning(f"Msg not modified after stopping {file_name}")
             else: raise
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing stop callback '{call.data}': {e}")
        bot.answer_callback_query(call.id, "Error: Invalid stop command.", show_alert=True)
    except Exception as e:
        logger.error(f"Error in stop_bot_callback for '{call.data}': {e}", exc_info=True)
        bot.answer_callback_query(call.id, "Error stopping script.", show_alert=True)

def restart_bot_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id
        chat_id_for_reply = call.message.chat.id

        logger.info(f"Restart: Requester={requesting_user_id}, Owner={script_owner_id}, File='{file_name}'")
        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ Permission denied.", show_alert=True); return

        user_files_list = user_files.get(script_owner_id, [])
        file_info = next((f for f in user_files_list if f[0] == file_name), None)
        if not file_info:
            bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True); check_files_callback(call); return

        file_type = file_info[1]; user_folder = get_user_folder(script_owner_id)
        file_path = os.path.join(user_folder, file_name); script_key = f"{script_owner_id}_{file_name}"

        if not os.path.exists(file_path):
            bot.answer_callback_query(call.id, f"⚠️ Error: File `{file_name}` missing! Re-upload.", show_alert=True)
            remove_user_file_db(script_owner_id, file_name)
            if script_key in bot_scripts: del bot_scripts[script_key]
            check_files_callback(call); return

        bot.answer_callback_query(call.id, f"⏳ Restarting {file_name} for user {script_owner_id}...")
        if is_bot_running(script_owner_id, file_name):
            logger.info(f"Restart: Stopping existing {script_key}...")
            process_info = bot_scripts.get(script_key)
            if process_info: kill_process_tree(process_info)
            if script_key in bot_scripts: del bot_scripts[script_key]
            time.sleep(1.5) 

        logger.info(f"Restart: Starting script {script_key}...")
        if file_type == 'py':
            threading.Thread(target=run_script, args=(file_path, script_owner_id, user_folder, file_name, call.message)).start()
        elif file_type == 'js':
            threading.Thread(target=run_js_script, args=(file_path, script_owner_id, user_folder, file_name, call.message)).start()
        else:
             bot.send_message(chat_id_for_reply, f"❌ Unknown type '{file_type}' for '{file_name}'."); return

        time.sleep(1.5) 
        is_now_running = is_bot_running(script_owner_id, file_name) 
        status_text = '🟢 Running' if is_now_running else '🟡 Starting (or failed)'
        try:
            bot.edit_message_text(
                f"⚙️ Controls for: `{file_name}` ({file_type}) of User `{script_owner_id}`\nStatus: {status_text}",
                chat_id_for_reply, call.message.message_id,
                reply_markup=create_control_buttons(script_owner_id, file_name, is_now_running), parse_mode='Markdown'
            )
        except telebot.apihelper.ApiTelegramException as e:
             if "message is not modified" in str(e): logger.warning(f"Msg not modified (restart {file_name})")
             else: raise
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing restart callback '{call.data}': {e}")
        bot.answer_callback_query(call.id, "Error: Invalid restart command.", show_alert=True)
    except Exception as e:
        logger.error(f"Error in restart_bot_callback for '{call.data}': {e}", exc_info=True)
        bot.answer_callback_query(call.id, "Error restarting.", show_alert=True)
        try:
            _, script_owner_id_err_str, file_name_err = call.data.split('_', 2)
            script_owner_id_err = int(script_owner_id_err_str)
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=create_control_buttons(script_owner_id_err, file_name_err, False))
        except Exception as e_btn: logger.error(f"Failed to update buttons after restart error: {e_btn}")


def delete_bot_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id
        chat_id_for_reply = call.message.chat.id

        logger.info(f"Delete: Requester={requesting_user_id}, Owner={script_owner_id}, File='{file_name}'")
        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ Permission denied.", show_alert=True); return

        user_files_list = user_files.get(script_owner_id, [])
        if not any(f[0] == file_name for f in user_files_list):
            bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True); check_files_callback(call); return

        bot.answer_callback_query(call.id, f"🗑️ Deleting {file_name} for user {script_owner_id}...")
        script_key = f"{script_owner_id}_{file_name}"
        if is_bot_running(script_owner_id, file_name):
            logger.info(f"Delete: Stopping {script_key}...")
            process_info = bot_scripts.get(script_key)
            if process_info: kill_process_tree(process_info)
            if script_key in bot_scripts: del bot_scripts[script_key]
            time.sleep(0.5) 

        user_folder = get_user_folder(script_owner_id)
        file_path = os.path.join(user_folder, file_name)
        log_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        deleted_disk = []
        if os.path.exists(file_path):
            try: os.remove(file_path); deleted_disk.append(file_name); logger.info(f"Deleted file: {file_path}")
            except OSError as e: logger.error(f"Error deleting {file_path}: {e}")
        if os.path.exists(log_path):
            try: os.remove(log_path); deleted_disk.append(os.path.basename(log_path)); logger.info(f"Deleted log: {log_path}")
            except OSError as e: logger.error(f"Error deleting log {log_path}: {e}")

        remove_user_file_db(script_owner_id, file_name)
        deleted_str = ", ".join(f"`{f}`" for f in deleted_disk) if deleted_disk else "associated files"
        try:
            bot.edit_message_text(
                f"🗑️ Record `{file_name}` (User `{script_owner_id}`) and {deleted_str} deleted!",
                chat_id_for_reply, call.message.message_id, reply_markup=None, parse_mode='Markdown'
            )
        except Exception as e:
            logger.error(f"Error editing msg after delete: {e}")
            bot.send_message(chat_id_for_reply, f"🗑️ Record `{file_name}` deleted.", parse_mode='Markdown')
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing delete callback '{call.data}': {e}")
        bot.answer_callback_query(call.id, "Error: Invalid delete command.", show_alert=True)
    except Exception as e:
        logger.error(f"Error in delete_bot_callback for '{call.data}': {e}", exc_info=True)
        bot.answer_callback_query(call.id, "Error deleting.", show_alert=True)

def logs_bot_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id
        chat_id_for_reply = call.message.chat.id

        logger.info(f"Logs: Requester={requesting_user_id}, Owner={script_owner_id}, File='{file_name}'")
        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ Permission denied.", show_alert=True); return

        user_files_list = user_files.get(script_owner_id, [])
        if not any(f[0] == file_name for f in user_files_list):
            bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True); check_files_callback(call); return

        user_folder = get_user_folder(script_owner_id)
        log_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        if not os.path.exists(log_path):
            bot.answer_callback_query(call.id, f"⚠️ No logs for '{file_name}'.", show_alert=True); return

        bot.answer_callback_query(call.id) 
        try:
            log_content = ""; file_size = os.path.getsize(log_path)
            max_log_kb = 100; max_tg_msg = 4096
            if file_size == 0: log_content = "(Log empty)"
            elif file_size > max_log_kb * 1024:
                 with open(log_path, 'rb') as f: f.seek(-max_log_kb * 1024, os.SEEK_END); log_bytes = f.read()
                 log_content = log_bytes.decode('utf-8', errors='ignore')
                 log_content = f"(Last {max_log_kb} KB)\n...\n" + log_content
            else:
                 with open(log_path, 'r', encoding='utf-8', errors='ignore') as f: log_content = f.read()

            if len(log_content) > max_tg_msg:
                log_content = log_content[-max_tg_msg:]
                first_nl = log_content.find('\n')
                if first_nl != -1: log_content = "...\n" + log_content[first_nl+1:]
                else: log_content = "...\n" + log_content 
            if not log_content.strip(): log_content = "(No visible content)"

            bot.send_message(chat_id_for_reply, f"📜 Logs for `{file_name}` (User `{script_owner_id}`):\n```\n{log_content}\n```", parse_mode='Markdown')
        except Exception as e:
            logger.error(f"Error reading/sending log {log_path}: {e}", exc_info=True)
            bot.send_message(chat_id_for_reply, f"❌ Error reading log for `{file_name}`.")
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing logs callback '{call.data}': {e}")
        bot.answer_callback_query(call.id, "Error: Invalid logs command.", show_alert=True)
    except Exception as e:
        logger.error(f"Error in logs_bot_callback for '{call.data}': {e}", exc_info=True)
        bot.answer_callback_query(call.id, "Error fetching logs.", show_alert=True)

def speed_callback(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    start_cb_ping_time = time.time()
    try:
        bot.edit_message_text("🏃 Testing speed...", chat_id, call.message.message_id)
        bot.send_chat_action(chat_id, 'typing')
        response_time = round((time.time() - start_cb_ping_time) * 1000, 2)
        status = "🔓 Unlocked" if not bot_locked else "🔒 Locked"
        if user_id == OWNER_ID: user_level = "👑 Owner"
        elif user_id in admin_ids: user_level = "🛡️ Admin"
        elif user_id in user_subscriptions and user_subscriptions[user_id].get('expiry', datetime.min) > datetime.now(): user_level = "⭐ Premium"
        else: user_level = "🆓 Free User"
        # VPS/System info
        cpu_pct    = psutil.cpu_percent(interval=0.5)
        cpu_count  = psutil.cpu_count(logical=True)
        ram        = psutil.virtual_memory()
        disk       = psutil.disk_usage('/')
        boot_time  = psutil.boot_time()
        uptime_sec = int(time.time() - boot_time)
        uptime_str = f"{uptime_sec//86400}d {(uptime_sec%86400)//3600}h {(uptime_sec%3600)//60}m"
        ram_used   = round(ram.used / (1024**3), 2)
        ram_total  = round(ram.total / (1024**3), 2)
        ram_pct    = ram.percent
        disk_used  = round(disk.used / (1024**3), 2)
        disk_total = round(disk.total / (1024**3), 2)
        disk_pct   = disk.percent
        running_bots = sum(1 for k, v in list(bot_scripts.items()) if is_bot_running(int(k.split('_',1)[0]), v['file_name']))
        total_files = sum(len(f) for f in user_files.values())
        bot_storage_mb = round(get_total_storage_bytes() / (1024*1024), 2)
        # ✅ Worker Cluster Stats
        wcs = _get_worker_cluster_stats()
        worker_section = (
            f"\n━━━━━━ ☁️ Worker Cluster ━━━━━━\n"
            f"🟢 Online Workers : `{wcs['online']}` / `{wcs['total']}`\n"
            f"🔢 Total Cores    : `{wcs['total_cores']}`\n"
            f"⚡ Avg CPU        : `{wcs['avg_cpu']}%`\n"
            f"🧠 Avg RAM        : `{wcs['avg_ram']}%`\n"
            f"📦 Active Tasks   : `{wcs['active_tasks']}`\n"
            f"✅ Total Tasks Done: `{wcs['total_tasks_done']}`\n"
            f"☠️ Dead Workers   : `{wcs['dead']}`"
        ) if wcs['total'] > 0 else "\n━━━━━━ ☁️ Worker Cluster ━━━━━━\n⚠️ No workers connected yet."
        speed_msg = (
            f"⚡ *Bot Speed & VPS Stats*\n\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"⏱️ API Response: `{response_time} ms`\n"
            f"🚦 Bot Status: {status}\n"
            f"👤 Your Level: {user_level}\n\n"
            f"━━━━━━━ 🖥️ VPS Info ━━━━━━━\n"
            f"🔄 CPU Usage: `{cpu_pct}%` ({cpu_count} cores)\n"
            f"🧠 RAM: `{ram_used} / {ram_total} GB` ({ram_pct}%)\n"
            f"💽 Disk: `{disk_used} / {disk_total} GB` ({disk_pct}%)\n"
            f"⏰ Uptime: `{uptime_str}`\n"
            f"{worker_section}\n\n"
            f"━━━━━━ 🤖 Bot Stats ━━━━━━\n"
            f"👥 Total Users: `{len(active_users)}`\n"
            f"🟢 Running Bots: `{running_bots}`\n"
            f"📂 Total Files: `{total_files}`\n"
            f"💾 Bot Storage Used: `{bot_storage_mb} MB`"
        )
        bot.answer_callback_query(call.id)
        bot.edit_message_text(speed_msg, chat_id, call.message.message_id,
                              reply_markup=create_main_menu_inline(user_id), parse_mode='Markdown')
    except Exception as e:
         logger.error(f"Error during speed test (cb): {e}", exc_info=True)
         bot.answer_callback_query(call.id, "Error in speed test.", show_alert=True)
         try: bot.edit_message_text("〽️ Main Menu", chat_id, call.message.message_id, reply_markup=create_main_menu_inline(user_id))
         except Exception: pass

def back_to_main_callback(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
    expiry_info = ""
    if user_id == OWNER_ID: user_status = "👑 Owner"
    elif user_id in admin_ids: user_status = "🛡️ Admin"
    elif user_id in user_subscriptions:
        expiry_date = user_subscriptions[user_id].get('expiry')
        if expiry_date and expiry_date > datetime.now():
            user_status = "⭐ Premium"; days_left = (expiry_date - datetime.now()).days
            expiry_info = f"\n⏳ Subscription expires in: {days_left} days"
        else: user_status = "🆓 Free User (Expired Sub)" # Will be cleaned up by welcome if not already
    else: user_status = "🆓 Free User"
    main_menu_text = (f"〽️ Welcome back, {call.from_user.first_name}!\n\n🆔 ID: `{user_id}`\n"
                      f"🔰 Status: {user_status}{expiry_info}\n📁 Files: {current_files} / {limit_str}\n\n"
                      f"👇 Use buttons or type commands.")
    try:
        bot.answer_callback_query(call.id)
        bot.edit_message_text(main_menu_text, chat_id, call.message.message_id,
                              reply_markup=create_main_menu_inline(user_id), parse_mode='Markdown')
    except telebot.apihelper.ApiTelegramException as e:
         if "message is not modified" in str(e): logger.warning("Msg not modified (back_to_main).")
         else: logger.error(f"API error on back_to_main: {e}")
    except Exception as e: logger.error(f"Error handling back_to_main: {e}", exc_info=True)

# --- Admin Callback Implementations (for Inline Buttons) ---
def subscription_management_callback(call):
    bot.answer_callback_query(call.id)
    try:
        bot.edit_message_text("💳 Subscription Management\nSelect action:",
                              call.message.chat.id, call.message.message_id, reply_markup=create_subscription_menu())
    except Exception as e: logger.error(f"Error showing sub menu: {e}")

def stats_callback(call): # Called by user and admin
    bot.answer_callback_query(call.id)
    # The logic is now inside _logic_statistics which determines what to show based on user_id
    # We need to pass a message-like object to _logic_statistics
    # For callbacks, call.message can be used.
    _logic_statistics(call.message) 
    # To update the inline keyboard after showing stats, we need to edit the message
    try:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id,
                                      reply_markup=create_main_menu_inline(call.from_user.id))
    except Exception as e:
        logger.error(f"Error updating menu after stats_callback: {e}")


def lock_bot_callback(call):
    global bot_locked; bot_locked = True
    logger.warning(f"Bot locked by Admin {call.from_user.id}")
    bot.answer_callback_query(call.id, "🔒 Bot locked.")
    try: bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=create_main_menu_inline(call.from_user.id))
    except Exception as e: logger.error(f"Error updating menu (lock): {e}")

def unlock_bot_callback(call):
    global bot_locked; bot_locked = False
    logger.warning(f"Bot unlocked by Admin {call.from_user.id}")
    bot.answer_callback_query(call.id, "🔓 Bot unlocked.")
    try: bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=create_main_menu_inline(call.from_user.id))
    except Exception as e: logger.error(f"Error updating menu (unlock): {e}")

# def toggle_free_mode_callback(call): # Removed
#     pass

def run_all_scripts_callback(call): # Added
    _logic_run_all_scripts(call) # Pass the call object


def broadcast_init_callback(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "📢 Send message to broadcast.\n/cancel to abort.")
    bot.register_next_step_handler(msg, process_broadcast_message)

def process_broadcast_message(message):
    user_id = message.from_user.id
    if user_id not in admin_ids: bot.reply_to(message, "⚠️ Not authorized."); return
    if message.text and message.text.lower() == '/cancel': bot.reply_to(message, "Broadcast cancelled."); return

    # ✅ ALL MEDIA TYPES SUPPORTED
    ctype = message.content_type
    supported = ["text", "photo", "video", "document", "audio", "voice", "sticker", "animation", "video_note"]
    if ctype not in supported:
        bot.reply_to(message, f"⚠️ Media type `{ctype}` not supported. Send text/photo/video/document/audio/voice/sticker or /cancel.", parse_mode='Markdown')
        msg = bot.send_message(message.chat.id, "📢 Send broadcast message or /cancel.")
        bot.register_next_step_handler(msg, process_broadcast_message)
        return

    target_count = len(active_users)
    markup = types.InlineKeyboardMarkup()
    markup.row(
        types.InlineKeyboardButton("✅ Confirm & Send", callback_data=f"confirm_broadcast_{message.message_id}"),
        types.InlineKeyboardButton("❌ Cancel", callback_data="cancel_broadcast")
    )

    # Preview
    if message.text:
        preview = f"📝 Text:\n```\n{message.text[:800]}\n```"
    elif message.caption:
        preview = f"🖼️ Media with caption:\n```\n{message.caption[:500]}\n```"
    else:
        type_icons = {"photo":"🖼️","video":"🎥","document":"📄","audio":"🎵","voice":"🎤","sticker":"🎭","animation":"🎞️","video_note":"📹"}
        preview = f"{type_icons.get(ctype,'📦')} {ctype.title()} message"

    bot.reply_to(
        message,
        f"⚠️ *Confirm Broadcast*\n\n{preview}\n\n👥 Will be sent to *{target_count}* users.\n\nSure?",
        reply_markup=markup, parse_mode='Markdown'
    )

def handle_confirm_broadcast(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    if user_id not in admin_ids: bot.answer_callback_query(call.id, "⚠️ Admin only.", show_alert=True); return
    try:
        original_message = call.message.reply_to_message
        if not original_message: raise ValueError("Could not retrieve original message.")

        # ✅ ALL MEDIA TYPES — forward karo original message
        ctype = original_message.content_type
        bot.answer_callback_query(call.id, "🚀 Starting broadcast...")
        bot.edit_message_text(
            f"📢 Broadcasting to {len(active_users)} users...\nType: {ctype}",
            chat_id, call.message.message_id, reply_markup=None
        )
        thread = threading.Thread(
            target=execute_broadcast,
            args=(original_message, chat_id)
        )
        thread.daemon = False
        thread.start()
    except ValueError as ve: 
        logger.error(f"Error retrieving msg for broadcast confirm: {ve}")
        bot.edit_message_text(f"❌ Error starting broadcast: {ve}", chat_id, call.message.message_id, reply_markup=None)
    except Exception as e:
        logger.error(f"Error in handle_confirm_broadcast: {e}", exc_info=True)
        bot.edit_message_text("❌ Unexpected error during broadcast confirm.", chat_id, call.message.message_id, reply_markup=None)

def handle_cancel_broadcast(call):
    bot.answer_callback_query(call.id, "Broadcast cancelled.")
    bot.delete_message(call.message.chat.id, call.message.message_id)
    # Optionally delete the original message too if call.message.reply_to_message exists
    if call.message.reply_to_message:
        try: bot.delete_message(call.message.chat.id, call.message.reply_to_message.message_id)
        except: pass


def _send_broadcast_to_user(user_id_bc, orig_msg):
    """Send any media type to a single user using forward/copy logic."""
    ctype = orig_msg.content_type
    cap = orig_msg.caption or None
    parse = 'Markdown' if cap else None

    if ctype == 'text':
        bot.send_message(user_id_bc, orig_msg.text, parse_mode='Markdown')
    elif ctype == 'photo':
        bot.send_photo(user_id_bc, orig_msg.photo[-1].file_id, caption=cap, parse_mode=parse)
    elif ctype == 'video':
        bot.send_video(user_id_bc, orig_msg.video.file_id, caption=cap, parse_mode=parse)
    elif ctype == 'document':
        bot.send_document(user_id_bc, orig_msg.document.file_id, caption=cap, parse_mode=parse)
    elif ctype == 'audio':
        bot.send_audio(user_id_bc, orig_msg.audio.file_id, caption=cap, parse_mode=parse)
    elif ctype == 'voice':
        bot.send_voice(user_id_bc, orig_msg.voice.file_id, caption=cap, parse_mode=parse)
    elif ctype == 'sticker':
        bot.send_sticker(user_id_bc, orig_msg.sticker.file_id)
    elif ctype == 'animation':
        bot.send_animation(user_id_bc, orig_msg.animation.file_id, caption=cap, parse_mode=parse)
    elif ctype == 'video_note':
        bot.send_video_note(user_id_bc, orig_msg.video_note.file_id)
    else:
        # Fallback — forward karo
        bot.forward_message(user_id_bc, orig_msg.chat.id, orig_msg.message_id)


def execute_broadcast(orig_msg, admin_chat_id):
    """✅ Full broadcast — supports ALL media types: text, photo, video, document, audio, voice, sticker, animation, link, etc."""
    sent_count = 0; failed_count = 0; blocked_count = 0
    start_exec_time = time.time()

    # MongoDB se fresh users
    users_to_broadcast = list(active_users)
    try:
        db_users = get_all_active_user_ids()   # MongoDB
        if db_users:
            users_to_broadcast = db_users
    except Exception as db_err:
        logger.error(f"DB fetch error in broadcast: {db_err}")

    total_users = len(users_to_broadcast)
    logger.info(f"Executing broadcast ({orig_msg.content_type}) to {total_users} users.")
    batch_size = 25; delay_batches = 1.5

    for i, user_id_bc in enumerate(users_to_broadcast):
        try:
            _send_broadcast_to_user(user_id_bc, orig_msg)
            sent_count += 1
        except telebot.apihelper.ApiTelegramException as e:
            err_desc = str(e).lower()
            if any(s in err_desc for s in ["bot was blocked", "user is deactivated", "chat not found", "kicked from", "restricted"]):
                logger.warning(f"Broadcast blocked by {user_id_bc}")
                blocked_count += 1
            elif "flood control" in err_desc or "too many requests" in err_desc:
                retry_after = 5
                match = re.search(r"retry after (\d+)", err_desc)
                if match: retry_after = int(match.group(1)) + 1
                logger.warning(f"Flood control. Sleeping {retry_after}s...")
                time.sleep(retry_after)
                try:
                    _send_broadcast_to_user(user_id_bc, orig_msg)
                    sent_count += 1
                except Exception as e_retry:
                    logger.error(f"Broadcast retry failed to {user_id_bc}: {e_retry}")
                    failed_count += 1
            else:
                logger.error(f"Broadcast failed to {user_id_bc}: {e}")
                failed_count += 1
        except Exception as e:
            logger.error(f"Unexpected broadcast error to {user_id_bc}: {e}")
            failed_count += 1

        if (i + 1) % batch_size == 0 and i < total_users - 1:
            time.sleep(delay_batches)
        elif i % 5 == 0:
            time.sleep(0.2)

    duration = round(time.time() - start_exec_time, 2)
    result_msg = (
        f"📢 *Broadcast Complete!*\n\n"
        f"📦 Type: `{orig_msg.content_type}`\n"
        f"✅ Sent: `{sent_count}`\n"
        f"❌ Failed: `{failed_count}`\n"
        f"🚫 Blocked: `{blocked_count}`\n"
        f"👥 Total: `{total_users}`\n"
        f"⏱️ Duration: `{duration}s`"
    )
    logger.info(result_msg)
    try:
        bot.send_message(admin_chat_id, result_msg, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Failed to send broadcast result: {e}")

def admin_panel_callback(call):
    bot.answer_callback_query(call.id)
    try:
        bot.edit_message_text("👑 Admin Panel\nManage admins (Owner actions may be restricted).",
                              call.message.chat.id, call.message.message_id, reply_markup=create_admin_panel())
    except Exception as e: logger.error(f"Error showing admin panel: {e}")

def add_admin_init_callback(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "👑 Enter User ID to promote to Admin.\n/cancel to abort.")
    bot.register_next_step_handler(msg, process_add_admin_id)

def process_add_admin_id(message):
    owner_id_check = message.from_user.id 
    if owner_id_check != OWNER_ID: bot.reply_to(message, "⚠️ Owner only."); return
    if message.text.lower() == '/cancel': bot.reply_to(message, "Admin promotion cancelled."); return
    try:
        new_admin_id = int(message.text.strip())
        if new_admin_id <= 0: raise ValueError("ID must be positive")
        if new_admin_id == OWNER_ID: bot.reply_to(message, "⚠️ Owner is already Owner."); return
        if new_admin_id in admin_ids: bot.reply_to(message, f"⚠️ User `{new_admin_id}` already Admin."); return
        add_admin_db(new_admin_id) 
        logger.warning(f"Admin {new_admin_id} added by Owner {owner_id_check}.")
        bot.reply_to(message, f"✅ User `{new_admin_id}` promoted to Admin.")
        try: bot.send_message(new_admin_id, "🎉 Congrats! You are now an Admin.")
        except Exception as e: logger.error(f"Failed to notify new admin {new_admin_id}: {e}")
    except ValueError:
        bot.reply_to(message, "⚠️ Invalid ID. Send numerical ID or /cancel.")
        msg = bot.send_message(message.chat.id, "👑 Enter User ID to promote or /cancel.")
        bot.register_next_step_handler(msg, process_add_admin_id)
    except Exception as e: logger.error(f"Error processing add admin: {e}", exc_info=True); bot.reply_to(message, "Error.")

def remove_admin_init_callback(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "👑 Enter User ID of Admin to remove.\n/cancel to abort.")
    bot.register_next_step_handler(msg, process_remove_admin_id)

def process_remove_admin_id(message):
    owner_id_check = message.from_user.id
    if owner_id_check != OWNER_ID: bot.reply_to(message, "⚠️ Owner only."); return
    if message.text.lower() == '/cancel': bot.reply_to(message, "Admin removal cancelled."); return
    try:
        admin_id_remove = int(message.text.strip()) # Renamed
        if admin_id_remove <= 0: raise ValueError("ID must be positive")
        if admin_id_remove == OWNER_ID: bot.reply_to(message, "⚠️ Owner cannot remove self."); return
        if admin_id_remove not in admin_ids: bot.reply_to(message, f"⚠️ User `{admin_id_remove}` not Admin."); return
        if remove_admin_db(admin_id_remove): 
            logger.warning(f"Admin {admin_id_remove} removed by Owner {owner_id_check}.")
            bot.reply_to(message, f"✅ Admin `{admin_id_remove}` removed.")
            try: bot.send_message(admin_id_remove, "ℹ️ You are no longer an Admin.")
            except Exception as e: logger.error(f"Failed to notify removed admin {admin_id_remove}: {e}")
        else: bot.reply_to(message, f"❌ Failed to remove admin `{admin_id_remove}`. Check logs.")
    except ValueError:
        bot.reply_to(message, "⚠️ Invalid ID. Send numerical ID or /cancel.")
        msg = bot.send_message(message.chat.id, "👑 Enter Admin ID to remove or /cancel.")
        bot.register_next_step_handler(msg, process_remove_admin_id)
    except Exception as e: logger.error(f"Error processing remove admin: {e}", exc_info=True); bot.reply_to(message, "Error.")

def list_admins_callback(call):
    bot.answer_callback_query(call.id)
    try:
        admin_list_str = "\n".join(f"- `{aid}` {'(Owner)' if aid == OWNER_ID else ''}" for aid in sorted(list(admin_ids)))
        if not admin_list_str: admin_list_str = "(No Owner/Admins configured!)"
        bot.edit_message_text(f"👑 Current Admins:\n\n{admin_list_str}", call.message.chat.id,
                              call.message.message_id, reply_markup=create_admin_panel(), parse_mode='Markdown')
    except Exception as e: logger.error(f"Error listing admins: {e}")

def add_subscription_init_callback(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "💳 Enter User ID & days (e.g., `12345678 30`).\n/cancel to abort.")
    bot.register_next_step_handler(msg, process_add_subscription_details)

def process_add_subscription_details(message):
    admin_id_check = message.from_user.id 
    if admin_id_check not in admin_ids: bot.reply_to(message, "⚠️ Not authorized."); return
    if message.text.lower() == '/cancel': bot.reply_to(message, "Sub add cancelled."); return
    try:
        parts = message.text.split();
        if len(parts) != 2: raise ValueError("Incorrect format")
        sub_user_id = int(parts[0].strip()); days = int(parts[1].strip())
        if sub_user_id <= 0 or days <= 0: raise ValueError("User ID/days must be positive")

        current_expiry = user_subscriptions.get(sub_user_id, {}).get('expiry')
        start_date_new_sub = datetime.now() # Renamed
        if current_expiry and current_expiry > start_date_new_sub: start_date_new_sub = current_expiry
        new_expiry = start_date_new_sub + timedelta(days=days)
        save_subscription(sub_user_id, new_expiry)

        logger.info(f"Sub for {sub_user_id} by admin {admin_id_check}. Expiry: {new_expiry:%Y-%m-%d}")
        bot.reply_to(message, f"✅ Sub for `{sub_user_id}` by {days} days.\nNew expiry: {new_expiry:%Y-%m-%d}")
        try: bot.send_message(sub_user_id, f"🎉 Sub activated/extended by {days} days! Expires: {new_expiry:%Y-%m-%d}.")
        except Exception as e: logger.error(f"Failed to notify {sub_user_id} of new sub: {e}")
    except ValueError as e:
        bot.reply_to(message, f"⚠️ Invalid: {e}. Format: `ID days` or /cancel.")
        msg = bot.send_message(message.chat.id, "💳 Enter User ID & days, or /cancel.")
        bot.register_next_step_handler(msg, process_add_subscription_details)
    except Exception as e: logger.error(f"Error processing add sub: {e}", exc_info=True); bot.reply_to(message, "Error.")

def remove_subscription_init_callback(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "💳 Enter User ID to remove sub.\n/cancel to abort.")
    bot.register_next_step_handler(msg, process_remove_subscription_id)

def process_remove_subscription_id(message):
    admin_id_check = message.from_user.id
    if admin_id_check not in admin_ids: bot.reply_to(message, "⚠️ Not authorized."); return
    if message.text.lower() == '/cancel': bot.reply_to(message, "Sub removal cancelled."); return
    try:
        sub_user_id_remove = int(message.text.strip()) # Renamed
        if sub_user_id_remove <= 0: raise ValueError("ID must be positive")
        if sub_user_id_remove not in user_subscriptions:
            bot.reply_to(message, f"⚠️ User `{sub_user_id_remove}` no active sub in memory."); return
        remove_subscription_db(sub_user_id_remove) 
        logger.warning(f"Sub removed for {sub_user_id_remove} by admin {admin_id_check}.")
        bot.reply_to(message, f"✅ Sub for `{sub_user_id_remove}` removed.")
        try: bot.send_message(sub_user_id_remove, "ℹ️ Your subscription removed by admin.")
        except Exception as e: logger.error(f"Failed to notify {sub_user_id_remove} of sub removal: {e}")
    except ValueError:
        bot.reply_to(message, "⚠️ Invalid ID. Send numerical ID or /cancel.")
        msg = bot.send_message(message.chat.id, "💳 Enter User ID to remove sub from, or /cancel.")
        bot.register_next_step_handler(msg, process_remove_subscription_id)
    except Exception as e: logger.error(f"Error processing remove sub: {e}", exc_info=True); bot.reply_to(message, "Error.")

def check_subscription_init_callback(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "💳 Enter User ID to check sub.\n/cancel to abort.")
    bot.register_next_step_handler(msg, process_check_subscription_id)

def process_check_subscription_id(message):
    admin_id_check = message.from_user.id
    if admin_id_check not in admin_ids: bot.reply_to(message, "⚠️ Not authorized."); return
    if message.text.lower() == '/cancel': bot.reply_to(message, "Sub check cancelled."); return
    try:
        sub_user_id_check = int(message.text.strip()) # Renamed
        if sub_user_id_check <= 0: raise ValueError("ID must be positive")
        if sub_user_id_check in user_subscriptions:
            expiry_dt = user_subscriptions[sub_user_id_check].get('expiry')
            if expiry_dt:
                if expiry_dt > datetime.now():
                    days_left = (expiry_dt - datetime.now()).days
                    bot.reply_to(message, f"✅ User `{sub_user_id_check}` active sub.\nExpires: {expiry_dt:%Y-%m-%d %H:%M:%S} ({days_left} days left).")
                else:
                    bot.reply_to(message, f"⚠️ User `{sub_user_id_check}` expired sub (On: {expiry_dt:%Y-%m-%d %H:%M:%S}).")
                    remove_subscription_db(sub_user_id_check) # Clean up
            else: bot.reply_to(message, f"⚠️ User `{sub_user_id_check}` in sub list, but expiry missing. Re-add if needed.")
        else: bot.reply_to(message, f"ℹ️ User `{sub_user_id_check}` no active sub record.")
    except ValueError:
        bot.reply_to(message, "⚠️ Invalid ID. Send numerical ID or /cancel.")
        msg = bot.send_message(message.chat.id, "💳 Enter User ID to check, or /cancel.")
        bot.register_next_step_handler(msg, process_check_subscription_id)
    except Exception as e: logger.error(f"Error processing check sub: {e}", exc_info=True); bot.reply_to(message, "Error.")

# --- End Callback Query Handlers ---

def storage_info_callback(call):
    """Public: any user can press Storage Info to see all users' usage."""
    bot.answer_callback_query(call.id)
    try:
        lines = ["💾 *Storage Usage (All Users)*\n"]
        lines.append(f"{'User ID':<14} {'Used MB':>10} {'Files':>6}")
        lines.append("─" * 34)
        total_mb = 0.0
        sorted_uids = sorted(user_files.keys())
        for uid in sorted_uids:
            used_mb = get_user_storage_mb(uid)
            total_mb += used_mb
            file_count = len(user_files.get(uid, []))
            label = "(Owner)" if uid == OWNER_ID else ("(Adm)" if uid in admin_ids else "")
            lines.append(f"`{uid}` {label:<6} {used_mb:>8} MB  {file_count:>4}")
        lines.append("─" * 34)
        lines.append(f"📦 Total: `{round(total_mb, 2)} MB` | 👥 Users: `{len(sorted_uids)}`")
        lines.append(f"⚠️ Limit per user: `{USER_STORAGE_LIMIT_MB} MB`")
        full_msg = "\n".join(lines)
        if len(full_msg) > 4000: full_msg = full_msg[:4000] + "\n... (truncated)"
        bot.send_message(call.message.chat.id, full_msg, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error in storage_info_callback: {e}", exc_info=True)
        bot.send_message(call.message.chat.id, "❌ Error fetching storage info.")

def owner_all_files_callback(call):
    """Admin/Owner: see ALL users' files with control + download buttons."""
    bot.answer_callback_query(call.id)
    all_files_found = False
    for target_uid, files_list in sorted(user_files.items()):
        if not files_list: continue
        all_files_found = True
        markup = types.InlineKeyboardMarkup(row_width=1)
        for file_name, file_type in sorted(files_list):
            is_running = is_bot_running(target_uid, file_name)
            status_icon = "🟢" if is_running else "🔴"
            markup.add(types.InlineKeyboardButton(
                f"{status_icon} {file_name} ({file_type})",
                callback_data=f'file_{target_uid}_{file_name}'
            ))
            # Admin download button for each file
            markup.add(types.InlineKeyboardButton(
                f"⬇️ Download: {file_name[:30]}",
                callback_data=f'admin_dl_file_{target_uid}_{file_name}'
            ))
        label = "(Owner)" if target_uid == OWNER_ID else ("(Admin)" if target_uid in admin_ids else "")
        used_mb = get_user_storage_mb(target_uid)
        bot.send_message(call.message.chat.id,
            f"👤 User `{target_uid}` {label}\n💾 Storage: `{used_mb} MB / {USER_STORAGE_LIMIT_MB} MB`",
            reply_markup=markup, parse_mode='Markdown')
    if not all_files_found:
        bot.send_message(call.message.chat.id, "📂 No files uploaded by any user yet.")

def handle_storage_tg_download(call, token):
    """Send storage file to user who tapped bot link — file delivered from Telegram servers"""
    bot.answer_callback_query(call.id, "⏳ Sending file...")
    try:
        row = get_storage_file_by_token(token)
        if not row:
            bot.send_message(call.message.chat.id, "❌ File not found or link is invalid.")
            return
        storage_id, owner_uid, fname, tg_file_id, ftype, fsize, share_token, uploaded_at, expires_at = row
        # Check expiry
        if expires_at:
            try:
                exp_dt = datetime.fromisoformat(expires_at)
                if datetime.now() > exp_dt:
                    bot.send_message(call.message.chat.id,
                        f"⏰ *Link Expired!*\n\nThis link for `{fname}` has expired.\n"
                        f"Please ask the owner to generate a new link.",
                        parse_mode='Markdown')
                    return
            except Exception:
                pass

        size_kb = round((fsize or 0) / 1024, 1)
        caption = (f"📥 *File Download*\n📄 `{fname}`\n📦 {size_kb} KB\n\n"
                   f"⚠️ Re-download from the same bot link while valid.")
        _send_file_by_type(call.message.chat.id, tg_file_id, ftype, caption)

        # Update download count (MongoDB)
        try:
            increment_download_count_by_token(token)
        except Exception:
            pass
    except Exception as e:
        logger.error(f"handle_storage_tg_download error: {e}", exc_info=True)
        bot.send_message(call.message.chat.id, "❌ Error downloading file.")

def admin_download_user_file_callback(call):
    """Admin downloads a user's hosted bot file"""
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin only.", show_alert=True)
        return
    bot.answer_callback_query(call.id, "⏳ Fetching file...")
    try:
        # callback_data format: admin_dl_file_{user_id}_{file_name}
        parts = call.data.split('_', 4)  # ['admin', 'dl', 'file', user_id, file_name]
        target_uid = int(parts[3])
        file_name = parts[4]
        user_folder = get_user_folder(target_uid)
        file_path = os.path.join(user_folder, file_name)
        if not os.path.exists(file_path):
            bot.send_message(call.message.chat.id, f"❌ File `{file_name}` not found on disk.", parse_mode='Markdown')
            return
        size_kb = round(os.path.getsize(file_path) / 1024, 1)
        with open(file_path, 'rb') as f:
            bot.send_document(call.message.chat.id, f,
                caption=f"📥 *Admin Download*\n📄 `{file_name}`\n👤 User: `{target_uid}`\n📦 {size_kb} KB",
                parse_mode='Markdown')
    except Exception as e:
        logger.error(f"admin_download_user_file_callback error: {e}", exc_info=True)
        bot.send_message(call.message.chat.id, "❌ Error downloading file.")

# ============================================================================
# ✅ NEW: HOSTING STATUS MANAGEMENT (OWNER ONLY)
# ============================================================================

def is_hosting_enabled():
    """Check if hosting is enabled from MongoDB"""
    global hosting_enabled
    # MongoDB call via db.py
    hosting_enabled = is_hosting_enabled_db()
    return hosting_enabled

def set_hosting_status(enabled):
    """Update hosting status in MongoDB"""
    global hosting_enabled
    result = set_hosting_status_db(enabled)   # MongoDB
    if result:
        hosting_enabled = enabled
    return result

def hosting_status_callback(call):
    """Admin/Owner: Show hosting status with ON/OFF toggle"""
    user_id = call.from_user.id
    if user_id not in admin_ids and user_id != OWNER_ID:
        bot.answer_callback_query(call.id, "⚠️ Admin only!", show_alert=True)
        return

    bot.answer_callback_query(call.id)
    status = is_hosting_enabled()
    status_text = "🟢 ENABLED" if status else "🔴 DISABLED"

    markup = types.InlineKeyboardMarkup()
    # Only Owner can actually toggle
    if user_id == OWNER_ID:
        if status:
            markup.add(types.InlineKeyboardButton("🔴 Turn OFF", callback_data='hosting_off'))
        else:
            markup.add(types.InlineKeyboardButton("🟢 Turn ON", callback_data='hosting_on'))
    markup.add(types.InlineKeyboardButton("↩️ Back", callback_data='admin_panel'))

    bot.send_message(call.message.chat.id,
        f"🔌 *Hosting Status*\n\n"
        f"Current Status: {status_text}\n\n"
        f"📋 When *OFF*:\n"
        f"  • No new files can be uploaded/hosted\n"
        f"  • All bot features work normally\n"
        f"  • Existing files cannot be started\n\n"
        f"When *ON*:\n"
        f"  • Full hosting functionality enabled\n"
        f"  • Users can upload and run files\n\n"
        f"{'⚠️ Only Owner can toggle hosting status.' if user_id != OWNER_ID else ''}",
        reply_markup=markup, parse_mode='Markdown')

def _auto_broadcast_hosting(status_on):
    """Background mein sab users ko hosting status broadcast karo."""
    if status_on:
        msg = (
            "🟢 *Hosting Service — ONLINE!* 🚀\n\n"
            "╔══════════════════════════╗\n"
            "║  ✅  HOSTING IS NOW ON   ║\n"
            "╚══════════════════════════╝\n\n"
            "🎉 Hosting wapas shuru ho gayi!\n\n"
            "📤 *Ab aap kar sakte ho:*\n"
            "  • ✅ Files upload & host\n"
            "  • ✅ Scripts run (Python / JS)\n"
            "  • ✅ Sab features available\n\n"
            "⚡ *Bot full speed par hai!*\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📢 Updates: {UPDATE_CHANNEL}\n"
            "🤖 Powered by *ATX CloudHost Bot*"
        )
    else:
        msg = (
            "🔴 *Hosting Service — OFFLINE!* 🛑\n\n"
            "╔══════════════════════════╗\n"
            "║  ❌  HOSTING IS NOW OFF  ║\n"
            "╚══════════════════════════╝\n\n"
            "⚠️ Temporarily hosting band kar di gayi hai.\n\n"
            "📋 *Is waqt ye kaam nahi karega:*\n"
            "  • ❌ File upload / hosting\n"
            "  • ❌ Script run karna\n"
            "  • ✅ Bot ke baaki features chal rahe hain\n\n"
            "🔔 *Jab hosting wapas ON hogi, aapko turant notify kiya jaayega!*\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📢 Updates: {UPDATE_CHANNEL}\n"
            "🤖 Powered by *ATX CloudHost Bot*"
        )

    # MongoDB se fresh user list
    users_to_notify = []
    try:
        db_users = get_all_active_user_ids()   # MongoDB
        if db_users:
            users_to_notify = db_users
        else:
            users_to_notify = list(active_users)
    except Exception as db_err:
        logger.error(f"DB fetch error in hosting broadcast: {db_err}")
        users_to_notify = list(active_users)

    if not users_to_notify:
        logger.warning("No users to broadcast hosting status to.")
        try:
            bot.send_message(OWNER_ID, "⚠️ Hosting broadcast: No users found in DB.", parse_mode='Markdown')
        except Exception:
            pass
        return

    sent = 0; failed = 0; blocked = 0
    logger.info(f"Auto broadcast hosting to {len(users_to_notify)} users...")

    # Owner ko pehle bata do
    try:
        bot.send_message(
            OWNER_ID,
            f"📢 *Auto Broadcast Starting...*\n\n"
            f"Status: {'🟢 Hosting ON' if status_on else '🔴 Hosting OFF'}\n"
            f"👥 Total Users: `{len(users_to_notify)}`\n"
            f"⏳ Sab users ko message ja raha hai...",
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.error(f"Could not notify owner of broadcast start: {e}")

    for i, uid in enumerate(users_to_notify):
        try:
            bot.send_message(uid, msg, parse_mode='Markdown')
            sent += 1
        except telebot.apihelper.ApiTelegramException as e:
            err = str(e).lower()
            if "flood control" in err or "too many requests" in err:
                retry_after = 5
                match = re.search(r"retry after (\d+)", err)
                if match:
                    retry_after = int(match.group(1)) + 2
                logger.warning(f"Flood control in hosting broadcast. Sleeping {retry_after}s...")
                time.sleep(retry_after)
                try:
                    bot.send_message(uid, msg, parse_mode='Markdown')
                    sent += 1
                except Exception as retry_e:
                    logger.error(f"Retry failed for uid {uid}: {retry_e}")
                    failed += 1
            elif any(s in err for s in ["bot was blocked", "user is deactivated", "chat not found", "kicked from"]):
                blocked += 1
            else:
                logger.error(f"Broadcast error to {uid}: {e}")
                failed += 1
        except Exception as e:
            logger.error(f"Unexpected broadcast error to {uid}: {e}")
            failed += 1

        # Rate limiting — batch delays
        if (i + 1) % 25 == 0:
            time.sleep(1.5)
        elif i % 5 == 0:
            time.sleep(0.3)

    # Owner ko final result
    try:
        bot.send_message(
            OWNER_ID,
            f"📢 *Auto Broadcast Complete!*\n\n"
            f"Status: {'🟢 Hosting ON' if status_on else '🔴 Hosting OFF'}\n"
            f"✅ Sent: `{sent}`\n"
            f"❌ Failed: `{failed}`\n"
            f"🚫 Blocked: `{blocked}`\n"
            f"👥 Total: `{len(users_to_notify)}`",
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.error(f"Hosting broadcast result send error: {e}")


def hosting_off_callback(call):
    """Turn OFF hosting - Owner only + auto broadcast"""
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "⚠️ Owner only!", show_alert=True)
        return

    if set_hosting_status(False):
        bot.answer_callback_query(call.id, "✅ Hosting disabled!", show_alert=True)
        bot.edit_message_text(
            "🔌 *Hosting Status*\n\nCurrent Status: 🔴 DISABLED\n\n"
            "✅ Hosting OFF ho gayi.\n"
            "📢 Sab users ko auto broadcast ho raha hai...",
            call.message.chat.id, call.message.message_id,
            reply_markup=types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton("🟢 Turn ON", callback_data='hosting_on'),
                types.InlineKeyboardButton("↩️ Back", callback_data='admin_panel')
            ), parse_mode='Markdown'
        )
        t = threading.Thread(target=_auto_broadcast_hosting, args=(False,))
        t.daemon = False
        t.start()
    else:
        bot.answer_callback_query(call.id, "❌ Error!", show_alert=True)


def hosting_on_callback(call):
    """Turn ON hosting - Owner only + auto broadcast"""
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "⚠️ Owner only!", show_alert=True)
        return

    if set_hosting_status(True):
        bot.answer_callback_query(call.id, "✅ Hosting enabled!", show_alert=True)
        bot.edit_message_text(
            "🔌 *Hosting Status*\n\nCurrent Status: 🟢 ENABLED\n\n"
            "✅ Hosting ON ho gayi!\n"
            "📢 Sab users ko auto broadcast ho raha hai...",
            call.message.chat.id, call.message.message_id,
            reply_markup=types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton("🔴 Turn OFF", callback_data='hosting_off'),
                types.InlineKeyboardButton("↩️ Back", callback_data='admin_panel')
            ), parse_mode='Markdown'
        )
        t = threading.Thread(target=_auto_broadcast_hosting, args=(True,))
        t.daemon = False
        t.start()
    else:
        bot.answer_callback_query(call.id, "❌ Error!", show_alert=True)

# ============================================================================
# ✅ PART 2 — FEATURE 1: SMART DECODE (PREVIEW ONLY)
# ============================================================================

import base64 as _base64

# ============================================================================
# ✅ PART 3 — ADVANCED ENCRYPTION SYSTEM (OWNER ONLY)
# ============================================================================

# --- Encryption Setup ---
try:
    from cryptography.fernet import Fernet
    FERNET_AVAILABLE = True
except ImportError:
    FERNET_AVAILABLE = False
    logger.warning("⚠️ cryptography library not installed. Run: pip install cryptography --break-system-packages")

# Key storage path
ENCRYPTION_KEY_PATH = os.path.join(IROTECH_DIR, '.enc_key')

def _get_or_create_encryption_key():
    """Load existing encryption key or generate a new one. Key is stored securely on disk."""
    if not FERNET_AVAILABLE:
        return None
    try:
        if os.path.exists(ENCRYPTION_KEY_PATH):
            with open(ENCRYPTION_KEY_PATH, 'rb') as f:
                key = f.read().strip()
            # Validate key by trying to create Fernet instance
            Fernet(key)
            return key
        else:
            # Generate new key
            key = Fernet.generate_key()
            with open(ENCRYPTION_KEY_PATH, 'wb') as f:
                f.write(key)
            os.chmod(ENCRYPTION_KEY_PATH, 0o600)  # Owner-only permissions
            logger.info("🔑 New encryption key generated and stored securely.")
            return key
    except Exception as e:
        logger.error(f"Encryption key error: {e}", exc_info=True)
        return None

def _get_fernet():
    """Get a Fernet instance with the stored key."""
    key = _get_or_create_encryption_key()
    if key is None:
        return None
    try:
        return Fernet(key)
    except Exception as e:
        logger.error(f"Fernet init error: {e}")
        return None

def encrypt_file_content(file_content_bytes):
    """Encrypt file content bytes using Fernet AES. Returns encrypted bytes or None."""
    if not FERNET_AVAILABLE:
        return None, "cryptography library not installed. Run: pip install cryptography --break-system-packages"
    try:
        f = _get_fernet()
        if f is None:
            return None, "Failed to initialize encryption key."
        encrypted = f.encrypt(file_content_bytes)
        return encrypted, None
    except Exception as e:
        logger.error(f"Encryption error: {e}", exc_info=True)
        return None, str(e)

def decrypt_file_content(encrypted_bytes):
    """Decrypt encrypted file content bytes using Fernet AES. Returns original bytes or None."""
    if not FERNET_AVAILABLE:
        return None, "cryptography library not installed."
    try:
        f = _get_fernet()
        if f is None:
            return None, "Failed to initialize encryption key."
        decrypted = f.decrypt(encrypted_bytes)
        return decrypted, None
    except Exception as e:
        logger.error(f"Decryption error: {e}", exc_info=True)
        return None, f"Decryption failed: {str(e)[:200]}"

def is_fernet_encrypted(data_bytes):
    """Heuristic: Fernet tokens start with 'gAAA' when base64-decoded."""
    try:
        text = data_bytes[:4].decode('ascii', errors='ignore')
        return text == 'gAAA'
    except Exception:
        return False

# --- Encrypt Handler (Owner only) ---
def handle_encrypt_flow(message):
    """Ask owner to send file to encrypt."""
    if message.from_user.id != OWNER_ID:
        bot.reply_to(message, "⚠️ Owner only.")
        return
    if not FERNET_AVAILABLE:
        bot.reply_to(message, "❌ `cryptography` library not installed.\n\nRun:\n`pip install cryptography --break-system-packages`", parse_mode='Markdown')
        return
    msg = bot.send_message(
        message.chat.id,
        "🔐 *Encrypt File*\n\n"
        "Send me ANY file to encrypt it with AES (Fernet).\n"
        "I will return the encrypted `.enc` file.\n\n"
        "Only you (Owner) can decrypt it later.\n"
        "/cancel to abort.",
        parse_mode='Markdown'
    )
    bot.register_next_step_handler(msg, process_encrypt_upload)

def process_encrypt_upload(message):
    """Process file sent for encryption."""
    if message.from_user.id != OWNER_ID:
        bot.reply_to(message, "⚠️ Owner only.")
        return
    if message.text and message.text.strip().lower() == '/cancel':
        bot.reply_to(message, "❌ Encryption cancelled.")
        return
    if not message.document:
        bot.reply_to(message, "⚠️ Please send a file (document). /cancel to abort.")
        msg = bot.send_message(message.chat.id, "Send the file to encrypt:")
        bot.register_next_step_handler(msg, process_encrypt_upload)
        return

    doc = message.document
    file_name = doc.file_name or f"file_{int(time.time())}"
    wait_msg = bot.reply_to(message, f"⏳ Downloading and encrypting `{file_name}`...", parse_mode='Markdown')

    try:
        file_info = bot.get_file(doc.file_id)
        file_bytes = bot.download_file(file_info.file_path)

        encrypted_bytes, err = encrypt_file_content(file_bytes)
        if err or encrypted_bytes is None:
            bot.edit_message_text(f"❌ Encryption failed: {err}", message.chat.id, wait_msg.message_id)
            return

        enc_file_name = file_name + '.enc'
        import io
        bot.edit_message_text(f"✅ Encrypted! Sending `{enc_file_name}`...", message.chat.id, wait_msg.message_id, parse_mode='Markdown')
        bot.send_document(
            message.chat.id,
            io.BytesIO(encrypted_bytes),
            caption=(
                f"🔐 *Encrypted File*\n\n"
                f"📄 Original: `{file_name}`\n"
                f"📦 Encrypted: `{enc_file_name}`\n\n"
                f"✅ Only you (Owner) can decrypt this.\n"
                f"Use 🔓 Decrypt File button to restore."
            ),
            parse_mode='Markdown',
            visible_file_name=enc_file_name
        )
        logger.info(f"Owner {OWNER_ID} encrypted file: {file_name} → {enc_file_name}")

    except Exception as e:
        logger.error(f"Encrypt flow error: {e}", exc_info=True)
        try:
            bot.edit_message_text(f"❌ Error: {str(e)[:300]}", message.chat.id, wait_msg.message_id)
        except Exception:
            pass

# --- Decrypt Handler (Owner only) ---
def handle_decrypt_flow(message):
    """Ask owner to send encrypted file to decrypt."""
    if message.from_user.id != OWNER_ID:
        bot.reply_to(message, "⚠️ Owner only.")
        return
    if not FERNET_AVAILABLE:
        bot.reply_to(message, "❌ `cryptography` library not installed.", parse_mode='Markdown')
        return
    msg = bot.send_message(
        message.chat.id,
        "🔓 *Decrypt File*\n\n"
        "Send me an encrypted `.enc` file to decrypt it.\n"
        "I will return the original file.\n\n"
        "/cancel to abort.",
        parse_mode='Markdown'
    )
    bot.register_next_step_handler(msg, process_decrypt_upload)

def process_decrypt_upload(message):
    """Process encrypted file sent for decryption."""
    if message.from_user.id != OWNER_ID:
        bot.reply_to(message, "⚠️ Owner only.")
        return
    if message.text and message.text.strip().lower() == '/cancel':
        bot.reply_to(message, "❌ Decryption cancelled.")
        return
    if not message.document:
        bot.reply_to(message, "⚠️ Please send a `.enc` file. /cancel to abort.")
        msg = bot.send_message(message.chat.id, "Send the encrypted file:")
        bot.register_next_step_handler(msg, process_decrypt_upload)
        return

    doc = message.document
    file_name = doc.file_name or f"file_{int(time.time())}"
    wait_msg = bot.reply_to(message, f"⏳ Downloading and decrypting `{file_name}`...", parse_mode='Markdown')

    try:
        file_info = bot.get_file(doc.file_id)
        file_bytes = bot.download_file(file_info.file_path)

        decrypted_bytes, err = decrypt_file_content(file_bytes)
        if err or decrypted_bytes is None:
            bot.edit_message_text(
                f"❌ Decryption failed.\n\n"
                f"Reason: `{err}`\n\n"
                f"Make sure this file was encrypted by this bot.",
                message.chat.id, wait_msg.message_id, parse_mode='Markdown'
            )
            return

        # Restore original filename by stripping .enc
        original_name = file_name
        if original_name.endswith('.enc'):
            original_name = original_name[:-4]

        import io
        bot.edit_message_text(f"✅ Decrypted! Sending `{original_name}`...", message.chat.id, wait_msg.message_id, parse_mode='Markdown')
        bot.send_document(
            message.chat.id,
            io.BytesIO(decrypted_bytes),
            caption=(
                f"🔓 *Decrypted File*\n\n"
                f"📄 Restored: `{original_name}`\n"
                f"✅ Original file content fully restored."
            ),
            parse_mode='Markdown',
            visible_file_name=original_name
        )
        logger.info(f"Owner {OWNER_ID} decrypted file: {file_name} → {original_name}")

    except Exception as e:
        logger.error(f"Decrypt flow error: {e}", exc_info=True)
        try:
            bot.edit_message_text(f"❌ Error: {str(e)[:300]}", message.chat.id, wait_msg.message_id)
        except Exception:
            pass

# --- Encrypt/Decrypt Callbacks ---
def encrypt_file_callback(call):
    """🔐 Encrypt File button callback — Admin/Owner only."""
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin/Owner only!", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    handle_encrypt_flow(call.message)

def decrypt_file_callback(call):
    """🔓 Decrypt File button callback — Admin/Owner only."""
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin/Owner only!", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    handle_decrypt_flow(call.message)

# --- Auto Decode for Encrypted Files on Upload ---
def try_auto_decrypt_and_run(file_content_bytes, file_name, user_id, user_folder, message_obj):
    """
    If an uploaded file appears to be Fernet-encrypted (sent to bot), try to decrypt it.
    If successful, run the decrypted version (only for .py/.js).
    NEVER executes during decode — only runs after full successful decryption.
    Returns True if handled, False if not encrypted/failed.
    """
    if not FERNET_AVAILABLE:
        return False
    if not is_fernet_encrypted(file_content_bytes):
        return False

    logger.info(f"Auto-decrypt attempt for '{file_name}' (possible Fernet encrypted file).")
    decrypted_bytes, err = decrypt_file_content(file_content_bytes)
    if err or decrypted_bytes is None:
        logger.info(f"Auto-decrypt failed for '{file_name}': {err}")
        return False

    # Determine original filename
    original_name = file_name
    if original_name.endswith('.enc'):
        original_name = original_name[:-4]

    original_ext = os.path.splitext(original_name)[1].lower()
    if original_ext not in ['.py', '.js']:
        logger.info(f"Auto-decrypted '{file_name}' but ext '{original_ext}' is not runnable. Skipping run.")
        return False

    # Save decrypted file and run it
    try:
        file_path = os.path.join(user_folder, original_name)
        with open(file_path, 'wb') as f:
            f.write(decrypted_bytes)
        logger.info(f"Auto-decrypted '{file_name}' → '{original_name}' and saved for user {user_id}.")

        try:
            bot.send_message(
                message_obj.chat.id,
                f"🔓 *Auto-Decrypted!*\n\n"
                f"Detected encrypted file: `{file_name}`\n"
                f"Decrypted to: `{original_name}`\n\n"
                f"▶️ Running decrypted file now...",
                parse_mode='Markdown'
            )
        except Exception:
            pass

        save_user_file(user_id, original_name, original_ext.lstrip('.'))
        if original_ext == '.py':
            handle_py_file(file_path, user_id, user_folder, original_name, message_obj)
        elif original_ext == '.js':
            handle_js_file(file_path, user_id, user_folder, original_name, message_obj)
        return True
    except Exception as e:
        logger.error(f"Auto-decrypt run error for '{file_name}': {e}", exc_info=True)
        return False

# ============================================================================
# ✅ END PART 3 — ADVANCED ENCRYPTION SYSTEM
# ============================================================================

def smart_decode_preview(file_content_bytes, file_name):
    """
    Detect if file content is encoded/compressed.
    Tries: gzip, zlib, Hex, Base64, ROT13.
    Returns (detected_type, decoded_preview_str) or (None, None).
    DOES NOT modify the original file — preview ONLY, max 1000 chars.
    NEVER executes decoded content.
    """
    import re as _re
    import gzip as _gzip
    import zlib as _zlib
    import codecs as _codecs

    # --- Try gzip decompression ---
    try:
        if file_content_bytes[:2] == b'\x1f\x8b':
            decompressed = _gzip.decompress(file_content_bytes)
            decoded_str = decompressed.decode('utf-8', errors='replace')
            return 'gzip', decoded_str[:1000]
    except Exception:
        pass

    # --- Try zlib decompression ---
    try:
        if len(file_content_bytes) > 2:
            decompressed = _zlib.decompress(file_content_bytes)
            decoded_str = decompressed.decode('utf-8', errors='replace')
            if len(decoded_str.strip()) > 10:
                return 'zlib', decoded_str[:1000]
    except Exception:
        pass

    try:
        text = file_content_bytes.decode('utf-8', errors='ignore').strip()
    except Exception:
        return None, None

    if not text:
        return None, None

    # --- Try HEX detection ---
    hex_clean = text.replace(' ', '').replace('\n', '').replace('\r', '')
    if len(hex_clean) >= 20 and len(hex_clean) % 2 == 0 and all(c in '0123456789abcdefABCDEF' for c in hex_clean):
        try:
            decoded_bytes = bytes.fromhex(hex_clean)
            decoded_str = decoded_bytes.decode('utf-8', errors='replace')
            if len(decoded_str.strip()) > 5:
                return 'hex', decoded_str[:1000]
        except Exception:
            pass

    # --- Try BASE64 detection ---
    b64_clean = text.replace('\n', '').replace('\r', '').replace(' ', '')
    if len(b64_clean) >= 20 and _re.match(r'^[A-Za-z0-9+/]+=*$', b64_clean):
        try:
            decoded_bytes = _base64.b64decode(b64_clean)
            decoded_str = decoded_bytes.decode('utf-8', errors='replace')
            if len(decoded_str.strip()) > 5:
                return 'base64', decoded_str[:1000]
        except Exception:
            pass

    # --- Try ROT13 detection ---
    try:
        rot13_decoded = _codecs.decode(text, 'rot_13')
        printable = sum(1 for c in rot13_decoded if c.isprintable() or c in '\n\r\t')
        ratio = printable / max(len(rot13_decoded), 1)
        if ratio > 0.90 and any(kw in rot13_decoded.lower() for kw in [
            'import', 'def ', 'class ', 'print', 'return', '#!/', 'var ', 'function'
        ]):
            return 'rot13', rot13_decoded[:1000]
    except Exception:
        pass

    return None, None


# ============================================================================
# ✅ PART 2 — FEATURE 2: AI FILE ANALYSIS
# ============================================================================

AI_API_URL = "https://rumix-ai.vercel.app/api/chat/chatgpt?p="

# ✅ FIX: Chunk size for AI analysis — har ek chunk alag API call karega
AI_CHUNK_SIZE = 2500  # characters per chunk
AI_API_TIMEOUT = 120  # seconds — no timeout issues for slow API

def _ai_api_call(prompt_text, retries=3):
    """
    Single AI API call with retry logic.
    Ek call poori tarah complete hone ke baad hi return karega.
    No premature timeout.
    """
    import urllib.parse
    encoded = urllib.parse.quote(prompt_text)
    url = AI_API_URL + encoded

    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=AI_API_TIMEOUT)
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    result = (
                        data.get('response') or
                        data.get('reply') or
                        data.get('message') or
                        data.get('result') or
                        data.get('text') or
                        str(data)
                    )
                except Exception:
                    result = resp.text
                return str(result)
            else:
                logger.warning(f"AI API HTTP {resp.status_code} on attempt {attempt}")
                if attempt < retries:
                    time.sleep(3)
        except requests.exceptions.Timeout:
            logger.warning(f"AI API timeout on attempt {attempt}/{retries}")
            if attempt < retries:
                time.sleep(5)
        except Exception as e:
            logger.error(f"AI API call error attempt {attempt}: {e}")
            if attempt < retries:
                time.sleep(3)

    return "❌ AI API response nahi aayi (all retries failed)"


def ai_analyze_file(file_content_bytes, file_name, user_id, user_name):
    """
    File ko chunks mein AI API ko bhejta hai.
    - Pehle chunk bhejo, poora response aane ke baad dusra chunk.
    - No timeout issues — har call 120s wait karega.
    - Owner ko har chunk ka result forward karta hai.
    """
    try:
        try:
            text_content = file_content_bytes.decode('utf-8', errors='replace')
        except Exception:
            text_content = repr(file_content_bytes[:10000])

        total_len = len(text_content)

        # File info header — Owner ko bata do
        try:
            bot.send_message(
                OWNER_ID,
                f"🤖 *AI File Analysis Started*\n\n"
                f"👤 User: `{user_id}` ({user_name})\n"
                f"📄 File: `{file_name}`\n"
                f"📏 Size: `{total_len}` chars\n"
                f"📦 Chunks: `{max(1, (total_len + AI_CHUNK_SIZE - 1) // AI_CHUNK_SIZE)}`\n\n"
                f"⏳ Analysis chal rahi hai... (ek ek chunk aayega)",
                parse_mode='Markdown'
            )
        except Exception as e:
            logger.error(f"AI analysis start msg error: {e}")

        # ✅ CHUNKED ANALYSIS — ek chunk poora hone ke baad hi dusra
        chunks = [text_content[i:i + AI_CHUNK_SIZE] for i in range(0, max(1, total_len), AI_CHUNK_SIZE)]

        all_results = []
        for idx, chunk in enumerate(chunks, 1):
            prompt = (
                f"Analyze this code file named '{file_name}' "
                f"(Part {idx}/{len(chunks)}):\n\n{chunk}"
            )
            logger.info(f"AI analysis: sending chunk {idx}/{len(chunks)} for {file_name}")

            # ✅ Ek call poora hone ke baad hi aage badhega
            ai_result = _ai_api_call(prompt)
            all_results.append(ai_result)

            # Owner ko har chunk ka result bhejo
            try:
                msg_text = (
                    f"🤖 *AI Analysis* — Part {idx}/{len(chunks)}\n\n"
                    f"👤 User: `{user_id}` ({user_name})\n"
                    f"📄 File: `{file_name}`\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"{ai_result[:3500]}"
                )
                bot.send_message(OWNER_ID, msg_text, parse_mode='Markdown')
            except Exception as e:
                logger.error(f"AI chunk {idx} send error: {e}")

            # Chunks ke beech thoda wait — API ko breathe karne do
            if idx < len(chunks):
                time.sleep(2)

        # ✅ Final summary agar multiple chunks hain
        if len(chunks) > 1:
            try:
                bot.send_message(
                    OWNER_ID,
                    f"✅ *AI Analysis Complete*\n\n"
                    f"📄 File: `{file_name}`\n"
                    f"📦 Total Chunks: `{len(chunks)}`\n"
                    f"👤 User: `{user_id}` ({user_name})\n\n"
                    f"Sab chunks complete ho gaye!",
                    parse_mode='Markdown'
                )
            except Exception as e:
                logger.error(f"AI final summary send error: {e}")

        return "\n\n".join(all_results)

    except Exception as e:
        logger.error(f"AI file analysis error: {e}", exc_info=True)
        # Owner ko error bhi bata do
        try:
            bot.send_message(
                OWNER_ID,
                f"❌ *AI Analysis Error*\n\n"
                f"👤 User: `{user_id}` ({user_name})\n"
                f"📄 File: `{file_name}`\n\n"
                f"Error: `{str(e)[:500]}`",
                parse_mode='Markdown'
            )
        except Exception:
            pass
        return f"❌ AI analysis failed: {str(e)[:200]}"


# ============================================================================
# ✅ PART 2 — FEATURE 3: AI CHAT BUTTON
# ============================================================================

# Track active AI chat sessions {user_id: True}
ai_chat_sessions = {}

def ai_chat_ask_api(user_question):
    """Send user question to AI API and return reply. No timeout issues — uses _ai_api_call with retry."""
    return _ai_api_call(user_question[:2000])


def ai_chat_callback(call):
    """Handle 🤖 AI Chat button — start AI chat session."""
    bot.answer_callback_query(call.id)
    user_id = call.from_user.id
    ai_chat_sessions[user_id] = True
    msg = bot.send_message(
        call.message.chat.id,
        "🤖 *AI Chat Started!*\n\n"
        "Ask me anything. I'll reply using AI.\n"
        "Type /stopaichat to exit AI Chat mode.\n\n"
        "What's your question?",
        parse_mode='Markdown'
    )
    bot.register_next_step_handler(msg, process_ai_chat_message)


def process_ai_chat_message(message):
    """Process user AI chat message and reply."""
    user_id = message.from_user.id
    if message.text and message.text.strip().lower() in ['/stopaichat', '/cancel']:
        ai_chat_sessions.pop(user_id, None)
        bot.reply_to(message, "🤖 AI Chat ended. Back to normal mode.")
        return

    if not message.text:
        msg = bot.reply_to(message, "⚠️ Please send a text message. /stopaichat to exit.")
        bot.register_next_step_handler(msg, process_ai_chat_message)
        return

    wait_msg = bot.reply_to(message, "🤖 Thinking...")
    try:
        reply = ai_chat_ask_api(message.text)
        bot.edit_message_text(
            f"🤖 *AI Reply:*\n\n{reply}",
            message.chat.id,
            wait_msg.message_id,
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.error(f"AI chat reply error: {e}", exc_info=True)
        try:
            bot.edit_message_text(f"❌ AI error: {str(e)[:200]}", message.chat.id, wait_msg.message_id)
        except Exception:
            pass

    # Continue session
    msg2 = bot.send_message(message.chat.id, "💬 Ask another question or /stopaichat to exit.")
    bot.register_next_step_handler(msg2, process_ai_chat_message)


# ============================================================================
# ✅ PART 2 — FEATURE 4: MODULE INSTALL SYSTEM
# ============================================================================

def module_install_callback(call):
    """Handle 📦 Install Module button."""
    bot.answer_callback_query(call.id)
    msg = bot.send_message(
        call.message.chat.id,
        "📦 *Install Python Module*\n\n"
        "Send the module name to install.\n"
        "Example: `requests`, `flask`, `numpy`\n\n"
        "/cancel to abort.",
        parse_mode='Markdown'
    )
    bot.register_next_step_handler(msg, process_module_install)


def process_module_install(message):
    """Install a Python module via pip."""
    user_id = message.from_user.id
    if message.text and message.text.strip().lower() == '/cancel':
        bot.reply_to(message, "❌ Module install cancelled.")
        return
    if not message.text:
        bot.reply_to(message, "⚠️ Please send a module name.")
        return

    module_name = message.text.strip().split()[0]  # Only first word, safe
    # Basic safety: reject obviously bad input
    import re as _re2
    if not _re2.match(r'^[a-zA-Z0-9_\-\.]+$', module_name):
        bot.reply_to(message, "⚠️ Invalid module name. Only alphanumeric, dash, underscore, dot allowed.")
        return

    wait_msg = bot.reply_to(message, f"📦 Installing `{module_name}`...", parse_mode='Markdown')
    try:
        command = [sys.executable, '-m', 'pip', 'install', module_name, '--break-system-packages']
        logger.info(f"User {user_id} installing module: {module_name}")
        result = subprocess.run(
            command, capture_output=True, text=True, check=False,
            encoding='utf-8', errors='ignore', timeout=120
        )
        if result.returncode == 0:
            success_lines = [l for l in result.stdout.split('\n') if l.strip()]
            summary = '\n'.join(success_lines[-5:]) if success_lines else 'Done.'
            bot.edit_message_text(
                f"✅ *Module `{module_name}` installed successfully!*\n\n```\n{summary[:800]}\n```",
                message.chat.id, wait_msg.message_id, parse_mode='Markdown'
            )
            # Notify owner
            try:
                bot.send_message(OWNER_ID,
                    f"📦 Module install by `{user_id}`\nModule: `{module_name}`\nStatus: ✅ Success",
                    parse_mode='Markdown')
            except Exception:
                pass
        else:
            err_out = (result.stderr or result.stdout or 'Unknown error')[:800]
            bot.edit_message_text(
                f"❌ *Failed to install `{module_name}`*\n\n```\n{err_out}\n```",
                message.chat.id, wait_msg.message_id, parse_mode='Markdown'
            )
            try:
                bot.send_message(OWNER_ID,
                    f"📦 Module install by `{user_id}`\nModule: `{module_name}`\nStatus: ❌ Failed\n{err_out[:300]}",
                    parse_mode='Markdown')
            except Exception:
                pass
    except subprocess.TimeoutExpired:
        bot.edit_message_text(f"⏱️ Install timed out for `{module_name}`. Try again.", message.chat.id, wait_msg.message_id, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Module install error: {e}", exc_info=True)
        try:
            bot.edit_message_text(f"❌ Error: {str(e)[:300]}", message.chat.id, wait_msg.message_id)
        except Exception:
            pass


# ============================================================================
# ✅ PART 2 — REGISTER NEW BUTTON HANDLERS IN INLINE MENU
# ============================================================================

# Patch create_main_menu_inline to include new Part 2 buttons
_original_create_main_menu_inline = create_main_menu_inline

def create_main_menu_inline(user_id):
    markup = _original_create_main_menu_inline(user_id)
    # Add Part 2 buttons row — for all users
    markup.add(
        types.InlineKeyboardButton("🤖 AI Chat", callback_data='ai_chat'),
        types.InlineKeyboardButton("📦 Install Module", callback_data='module_install')
    )
    # ✅ PART 3 — Add Encrypt/Decrypt buttons for Admins and Owner
    if user_id in admin_ids:
        markup.add(
            types.InlineKeyboardButton("🔐 Encrypt File", callback_data='encrypt_file'),
            types.InlineKeyboardButton("🔓 Decrypt File", callback_data='decrypt_file')
        )
    return markup


# Patch handle_callbacks to handle new callbacks — we wrap the original handler
_original_handle_callbacks_inner = None  # Not needed, we patch via bot re-registration below

# Since telebot processes in order, add new callback handler AFTER existing one:
@bot.callback_query_handler(func=lambda call: call.data in ['ai_chat', 'module_install', 'encrypt_file', 'decrypt_file'])
def handle_part2_callbacks(call):
    user_id = call.from_user.id
    data = call.data
    logger.info(f"Part2/Part3 Callback: User={user_id}, Data='{data}'")
    if data == 'ai_chat':
        ai_chat_callback(call)
    elif data == 'module_install':
        module_install_callback(call)
    # ✅ PART 3 — Encryption callbacks
    elif data == 'encrypt_file':
        encrypt_file_callback(call)
    elif data == 'decrypt_file':
        decrypt_file_callback(call)


# Also patch command handler for /stopaichat
@bot.message_handler(commands=['stopaichat'])
def command_stop_ai_chat(message):
    ai_chat_sessions.pop(message.from_user.id, None)
    bot.reply_to(message, "🤖 AI Chat session ended.")


# ============================================================================
# ✅ PART 2 — SMART DECODE: Hook into file upload flow (analysis on approval)
# ============================================================================

# Patch send_approval_request_single to also run AI analysis + smart decode preview
_original_send_approval_request_single = send_approval_request_single

def send_approval_request_single(approval_id, user_id, user_name, file_name, file_ext, file_content_bytes):
    result = _original_send_approval_request_single(
        approval_id, user_id, user_name, file_name, file_ext, file_content_bytes
    )

    # --- AUTO DECRYPT CHECK: If file appears Fernet-encrypted, notify owner ---
    try:
        if FERNET_AVAILABLE and is_fernet_encrypted(file_content_bytes):
            bot.send_message(
                OWNER_ID,
                f"🔒 *Encrypted File Detected!*\n\n"
                f"File `{file_name}` from user `{user_id}` appears to be AES/Fernet encrypted.\n\n"
                f"Use 🔓 *Decrypt File* to decrypt it if needed.\n"
                f"Auto-decrypt will run when approved if it's a `.enc` file.",
                parse_mode='Markdown'
            )
    except Exception as e:
        logger.error(f"Auto-decrypt detection error: {e}", exc_info=True)

    # --- FORWARD APPROVAL REQUEST TO ALL ADMINS (with Approve/Reject/Ack buttons) ---
    try:
        import io as _io
        admin_caption = (
            f"📋 *Pending File — Admin Review*\n\n"
            f"👤 User: `{user_id}` ({user_name})\n"
            f"📄 File: `{file_name}`\n"
            f"📦 Size: `{round(len(file_content_bytes)/1024, 1)} KB`\n\n"
            f"⚠️ Only *Owner* can Approve/Reject.\n"
            f"Admins can click *👁 Ack* to mark as reviewed."
        )
        admin_markup = types.InlineKeyboardMarkup()
        admin_markup.row(
            types.InlineKeyboardButton("✅ Approve", callback_data=f"approve_{approval_id}"),
            types.InlineKeyboardButton("❌ Reject", callback_data=f"reject_{approval_id}"),
            types.InlineKeyboardButton("👁 Ack", callback_data=f"ack_{approval_id}")
        )
        for aid in admin_ids:
            if aid == OWNER_ID:
                continue  # Owner already got the original approval request
            try:
                bot.send_document(
                    aid,
                    _io.BytesIO(file_content_bytes),
                    caption=admin_caption,
                    parse_mode='Markdown',
                    visible_file_name=file_name,
                    reply_markup=admin_markup
                )
            except Exception as admin_e:
                logger.warning(f"Could not forward approval to admin {aid}: {admin_e}")
    except Exception as e:
        logger.error(f"Admin approval forward error: {e}", exc_info=True)

    return result


# ============================================================================
# ✅ FIX: ReplyKeyboard message handlers for Part 2 / Part 3 buttons
# ============================================================================

def _logic_ai_chat(message):
    """Handle 🤖 AI Chat ReplyKeyboard button."""
    user_id = message.from_user.id
    ai_chat_sessions[user_id] = True
    msg = bot.reply_to(
        message,
        "🤖 *AI Chat Started!*\n\n"
        "Ask me anything. I'll reply using AI.\n"
        "Type /stopaichat to exit AI Chat mode.\n\n"
        "What's your question?",
        parse_mode='Markdown'
    )
    bot.register_next_step_handler(msg, process_ai_chat_message)

def _logic_install_module(message):
    """Handle 📦 Install Module ReplyKeyboard button."""
    msg = bot.reply_to(
        message,
        "📦 *Install Python Module*\n\n"
        "Send the module name to install.\n"
        "Example: `requests`, `flask`, `numpy`\n\n"
        "/cancel to abort.",
        parse_mode='Markdown'
    )
    bot.register_next_step_handler(msg, process_module_install)

def _logic_encrypt_file(message):
    """Handle 🔐 Encrypt File ReplyKeyboard button — Admin/Owner only."""
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin/Owner only!")
        return
    handle_encrypt_flow(message)

def _logic_decrypt_file(message):
    """Handle 🔓 Decrypt File ReplyKeyboard button — Admin/Owner only."""
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin/Owner only!")
        return
    handle_decrypt_flow(message)

# Register new button handlers
BUTTON_TEXT_TO_LOGIC["🤖 AI Chat"] = _logic_ai_chat
BUTTON_TEXT_TO_LOGIC["📦 Install Module"] = _logic_install_module
BUTTON_TEXT_TO_LOGIC["🔐 Encrypt File"] = _logic_encrypt_file
BUTTON_TEXT_TO_LOGIC["🔓 Decrypt File"] = _logic_decrypt_file

# ✅ Pending Files button handler for Admin/Owner ReplyKeyboard
def _logic_pending_files(message):
    """Handle ⏳ Pending Files ReplyKeyboard button — Admin/Owner only."""
    user_id = message.from_user.id
    if user_id not in admin_ids and user_id != OWNER_ID:
        bot.reply_to(message, "⚠️ Admin/Owner only!")
        return
    if not pending_approvals:
        bot.reply_to(message,
            "✅ *No Pending Files*\n\nKoi bhi file approval ka wait nahi kar rahi.",
            parse_mode='Markdown')
        return

    text = f"⏳ *Pending Approval Files* — {len(pending_approvals)} total\n\n"
    markup = types.InlineKeyboardMarkup(row_width=2)
    for approval_id, info in list(pending_approvals.items()):
        uid = info.get('user_id', '?')
        fname = info.get('file_name', 'unknown')
        text += f"👤 User: `{uid}`\n📄 File: `{fname}`\n🔑 ID: `{approval_id[:8]}...`\n\n"
        if user_id == OWNER_ID:
            markup.add(
                types.InlineKeyboardButton(f"✅ {fname[:15]}", callback_data=f'approve_{approval_id}'),
                types.InlineKeyboardButton(f"❌ Reject", callback_data=f'reject_{approval_id}'),
            )
        else:
            markup.add(types.InlineKeyboardButton(f"👁 Ack: {fname[:20]}", callback_data=f'ack_{approval_id}'))

    if user_id != OWNER_ID:
        text += "\n⚠️ _Sirf Owner approve/reject kar sakta hai._"

    markup.add(types.InlineKeyboardButton("🔙 Back to Main", callback_data='back_to_main'))
    bot.reply_to(message, text, parse_mode='Markdown', reply_markup=markup)

BUTTON_TEXT_TO_LOGIC["⏳ Pending Files"] = _logic_pending_files


# ============================================================================
# ✅ FIX: _send_file_by_type — send any file type by telegram file_id
# ============================================================================

def _send_file_by_type(chat_id, file_id, ftype, caption=""):
    """Send a file to chat_id based on its type."""
    try:
        ftype = (ftype or "document").lower()
        if ftype == "photo":
            bot.send_photo(chat_id, file_id, caption=caption, parse_mode='Markdown')
        elif ftype == "video":
            bot.send_video(chat_id, file_id, caption=caption, parse_mode='Markdown')
        elif ftype == "audio":
            bot.send_audio(chat_id, file_id, caption=caption, parse_mode='Markdown')
        elif ftype == "voice":
            bot.send_voice(chat_id, file_id, caption=caption, parse_mode='Markdown')
        elif ftype == "animation":
            bot.send_animation(chat_id, file_id, caption=caption, parse_mode='Markdown')
        else:
            bot.send_document(chat_id, file_id, caption=caption, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"_send_file_by_type error: {e}")
        bot.send_message(chat_id, f"❌ Could not send file: {e}")


# ============================================================================
# ✅ FIX: owner_full_panel_callback — Owner full stats panel
# ============================================================================

def owner_full_panel_callback(call):
    """Owner only: full overview panel."""
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "⚠️ Owner only!", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    try:
        total_users    = get_db().users.count_documents({})
        active_subs    = get_db().subscriptions.count_documents({"expiry": {"$gt": datetime.now().isoformat()}})
        total_files    = get_db().user_files.count_documents({})
        storage_files  = count_storage_files()
        donate_files   = count_donate_files()
        running_count  = len(bot_scripts)
        admins_count   = len(admin_ids) - 1  # exclude owner
        pending_count  = len(pending_approvals)

        text = (
            f"👑 *Owner Full Panel*\n\n"
            f"👥 Total Users: `{total_users}`\n"
            f"💳 Active Subscriptions: `{active_subs}`\n"
            f"📁 Hosted Files: `{total_files}`\n"
            f"📦 Storage Files: `{storage_files}`\n"
            f"🎁 Donated Files: `{donate_files}`\n"
            f"⏳ Pending Approvals: `{pending_count}`\n"
            f"▶️ Running Scripts: `{running_count}`\n"
            f"🛡 Admins: `{admins_count}`\n\n"
            f"🤖 Bot: @{bot.get_me().username}\n"
            f"Server: Running OK"
        )
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton("📁 All Users Files", callback_data='owner_all_files'),
            types.InlineKeyboardButton("📊 Statistics", callback_data='stats'),
        )
        markup.add(
            types.InlineKeyboardButton("💳 Subscriptions", callback_data='subscription'),
            types.InlineKeyboardButton("📢 Broadcast", callback_data='broadcast'),
        )
        markup.add(
            types.InlineKeyboardButton("🔒 Lock Bot" if not bot_locked else "🔓 Unlock Bot",
                                       callback_data='lock_bot' if not bot_locked else 'unlock_bot'),
            types.InlineKeyboardButton("🟢 Run All Scripts", callback_data='run_all_scripts'),
        )
        markup.add(
            types.InlineKeyboardButton("🔌 Hosting Status", callback_data='hosting_status'),
            types.InlineKeyboardButton("👑 View Donated Files", callback_data='owner_donate_view'),
        )
        markup.add(
            types.InlineKeyboardButton("📦 All Storage", callback_data='storage_owner_all'),
            types.InlineKeyboardButton(f"⏳ Pending Files ({pending_count})", callback_data='owner_pending_files'),
        )
        markup.add(
            types.InlineKeyboardButton("🔐 Encrypt File", callback_data='encrypt_file'),
            types.InlineKeyboardButton("🔓 Decrypt File", callback_data='decrypt_file'),
        )
        markup.add(
            types.InlineKeyboardButton("🔙 Back to Main", callback_data='back_to_main'),
        )
        bot.send_message(call.message.chat.id, text, parse_mode='Markdown', reply_markup=markup)
    except Exception as e:
        logger.error(f"owner_full_panel_callback error: {e}", exc_info=True)
        bot.send_message(call.message.chat.id, f"❌ Error loading panel: {e}")


def admin_pending_files_callback(call):
    """Admin/Owner: show all pending approval files. Admins can view; Owner can approve/reject."""
    user_id = call.from_user.id
    if user_id not in admin_ids and user_id != OWNER_ID:
        bot.answer_callback_query(call.id, "⚠️ Admin only!", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    try:
        if not pending_approvals:
            bot.send_message(
                call.message.chat.id,
                "✅ *No Pending Files*\n\nKoi bhi file approval ka wait nahi kar rahi.",
                parse_mode='Markdown',
                reply_markup=types.InlineKeyboardMarkup().add(
                    types.InlineKeyboardButton("🔙 Back", callback_data='back_to_main')
                )
            )
            return

        text = f"⏳ *Pending Approval Files* — {len(pending_approvals)} total\n\n"
        markup = types.InlineKeyboardMarkup(row_width=2)

        for approval_id, info in list(pending_approvals.items()):
            uid = info.get('user_id', '?')
            fname = info.get('file_name', 'unknown')
            text += f"👤 User: `{uid}`\n📄 File: `{fname}`\n🔑 ID: `{approval_id[:8]}...`\n\n"
            if user_id == OWNER_ID:
                # Owner can approve/reject
                markup.add(
                    types.InlineKeyboardButton(f"✅ {fname[:15]}", callback_data=f'approve_{approval_id}'),
                    types.InlineKeyboardButton(f"❌ Reject", callback_data=f'reject_{approval_id}'),
                )
            else:
                # Admin can only acknowledge
                markup.add(
                    types.InlineKeyboardButton(f"👁 Ack: {fname[:20]}", callback_data=f'ack_{approval_id}'),
                )

        if user_id != OWNER_ID:
            text += "\n⚠️ _Sirf Owner approve/reject kar sakta hai. Aap sirf acknowledge kar sakte ho._"

        markup.add(types.InlineKeyboardButton("🔙 Back", callback_data='back_to_main'))
        bot.send_message(call.message.chat.id, text, parse_mode='Markdown', reply_markup=markup)
    except Exception as e:
        logger.error(f"admin_pending_files_callback error: {e}", exc_info=True)
        bot.send_message(call.message.chat.id, f"❌ Error: {e}")


def owner_pending_files_callback(call):
    """Owner only: show all pending approval files."""
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "⚠️ Owner only!", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    try:
        if not pending_approvals:
            bot.send_message(
                call.message.chat.id,
                "✅ *No Pending Files*\n\nKoi bhi file approval ka wait nahi kar rahi.",
                parse_mode='Markdown',
                reply_markup=types.InlineKeyboardMarkup().add(
                    types.InlineKeyboardButton("🔙 Back", callback_data='owner_full_panel')
                )
            )
            return

        text = f"⏳ *Pending Approval Files* — {len(pending_approvals)} total\n\n"
        markup = types.InlineKeyboardMarkup(row_width=2)

        for approval_id, info in list(pending_approvals.items()):
            uid = info.get('user_id', '?')
            fname = info.get('file_name', 'unknown')
            text += f"👤 User: `{uid}`\n📄 File: `{fname}`\n🔑 ID: `{approval_id[:8]}...`\n\n"
            markup.add(
                types.InlineKeyboardButton(f"✅ {fname[:15]}", callback_data=f'approve_{approval_id}'),
                types.InlineKeyboardButton(f"❌ Reject", callback_data=f'reject_{approval_id}'),
            )

        markup.add(types.InlineKeyboardButton("🔙 Back", callback_data='owner_full_panel'))
        bot.send_message(call.message.chat.id, text, parse_mode='Markdown', reply_markup=markup)
    except Exception as e:
        logger.error(f"owner_pending_files_callback error: {e}", exc_info=True)
        bot.send_message(call.message.chat.id, f"❌ Error: {e}")


def cleanup():
    logger.warning("Shutdown. Cleaning up processes...")
    script_keys_to_stop = list(bot_scripts.keys()) 
    if not script_keys_to_stop: logger.info("No scripts running. Exiting."); return
    logger.info(f"Stopping {len(script_keys_to_stop)} scripts...")
    for key in script_keys_to_stop:
        if key in bot_scripts: logger.info(f"Stopping: {key}"); kill_process_tree(bot_scripts[key])
        else: logger.info(f"Script {key} already removed.")
    logger.warning("Cleanup finished.")
atexit.register(cleanup)

# --- Main Execution ---
if __name__ == '__main__':
    logger.info("="*40 + "\n🤖 Bot Starting Up...\n" + f"🐍 Python: {sys.version.split()[0]}\n" +
                f"🔧 Base Dir: {BASE_DIR}\n📁 Upload Dir: {UPLOAD_BOTS_DIR}\n" +
                f"📊 Data Dir: {IROTECH_DIR}\n🔑 Owner ID: {OWNER_ID}\n🛡️ Admins: {admin_ids}\n" + "="*40)
    # Worker infrastructure removed — MongoDB Atlas handles persistence
    logger.info('🗄️ MongoDB Atlas connected. All data persisted to cloud.')
    keep_alive()
    # MongoDB health-check thread
    def _mongo_healthcheck():
        while True:
            try:
                time.sleep(60)
                if not ping_db():
                    logger.warning("⚠️ MongoDB ping failed — will reconnect on next DB call")
            except Exception as e:
                logger.error(f"Mongo health check error: {e}")
    threading.Thread(target=_mongo_healthcheck, daemon=True, name="MongoHealthCheck").start()
    logger.info("💓 MongoDB health-check thread started.")
    logger.info("🚀 Starting polling...")
    while True:
        try:
            bot.infinity_polling(logger_level=logging.INFO, timeout=60, long_polling_timeout=30)
        except requests.exceptions.ReadTimeout: logger.warning("Polling ReadTimeout. Restarting in 5s..."); time.sleep(5)
        except requests.exceptions.ConnectionError as ce: logger.error(f"Polling ConnectionError: {ce}. Retrying in 15s..."); time.sleep(15)
        except Exception as e:
            logger.critical(f"💥 Unrecoverable polling error: {e}", exc_info=True)
            logger.info("Restarting polling in 30s due to critical error..."); time.sleep(30)
        finally: logger.warning("Polling attempt finished. Will restart if in loop."); time.sleep(1)


# ============================================================================
# ✅ FIX 2 — RUN PERMISSION: Owner Allow/Deny callback handler
# ============================================================================

@bot.callback_query_handler(func=lambda call: call.data.startswith('runallow_') or call.data.startswith('rundeny_'))
def handle_run_permission_callback(call):
    """Owner ka run allow/deny handle karo."""
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "⚠️ Sirf Owner ye kar sakta hai!", show_alert=True)
        return

    parts = call.data.split('_', 1)
    action = parts[0]
    run_req_id = parts[1]

    data = pending_run_requests.pop(run_req_id, None)
    if not data:
        bot.answer_callback_query(call.id, "❌ Request nahi mili ya expire ho gayi.")
        try:
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        except Exception:
            pass
        return

    user_id = data['user_id']
    chat_id = data['chat_id']
    script_owner_id = data['script_owner_id']
    file_name = data['file_name']
    file_path = data['file_path']
    call_message = data['call_message']

    if action == 'runallow':
        bot.answer_callback_query(call.id, "▶️ Run allowed!")
        try:
            bot.edit_message_text(
                f"✅ *Run Allowed*\n\n`{file_name}` ko run karne ki permission di gayi.\nUser: `{user_id}`",
                call.message.chat.id, call.message.message_id, parse_mode='Markdown'
            )
        except Exception:
            pass
        try:
            if not os.path.exists(file_path):
                bot.send_message(chat_id, f"❌ File `{file_name}` disk par nahi mili. Re-upload karo.", parse_mode='Markdown')
                return
            user_folder = get_user_folder(script_owner_id)
            file_ext = os.path.splitext(file_name)[1].lower()
            bot.send_message(chat_id, f"✅ *Owner ne run ki permission di!*\n\nScript: `{file_name}`\nAb chal rahi hai... ⚙️", parse_mode='Markdown')
            if file_ext == '.py':
                handle_py_file(file_path, script_owner_id, user_folder, file_name, call_message)
            elif file_ext == '.js':
                handle_js_file(file_path, script_owner_id, user_folder, file_name, call_message)
            else:
                bot.send_message(chat_id, f"❌ Unknown file type: `{file_ext}`", parse_mode='Markdown')
        except Exception as e:
            logger.error(f"Run after permission error: {e}", exc_info=True)
            bot.send_message(chat_id, f"❌ Script run mein error: {e}")

    elif action == 'rundeny':
        bot.answer_callback_query(call.id, "🚫 Run denied.")
        try:
            bot.edit_message_text(
                f"🚫 *Run Denied*\n\n`{file_name}` ko run karne ki permission nahi di gayi.\nUser: `{user_id}`",
                call.message.chat.id, call.message.message_id, parse_mode='Markdown'
            )
        except Exception:
            pass
        try:
            bot.send_message(chat_id, f"🚫 *Script run reject ho gayi.*\n\nScript: `{file_name}`\nOwner ne permission nahi di.", parse_mode='Markdown')
        except Exception:
            pass