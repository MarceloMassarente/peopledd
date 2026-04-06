from __future__ import annotations

"""
peopledd.services.cache
=======================
Two-level pipeline cache:
  L1 (hot)  — InMemoryDocumentStore (vendor) — in-process, sub-ms reads
  L2 (warm) — SQLite (stdlib)                — cross-run persistence, file-based

TTL policies:
  FRE / RI governance data   → 7 days   (slow-changing public filings)
  Harvest LinkedIn profiles  → 24 hours (profiles update infrequently)
  Search results / URLs      → 6 hours  (balance freshness vs. cost)
  Strategy scraping output   → 12 hours

Key schema: sha256(kind + ":" + canonical_key)

Usage:
    cache = PipelineCache()

    # Write
    cache.set("profile", "https://linkedin.com/in/joao", data_dict, ttl_hours=24)

    # Read
    data = cache.get("profile", "https://linkedin.com/in/joao")  # None if miss/expired

    # Warm L1 from L2 on startup (optional)
    cache.warm_l1(kind="profile", limit=200)
"""

import hashlib
import json
import logging
import os
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from peopledd.vendor.document_store import InMemoryDocumentStore

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Default TTLs (hours)
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_TTL: dict[str, int] = {
    "fre":        24 * 7,    # 7 days — FRE filings
    "ri":         24 * 7,    # 7 days — RI governance page
    "strategy":   12,        # 12h    — strategy extraction
    "profile":    24,        # 24h    — Harvest LinkedIn profile
    "search":     6,         # 6h     — search result URLs
    "company":    24 * 3,    # 3 days — Exa company lookup
    "semantic_fusion": 24 * 7,  # 7 days — n1c output (ingestion-shaped key)
    "default":    6,
}

# ─────────────────────────────────────────────────────────────────────────────
# SQLite L2 backend
# ─────────────────────────────────────────────────────────────────────────────

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS pipeline_cache (
    key        TEXT PRIMARY KEY,
    kind       TEXT NOT NULL,
    value_json TEXT NOT NULL,
    expires_at REAL NOT NULL,
    created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_kind ON pipeline_cache(kind);
CREATE INDEX IF NOT EXISTS idx_expires ON pipeline_cache(expires_at);
"""


class _SQLiteBackend:
    def __init__(self, db_path: str | Path):
        self._path = str(db_path)
        self._init_db()

    def _init_db(self) -> None:
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        with self._connect() as conn:
            conn.executescript(_CREATE_TABLE)

    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(self._path, check_same_thread=False, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get(self, key: str) -> Any | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT value_json, expires_at FROM pipeline_cache WHERE key = ?",
                (key,),
            ).fetchone()
        if row is None:
            return None
        if time.time() > row["expires_at"]:
            self.delete(key)
            return None
        try:
            return json.loads(row["value_json"])
        except Exception:
            return None

    def set(self, key: str, kind: str, value: Any, ttl_hours: int) -> None:
        now = time.time()
        expires = now + ttl_hours * 3600
        value_json = json.dumps(value, ensure_ascii=False, default=str)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO pipeline_cache (key, kind, value_json, expires_at, created_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value_json = excluded.value_json,
                    expires_at = excluded.expires_at
                """,
                (key, kind, value_json, expires, now),
            )

    def delete(self, key: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM pipeline_cache WHERE key = ?", (key,))

    def evict_expired(self) -> int:
        """Remove all expired entries. Returns count removed."""
        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM pipeline_cache WHERE expires_at < ?",
                (time.time(),),
            )
            return cursor.rowcount

    def list_by_kind(self, kind: str, limit: int = 500) -> list[dict]:
        now = time.time()
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT key, value_json FROM pipeline_cache WHERE kind = ? AND expires_at > ? LIMIT ?",
                (kind, now, limit),
            ).fetchall()
        out = []
        for row in rows:
            try:
                out.append({"key": row["key"], "value": json.loads(row["value_json"])})
            except Exception:
                pass
        return out

    def stats(self) -> dict[str, Any]:
        now = time.time()
        with self._connect() as conn:
            total = conn.execute("SELECT COUNT(*) FROM pipeline_cache").fetchone()[0]
            active = conn.execute(
                "SELECT COUNT(*) FROM pipeline_cache WHERE expires_at > ?", (now,)
            ).fetchone()[0]
            expired = total - active
            by_kind = conn.execute(
                "SELECT kind, COUNT(*) as cnt FROM pipeline_cache WHERE expires_at > ? GROUP BY kind",
                (now,),
            ).fetchall()
        return {
            "total": total,
            "active": active,
            "expired": expired,
            "by_kind": {row["kind"]: row["cnt"] for row in by_kind},
            "db_path": self._path,
            "db_size_bytes": os.path.getsize(self._path) if os.path.exists(self._path) else 0,
        }


# ─────────────────────────────────────────────────────────────────────────────
# PipelineCache — two-level (L1: in-memory, L2: SQLite)
# ─────────────────────────────────────────────────────────────────────────────

def _cache_key(kind: str, raw_key: str) -> str:
    """SHA-256 hash of 'kind:raw_key' — safe as SQLite primary key."""
    return hashlib.sha256(f"{kind}:{raw_key}".encode()).hexdigest()


_DEFAULT_DB_PATH = os.environ.get(
    "PEOPLEDD_CACHE_PATH",
    str(Path.home() / ".cache" / "peopledd" / "pipeline.sqlite"),
)


class PipelineCache:
    """
    Two-level pipeline cache.

    L1 = InMemoryDocumentStore (fast, process-scoped, configurable TTL)
    L2 = SQLite (persistent, cross-run, file-based)

    All reads check L1 first (O(1)), then L2 (disk read).
    All writes go to both levels.
    """

    def __init__(
        self,
        db_path: str | Path | None = None,
        l1_max_size: int = 1000,
        l1_ttl_seconds: int = 3600,
        enable_l2: bool = True,
    ):
        self._l1 = InMemoryDocumentStore(
            max_size=l1_max_size,
            ttl_seconds=l1_ttl_seconds,
        )
        self._enable_l2 = enable_l2
        if enable_l2:
            path = db_path or _DEFAULT_DB_PATH
            try:
                self._l2 = _SQLiteBackend(path)
                logger.info(f"[PipelineCache] L2 SQLite initialized: {path}")
            except Exception as e:
                logger.warning(f"[PipelineCache] L2 SQLite failed, running L1 only: {e}")
                self._l2 = None
                self._enable_l2 = False
        else:
            self._l2 = None

    def _ttl(self, kind: str, ttl_hours: int | None) -> int:
        if ttl_hours is not None:
            return ttl_hours
        return DEFAULT_TTL.get(kind, DEFAULT_TTL["default"])

    def get(self, kind: str, raw_key: str) -> Any | None:
        """
        Read from L1 (fast) then L2 (disk).
        Promotes L2 hits to L1.
        """
        key = _cache_key(kind, raw_key)

        # L1
        hit = self._l1.get(key)
        if hit is not None:
            return hit

        # L2
        if self._enable_l2 and self._l2:
            hit = self._l2.get(key)
            if hit is not None:
                # Promote to L1
                self._l1.set(key, hit)
                return hit

        return None

    def set(
        self,
        kind: str,
        raw_key: str,
        value: Any,
        ttl_hours: int | None = None,
    ) -> None:
        """Write to both L1 and L2."""
        key = _cache_key(kind, raw_key)
        ttl = self._ttl(kind, ttl_hours)

        # L1 (TTL in seconds)
        self._l1.set(key, value)

        # L2 (TTL in hours)
        if self._enable_l2 and self._l2:
            try:
                self._l2.set(key, kind, value, ttl)
            except Exception as e:
                logger.warning(f"[PipelineCache] L2 write failed: {e}")

    def delete(self, kind: str, raw_key: str) -> None:
        key = _cache_key(kind, raw_key)
        self._l1.delete(key)
        if self._enable_l2 and self._l2:
            self._l2.delete(key)

    def evict_expired_l2(self) -> int:
        """Evict expired entries from L2 SQLite. Returns count removed."""
        if self._enable_l2 and self._l2:
            removed = self._l2.evict_expired()
            logger.info(f"[PipelineCache] Evicted {removed} expired L2 entries")
            return removed
        return 0

    def warm_l1(self, kind: str, limit: int = 200) -> int:
        """
        Pre-warm L1 from L2 for a given kind.
        Useful at startup for frequently accessed data (e.g. profiles).
        Returns count loaded.
        """
        if not self._enable_l2 or not self._l2:
            return 0
        entries = self._l2.list_by_kind(kind, limit=limit)
        for entry in entries:
            self._l1.set(entry["key"], entry["value"])
        logger.info(f"[PipelineCache] Warmed L1 with {len(entries)} '{kind}' entries")
        return len(entries)

    def stats(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "l1_size": len(self._l1),
            "l2_enabled": self._enable_l2,
        }
        if self._enable_l2 and self._l2:
            out["l2"] = self._l2.stats()
        return out


# ─────────────────────────────────────────────────────────────────────────────
# Global singleton (lazy init)
# ─────────────────────────────────────────────────────────────────────────────

_global_cache: PipelineCache | None = None


def get_pipeline_cache(
    db_path: str | Path | None = None,
    **kwargs: Any,
) -> PipelineCache:
    """
    Returns the global PipelineCache singleton.
    Creates it on first call. Thread-unsafe (single-process use).

    Usage:
        cache = get_pipeline_cache()
        data  = cache.get("profile", linkedin_url) or fetch_profile(linkedin_url)
        cache.set("profile", linkedin_url, data)
    """
    global _global_cache
    if _global_cache is None:
        _global_cache = PipelineCache(db_path=db_path, **kwargs)
    return _global_cache
