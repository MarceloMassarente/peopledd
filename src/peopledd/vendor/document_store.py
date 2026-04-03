from __future__ import annotations

"""
peopledd.vendor.document_store
==============================
Standalone in-process document cache — ported from deepsearch InMemoryDocumentStore.

Interface matches what peopledd uses from deepsearch:

    store = DocumentCache(max_size=500, ttl_seconds=3600)
    store.get(key)           → str | None
    store.set(key, content)  → None
    store.delete(key)        → None
    store.clear()            → None

Also provides the add_documents / get_documents interface used in harvest_adapter.py:

    store.add_documents(user_id, session_id, documents, stage)
    store.get_documents(user_id, session_id)  → list[dict]
"""

import json
import logging
import time
from collections import OrderedDict
from typing import Any

logger = logging.getLogger(__name__)


class DocumentCache:
    """
    LRU + TTL in-memory document store.

    Replacement for deepsearch InMemoryDocumentStore.
    Thread-unsafe (single-process, single-thread use).
    """

    def __init__(
        self,
        max_size: int = 500,
        ttl_seconds: int = 3600,
        eviction_policy: str = "lru",  # only lru supported
    ):
        self._max_size = max(1, max_size)
        self._ttl = ttl_seconds
        self._store: OrderedDict[str, tuple[float, Any]] = OrderedDict()

    # ── Core KV interface ──────────────────────────────────────────────────────

    def _evict_expired(self) -> None:
        now = time.time()
        expired = [k for k, (ts, _) in self._store.items() if now - ts > self._ttl]
        for k in expired:
            del self._store[k]

    def _evict_lru(self) -> None:
        """Remove oldest entry when over capacity."""
        while len(self._store) >= self._max_size:
            self._store.popitem(last=False)

    def get(self, key: str) -> Any | None:
        self._evict_expired()
        entry = self._store.get(key)
        if entry is None:
            return None
        ts, value = entry
        # Move to end (most recently used)
        self._store.move_to_end(key)
        return value

    def set(self, key: str, value: Any) -> None:
        self._evict_expired()
        self._evict_lru()
        self._store[key] = (time.time(), value)
        self._store.move_to_end(key)

    def delete(self, key: str) -> None:
        self._store.pop(key, None)

    def clear(self) -> None:
        self._store.clear()

    def __len__(self) -> int:
        self._evict_expired()
        return len(self._store)

    # ── harvest_adapter.py compatible interface ───────────────────────────────

    def _build_key(self, user_id: str, session_id: str) -> str:
        return f"{user_id}::{session_id}"

    def add_documents(
        self,
        user_id: str,
        session_id: str,
        documents: list[dict],
        stage: str = "default",
    ) -> None:
        """
        Store documents list under (user_id, session_id) key.
        Matches deepsearch InMemoryDocumentStore.add_documents() signature.
        """
        key = self._build_key(user_id, session_id)
        existing = self.get(key) or []
        self.set(key, existing + documents)

    def get_documents(
        self,
        user_id: str,
        session_id: str,
    ) -> list[dict]:
        """
        Retrieve documents for (user_id, session_id).
        Matches deepsearch InMemoryDocumentStore.get_documents() signature.
        """
        key = self._build_key(user_id, session_id)
        return self.get(key) or []


# ─────────────────────────────────────────────────────────────────────────────
# Compat factory for harvest_adapter pattern:
# store = InMemoryDocumentStore(valves=valves)
# where valves has DOC_STORE_MAX_SIZE, DOC_STORE_DEFAULT_TTL_SECONDS, etc.
# ─────────────────────────────────────────────────────────────────────────────

class InMemoryDocumentStore(DocumentCache):
    """
    Drop-in replacement for deepsearch InMemoryDocumentStore.
    Accepts a `valves` object (or simple namespace) with optional attributes:
        DOC_STORE_MAX_SIZE             default 500
        DOC_STORE_DEFAULT_TTL_SECONDS  default 3600
        DOC_STORE_EVICTION_POLICY      ignored (always LRU)
    """

    def __init__(self, valves: Any = None, **kwargs: Any):
        max_size = 500
        ttl = 3600

        if valves is not None:
            max_size = getattr(valves, "DOC_STORE_MAX_SIZE", max_size)
            ttl = getattr(valves, "DOC_STORE_DEFAULT_TTL_SECONDS", ttl)

        # keyword overrides from direct kwargs
        max_size = kwargs.get("max_size", max_size)
        ttl = kwargs.get("ttl_seconds", ttl)

        super().__init__(max_size=int(max_size), ttl_seconds=int(ttl))
        logger.debug(f"[InMemoryDocumentStore] initialized max_size={max_size}, ttl={ttl}s")
