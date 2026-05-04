"""
Distributed Cache Node – MESI coherence protocol + LRU replacement.

MESI States
-----------
M  Modified  – dirty, only this cache has a valid copy
E  Exclusive – clean, only this cache has a copy
S  Shared    – clean, multiple caches may have a copy
I  Invalid   – no valid data

Transitions are broadcast via Raft so all nodes agree on ownership.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from aiohttp import web

from src.nodes.base_node import BaseNode
from src.utils.config import AppConfig

logger = logging.getLogger(__name__)


class MESIState(str, Enum):
    MODIFIED  = "M"
    EXCLUSIVE = "E"
    SHARED    = "S"
    INVALID   = "I"


@dataclass
class CacheLine:
    key:        str
    value:      Any
    state:      MESIState
    owner:      str          # node_id that last wrote
    version:    int  = 0
    accessed_at: float = field(default_factory=time.time)
    created_at:  float = field(default_factory=time.time)
    ttl:         float = 300.0

    @property
    def is_expired(self) -> bool:
        return time.time() > self.created_at + self.ttl


# ── Commands ──────────────────────────────────────────────────────────────────

CMD_WRITE       = "cache_write"
CMD_INVALIDATE  = "cache_invalidate"
CMD_DOWNGRADE   = "cache_downgrade"   # M/E → S


class LRUCache:
    """Thread-safe (asyncio) LRU cache with capacity limit."""

    def __init__(self, capacity: int):
        self.capacity = capacity
        self._data: OrderedDict[str, CacheLine] = OrderedDict()
        self.hits   = 0
        self.misses = 0
        self.evictions = 0

    def get(self, key: str) -> Optional[CacheLine]:
        line = self._data.get(key)
        if line is None or line.is_expired or line.state == MESIState.INVALID:
            self.misses += 1
            return None
        self._data.move_to_end(key)
        line.accessed_at = time.time()
        self.hits += 1
        return line

    def put(self, line: CacheLine) -> Optional[str]:
        """Insert/update. Returns evicted key if capacity exceeded."""
        evicted = None
        if line.key in self._data:
            self._data.move_to_end(line.key)
        else:
            if len(self._data) >= self.capacity:
                evicted, _ = self._data.popitem(last=False)
                self.evictions += 1
        self._data[line.key] = line
        return evicted

    def invalidate(self, key: str) -> None:
        if key in self._data:
            self._data[key].state = MESIState.INVALID

    def remove(self, key: str) -> None:
        self._data.pop(key, None)

    def downgrade(self, key: str) -> None:
        line = self._data.get(key)
        if line and line.state in (MESIState.MODIFIED, MESIState.EXCLUSIVE):
            line.state = MESIState.SHARED

    def all_keys(self) -> List[str]:
        return list(self._data.keys())

    def stats(self) -> dict:
        total = self.hits + self.misses
        return {
            "size":       len(self._data),
            "capacity":   self.capacity,
            "hits":       self.hits,
            "misses":     self.misses,
            "evictions":  self.evictions,
            "hit_rate":   self.hits / total if total else 0.0,
        }


class CacheNode(BaseNode):
    """
    Distributed cache with MESI coherence via Raft.

    Read path:  check local cache → if INVALID/miss → fetch from Raft state.
    Write path: propose CMD_WRITE → apply_command updates all nodes.
    Invalidation: propose CMD_INVALIDATE → all nodes mark key invalid.
    """

    def __init__(self, cfg: AppConfig, node_id: str, peers: Dict[str, str]):
        super().__init__(
            node_id  = node_id,
            host     = cfg.node.host,
            port     = cfg.node.port,
            peers    = peers,
            data_dir = cfg.node.data_dir,
            raft_cfg = cfg.raft,
        )
        cap  = cfg.cache.max_size
        self._local_cache = LRUCache(capacity=cap)
        self._default_ttl = cfg.cache.ttl
        self._metrics_interval = cfg.cache.metrics_interval

        # Global truth: key → (value, version, owner)
        self._global_store: Dict[str, Tuple[Any, int, str]] = {}

        self._metrics_task: Optional[asyncio.Task] = None

        self._extra_routes = [
            ("GET",  "/cache/{key}",  self._http_get),
            ("PUT",  "/cache/{key}",  self._http_put),
            ("DELETE", "/cache/{key}", self._http_delete),
            ("GET",  "/cache/stats",  self._http_stats),
        ]

    async def start(self) -> None:
        await super().start()
        self._metrics_task = asyncio.create_task(self._metrics_loop())

    async def stop(self) -> None:
        if self._metrics_task:
            self._metrics_task.cancel()
        await super().stop()

    # ── State machine ─────────────────────────────────────────────────────────

    async def apply_command(self, index: int, term: int, command: Any) -> Any:
        op = command.get("op")

        if op == CMD_WRITE:
            key     = command["key"]
            value   = command["value"]
            owner   = command["owner"]
            ttl     = command.get("ttl", self._default_ttl)
            version = command.get("version", 0)

            # Invalidate other nodes' copies (they are now S → I or E → I)
            # For nodes that are NOT the owner: set INVALID so next read re-fetches
            existing = self._global_store.get(key)
            new_ver  = (existing[1] + 1) if existing else 1

            self._global_store[key] = (value, new_ver, owner)

            if owner == self.node_id:
                line = CacheLine(
                    key=key, value=value,
                    state=MESIState.MODIFIED if existing else MESIState.EXCLUSIVE,
                    owner=owner, version=new_ver, ttl=ttl,
                )
            else:
                line = CacheLine(
                    key=key, value=value,
                    state=MESIState.INVALID,
                    owner=owner, version=new_ver, ttl=ttl,
                )
            self._local_cache.put(line)
            return {"written": True, "version": new_ver}

        if op == CMD_INVALIDATE:
            key = command["key"]
            self._global_store.pop(key, None)
            self._local_cache.invalidate(key)
            return {"invalidated": True}

        if op == CMD_DOWNGRADE:
            key = command["key"]
            self._local_cache.downgrade(key)
            return {"downgraded": True}

        return None

    # ── Public API ────────────────────────────────────────────────────────────

    async def cache_get(self, key: str) -> Optional[Any]:
        # 1. Check local LRU
        line = self._local_cache.get(key)
        if line is not None:
            return line.value

        # 2. Fall back to global store (valid on all nodes via Raft replication)
        entry = self._global_store.get(key)
        if entry is None:
            return None

        value, version, owner = entry
        # Promote to local cache as SHARED
        self._local_cache.put(CacheLine(
            key=key, value=value,
            state=MESIState.SHARED,
            owner=owner, version=version,
        ))
        return value

    async def cache_put(self, key: str, value: Any, ttl: Optional[float] = None) -> int:
        cmd = {
            "op":      CMD_WRITE,
            "key":     key,
            "value":   value,
            "owner":   self.node_id,
            "ttl":     ttl or self._default_ttl,
        }
        result = await self.propose(cmd)
        return result.get("version", 0) if result else 0

    async def cache_delete(self, key: str) -> bool:
        await self.propose({"op": CMD_INVALIDATE, "key": key})
        return True

    # ── Metrics loop ──────────────────────────────────────────────────────────

    async def _metrics_loop(self) -> None:
        g = self.metrics.gauge
        while True:
            await asyncio.sleep(self._metrics_interval)
            stats = self._local_cache.stats()
            g("cache_size",       "Cache size").set(stats["size"])
            g("cache_hits",       "Cache hits").set(stats["hits"])
            g("cache_misses",     "Cache misses").set(stats["misses"])
            g("cache_evictions",  "Cache evictions").set(stats["evictions"])
            g("cache_hit_rate",   "Cache hit rate").set(stats["hit_rate"])

    # ── HTTP handlers ─────────────────────────────────────────────────────────

    async def _http_get(self, request: web.Request) -> web.Response:
        key   = request.match_info["key"]
        value = await self.cache_get(key)
        if value is None:
            return web.json_response({"found": False}, status=404)
        return web.json_response({"found": True, "key": key, "value": value})

    async def _http_put(self, request: web.Request) -> web.Response:
        key  = request.match_info["key"]
        body = await request.json()
        ver  = await self.cache_put(key, body["value"], body.get("ttl"))
        return web.json_response({"stored": True, "version": ver})

    async def _http_delete(self, request: web.Request) -> web.Response:
        key = request.match_info["key"]
        await self.cache_delete(key)
        return web.json_response({"deleted": True})

    async def _http_stats(self, request: web.Request) -> web.Response:
        return web.json_response({
            "node_id":      self.node_id,
            "local_cache":  self._local_cache.stats(),
            "global_keys":  len(self._global_store),
        })