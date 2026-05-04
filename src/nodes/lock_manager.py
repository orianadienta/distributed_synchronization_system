"""
Distributed Lock Manager built on top of RaftNode.

Features
--------
* Shared (read) and Exclusive (write) locks
* TTL-based automatic expiry
* Distributed deadlock detection (wait-for graph + cycle detection)
* All locking decisions go through Raft log → consistent across cluster
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple

from aiohttp import web

from src.nodes.base_node import BaseNode
from src.utils.config import AppConfig

logger = logging.getLogger(__name__)


class LockMode(str, Enum):
    SHARED    = "shared"     # multiple holders allowed, no writer
    EXCLUSIVE = "exclusive"  # only one holder, no readers


@dataclass
class LockRequest:
    request_id: str
    lock_name:  str
    mode:       LockMode
    holder_id:  str
    acquired_at: float = field(default_factory=time.time)
    ttl:         float = 30.0

    @property
    def expires_at(self) -> float:
        return self.acquired_at + self.ttl

    @property
    def is_expired(self) -> bool:
        return time.time() > self.expires_at


@dataclass
class LockState:
    name:    str
    mode:    Optional[LockMode] = None
    holders: List[LockRequest]  = field(default_factory=list)
    waiters: List[LockRequest]  = field(default_factory=list)

    def can_acquire(self, mode: LockMode, requester: str) -> bool:
        active = [h for h in self.holders if not h.is_expired]
        if not active:
            return True
        if mode == LockMode.EXCLUSIVE:
            return False
        # SHARED can be granted if current holders are also SHARED
        return all(h.mode == LockMode.SHARED for h in active) and self.mode == LockMode.SHARED


# ── Commands encoded in Raft log ─────────────────────────────────────────────

CMD_ACQUIRE = "acquire"
CMD_RELEASE = "release"
CMD_EXPIRE  = "expire"


class LockManagerNode(BaseNode):
    """
    Raft-backed distributed lock manager.

    State machine: dict of lock name → LockState.
    All mutations go through Raft.propose() → apply_command().
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
        self._lock_ttl = cfg.lock.default_ttl
        self._deadlock_interval = cfg.lock.deadlock_detection_interval

        # ── State machine (only valid on leader; replicated via Raft) ────
        self._locks: Dict[str, LockState] = {}

        # ── Deadlock detection: holder_id → set of IDs it is waiting for ─
        self._wait_for: Dict[str, Set[str]] = defaultdict(set)

        # ── Background tasks ─────────────────────────────────────────────
        self._expiry_task:   Optional[asyncio.Task] = None
        self._deadlock_task: Optional[asyncio.Task] = None

        # Register extra HTTP routes
        self._extra_routes = [
            ("POST", "/lock/acquire",     self._http_acquire),
            ("POST", "/lock/release",     self._http_release),
            ("GET",  "/lock/status",      self._http_lock_status),
            ("GET",  "/lock/deadlocks",   self._http_deadlocks),
        ]

    # ─────────────────────────────────────────────────────────────────────────
    # Lifecycle
    # ─────────────────────────────────────────────────────────────────────────

    async def start(self) -> None:
        await super().start()
        self._expiry_task   = asyncio.create_task(self._expiry_loop())
        self._deadlock_task = asyncio.create_task(self._deadlock_loop())
        logger.info("[LockManager %s] Started", self.node_id)

    async def stop(self) -> None:
        for task in (self._expiry_task, self._deadlock_task):
            if task:
                task.cancel()
        await super().stop()

    # ─────────────────────────────────────────────────────────────────────────
    # Public API (called by HTTP handlers or tests)
    # ─────────────────────────────────────────────────────────────────────────

    async def acquire_lock(
        self,
        lock_name:  str,
        mode:       LockMode,
        holder_id:  str,
        ttl:        Optional[float] = None,
        timeout:    float           = 10.0,
    ) -> str:
        """
        Acquire a lock.  Returns a request_id on success.
        Raises TimeoutError if lock not granted or timeout exceeded.
        Raises PermissionError if this node is not the leader.

        Design note: RaftNode.propose() blocks until apply_command() completes
        and returns its result directly, so we use the return value instead of
        a separate asyncio.Future — the old double-wait pattern caused the
        future to already be resolved (and popped) before we awaited it.
        """
        request_id = str(uuid.uuid4())
        command = {
            "op":         CMD_ACQUIRE,
            "request_id": request_id,
            "lock_name":  lock_name,
            "mode":       mode.value,
            "holder_id":  holder_id,
            "ttl":        ttl or self._lock_ttl,
        }

        try:
            result = await self.propose(command, timeout=timeout)
        except TimeoutError:
            raise TimeoutError(f"Lock '{lock_name}' not acquired within {timeout}s")

        if result is None or not result.get("granted"):
            reason = (result or {}).get("reason", "unknown")
            raise TimeoutError(f"Lock '{lock_name}' denied: {reason}")

        return request_id

    async def release_lock(self, lock_name: str, request_id: str, holder_id: str) -> bool:
        command = {
            "op":         CMD_RELEASE,
            "request_id": request_id,
            "lock_name":  lock_name,
            "holder_id":  holder_id,
        }
        await self.propose(command)
        return True

    # ─────────────────────────────────────────────────────────────────────────
    # State machine (Raft apply_command)
    # ─────────────────────────────────────────────────────────────────────────

    async def apply_command(self, index: int, term: int, command: Any) -> Any:
        op = command.get("op")
        if op == CMD_ACQUIRE:
            return self._sm_acquire(command)
        if op == CMD_RELEASE:
            return self._sm_release(command)
        if op == CMD_EXPIRE:
            return self._sm_expire(command)
        logger.warning("[LockManager] Unknown op: %s", op)
        return None

    def _sm_acquire(self, cmd: dict) -> dict:
        request_id = cmd["request_id"]
        lock_name  = cmd["lock_name"]
        mode       = LockMode(cmd["mode"])
        holder_id  = cmd["holder_id"]
        ttl        = cmd.get("ttl", self._lock_ttl)

        if lock_name not in self._locks:
            self._locks[lock_name] = LockState(name=lock_name)

        ls = self._locks[lock_name]
        # Clean expired holders first
        ls.holders = [h for h in ls.holders if not h.is_expired]

        req = LockRequest(
            request_id = request_id,
            lock_name  = lock_name,
            mode       = mode,
            holder_id  = holder_id,
            ttl        = ttl,
        )

        if ls.can_acquire(mode, holder_id):
            ls.holders.append(req)
            ls.mode = mode
            result  = {"granted": True, "request_id": request_id}
        else:
            ls.waiters.append(req)
            # Update wait-for graph
            for holder in ls.holders:
                self._wait_for[holder_id].add(holder.holder_id)
            result = {"granted": False, "reason": "lock held by another process", "request_id": request_id}

        return result

    def _sm_release(self, cmd: dict) -> dict:
        lock_name  = cmd["lock_name"]
        request_id = cmd["request_id"]
        holder_id  = cmd["holder_id"]

        if lock_name not in self._locks:
            return {"released": False, "reason": "lock not found"}

        ls = self._locks[lock_name]
        before = len(ls.holders)
        ls.holders = [h for h in ls.holders if h.request_id != request_id]
        released = len(ls.holders) < before

        # Remove from wait-for graph
        self._wait_for.pop(holder_id, None)
        for waiting_set in self._wait_for.values():
            waiting_set.discard(holder_id)

        # Try to grant waiting requests
        self._try_grant_waiters(ls)

        if not ls.holders and not ls.waiters:
            del self._locks[lock_name]

        return {"released": released}

    def _sm_expire(self, cmd: dict) -> dict:
        lock_name  = cmd["lock_name"]
        request_id = cmd["request_id"]

        if lock_name not in self._locks:
            return {}
        ls = self._locks[lock_name]
        ls.holders = [h for h in ls.holders if h.request_id != request_id]
        self._try_grant_waiters(ls)
        return {}

    def _try_grant_waiters(self, ls: LockState) -> None:
        """After a release, attempt to grant queued waiters."""
        granted = []
        for waiter in list(ls.waiters):
            if ls.can_acquire(waiter.mode, waiter.holder_id):
                ls.holders.append(waiter)
                ls.mode = waiter.mode
                granted.append(waiter)
                if waiter.mode == LockMode.EXCLUSIVE:
                    break   # only one exclusive holder allowed
            else:
                break       # preserve FIFO ordering
        for w in granted:
            ls.waiters.remove(w)

    # ─────────────────────────────────────────────────────────────────────────
    # Background: expiry loop
    # ─────────────────────────────────────────────────────────────────────────

    async def _expiry_loop(self) -> None:
        while True:
            await asyncio.sleep(1.0)
            if not self.is_leader():
                continue
            now = time.time()
            for lock_name, ls in list(self._locks.items()):
                for holder in list(ls.holders):
                    if holder.is_expired:
                        logger.info("[LockManager] Expiring lock '%s' holder=%s",
                                    lock_name, holder.holder_id)
                        try:
                            await self.propose({
                                "op":         CMD_EXPIRE,
                                "lock_name":  lock_name,
                                "request_id": holder.request_id,
                            })
                        except Exception:
                            pass

    # ─────────────────────────────────────────────────────────────────────────
    # Background: deadlock detection (cycle detection in wait-for graph)
    # ─────────────────────────────────────────────────────────────────────────

    async def _deadlock_loop(self) -> None:
        while True:
            await asyncio.sleep(self._deadlock_interval)
            if not self.is_leader():
                continue
            cycles = self._detect_deadlocks()
            for cycle in cycles:
                logger.warning("[LockManager] Deadlock detected: %s", " → ".join(cycle))
                # Resolution: release one lock from the youngest holder
                self._resolve_deadlock(cycle)

    def _detect_deadlocks(self) -> List[List[str]]:
        """Return all simple cycles in the wait-for graph (DFS)."""
        visited:   Dict[str, str] = {}   # node → "grey" | "black"
        path:      List[str]      = []
        cycles:    List[List[str]]= []

        def dfs(node: str) -> None:
            visited[node] = "grey"
            path.append(node)
            for neighbour in self._wait_for.get(node, set()):
                if neighbour not in visited:
                    dfs(neighbour)
                elif visited[neighbour] == "grey":
                    # Found a cycle
                    idx = path.index(neighbour)
                    cycles.append(path[idx:] + [neighbour])
            path.pop()
            visited[node] = "black"

        for node in list(self._wait_for.keys()):
            if node not in visited:
                dfs(node)
        return cycles

    def _resolve_deadlock(self, cycle: List[str]) -> None:
        """Abort one victim from the cycle to break the deadlock."""
        victim = cycle[0]  # simplistic: pick first
        # Find and release any lock held by victim
        for lock_name, ls in self._locks.items():
            for holder in ls.holders:
                if holder.holder_id == victim:
                    logger.warning(
                        "[LockManager] Breaking deadlock: releasing '%s' held by %s",
                        lock_name, victim,
                    )
                    asyncio.create_task(self.propose({
                        "op":         CMD_RELEASE,
                        "lock_name":  lock_name,
                        "request_id": holder.request_id,
                        "holder_id":  victim,
                    }))
                    return

    # ─────────────────────────────────────────────────────────────────────────
    # HTTP handlers
    # ─────────────────────────────────────────────────────────────────────────

    async def _http_acquire(self, request: web.Request) -> web.Response:
        body = await request.json()
        try:
            req_id = await self.acquire_lock(
                lock_name = body["lock_name"],
                mode      = LockMode(body.get("mode", "exclusive")),
                holder_id = body["holder_id"],
                ttl       = body.get("ttl"),
                timeout   = body.get("timeout", 10.0),
            )
            return web.json_response({"status": "acquired", "request_id": req_id})
        except PermissionError as e:
            return web.json_response({"status": "error", "reason": str(e)}, status=503)
        except TimeoutError as e:
            return web.json_response({"status": "timeout", "reason": str(e)}, status=408)

    async def _http_release(self, request: web.Request) -> web.Response:
        body = await request.json()
        ok = await self.release_lock(
            lock_name  = body["lock_name"],
            request_id = body["request_id"],
            holder_id  = body["holder_id"],
        )
        return web.json_response({"status": "released" if ok else "not_found"})

    async def _http_lock_status(self, request: web.Request) -> web.Response:
        status = {}
        for name, ls in self._locks.items():
            status[name] = {
                "mode":    ls.mode.value if ls.mode else None,
                "holders": [
                    {"holder_id": h.holder_id, "mode": h.mode.value, "expires_at": h.expires_at}
                    for h in ls.holders
                ],
                "waiters": [
                    {"holder_id": w.holder_id, "mode": w.mode.value}
                    for w in ls.waiters
                ],
            }
        return web.json_response(status)

    async def _http_deadlocks(self, request: web.Request) -> web.Response:
        cycles = self._detect_deadlocks()
        return web.json_response({"deadlocks": cycles, "wait_for": {
            k: list(v) for k, v in self._wait_for.items()
        }})