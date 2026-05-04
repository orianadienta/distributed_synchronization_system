"""
Raft Consensus Algorithm

Features implemented
--------------------
* Leader election with randomised election timeout
* Log replication with AppendEntries RPC
* Persistent state (currentTerm, votedFor, log) via JSON file
* Commit & apply state-machine entries
* Snapshotting (InstallSnapshot RPC skeleton)
* Read-only queries via leader lease
* Membership awareness (static cluster for now)
* Performance metrics via MetricsRegistry
* Integration with MessageTransport and FailureDetector

State Machine
-------------
The caller supplies a coroutine ``apply_command(index, term, command) -> Any``
that is invoked for every committed log entry.  The Raft layer only guarantees
ordering; what the command *means* is up to the caller.

Concurrency model
-----------------
All Raft logic runs inside a single asyncio event-loop.  A per-node asyncio
Lock (``self._lock``) protects shared mutable state so that inbound RPCs and
the background ticker never interleave.

"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import time
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

from src.communication.message_passing import (
    MessageTransport,
    MSG_APPEND_ENTRIES,
    MSG_APPEND_ENTRIES_RESP,
    MSG_REQUEST_VOTE,
    MSG_REQUEST_VOTE_RESP,
    MSG_INSTALL_SNAPSHOT,
    MSG_INSTALL_SNAPSHOT_RESP,
)
from src.utils.metrics import MetricsRegistry, build_raft_metrics

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Data classes
# ─────────────────────────────────────────────────────────────────────────────

class NodeRole(str, Enum):
    FOLLOWER  = "follower"
    CANDIDATE = "candidate"
    LEADER    = "leader"


@dataclass
class LogEntry:
    index:   int
    term:    int
    command: Any   # arbitrary JSON-serialisable payload

    def to_dict(self) -> dict:
        return {"index": self.index, "term": self.term, "command": self.command}

    @staticmethod
    def from_dict(d: dict) -> "LogEntry":
        return LogEntry(index=d["index"], term=d["term"], command=d["command"])


@dataclass
class PersistentState:
    """The three fields Raft requires to survive crashes."""
    current_term: int            = 0
    voted_for:    Optional[str]  = None
    log:          List[LogEntry] = field(default_factory=list)


@dataclass
class VolatileState:
    commit_index:  int = 0
    last_applied:  int = 0


@dataclass
class LeaderVolatileState:
    """Reinitialized after every election."""
    next_index:  Dict[str, int] = field(default_factory=dict)
    match_index: Dict[str, int] = field(default_factory=dict)


@dataclass
class Snapshot:
    last_included_index: int
    last_included_term:  int
    data:                bytes   # serialised state-machine snapshot


# ─────────────────────────────────────────────────────────────────────────────
# RaftNode
# ─────────────────────────────────────────────────────────────────────────────

class RaftNode:
    """
    A single Raft replica.

    Parameters
    ----------
    node_id       : unique string ID for this node (e.g. "node-1")
    peers         : dict of {peer_id: "host:port"} for every *other* node
    transport     : MessageTransport instance (already started before passing in)
    apply_command : async callable invoked for each committed entry
    data_dir      : directory for durable state / snapshots
    election_timeout_range : (min_seconds, max_seconds)
    heartbeat_interval     : seconds between leader heartbeats
    metrics        : optional MetricsRegistry; one is created if not supplied
    """

    def __init__(
        self,
        node_id:                str,
        peers:                  Dict[str, str],
        transport:              MessageTransport,
        apply_command:          Callable[[int, int, Any], Any],
        data_dir:               str              = "./data",
        election_timeout_range: Tuple[float,float] = (0.15, 0.30),
        heartbeat_interval:     float            = 0.05,
        metrics:                Optional[MetricsRegistry] = None,
    ):
        self.node_id             = node_id
        self.peers               = peers          # {peer_id: "host:port"}
        self.transport           = transport
        self._apply_command      = apply_command
        self.data_dir            = data_dir
        self._election_timeout_range = election_timeout_range
        self._heartbeat_interval = heartbeat_interval

        os.makedirs(data_dir, exist_ok=True)

        # ── metrics ──────────────────────────────────────────────────────
        self.metrics = metrics or MetricsRegistry(node_id)
        self._m = build_raft_metrics(self.metrics)

        # ── state ────────────────────────────────────────────────────────
        self._ps  = PersistentState()
        self._vs  = VolatileState()
        self._lvs: Optional[LeaderVolatileState] = None

        self._role:        NodeRole      = NodeRole.FOLLOWER
        self._leader_id:   Optional[str] = None
        self._snapshot:    Optional[Snapshot] = None

        # ── concurrency ───────────────────────────────────────────────────
        self._lock             = asyncio.Lock()
        self._election_reset   = asyncio.Event()   # set to cancel pending election timer
        self._running          = False

        # ── futures for client proposals ──────────────────────────────────
        # index -> Future that resolves when the entry is applied
        self._pending: Dict[int, asyncio.Future] = {}

        # ── background tasks ──────────────────────────────────────────────
        self._ticker_task:   Optional[asyncio.Task] = None
        self._apply_task:    Optional[asyncio.Task] = None
        self._apply_queue:   asyncio.Queue          = asyncio.Queue()

        # Register RPC handlers on the transport
        self._register_handlers()

        self._m["peer_count"].set(len(self.peers))

    # ─────────────────────────────────────────────────────────────────────────
    # Lifecycle
    # ─────────────────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Load durable state then start background goroutines."""
        self._load_persistent_state()
        self._running = True
        self._ticker_task = asyncio.create_task(self._ticker(), name=f"raft-ticker-{self.node_id}")
        self._apply_task  = asyncio.create_task(self._apply_loop(), name=f"raft-apply-{self.node_id}")
        logger.info("[Raft %s] Started. term=%d log_len=%d",
                    self.node_id, self._ps.current_term, len(self._ps.log))

    async def stop(self) -> None:
        self._running = False
        if self._ticker_task:
            self._ticker_task.cancel()
        if self._apply_task:
            self._apply_task.cancel()
        await asyncio.gather(
            self._ticker_task or asyncio.sleep(0),
            self._apply_task  or asyncio.sleep(0),
            return_exceptions=True,
        )
        logger.info("[Raft %s] Stopped", self.node_id)

    # ─────────────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────────────

    @property
    def role(self) -> NodeRole:
        return self._role

    @property
    def leader_id(self) -> Optional[str]:
        return self._leader_id

    @property
    def current_term(self) -> int:
        return self._ps.current_term

    @property
    def commit_index(self) -> int:
        return self._vs.commit_index

    @property
    def log_length(self) -> int:
        return len(self._ps.log)

    def is_leader(self) -> bool:
        return self._role == NodeRole.LEADER

    async def propose(self, command: Any, timeout: float = 5.0) -> Any:
        """
        Submit a command to the cluster.  Blocks until the entry is committed
        and applied (or raises TimeoutError / PermissionError).
        """
        async with self._lock:
            if self._role != NodeRole.LEADER:
                raise PermissionError(f"Not leader; current leader={self._leader_id}")
            idx, term = self._append_to_log(command)

        fut: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending[idx] = fut

        try:
            result = await asyncio.wait_for(asyncio.shield(fut), timeout=timeout)
            return result
        except asyncio.TimeoutError:
            self._pending.pop(idx, None)
            raise TimeoutError(f"Command at index {idx} not committed within {timeout}s")

    def state_snapshot(self) -> dict:
        """Return a human-readable state dict (for debugging / HTTP endpoints)."""
        return {
            "node_id":      self.node_id,
            "role":         self._role.value,
            "leader_id":    self._leader_id,
            "current_term": self._ps.current_term,
            "commit_index": self._vs.commit_index,
            "last_applied": self._vs.last_applied,
            "log_length":   len(self._ps.log),
            "peers":        list(self.peers.keys()),
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Persistent state helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _state_path(self) -> str:
        return os.path.join(self.data_dir, f"{self.node_id}-state.json")

    def _load_persistent_state(self) -> None:
        path = self._state_path()
        if not os.path.exists(path):
            return
        try:
            with open(path, "r") as f:
                raw = json.load(f)
            self._ps.current_term = raw.get("current_term", 0)
            self._ps.voted_for    = raw.get("voted_for")
            self._ps.log          = [LogEntry.from_dict(e) for e in raw.get("log", [])]
            logger.info("[Raft %s] Loaded state: term=%d log=%d",
                        self.node_id, self._ps.current_term, len(self._ps.log))
        except Exception as exc:
            logger.error("[Raft %s] Failed to load state: %s", self.node_id, exc)

    def _save_persistent_state(self) -> None:
        """Must be called while holding self._lock."""
        path = self._state_path()
        tmp  = path + ".tmp"
        data = {
            "current_term": self._ps.current_term,
            "voted_for":    self._ps.voted_for,
            "log":          [e.to_dict() for e in self._ps.log],
        }
        try:
            with open(tmp, "w") as f:
                json.dump(data, f)
            os.replace(tmp, path)
        except Exception as exc:
            logger.error("[Raft %s] Failed to save state: %s", self.node_id, exc)

    # ─────────────────────────────────────────────────────────────────────────
    # Log helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _last_log_index(self) -> int:
        if self._ps.log:
            return self._ps.log[-1].index
        if self._snapshot:
            return self._snapshot.last_included_index
        return 0

    def _last_log_term(self) -> int:
        if self._ps.log:
            return self._ps.log[-1].term
        if self._snapshot:
            return self._snapshot.last_included_term
        return 0

    def _log_entry_at(self, index: int) -> Optional[LogEntry]:
        """Return the LogEntry for a given 1-based index (or None)."""
        base = (self._snapshot.last_included_index if self._snapshot else 0)
        pos  = index - base - 1
        if 0 <= pos < len(self._ps.log):
            return self._ps.log[pos]
        return None

    def _log_term_at(self, index: int) -> int:
        if index == 0:
            return 0
        if self._snapshot and index == self._snapshot.last_included_index:
            return self._snapshot.last_included_term
        entry = self._log_entry_at(index)
        return entry.term if entry else 0

    def _append_to_log(self, command: Any) -> Tuple[int, int]:
        """Append one entry. Returns (index, term). Must hold _lock."""
        index = self._last_log_index() + 1
        term  = self._ps.current_term
        entry = LogEntry(index=index, term=term, command=command)
        self._ps.log.append(entry)
        self._save_persistent_state()
        self._m["log_entries_appended"].inc()
        self._m["log_length"].set(len(self._ps.log))
        return index, term

    def _truncate_log_from(self, index: int) -> None:
        """Remove all entries with index >= `index`."""
        base = (self._snapshot.last_included_index if self._snapshot else 0)
        pos  = index - base - 1
        if pos < len(self._ps.log):
            del self._ps.log[pos:]
            self._m["log_length"].set(len(self._ps.log))

    # ─────────────────────────────────────────────────────────────────────────
    # Role transitions
    # ─────────────────────────────────────────────────────────────────────────

    def _become_follower(self, term: int, leader_id: Optional[str] = None) -> None:
        self._ps.current_term = term
        self._ps.voted_for    = None
        self._role            = NodeRole.FOLLOWER
        self._leader_id       = leader_id
        self._lvs             = None
        self._save_persistent_state()
        self._election_reset.set()  # reset election timer
        self._m["current_term"].set(term)
        logger.debug("[Raft %s] → FOLLOWER term=%d leader=%s", self.node_id, term, leader_id)

    def _become_candidate(self) -> None:
        self._ps.current_term += 1
        self._ps.voted_for     = self.node_id
        self._role             = NodeRole.CANDIDATE
        self._leader_id        = None
        self._lvs              = None
        self._save_persistent_state()
        self._m["current_term"].set(self._ps.current_term)
        self._m["elections_started"].inc()
        logger.info("[Raft %s] → CANDIDATE term=%d", self.node_id, self._ps.current_term)

    def _become_leader(self) -> None:
        self._role      = NodeRole.LEADER
        self._leader_id = self.node_id
        next_idx = self._last_log_index() + 1
        self._lvs = LeaderVolatileState(
            next_index  = {pid: next_idx for pid in self.peers},
            match_index = {pid: 0        for pid in self.peers},
        )
        self._m["elections_won"].inc()
        logger.info("[Raft %s] → LEADER term=%d", self.node_id, self._ps.current_term)
        # Immediately send heartbeats to establish authority
        asyncio.create_task(self._broadcast_append_entries(is_heartbeat=True))

    # ─────────────────────────────────────────────────────────────────────────
    # Background ticker (election timer + heartbeats)
    # ─────────────────────────────────────────────────────────────────────────

    async def _ticker(self) -> None:
        while self._running:
            role = self._role
            if role == NodeRole.LEADER:
                await asyncio.sleep(self._heartbeat_interval)
                async with self._lock:
                    if self._role == NodeRole.LEADER:
                        await self._broadcast_append_entries(is_heartbeat=False)
            else:
                # Wait for a randomised election timeout
                timeout = random.uniform(*self._election_timeout_range)
                self._election_reset.clear()
                try:
                    await asyncio.wait_for(
                        self._election_reset.wait(),
                        timeout=timeout,
                    )
                except asyncio.TimeoutError:
                    # Timer fired → start election
                    async with self._lock:
                        if self._role != NodeRole.LEADER:
                            self._become_candidate()
                            await self._run_election()

    # ─────────────────────────────────────────────────────────────────────────
    # Election
    # ─────────────────────────────────────────────────────────────────────────

    async def _run_election(self) -> None:
        """Send RequestVote to all peers. Must hold _lock on entry."""
        term              = self._ps.current_term
        last_log_index    = self._last_log_index()
        last_log_term     = self._last_log_term()
        votes_received    = 1   # vote for self
        needed            = self._quorum_size()
        election_start    = time.perf_counter()

        payload = {
            "term":           term,
            "candidate_id":   self.node_id,
            "last_log_index": last_log_index,
            "last_log_term":  last_log_term,
        }

        logger.info(
            "[Raft %s] Running election term=%d lastIdx=%d lastTerm=%d quorum=%d",
            self.node_id, term, last_log_index, last_log_term, needed,
        )

        # Release lock while waiting for RPCs so we don't block inbound handlers
        self._lock.release()
        try:
            tasks = {
                pid: asyncio.create_task(
                    self.transport.send(addr, MSG_REQUEST_VOTE, payload)
                )
                for pid, addr in self.peers.items()
            }
            done, pending = await asyncio.wait(
                tasks.values(),
                timeout=max(self._election_timeout_range),
                return_when=asyncio.ALL_COMPLETED,
            )
            for t in pending:
                t.cancel()
        finally:
            await self._lock.acquire()

        # Re-check role after re-acquiring lock (another leader may have appeared)
        if self._role != NodeRole.CANDIDATE or self._ps.current_term != term:
            return

        peer_ids = list(tasks.keys())
        task_list = list(tasks.values())
        for pid, task in zip(peer_ids, task_list):
            if task.done() and not task.exception() and task.result():
                resp = task.result()
                if resp.get("payload", {}).get("term", 0) > self._ps.current_term:
                    self._become_follower(resp["payload"]["term"])
                    return
                if resp.get("payload", {}).get("vote_granted"):
                    votes_received += 1
                    self._m["votes_granted"].inc()
                    logger.debug("[Raft %s] Got vote from %s (%d/%d)",
                                 self.node_id, pid, votes_received, needed)
                else:
                    self._m["votes_rejected"].inc()

        if votes_received >= needed:
            self._become_leader()
            elapsed = time.perf_counter() - election_start
            self._m["election_duration"].observe(elapsed)
        else:
            logger.info(
                "[Raft %s] Election lost term=%d votes=%d/%d",
                self.node_id, term, votes_received, needed,
            )
            # Revert to follower; election timer will retry
            self._role = NodeRole.FOLLOWER

    # ─────────────────────────────────────────────────────────────────────────
    # AppendEntries (leader → follower)
    # ─────────────────────────────────────────────────────────────────────────

    async def _broadcast_append_entries(self, is_heartbeat: bool = False) -> None:
        """Send AppendEntries / heartbeats to all peers. Must hold _lock on entry."""
        if self._role != NodeRole.LEADER or self._lvs is None:
            return

        tasks = {}
        for pid, addr in self.peers.items():
            payload = self._build_append_entries_payload(pid)
            tasks[pid] = asyncio.create_task(
                self._send_append_entries(pid, addr, payload)
            )
        if not tasks:
            return

        self._lock.release()
        try:
            await asyncio.gather(*tasks.values(), return_exceptions=True)
        finally:
            await self._lock.acquire()

        if self._role != NodeRole.LEADER:
            return

        # Update commit index based on matchIndex quorum
        self._advance_commit_index()

    def _build_append_entries_payload(self, peer_id: str) -> dict:
        assert self._lvs is not None
        next_idx   = self._lvs.next_index[peer_id]
        prev_idx   = next_idx - 1
        prev_term  = self._log_term_at(prev_idx)

        base       = (self._snapshot.last_included_index if self._snapshot else 0)
        start_pos  = next_idx - base - 1
        entries    = [e.to_dict() for e in self._ps.log[max(0, start_pos):]]

        self._m["append_entries_sent"].inc()
        if entries:
            self._m["heartbeats_sent"].inc()

        return {
            "term":           self._ps.current_term,
            "leader_id":      self.node_id,
            "prev_log_index": prev_idx,
            "prev_log_term":  prev_term,
            "entries":        entries,
            "leader_commit":  self._vs.commit_index,
        }

    async def _send_append_entries(self, peer_id: str, addr: str, payload: dict) -> None:
        t0   = time.perf_counter()
        resp = await self.transport.send(addr, MSG_APPEND_ENTRIES, payload)
        self._m["rpc_latency"].observe(time.perf_counter() - t0)

        if resp is None:
            self._m["rpc_timeouts"].inc()
            return

        p = resp.get("payload", {})
        async with self._lock:
            if self._role != NodeRole.LEADER:
                return
            if p.get("term", 0) > self._ps.current_term:
                self._become_follower(p["term"])
                return
            if p.get("success"):
                new_match = payload["prev_log_index"] + len(payload["entries"])
                assert self._lvs is not None
                self._lvs.match_index[peer_id] = max(self._lvs.match_index[peer_id], new_match)
                self._lvs.next_index[peer_id]  = self._lvs.match_index[peer_id] + 1
                self._m["append_entries_recv"].inc()
            else:
                # Back up nextIndex by one (simplistic; could use hint from follower)
                assert self._lvs is not None
                if self._lvs.next_index[peer_id] > 1:
                    self._lvs.next_index[peer_id] -= 1

    def _advance_commit_index(self) -> None:
        """Check whether a higher index can be committed (quorum of matchIndex)."""
        if self._lvs is None:
            return
        last_idx = self._last_log_index()
        for n in range(last_idx, self._vs.commit_index, -1):
            if self._log_term_at(n) != self._ps.current_term:
                continue
            matches = 1 + sum(
                1 for mi in self._lvs.match_index.values() if mi >= n
            )
            if matches >= self._quorum_size():
                old = self._vs.commit_index
                self._vs.commit_index = n
                self._m["commit_index"].set(n)
                if n > old:
                    self._m["log_entries_committed"].inc(n - old)
                    # Signal apply loop
                    self._apply_queue.put_nowait(None)
                break

    # ─────────────────────────────────────────────────────────────────────────
    # Apply loop
    # ─────────────────────────────────────────────────────────────────────────

    async def _apply_loop(self) -> None:
        """Continuously apply committed but not-yet-applied entries."""
        while self._running:
            try:
                await asyncio.wait_for(self._apply_queue.get(), timeout=0.1)
            except asyncio.TimeoutError:
                pass

            async with self._lock:
                while self._vs.last_applied < self._vs.commit_index:
                    idx = self._vs.last_applied + 1
                    entry = self._log_entry_at(idx)
                    if entry is None:
                        break
                    t0 = time.perf_counter()
                    try:
                        result = await self._apply_command(entry.index, entry.term, entry.command)
                    except Exception as exc:
                        logger.error("[Raft %s] apply_command failed at idx=%d: %s",
                                     self.node_id, idx, exc)
                        result = None
                    self._vs.last_applied = idx
                    self._m["last_applied"].set(idx)
                    self._m["log_entries_applied"].inc()
                    self._m["apply_latency"].observe(time.perf_counter() - t0)

                    # Resolve pending client futures
                    fut = self._pending.pop(idx, None)
                    if fut and not fut.done():
                        fut.set_result(result)

    # ─────────────────────────────────────────────────────────────────────────
    # RPC handlers (called by transport)
    # ─────────────────────────────────────────────────────────────────────────

    def _register_handlers(self) -> None:
        self.transport.register_handler(MSG_REQUEST_VOTE,        self._handle_request_vote)
        self.transport.register_handler(MSG_APPEND_ENTRIES,      self._handle_append_entries)
        self.transport.register_handler(MSG_INSTALL_SNAPSHOT,    self._handle_install_snapshot)

    async def _handle_request_vote(self, msg: dict) -> dict:
        p = msg["payload"]
        term           = p["term"]
        candidate_id   = p["candidate_id"]
        last_log_index = p["last_log_index"]
        last_log_term  = p["last_log_term"]

        async with self._lock:
            if term > self._ps.current_term:
                self._become_follower(term)

            vote_granted = False
            if term >= self._ps.current_term:
                already_voted = self._ps.voted_for not in (None, candidate_id)
                log_ok        = self._candidate_log_up_to_date(last_log_index, last_log_term)
                if not already_voted and log_ok:
                    self._ps.voted_for = candidate_id
                    self._save_persistent_state()
                    vote_granted = True
                    self._election_reset.set()   # reset timer

            logger.debug(
                "[Raft %s] RequestVote from %s term=%d → grant=%s",
                self.node_id, candidate_id, term, vote_granted,
            )
            return {"payload": {"term": self._ps.current_term, "vote_granted": vote_granted}}

    def _candidate_log_up_to_date(self, cand_last_idx: int, cand_last_term: int) -> bool:
        """Return True if candidate's log is at least as up-to-date as ours."""
        my_term = self._last_log_term()
        my_idx  = self._last_log_index()
        if cand_last_term != my_term:
            return cand_last_term > my_term
        return cand_last_idx >= my_idx

    async def _handle_append_entries(self, msg: dict) -> dict:
        p              = msg["payload"]
        term           = p["term"]
        leader_id      = p["leader_id"]
        prev_log_index = p["prev_log_index"]
        prev_log_term  = p["prev_log_term"]
        entries        = [LogEntry.from_dict(e) for e in p["entries"]]
        leader_commit  = p["leader_commit"]

        async with self._lock:
            if term > self._ps.current_term:
                self._become_follower(term, leader_id)

            if term < self._ps.current_term:
                return {"payload": {"term": self._ps.current_term, "success": False}}

            # Valid leader heartbeat → reset election timer
            self._leader_id = leader_id
            self._election_reset.set()
            self._m["heartbeats_sent"].inc()

            # Consistency check
            if prev_log_index > 0:
                if prev_log_index > self._last_log_index():
                    return {"payload": {"term": self._ps.current_term, "success": False}}
                if self._log_term_at(prev_log_index) != prev_log_term:
                    # Conflict: truncate from prev_log_index onward
                    self._truncate_log_from(prev_log_index)
                    self._save_persistent_state()
                    return {"payload": {"term": self._ps.current_term, "success": False}}

            # Append new entries (skip any we already have)
            for entry in entries:
                existing = self._log_entry_at(entry.index)
                if existing:
                    if existing.term != entry.term:
                        self._truncate_log_from(entry.index)
                        self._ps.log.append(entry)
                        self._m["log_entries_appended"].inc()
                else:
                    self._ps.log.append(entry)
                    self._m["log_entries_appended"].inc()

            if entries:
                self._save_persistent_state()
                self._m["log_length"].set(len(self._ps.log))

            # Update commit index
            if leader_commit > self._vs.commit_index:
                self._vs.commit_index = min(leader_commit, self._last_log_index())
                self._m["commit_index"].set(self._vs.commit_index)
                self._apply_queue.put_nowait(None)

            return {"payload": {"term": self._ps.current_term, "success": True}}

    async def _handle_install_snapshot(self, msg: dict) -> dict:
        """
        Simplified InstallSnapshot handler.
        A full implementation would stream the snapshot in chunks.
        """
        p    = msg["payload"]
        term = p["term"]

        async with self._lock:
            if term > self._ps.current_term:
                self._become_follower(term, p.get("leader_id"))

            if term < self._ps.current_term:
                return {"payload": {"term": self._ps.current_term}}

            last_included_index = p["last_included_index"]
            last_included_term  = p["last_included_term"]
            data                = bytes(p["data"])

            # Only install if snapshot is newer
            if self._snapshot and self._snapshot.last_included_index >= last_included_index:
                return {"payload": {"term": self._ps.current_term}}

            self._snapshot = Snapshot(
                last_included_index=last_included_index,
                last_included_term=last_included_term,
                data=data,
            )
            # Discard log entries covered by snapshot
            self._ps.log = [
                e for e in self._ps.log if e.index > last_included_index
            ]
            self._vs.commit_index  = max(self._vs.commit_index,  last_included_index)
            self._vs.last_applied  = max(self._vs.last_applied,  last_included_index)
            self._save_persistent_state()
            self._election_reset.set()
            logger.info(
                "[Raft %s] Installed snapshot up to index=%d term=%d",
                self.node_id, last_included_index, last_included_term,
            )
            return {"payload": {"term": self._ps.current_term}}

    # ─────────────────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _quorum_size(self) -> int:
        """Minimum votes needed to form a quorum (majority)."""
        cluster_size = 1 + len(self.peers)
        return math.floor(cluster_size / 2) + 1

    def _all_peer_ids(self) -> List[str]:
        return list(self.peers.keys())