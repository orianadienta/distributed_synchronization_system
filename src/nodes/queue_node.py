"""
Distributed Queue Node – consistent hashing + at-least-once delivery.
(Stub with core structure; extend for full persistence via Redis.)
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from aiohttp import web

from src.nodes.base_node import BaseNode
from src.utils.config import AppConfig

logger = logging.getLogger(__name__)


# ── Consistent Hash Ring ─────────────────────────────────────────────────────

class ConsistentHashRing:
    """Maps queue names → node IDs using virtual nodes."""

    def __init__(self, virtual_nodes: int = 150):
        self.virtual_nodes = virtual_nodes
        self._ring: Dict[int, str] = {}
        self._sorted_keys: List[int] = []

    def _hash(self, key: str) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def add_node(self, node_id: str) -> None:
        for i in range(self.virtual_nodes):
            h = self._hash(f"{node_id}:vn{i}")
            self._ring[h] = node_id
        self._sorted_keys = sorted(self._ring.keys())

    def remove_node(self, node_id: str) -> None:
        for i in range(self.virtual_nodes):
            h = self._hash(f"{node_id}:vn{i}")
            self._ring.pop(h, None)
        self._sorted_keys = sorted(self._ring.keys())

    def get_node(self, key: str) -> Optional[str]:
        if not self._ring:
            return None
        h = self._hash(key)
        for k in self._sorted_keys:
            if h <= k:
                return self._ring[k]
        return self._ring[self._sorted_keys[0]]

    def get_replicas(self, key: str, count: int) -> List[str]:
        """Return `count` distinct nodes for replication."""
        if not self._ring:
            return []
        h   = self._hash(key)
        idx = 0
        for i, k in enumerate(self._sorted_keys):
            if h <= k:
                idx = i
                break
        seen: List[str] = []
        for i in range(len(self._sorted_keys)):
            node = self._ring[self._sorted_keys[(idx + i) % len(self._sorted_keys)]]
            if node not in seen:
                seen.append(node)
            if len(seen) == count:
                break
        return seen


# ── Message dataclass ─────────────────────────────────────────────────────────

@dataclass
class QueueMessage:
    message_id:  str
    queue_name:  str
    payload:     Any
    producer_id: str
    created_at:  float = field(default_factory=time.time)
    ack_timeout: float = 30.0
    attempts:    int   = 0
    delivered_at: Optional[float] = None

    def to_dict(self) -> dict:
        return {
            "message_id":  self.message_id,
            "queue_name":  self.queue_name,
            "payload":     self.payload,
            "producer_id": self.producer_id,
            "created_at":  self.created_at,
            "ack_timeout": self.ack_timeout,
            "attempts":    self.attempts,
        }

    @staticmethod
    def from_dict(d: dict) -> "QueueMessage":
        return QueueMessage(**{k: d[k] for k in d if k in QueueMessage.__dataclass_fields__})


# ── Commands ──────────────────────────────────────────────────────────────────

CMD_ENQUEUE = "enqueue"
CMD_ACK     = "ack"
CMD_NACK    = "nack"


class QueueNode(BaseNode):
    """
    Distributed queue node backed by Raft for ordering guarantees.

    * Producers call  POST /queue/enqueue
    * Consumers call  POST /queue/consume  (returns one message)
    * Consumers ack   POST /queue/ack
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
        vn = cfg.queue.virtual_nodes
        self._ring = ConsistentHashRing(virtual_nodes=vn)
        self._ring.add_node(node_id)
        for pid in peers:
            self._ring.add_node(pid)

        # State machine
        self._queues:     Dict[str, List[QueueMessage]] = defaultdict(list)
        self._in_flight:  Dict[str, QueueMessage]       = {}   # message_id → msg
        self._ack_timeout = cfg.queue.ack_timeout

        # Background redelivery
        self._redelivery_task: Optional[asyncio.Task] = None

        self._extra_routes = [
            ("POST", "/queue/enqueue",  self._http_enqueue),
            ("POST", "/queue/consume",  self._http_consume),
            ("POST", "/queue/ack",      self._http_ack),
            ("GET",  "/queue/status",   self._http_status_q),
        ]

    async def start(self) -> None:
        await super().start()
        self._redelivery_task = asyncio.create_task(self._redelivery_loop())

    async def stop(self) -> None:
        if self._redelivery_task:
            self._redelivery_task.cancel()
        await super().stop()

    # ── State machine ─────────────────────────────────────────────────────────

    async def apply_command(self, index: int, term: int, command: Any) -> Any:
        op = command.get("op")
        if op == CMD_ENQUEUE:
            msg = QueueMessage.from_dict(command["message"])
            self._queues[msg.queue_name].append(msg)
            return {"enqueued": True, "message_id": msg.message_id}
        if op == CMD_ACK:
            self._in_flight.pop(command["message_id"], None)
            return {"acked": True}
        if op == CMD_NACK:
            mid = command["message_id"]
            msg = self._in_flight.pop(mid, None)
            if msg:
                msg.attempts += 1
                self._queues[msg.queue_name].insert(0, msg)   # re-queue at front
            return {"nacked": True}
        return None

    # ── Public API ────────────────────────────────────────────────────────────

    async def enqueue(self, queue_name: str, payload: Any, producer_id: str) -> str:
        message = QueueMessage(
            message_id  = str(uuid.uuid4()),
            queue_name  = queue_name,
            payload     = payload,
            producer_id = producer_id,
        )
        await self.propose({"op": CMD_ENQUEUE, "message": message.to_dict()})
        return message.message_id

    def consume(self, queue_name: str) -> Optional[QueueMessage]:
        """Dequeue one message and put it in_flight (not Raft-replicated for speed)."""
        q = self._queues.get(queue_name, [])
        if not q:
            return None
        msg = q.pop(0)
        msg.delivered_at = time.time()
        msg.attempts += 1
        self._in_flight[msg.message_id] = msg
        return msg

    async def ack(self, message_id: str) -> bool:
        await self.propose({"op": CMD_ACK, "message_id": message_id})
        return True

    async def nack(self, message_id: str) -> bool:
        await self.propose({"op": CMD_NACK, "message_id": message_id})
        return True

    # ── At-least-once: redelivery of timed-out in-flight messages ────────────

    async def _redelivery_loop(self) -> None:
        while True:
            await asyncio.sleep(5.0)
            now = time.time()
            timed_out = [
                msg for msg in list(self._in_flight.values())
                if msg.delivered_at and (now - msg.delivered_at) > self._ack_timeout
            ]
            for msg in timed_out:
                logger.warning("[QueueNode] Redelivering %s (ack timed out)", msg.message_id)
                await self.nack(msg.message_id)

    # ── HTTP handlers ─────────────────────────────────────────────────────────

    async def _http_enqueue(self, request: web.Request) -> web.Response:
        body = await request.json()
        mid  = await self.enqueue(
            queue_name  = body["queue_name"],
            payload     = body["payload"],
            producer_id = body.get("producer_id", "anonymous"),
        )
        return web.json_response({"message_id": mid})

    async def _http_consume(self, request: web.Request) -> web.Response:
        body = await request.json()
        msg  = self.consume(body["queue_name"])
        if msg is None:
            return web.json_response({"message": None})
        return web.json_response({"message": msg.to_dict()})

    async def _http_ack(self, request: web.Request) -> web.Response:
        body = await request.json()
        op   = body.get("op", "ack")
        mid  = body["message_id"]
        if op == "ack":
            await self.ack(mid)
        else:
            await self.nack(mid)
        return web.json_response({"status": op})

    async def _http_status_q(self, request: web.Request) -> web.Response:
        return web.json_response({
            "queues": {
                name: {"depth": len(msgs), "in_flight": sum(
                    1 for m in self._in_flight.values() if m.queue_name == name
                )}
                for name, msgs in self._queues.items()
            }
        })