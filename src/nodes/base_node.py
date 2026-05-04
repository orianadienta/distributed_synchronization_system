"""
BaseNode – wires together MessageTransport, FailureDetector, and RaftNode.

Subclasses (LockManagerNode, QueueNode, CacheNode) extend this by:
1. Overriding ``apply_command`` to update their local state machine.
2. Adding extra HTTP routes to the aiohttp app if needed.
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Callable, Dict, Optional

from aiohttp import web

from src.communication.failure_detector import FailureDetector
from src.communication.message_passing import MessageTransport
from src.consensus.raft import RaftNode, NodeRole
from src.utils.config import NodeConfig, RaftConfig
from src.utils.metrics import MetricsRegistry

logger = logging.getLogger(__name__)


class BaseNode:
    """
    Fully assembled distributed node.

    Parameters
    ----------
    node_id : str
        Unique identifier (e.g. "node-1").
    host    : str
        Bind address for the HTTP server.
    port    : int
        HTTP port.
    peers   : Dict[str, str]
        ``{peer_id: "host:port"}`` for all *other* nodes.
    data_dir : str
        Directory for durable Raft state.
    node_cfg : NodeConfig   (optional)
    raft_cfg : RaftConfig   (optional)
    """

    def __init__(
        self,
        node_id:  str,
        host:     str,
        port:     int,
        peers:    Dict[str, str],
        data_dir: str        = "./data",
        node_cfg: Optional[NodeConfig] = None,
        raft_cfg: Optional[RaftConfig] = None,
    ):
        self.node_id  = node_id
        self.host     = host
        self.port     = port
        self.peers    = peers
        self.data_dir = data_dir

        self.metrics = MetricsRegistry(node_id)

        # Transport
        rpc_timeout = raft_cfg.rpc_timeout if raft_cfg else 0.10
        self.transport = MessageTransport(
            node_id    = node_id,
            host       = host,
            port       = port,
            rpc_timeout= rpc_timeout,
        )

        # Failure detector
        self.failure_detector = FailureDetector(
            on_suspect = self._on_peer_suspect,
            on_recover = self._on_peer_recover,
        )
        for pid in peers:
            self.failure_detector.register_peer(pid)

        # Raft
        et_min = raft_cfg.election_timeout_min if raft_cfg else 0.15
        et_max = raft_cfg.election_timeout_max if raft_cfg else 0.30
        hb     = raft_cfg.heartbeat_interval   if raft_cfg else 0.05

        self.raft = RaftNode(
            node_id                = node_id,
            peers                  = peers,
            transport              = self.transport,
            apply_command          = self.apply_command,
            data_dir               = os.path.join(data_dir, node_id),
            election_timeout_range = (et_min, et_max),
            heartbeat_interval     = hb,
            metrics                = self.metrics,
        )

        # Extra routes can be registered by subclasses before start()
        self._extra_routes: list = []

    # ─────────────────────────────────────────────────────────────────────────
    # Override in subclasses
    # ─────────────────────────────────────────────────────────────────────────

    async def apply_command(self, index: int, term: int, command: Any) -> Any:
        """
        Called by Raft for every committed log entry.
        Subclasses MUST override this to update their state machine.
        """
        logger.debug("[%s] apply_command idx=%d term=%d cmd=%s", self.node_id, index, term, command)
        return None

    def _on_peer_suspect(self, peer_id: str) -> None:
        logger.warning("[%s] Failure detector suspects peer %s", self.node_id, peer_id)

    def _on_peer_recover(self, peer_id: str) -> None:
        logger.info("[%s] Peer %s recovered", self.node_id, peer_id)

    # ─────────────────────────────────────────────────────────────────────────
    # Lifecycle
    # ─────────────────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._register_status_routes()
        await self.transport.start()
        await self.failure_detector.start()
        await self.raft.start()
        logger.info("[BaseNode %s] Started", self.node_id)

    async def stop(self) -> None:
        await self.raft.stop()
        await self.failure_detector.stop()
        await self.transport.stop()
        logger.info("[BaseNode %s] Stopped", self.node_id)

    # ─────────────────────────────────────────────────────────────────────────
    # Status HTTP endpoints
    # ─────────────────────────────────────────────────────────────────────────

    def _register_status_routes(self) -> None:
        app = self.transport._app
        app.router.add_get("/status",  self._http_status)
        app.router.add_get("/metrics", self._http_metrics)
        for method, path, handler in self._extra_routes:
            app.router.add_route(method, path, handler)

    async def _http_status(self, request: web.Request) -> web.Response:
        return web.json_response({
            "raft":    self.raft.state_snapshot(),
            "phi":     self.failure_detector.all_phi(),
            "metrics": self.metrics.snapshot(),
        })

    async def _http_metrics(self, request: web.Request) -> web.Response:
        return web.json_response(self.metrics.snapshot())

    # ─────────────────────────────────────────────────────────────────────────
    # Convenience
    # ─────────────────────────────────────────────────────────────────────────

    def is_leader(self) -> bool:
        return self.raft.is_leader()

    async def propose(self, command: Any, timeout: float = 5.0) -> Any:
        return await self.raft.propose(command, timeout=timeout)