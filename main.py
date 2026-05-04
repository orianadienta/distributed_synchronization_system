#!/usr/bin/env python3
"""
main.py – Entrypoint for the distributed sync system.

Reads NODE_TYPE from the environment and starts the appropriate node:
    lock   → LockManagerNode
    queue  → QueueNode
    cache  → CacheNode

Peer list format (PEERS env var):
    peer_id:host:port,peer_id:host:port,...
    e.g.  node-2:127.0.0.1:8002,node-3:127.0.0.1:8003

Usage
-----
    # Single lock-manager node (dev):
    NODE_ID=node-1 NODE_PORT=8001 NODE_TYPE=lock \
        PEERS="node-2:127.0.0.1:8002,node-3:127.0.0.1:8003" \
        python main.py

    # Via Docker Compose:
    docker compose -f docker/docker-compose.yml up
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys

from dotenv import load_dotenv

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level    = getattr(logging, log_level, logging.INFO),
    format   = "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt  = "%Y-%m-%dT%H:%M:%S",
    stream   = sys.stdout,
)
logger = logging.getLogger("main")


# ── Peer parsing ──────────────────────────────────────────────────────────────

def parse_peers(raw: str) -> dict[str, str]:
    """
    Parse  "id:host:port,id:host:port"  →  {id: "host:port", ...}
    Supports both formats:
        node-2:127.0.0.1:8002
        node-2:node-2:8002     (hostname same as service name in Docker)
    """
    peers: dict[str, str] = {}
    if not raw or not raw.strip():
        return peers
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        parts = entry.split(":", 2)
        if len(parts) != 3:
            logger.warning("Ignoring malformed peer entry: %r (expected id:host:port)", entry)
            continue
        peer_id, host, port = parts
        peers[peer_id.strip()] = f"{host.strip()}:{port.strip()}"
    return peers


# ── Node factory ──────────────────────────────────────────────────────────────

def build_node(node_type: str, cfg, peers: dict[str, str]):
    if node_type == "lock":
        from src.nodes.lock_manager import LockManagerNode
        return LockManagerNode(cfg=cfg, node_id=cfg.node.node_id, peers=peers)

    if node_type == "queue":
        from src.nodes.queue_node import QueueNode
        return QueueNode(cfg=cfg, node_id=cfg.node.node_id, peers=peers)

    if node_type == "cache":
        from src.nodes.cache_node import CacheNode
        return CacheNode(cfg=cfg, node_id=cfg.node.node_id, peers=peers)

    raise ValueError(
        f"Unknown NODE_TYPE={node_type!r}. "
        f"Valid options: lock | queue | cache"
    )


# ── Main ──────────────────────────────────────────────────────────────────────

async def _run() -> None:
    from src.utils.config import get_config
    cfg = get_config()

    node_type = os.getenv("NODE_TYPE", "lock").lower()
    raw_peers = os.getenv("PEERS", "")
    peers     = parse_peers(raw_peers)

    logger.info(
        "Starting node  id=%s  type=%s  host=%s  port=%d  peers=%s",
        cfg.node.node_id,
        node_type,
        cfg.node.host,
        cfg.node.port,
        peers,
    )

    node = build_node(node_type, cfg, peers)

    # ── Graceful shutdown ─────────────────────────────────────────────────────
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _signal_handler():
        logger.info("Received shutdown signal")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler for all signals
            pass

    await node.start()

    logger.info(
        "Node %s is UP on %s:%d – waiting for shutdown signal",
        cfg.node.node_id, cfg.node.host, cfg.node.port,
    )

    # ── Keep-alive loop (print periodic status) ───────────────────────────────
    async def _heartbeat_log():
        while not stop_event.is_set():
            await asyncio.sleep(30)
            snap = node.raft.state_snapshot()
            logger.info(
                "[Status] role=%s term=%d commit=%d applied=%d log=%d",
                snap["role"],
                snap["current_term"],
                snap["commit_index"],
                snap.get("last_applied", 0),
                snap["log_length"],
            )

    status_task = asyncio.create_task(_heartbeat_log())

    await stop_event.wait()

    logger.info("Shutting down node %s …", cfg.node.node_id)
    status_task.cancel()
    await node.stop()
    logger.info("Node %s stopped cleanly.", cfg.node.node_id)


def main() -> None:
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()