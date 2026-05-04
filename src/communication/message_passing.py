"""
Async HTTP-based message passing between Raft nodes.
Each node exposes an HTTP server; peers call its endpoints.
"""
import asyncio
import json
import logging
import time
from typing import Any, Dict, Optional, Tuple

import aiohttp
from aiohttp import web

logger = logging.getLogger(__name__)

# ── Message types (kept as plain strings for JSON-friendliness) ────────────

MSG_REQUEST_VOTE        = "request_vote"
MSG_REQUEST_VOTE_RESP   = "request_vote_response"
MSG_APPEND_ENTRIES      = "append_entries"
MSG_APPEND_ENTRIES_RESP = "append_entries_response"
MSG_INSTALL_SNAPSHOT    = "install_snapshot"
MSG_INSTALL_SNAPSHOT_RESP = "install_snapshot_response"


def make_message(msg_type: str, sender_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "type": msg_type,
        "sender_id": sender_id,
        "timestamp": time.time(),
        "payload": payload,
    }


# ── Transport ──────────────────────────────────────────────────────────────

class MessageTransport:
    """
    Low-level HTTP transport.
    - Sends JSON POST requests to peers.
    - Runs an aiohttp server to receive inbound messages.
    """

    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        rpc_timeout: float = 0.10,
    ):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.rpc_timeout = rpc_timeout

        self._app = web.Application()
        self._runner: Optional[web.AppRunner] = None
        self._session: Optional[aiohttp.ClientSession] = None

        # Registered message handlers: msg_type -> coroutine(msg) -> response dict
        self._handlers: Dict[str, Any] = {}

        self._setup_routes()

    # ── Route setup ───────────────────────────────────────────────────────

    def _setup_routes(self) -> None:
        self._app.router.add_post("/rpc", self._handle_rpc)
        self._app.router.add_get("/health", self._handle_health)

    async def _handle_rpc(self, request: web.Request) -> web.Response:
        try:
            msg = await request.json()
            msg_type = msg.get("type")
            handler = self._handlers.get(msg_type)
            if handler is None:
                return web.json_response({"error": f"No handler for {msg_type}"}, status=404)
            result = await handler(msg)
            return web.json_response(result)
        except Exception as exc:
            logger.exception("Error handling RPC: %s", exc)
            return web.json_response({"error": str(exc)}, status=500)

    async def _handle_health(self, request: web.Request) -> web.Response:
        return web.json_response({"node_id": self.node_id, "status": "ok"})

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.rpc_timeout)
        )
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.host, self.port)
        await site.start()
        logger.info("[Transport %s] Listening on %s:%d", self.node_id, self.host, self.port)

    async def stop(self) -> None:
        if self._session:
            await self._session.close()
        if self._runner:
            await self._runner.cleanup()
        logger.info("[Transport %s] Stopped", self.node_id)

    # ── Registration ──────────────────────────────────────────────────────

    def register_handler(self, msg_type: str, handler) -> None:
        """Register an async handler coroutine for a message type."""
        self._handlers[msg_type] = handler

    # ── Sending ───────────────────────────────────────────────────────────

    async def send(
        self,
        peer_addr: str,
        msg_type: str,
        payload: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """
        Send a message to peer_addr (format: "host:port") and await response.
        Returns None on any error (timeout, connection refused, bad status).
        """
        if self._session is None:
            raise RuntimeError("Transport not started")

        msg = make_message(msg_type, self.node_id, payload)
        url = f"http://{peer_addr}/rpc"

        try:
            async with self._session.post(url, json=msg) as resp:
                if resp.status == 200:
                    return await resp.json()
                body = await resp.text()
                logger.debug("[Transport] %s returned %d: %s", peer_addr, resp.status, body[:200])
                return None
        except asyncio.TimeoutError:
            logger.debug("[Transport] Timeout sending %s to %s", msg_type, peer_addr)
            return None
        except aiohttp.ClientConnectorError:
            logger.debug("[Transport] Cannot connect to %s", peer_addr)
            return None
        except Exception as exc:
            logger.debug("[Transport] Error sending to %s: %s", peer_addr, exc)
            return None

    async def broadcast(
        self,
        peers: list,
        msg_type: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        """Send to all peers concurrently. Returns {peer_addr: response_or_None}."""
        tasks = {peer: self.send(peer, msg_type, payload) for peer in peers}
        results = await asyncio.gather(*tasks.values(), return_exceptions=False)
        return dict(zip(tasks.keys(), results))