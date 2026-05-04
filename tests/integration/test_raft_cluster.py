"""
Integration tests: 3-node Raft cluster running in the same process.
Each node has a real asyncio server on a different loopback port.
"""
import asyncio
import os
import sys
import tempfile
import time
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from src.consensus.raft import RaftNode, NodeRole
from src.communication.message_passing import MessageTransport
from src.utils.metrics import MetricsRegistry

# ── Ports for the test cluster ────────────────────────────────────────────────
BASE_PORT = 19700


def peer_addr(port: int) -> str:
    return f"127.0.0.1:{port}"


async def build_cluster(n: int = 3, tmpdir: str = None):
    """Start an n-node Raft cluster. Returns list of RaftNode."""
    if tmpdir is None:
        tmpdir = tempfile.mkdtemp()

    ports   = [BASE_PORT + i for i in range(n)]
    node_ids = [f"node-{i+1}" for i in range(n)]
    applied: dict = {nid: [] for nid in node_ids}

    # Build all peers maps
    peers_maps = {}
    for i, nid in enumerate(node_ids):
        peers_maps[nid] = {
            oid: peer_addr(ports[j])
            for j, oid in enumerate(node_ids)
            if j != i
        }

    nodes = []
    transports = []
    for i, nid in enumerate(node_ids):
        transport = MessageTransport(
            node_id     = nid,
            host        = "127.0.0.1",
            port        = ports[i],
            rpc_timeout = 0.20,
        )
        transports.append(transport)

        captured = applied[nid]
        async def _apply(idx, term, cmd, captured=captured):
            captured.append((idx, term, cmd))
            return cmd

        metrics = MetricsRegistry(nid)
        node = RaftNode(
            node_id                = nid,
            peers                  = peers_maps[nid],
            transport              = transport,
            apply_command          = _apply,
            data_dir               = os.path.join(tmpdir, nid),
            election_timeout_range = (0.10, 0.20),
            heartbeat_interval     = 0.03,
            metrics                = metrics,
        )
        nodes.append(node)

    # Start transports first, then nodes
    for t in transports:
        await t.start()
    for node in nodes:
        await node.start()

    return nodes, transports, applied


async def stop_cluster(nodes, transports):
    for node in nodes:
        await node.stop()
    for t in transports:
        await t.stop()


async def wait_for_leader(nodes, timeout=5.0) -> RaftNode:
    deadline = time.time() + timeout
    while time.time() < deadline:
        leaders = [n for n in nodes if n.is_leader()]
        if len(leaders) == 1:
            return leaders[0]
        await asyncio.sleep(0.05)
    raise TimeoutError("No leader elected within timeout")


# ── Tests ─────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_leader_elected():
    nodes, transports, applied = await build_cluster(3)
    try:
        leader = await wait_for_leader(nodes, timeout=5.0)
        assert leader is not None
        assert leader.role == NodeRole.LEADER
        # Exactly one leader
        leaders = [n for n in nodes if n.is_leader()]
        assert len(leaders) == 1
    finally:
        await stop_cluster(nodes, transports)


@pytest.mark.asyncio
async def test_propose_and_commit():
    nodes, transports, applied = await build_cluster(3)
    try:
        leader = await wait_for_leader(nodes)
        result = await leader.propose({"key": "hello", "value": "world"})
        assert result == {"key": "hello", "value": "world"}
        # Wait for all nodes to apply
        await asyncio.sleep(0.3)
        for nid, entries in applied.items():
            cmds = [e[2] for e in entries]
            assert {"key": "hello", "value": "world"} in cmds, \
                f"Node {nid} did not apply the command. Applied: {cmds}"
    finally:
        await stop_cluster(nodes, transports)


@pytest.mark.asyncio
async def test_multiple_proposals():
    nodes, transports, applied = await build_cluster(3)
    try:
        leader = await wait_for_leader(nodes)
        for i in range(10):
            await leader.propose({"seq": i})
        await asyncio.sleep(0.5)
        for nid, entries in applied.items():
            cmds = [e[2] for e in entries]
            for i in range(10):
                assert {"seq": i} in cmds, \
                    f"Node {nid} missing seq={i}"
    finally:
        await stop_cluster(nodes, transports)


@pytest.mark.asyncio
async def test_leader_term_monotonically_increases():
    nodes, transports, applied = await build_cluster(3)
    try:
        leader = await wait_for_leader(nodes)
        term1 = leader.current_term
        assert term1 >= 1
        # All nodes should be in same term
        await asyncio.sleep(0.1)
        for node in nodes:
            assert node.current_term >= term1
    finally:
        await stop_cluster(nodes, transports)


@pytest.mark.asyncio
async def test_follower_rejects_propose():
    nodes, transports, applied = await build_cluster(3)
    try:
        leader = await wait_for_leader(nodes)
        follower = next(n for n in nodes if not n.is_leader())
        with pytest.raises(PermissionError):
            await follower.propose({"cmd": "test"})
    finally:
        await stop_cluster(nodes, transports)


@pytest.mark.asyncio
async def test_commit_index_advances():
    nodes, transports, applied = await build_cluster(3)
    try:
        leader = await wait_for_leader(nodes)
        await leader.propose({"x": 1})
        await asyncio.sleep(0.3)
        assert leader.commit_index >= 1
    finally:
        await stop_cluster(nodes, transports)