"""
Performance tests for the Raft consensus implementation.

Measures:
- Election latency (time from timeout to leader elected)
- Throughput: proposals/second a single leader can sustain
- Commit latency: end-to-end from propose() to apply_command()
- Log replication lag across followers

Run with:
    pytest tests/performance/test_raft_performance.py -v -s --timeout=60
"""
from __future__ import annotations

import asyncio
import os
import statistics
import sys
import tempfile
import time
from typing import Dict, List, Tuple

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from src.communication.message_passing import MessageTransport
from src.consensus.raft import NodeRole, RaftNode
from src.utils.metrics import MetricsRegistry

# ── Ports ─────────────────────────────────────────────────────────────────────
BASE_PORT = 19800

# ── Cluster factory ───────────────────────────────────────────────────────────

Applied = Dict[str, List[Tuple[int, int, object]]]


async def build_cluster(
    n: int = 3,
    tmpdir: str = None,
    election_timeout: Tuple[float, float] = (0.08, 0.15),
    heartbeat: float = 0.02,
    port_offset: int = 0,
) -> Tuple[List[RaftNode], List[MessageTransport], Applied]:
    if tmpdir is None:
        tmpdir = tempfile.mkdtemp()

    ports    = [BASE_PORT + port_offset + i for i in range(n)]
    node_ids = [f"pnode-{port_offset}-{i+1}" for i in range(n)]
    applied: Applied = {nid: [] for nid in node_ids}

    peers_maps = {
        nid: {
            oid: f"127.0.0.1:{ports[j]}"
            for j, oid in enumerate(node_ids)
            if j != i
        }
        for i, nid in enumerate(node_ids)
    }

    transports: List[MessageTransport] = []
    nodes: List[RaftNode] = []

    for i, nid in enumerate(node_ids):
        t = MessageTransport(
            node_id=nid,
            host="127.0.0.1",
            port=ports[i],
            rpc_timeout=0.15,
        )
        transports.append(t)

        captured = applied[nid]

        async def _apply(idx, term, cmd, cap=captured):
            cap.append((idx, term, cmd))
            return cmd

        node = RaftNode(
            node_id=nid,
            peers=peers_maps[nid],
            transport=t,
            apply_command=_apply,
            data_dir=os.path.join(tmpdir, nid),
            election_timeout_range=election_timeout,
            heartbeat_interval=heartbeat,
            metrics=MetricsRegistry(nid),
        )
        nodes.append(node)

    for t in transports:
        await t.start()
    for node in nodes:
        await node.start()

    return nodes, transports, applied


async def stop_cluster(nodes: List[RaftNode], transports: List[MessageTransport]) -> None:
    for node in nodes:
        await node.stop()
    for t in transports:
        await t.stop()


async def wait_for_leader(nodes: List[RaftNode], timeout: float = 5.0) -> RaftNode:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        leaders = [n for n in nodes if n.is_leader()]
        if len(leaders) == 1:
            return leaders[0]
        await asyncio.sleep(0.01)
    raise TimeoutError(f"No leader elected within {timeout}s")


# ─────────────────────────────────────────────────────────────────────────────
# 1. Election latency
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_election_latency():
    """
    Measure how long it takes from cluster start until a leader is elected.
    Runs 5 independent clusters; reports min/mean/max.
    """
    RUNS   = 5
    OFFSET = 0      # port offset block
    latencies: List[float] = []

    for run in range(RUNS):
        nodes, transports, _ = await build_cluster(
            n=3,
            election_timeout=(0.08, 0.15),
            heartbeat=0.02,
            port_offset=OFFSET + run * 3,
        )
        t0 = time.monotonic()
        try:
            await wait_for_leader(nodes, timeout=5.0)
            elapsed = time.monotonic() - t0
            latencies.append(elapsed)
        finally:
            await stop_cluster(nodes, transports)

    print("\n── Election latency ───────────────────────────────────")
    print(f"  samples : {RUNS}")
    print(f"  min     : {min(latencies)*1000:.1f} ms")
    print(f"  mean    : {statistics.mean(latencies)*1000:.1f} ms")
    print(f"  max     : {max(latencies)*1000:.1f} ms")
    if len(latencies) > 1:
        print(f"  stdev   : {statistics.stdev(latencies)*1000:.1f} ms")

    # Assertion: all elections should complete in < 1 second
    assert all(l < 1.0 for l in latencies), \
        f"Some elections took > 1s: {latencies}"


# ─────────────────────────────────────────────────────────────────────────────
# 2. Proposal throughput
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_proposal_throughput():
    """
    Submit N sequential proposals and measure proposals/second.
    Sequential (not concurrent) to stress the commit path.
    """
    N_PROPOSALS = 50
    OFFSET      = 30

    nodes, transports, applied = await build_cluster(
        n=3,
        election_timeout=(0.08, 0.15),
        heartbeat=0.02,
        port_offset=OFFSET,
    )
    try:
        leader = await wait_for_leader(nodes)

        t0 = time.monotonic()
        for i in range(N_PROPOSALS):
            await leader.propose({"seq": i, "payload": "x" * 64})
        elapsed = time.monotonic() - t0

        throughput = N_PROPOSALS / elapsed

        print("\n── Proposal throughput (sequential) ───────────────────")
        print(f"  proposals : {N_PROPOSALS}")
        print(f"  elapsed   : {elapsed*1000:.1f} ms")
        print(f"  throughput: {throughput:.1f} proposals/s")

        # Very conservative: we should manage at least 10 proposals/s
        assert throughput >= 10, f"Throughput too low: {throughput:.1f} p/s"

        # Wait for all to be applied across followers
        await asyncio.sleep(0.5)
        for nid, entries in applied.items():
            seqs = {e[2]["seq"] for e in entries if isinstance(e[2], dict) and "seq" in e[2]}
            assert len(seqs) == N_PROPOSALS, \
                f"Node {nid} only applied {len(seqs)}/{N_PROPOSALS} entries"

    finally:
        await stop_cluster(nodes, transports)


# ─────────────────────────────────────────────────────────────────────────────
# 3. Concurrent proposals
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_concurrent_proposals():
    """
    Fire N proposals concurrently from the same leader.
    All must be committed and applied on every node.
    """
    N_CONCURRENT = 20
    OFFSET       = 60

    nodes, transports, applied = await build_cluster(
        n=3,
        election_timeout=(0.08, 0.15),
        heartbeat=0.02,
        port_offset=OFFSET,
    )
    try:
        leader = await wait_for_leader(nodes)

        t0 = time.monotonic()
        tasks = [
            asyncio.create_task(leader.propose({"cseq": i}))
            for i in range(N_CONCURRENT)
        ]
        results = await asyncio.gather(*tasks)
        elapsed = time.monotonic() - t0

        throughput = N_CONCURRENT / elapsed
        print("\n── Concurrent proposal throughput ─────────────────────")
        print(f"  concurrent proposals : {N_CONCURRENT}")
        print(f"  elapsed              : {elapsed*1000:.1f} ms")
        print(f"  throughput           : {throughput:.1f} proposals/s")

        assert len(results) == N_CONCURRENT

        await asyncio.sleep(0.5)
        for nid, entries in applied.items():
            seqs = {e[2]["cseq"] for e in entries if isinstance(e[2], dict) and "cseq" in e[2]}
            assert len(seqs) == N_CONCURRENT, \
                f"Node {nid} only applied {len(seqs)}/{N_CONCURRENT} concurrent entries"

    finally:
        await stop_cluster(nodes, transports)


# ─────────────────────────────────────────────────────────────────────────────
# 4. Commit latency
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_commit_latency():
    """
    Measure per-proposal round-trip time: propose() → apply_command() resolved.
    """
    N          = 30
    OFFSET     = 90
    latencies: List[float] = []

    nodes, transports, applied = await build_cluster(
        n=3,
        election_timeout=(0.08, 0.15),
        heartbeat=0.02,
        port_offset=OFFSET,
    )
    try:
        leader = await wait_for_leader(nodes)

        for i in range(N):
            t0 = time.monotonic()
            await leader.propose({"lat_seq": i})
            latencies.append(time.monotonic() - t0)

        print("\n── Commit latency (propose → apply) ────────────────────")
        print(f"  samples : {N}")
        print(f"  min     : {min(latencies)*1000:.2f} ms")
        print(f"  mean    : {statistics.mean(latencies)*1000:.2f} ms")
        print(f"  p95     : {sorted(latencies)[int(N*0.95)]*1000:.2f} ms")
        print(f"  max     : {max(latencies)*1000:.2f} ms")
        if len(latencies) > 1:
            print(f"  stdev   : {statistics.stdev(latencies)*1000:.2f} ms")

        # p95 should be under 500 ms on loopback
        p95 = sorted(latencies)[int(N * 0.95)]
        assert p95 < 0.5, f"p95 commit latency too high: {p95*1000:.2f} ms"

    finally:
        await stop_cluster(nodes, transports)


# ─────────────────────────────────────────────────────────────────────────────
# 5. Replication lag
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_replication_lag():
    """
    After the leader commits an entry, measure how long followers take
    to apply it.  We do this by checking applied counts with a tight loop.
    """
    N_ENTRIES = 10
    OFFSET    = 120
    lag_times: List[float] = []

    nodes, transports, applied = await build_cluster(
        n=3,
        election_timeout=(0.08, 0.15),
        heartbeat=0.02,
        port_offset=OFFSET,
    )
    try:
        leader = await wait_for_leader(nodes)
        followers = [n for n in nodes if not n.is_leader()]

        for seq in range(N_ENTRIES):
            t0 = time.monotonic()
            await leader.propose({"lag_seq": seq})
            committed_at = time.monotonic()

            # Wait for all followers to apply this entry
            target_seq = seq
            deadline   = time.monotonic() + 2.0
            all_applied = False
            while time.monotonic() < deadline:
                counts = [
                    sum(
                        1 for e in applied[f.node_id]
                        if isinstance(e[2], dict) and e[2].get("lag_seq") == target_seq
                    )
                    for f in followers
                ]
                if all(c >= 1 for c in counts):
                    all_applied = True
                    break
                await asyncio.sleep(0.005)

            lag = time.monotonic() - committed_at
            lag_times.append(lag)
            assert all_applied, f"Entry lag_seq={seq} not applied by all followers"

        print("\n── Replication lag (commit → follower applied) ─────────")
        print(f"  entries : {N_ENTRIES}")
        print(f"  min     : {min(lag_times)*1000:.2f} ms")
        print(f"  mean    : {statistics.mean(lag_times)*1000:.2f} ms")
        print(f"  max     : {max(lag_times)*1000:.2f} ms")

        assert statistics.mean(lag_times) < 0.2, \
            f"Mean replication lag too high: {statistics.mean(lag_times)*1000:.2f} ms"

    finally:
        await stop_cluster(nodes, transports)


# ─────────────────────────────────────────────────────────────────────────────
# 6. Log growth under load
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_log_consistency_after_burst():
    """
    After a burst of proposals, verify that every node has the same
    log length and commit index.
    """
    N      = 40
    OFFSET = 150

    nodes, transports, _ = await build_cluster(
        n=3,
        election_timeout=(0.08, 0.15),
        heartbeat=0.02,
        port_offset=OFFSET,
    )
    try:
        leader = await wait_for_leader(nodes)

        for i in range(N):
            await leader.propose({"burst": i})

        # Give followers time to catch up
        await asyncio.sleep(0.5)

        commit_indices = [n.commit_index for n in nodes]
        log_lengths    = [n.log_length   for n in nodes]

        print("\n── Log consistency after burst ─────────────────────────")
        print(f"  commit_indices : {commit_indices}")
        print(f"  log_lengths    : {log_lengths}")

        # All commit indices should be equal (or within 1 of leader)
        assert max(commit_indices) - min(commit_indices) <= 1, \
            f"Commit indices diverged: {commit_indices}"
        assert max(log_lengths) - min(log_lengths) <= 1, \
            f"Log lengths diverged: {log_lengths}"

    finally:
        await stop_cluster(nodes, transports)