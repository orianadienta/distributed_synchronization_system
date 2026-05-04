"""
Unit tests for Raft core logic (no network, no asyncio overhead).
We test the pure-Python helpers directly.
"""
import asyncio
import math
import os
import sys
import time
import tempfile
import pytest

# Make sure src/ is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from src.consensus.raft import (
    LogEntry,
    NodeRole,
    RaftNode,
    PersistentState,
    VolatileState,
)
from src.utils.metrics import MetricsRegistry


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

class FakeTransport:
    """Minimal transport stub – no actual network."""

    def __init__(self):
        self._handlers = {}
        self._app = _FakeApp()

    def register_handler(self, msg_type, handler):
        self._handlers[msg_type] = handler

    async def start(self): pass
    async def stop(self):  pass
    async def send(self, *args, **kwargs): return None
    async def broadcast(self, *args, **kwargs): return {}


class _FakeApp:
    class _Router:
        def add_post(self, *a): pass
        def add_get(self, *a):  pass
        def add_route(self, *a): pass
    router = _Router()


async def _noop_apply(index, term, command):
    return None


def make_node(node_id="node-1", peers=None, tmpdir=None):
    if peers is None:
        peers = {}
    if tmpdir is None:
        tmpdir = tempfile.mkdtemp()
    transport = FakeTransport()
    metrics   = MetricsRegistry(node_id)
    node = RaftNode(
        node_id        = node_id,
        peers          = peers,
        transport      = transport,
        apply_command  = _noop_apply,
        data_dir       = tmpdir,
        metrics        = metrics,
    )
    return node


# ─────────────────────────────────────────────────────────────────────────────
# LogEntry serialisation
# ─────────────────────────────────────────────────────────────────────────────

def test_log_entry_roundtrip():
    entry = LogEntry(index=5, term=3, command={"action": "set", "key": "x", "value": 42})
    assert LogEntry.from_dict(entry.to_dict()) == entry


def test_log_entry_primitive_command():
    entry = LogEntry(index=1, term=1, command="hello")
    assert LogEntry.from_dict(entry.to_dict()).command == "hello"


# ─────────────────────────────────────────────────────────────────────────────
# Quorum calculation
# ─────────────────────────────────────────────────────────────────────────────

def test_quorum_single_node():
    node = make_node()
    assert node._quorum_size() == 1


def test_quorum_three_nodes():
    node = make_node(peers={"n2": "h:2", "n3": "h:3"})
    assert node._quorum_size() == 2


def test_quorum_five_nodes():
    node = make_node(peers={f"n{i}": f"h:{i}" for i in range(2, 6)})
    assert node._quorum_size() == 3


# ─────────────────────────────────────────────────────────────────────────────
# Log helpers
# ─────────────────────────────────────────────────────────────────────────────

def test_last_log_index_empty():
    node = make_node()
    assert node._last_log_index() == 0
    assert node._last_log_term()  == 0


def test_append_and_retrieve():
    node = make_node()
    node._ps.current_term = 1
    idx, term = node._append_to_log({"key": "a"})
    assert idx == 1
    assert term == 1
    assert node._last_log_index() == 1
    entry = node._log_entry_at(1)
    assert entry is not None
    assert entry.command == {"key": "a"}


def test_truncate_log():
    node = make_node()
    node._ps.current_term = 1
    for i in range(5):
        node._append_to_log(f"cmd{i}")
    assert node._last_log_index() == 5
    node._truncate_log_from(3)
    assert node._last_log_index() == 2
    assert node._log_entry_at(3) is None


# ─────────────────────────────────────────────────────────────────────────────
# Role transitions
# ─────────────────────────────────────────────────────────────────────────────

def test_become_follower():
    node = make_node()
    node._become_follower(term=5, leader_id="node-2")
    assert node._role       == NodeRole.FOLLOWER
    assert node._ps.current_term == 5
    assert node._leader_id  == "node-2"
    assert node._ps.voted_for is None


def test_become_candidate():
    node = make_node()
    node._ps.current_term = 2
    node._become_candidate()
    assert node._role            == NodeRole.CANDIDATE
    assert node._ps.current_term == 3
    assert node._ps.voted_for    == "node-1"


@pytest.mark.asyncio
async def test_become_leader():
    node = make_node(peers={"n2": "h:2"})
    node._ps.current_term = 1
    node._become_leader()
    assert node._role      == NodeRole.LEADER
    assert node._leader_id == "node-1"
    assert node._lvs is not None
    assert "n2" in node._lvs.next_index
    await asyncio.sleep(0) 


# ─────────────────────────────────────────────────────────────────────────────
# Candidate log up-to-date check
# ─────────────────────────────────────────────────────────────────────────────

def test_candidate_log_up_to_date_empty_log():
    node = make_node()
    assert node._candidate_log_up_to_date(0, 0) is True


def test_candidate_log_stale_term():
    node = make_node()
    node._ps.current_term = 3
    node._append_to_log("cmd")
    # Candidate has lower term on last entry
    assert node._candidate_log_up_to_date(5, 2) is False


def test_candidate_log_equal_term_longer():
    node = make_node()
    node._ps.current_term = 2
    node._append_to_log("a")
    node._append_to_log("b")
    # Candidate same term, longer log → ok
    assert node._candidate_log_up_to_date(3, 2) is True


# ─────────────────────────────────────────────────────────────────────────────
# Persistent state save/load
# ─────────────────────────────────────────────────────────────────────────────

def test_persistent_state_roundtrip():
    with tempfile.TemporaryDirectory() as tmpdir:
        node = make_node(tmpdir=tmpdir)
        node._ps.current_term = 7
        node._ps.voted_for    = "node-3"
        node._append_to_log({"x": 1})
        node._append_to_log({"x": 2})
        node._save_persistent_state()

        # New node, same dir → loads state
        node2 = make_node(tmpdir=tmpdir)
        node2._load_persistent_state()
        assert node2._ps.current_term == 7
        assert node2._ps.voted_for    == "node-3"
        assert len(node2._ps.log)     == 2


# ─────────────────────────────────────────────────────────────────────────────
# Advance commit index
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_advance_commit_index():
    node = make_node(peers={"n2": "h:2", "n3": "h:3"})
    node._ps.current_term = 1
    node._become_leader()

    node._append_to_log("a")
    node._append_to_log("b")
    node._append_to_log("c")

    # Simulate n2 and n3 have replicated up to index 2
    node._lvs.match_index["n2"] = 2
    node._lvs.match_index["n3"] = 2

    node._advance_commit_index()
    assert node._vs.commit_index == 2


# ─────────────────────────────────────────────────────────────────────────────
# RPC handler: RequestVote
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_request_vote_grant():
    node = make_node()
    node._ps.current_term = 1

    msg = {
        "payload": {
            "term":           2,
            "candidate_id":   "node-2",
            "last_log_index": 0,
            "last_log_term":  0,
        }
    }
    resp = await node._handle_request_vote(msg)
    assert resp["payload"]["vote_granted"] is True
    assert node._ps.voted_for == "node-2"


@pytest.mark.asyncio
async def test_request_vote_reject_stale_term():
    node = make_node()
    node._ps.current_term = 5

    msg = {
        "payload": {
            "term":           3,
            "candidate_id":   "node-2",
            "last_log_index": 0,
            "last_log_term":  0,
        }
    }
    resp = await node._handle_request_vote(msg)
    assert resp["payload"]["vote_granted"] is False


@pytest.mark.asyncio
async def test_request_vote_reject_already_voted():
    node = make_node()
    node._ps.current_term = 2
    node._ps.voted_for    = "node-3"

    msg = {
        "payload": {
            "term":           2,
            "candidate_id":   "node-2",
            "last_log_index": 0,
            "last_log_term":  0,
        }
    }
    resp = await node._handle_request_vote(msg)
    assert resp["payload"]["vote_granted"] is False


# ─────────────────────────────────────────────────────────────────────────────
# RPC handler: AppendEntries (heartbeat)
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_append_entries_heartbeat():
    node = make_node()
    node._ps.current_term = 1

    msg = {
        "payload": {
            "term":           1,
            "leader_id":      "node-2",
            "prev_log_index": 0,
            "prev_log_term":  0,
            "entries":        [],
            "leader_commit":  0,
        }
    }
    resp = await node._handle_append_entries(msg)
    assert resp["payload"]["success"] is True
    assert node._leader_id == "node-2"


@pytest.mark.asyncio
async def test_append_entries_adds_log():
    node = make_node()
    node._ps.current_term = 1

    entries = [{"index": 1, "term": 1, "command": "cmd1"}]
    msg = {
        "payload": {
            "term":           1,
            "leader_id":      "leader",
            "prev_log_index": 0,
            "prev_log_term":  0,
            "entries":        entries,
            "leader_commit":  0,
        }
    }
    resp = await node._handle_append_entries(msg)
    assert resp["payload"]["success"] is True
    assert node._last_log_index() == 1
    assert node._log_entry_at(1).command == "cmd1"


@pytest.mark.asyncio
async def test_append_entries_conflict_truncates():
    node = make_node()
    node._ps.current_term = 2
    # Existing log: indices 1,2 at term 1
    node._ps.log = [
        LogEntry(1, 1, "old-1"),
        LogEntry(2, 1, "old-2"),
    ]

    # Leader sends entry 2 at term 2 (conflict!)
    entries = [{"index": 2, "term": 2, "command": "new-2"}]
    msg = {
        "payload": {
            "term":           2,
            "leader_id":      "leader",
            "prev_log_index": 1,
            "prev_log_term":  1,
            "entries":        entries,
            "leader_commit":  0,
        }
    }
    resp = await node._handle_append_entries(msg)
    assert resp["payload"]["success"] is True
    assert node._last_log_index() == 2
    assert node._log_entry_at(2).term == 2
    assert node._log_entry_at(2).command == "new-2"


@pytest.mark.asyncio
async def test_append_entries_reject_lower_term():
    node = make_node()
    node._ps.current_term = 5

    msg = {
        "payload": {
            "term":           3,
            "leader_id":      "old-leader",
            "prev_log_index": 0,
            "prev_log_term":  0,
            "entries":        [],
            "leader_commit":  0,
        }
    }
    resp = await node._handle_append_entries(msg)
    assert resp["payload"]["success"] is False


# ─────────────────────────────────────────────────────────────────────────────
# Metrics integration
# ─────────────────────────────────────────────────────────────────────────────

def test_metrics_populated_on_log_append():
    node = make_node()
    node._ps.current_term = 1
    initial = node.metrics._counters.get("raft_log_entries_appended")
    node._append_to_log("x")
    assert node.metrics._counters["raft_log_entries_appended"].value >= 1


def test_metrics_elections_started():
    node = make_node()
    node._ps.current_term = 0
    node._become_candidate()
    assert node.metrics._counters["raft_elections_started"].value == 1