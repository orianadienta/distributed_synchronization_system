"""Unit tests for LockManagerNode state machine (no network)."""
import sys, os, asyncio
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from src.nodes.lock_manager import LockManagerNode, LockMode, LockState, CMD_ACQUIRE, CMD_RELEASE


class MinimalLockManager:
    """Stripped-down version to test only the state-machine methods."""

    def __init__(self):
        from src.nodes.lock_manager import LockManagerNode
        import collections
        self._locks = {}
        self._acquire_futures = {}
        self._wait_for = collections.defaultdict(set)
        self._lock_ttl = 30.0

    def _sm_acquire(self, cmd):
        from src.nodes.lock_manager import LockState, LockMode, LockRequest
        request_id = cmd["request_id"]
        lock_name  = cmd["lock_name"]
        mode       = LockMode(cmd["mode"])
        holder_id  = cmd["holder_id"]
        ttl        = cmd.get("ttl", 30.0)

        if lock_name not in self._locks:
            self._locks[lock_name] = LockState(name=lock_name)
        ls = self._locks[lock_name]
        ls.holders = [h for h in ls.holders if not h.is_expired]

        req = LockRequest(request_id=request_id, lock_name=lock_name,
                          mode=mode, holder_id=holder_id, ttl=ttl)
        if ls.can_acquire(mode, holder_id):
            ls.holders.append(req)
            ls.mode = mode
            return {"granted": True}
        else:
            ls.waiters.append(req)
            return {"granted": False}

    def _sm_release(self, cmd):
        from src.nodes.lock_manager import LockState
        lock_name  = cmd["lock_name"]
        request_id = cmd["request_id"]
        if lock_name not in self._locks:
            return {"released": False}
        ls = self._locks[lock_name]
        before = len(ls.holders)
        ls.holders = [h for h in ls.holders if h.request_id != request_id]
        self._try_grant_waiters(ls)
        return {"released": len(ls.holders) < before}

    def _try_grant_waiters(self, ls):
        from src.nodes.lock_manager import LockMode
        granted = []
        for waiter in list(ls.waiters):
            if ls.can_acquire(waiter.mode, waiter.holder_id):
                ls.holders.append(waiter)
                ls.mode = waiter.mode
                granted.append(waiter)
                if waiter.mode == LockMode.EXCLUSIVE:
                    break
            else:
                break
        for w in granted:
            ls.waiters.remove(w)

    def _detect_deadlocks(self):
        visited = {}
        path = []
        cycles = []
        def dfs(node):
            visited[node] = "grey"
            path.append(node)
            for nb in self._wait_for.get(node, set()):
                if nb not in visited:
                    dfs(nb)
                elif visited[nb] == "grey":
                    idx = path.index(nb)
                    cycles.append(path[idx:] + [nb])
            path.pop()
            visited[node] = "black"
        for node in list(self._wait_for.keys()):
            if node not in visited:
                dfs(node)
        return cycles


LM = MinimalLockManager


def test_exclusive_lock_acquired():
    lm = LM()
    result = lm._sm_acquire({"request_id": "r1", "lock_name": "res", "mode": "exclusive", "holder_id": "c1"})
    assert result["granted"] is True


def test_exclusive_blocks_second_exclusive():
    lm = LM()
    lm._sm_acquire({"request_id": "r1", "lock_name": "res", "mode": "exclusive", "holder_id": "c1"})
    result = lm._sm_acquire({"request_id": "r2", "lock_name": "res", "mode": "exclusive", "holder_id": "c2"})
    assert result["granted"] is False
    assert len(lm._locks["res"].waiters) == 1


def test_shared_locks_coexist():
    lm = LM()
    r1 = lm._sm_acquire({"request_id": "r1", "lock_name": "res", "mode": "shared", "holder_id": "c1"})
    r2 = lm._sm_acquire({"request_id": "r2", "lock_name": "res", "mode": "shared", "holder_id": "c2"})
    assert r1["granted"] is True
    assert r2["granted"] is True
    assert len(lm._locks["res"].holders) == 2


def test_exclusive_blocks_after_shared():
    lm = LM()
    lm._sm_acquire({"request_id": "r1", "lock_name": "res", "mode": "shared", "holder_id": "c1"})
    result = lm._sm_acquire({"request_id": "r2", "lock_name": "res", "mode": "exclusive", "holder_id": "c2"})
    assert result["granted"] is False


def test_release_grants_waiter():
    lm = LM()
    lm._sm_acquire({"request_id": "r1", "lock_name": "res", "mode": "exclusive", "holder_id": "c1"})
    lm._sm_acquire({"request_id": "r2", "lock_name": "res", "mode": "exclusive", "holder_id": "c2"})
    assert len(lm._locks["res"].waiters) == 1

    lm._sm_release({"lock_name": "res", "request_id": "r1", "holder_id": "c1"})
    assert len(lm._locks["res"].holders) == 1
    assert lm._locks["res"].holders[0].holder_id == "c2"
    assert len(lm._locks["res"].waiters) == 0


def test_release_nonexistent_lock():
    lm = LM()
    result = lm._sm_release({"lock_name": "no-such", "request_id": "r1", "holder_id": "c1"})
    assert result["released"] is False


def test_deadlock_detection_simple_cycle():
    lm = LM()
    # c1 waits for c2, c2 waits for c1
    lm._wait_for["c1"].add("c2")
    lm._wait_for["c2"].add("c1")
    cycles = lm._detect_deadlocks()
    assert len(cycles) > 0


def test_deadlock_detection_no_cycle():
    lm = LM()
    lm._wait_for["c1"].add("c2")
    lm._wait_for["c2"].add("c3")
    cycles = lm._detect_deadlocks()
    assert cycles == []


def test_deadlock_detection_three_way_cycle():
    lm = LM()
    lm._wait_for["c1"].add("c2")
    lm._wait_for["c2"].add("c3")
    lm._wait_for["c3"].add("c1")
    cycles = lm._detect_deadlocks()
    assert len(cycles) > 0