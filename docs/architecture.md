# Architecture

## Overview

The distributed sync system is a collection of three independent subsystems that all share the same underlying consensus layer: **Raft**.  Every subsystem runs as a cluster of at least three nodes so that the system can tolerate the failure of any single node while still making progress.

```
┌────────────────────────────────────────────────────────────┐
│                      Client / Application                  │
└──────────┬──────────────────┬──────────────────┬───────────┘
           │                  │                  │
    ┌──────▼──────┐   ┌───────▼──────┐   ┌───────▼──────┐
    │   Lock      │   │   Queue      │   │   Cache      │
    │   Manager   │   │   System     │   │   System     │
    │  (3 nodes)  │   │  (3 nodes)   │   │  (3 nodes)   │
    └──────┬──────┘   └───────┬──────┘   └───────┬──────┘
           │                  │                  │
    ┌──────▼──────────────────▼──────────────────▼──────┐
    │               Raft Consensus Layer                 │
    │     leader election · log replication · commits    │
    └──────────────────────┬─────────────────────────────┘
                           │
                   ┌───────▼───────┐
                   │    Redis      │
                   │ (shared state │
                   │  & pub/sub)   │
                   └───────────────┘
```

---

## Raft Consensus

### Why Raft?

Raft was chosen over Paxos because its design explicitly prioritises understandability.  All three subsystems need the same guarantee: a **linearisable, totally-ordered log** that survives node failures.  Raft provides this with a clear role model (leader / follower / candidate) and a well-defined set of RPCs.

### Roles

| Role | Responsibilities |
|------|-----------------|
| **Leader** | Accepts client proposals, replicates log entries to followers, advances commit index |
| **Follower** | Accepts and persists AppendEntries from the leader, resets election timer on heartbeat |
| **Candidate** | Campaigns for election by sending RequestVote to all peers |

### Election

1. A follower's election timer fires (random in `[ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX]`).
2. The node transitions to **Candidate**, increments its term, votes for itself, and broadcasts **RequestVote**.
3. A candidate becomes **Leader** when it receives votes from a strict majority of the cluster.
4. Split-vote? The timer fires again with a fresh random delay, restarting the election.

The randomised timeout (150–300 ms default) makes split elections statistically rare.

### Log Replication

```
Client → propose(cmd)
           │
           ▼
        Leader appends to log (in-memory + disk)
           │
           ├─── AppendEntries RPC → Follower-1 ──► ack
           ├─── AppendEntries RPC → Follower-2 ──► ack
           │
           ▼  (quorum of acks received)
        Leader advances commitIndex
           │
           ▼
        apply_command() called on each node as lastApplied catches up
           │
           ▼
        Client Future resolved with result
```

Followers that fall behind receive the missing entries in the next `AppendEntries` call.  The leader backs off `nextIndex[peer]` by one slot per rejection until it finds the divergence point.

### Persistence

Three fields must survive a crash (§5.4 of the paper):

| Field | Where stored |
|-------|-------------|
| `currentTerm` | `<data_dir>/<node_id>-state.json` (atomic rename) |
| `votedFor` | same file |
| `log[]` | same file (full serialisation; snapshots trim it) |

### Snapshotting

When the log exceeds `SNAPSHOT_THRESHOLD` entries the leader can compact it into a snapshot.  The snapshot stores the serialised state-machine image up to `lastIncludedIndex`.  Followers that are very far behind receive the snapshot via **InstallSnapshot** RPC instead of individual log entries.

---

## Subsystem Designs

### Distributed Lock Manager

```
acquire_lock(name, mode, holder_id, ttl)
    │
    ├─ if not leader → PermissionError (client should retry on leader)
    │
    └─ propose({op: "acquire", ...})
              │
              ▼ Raft commits
          apply_command()
              │
              ├─ can_acquire? → add to holders, resolve Future (granted=True)
              └─ else        → add to waiters, resolve Future (granted=False)
```

**Lock modes**

| Mode | Semantics |
|------|-----------|
| `shared` | Multiple holders allowed; no writer |
| `exclusive` | Only one holder; no readers |

**TTL expiry** – A background coroutine on the leader scans every second and proposes `CMD_EXPIRE` for any holder whose `expires_at` has passed.  This prevents stale locks from blocking the system if a client crashes.

**Deadlock detection** – Every time a `CMD_ACQUIRE` is queued into `waiters`, the waiter's ID is added to the wait-for graph.  A DFS-based cycle detector runs every `DEADLOCK_DETECTION_INTERVAL` seconds.  If a cycle is found, the youngest holder in the cycle is chosen as a victim and its lock is force-released.

### Distributed Queue

**Consistent hashing** maps queue names → responsible nodes using a hash ring with 150 virtual nodes per real node.  This distributes load evenly and minimises reshuffling when nodes join or leave.

```
Enqueue flow:
  producer → POST /queue/enqueue
               │
               └─ propose(CMD_ENQUEUE) → Raft
                           │
                           ▼
                      apply_command → _queues[name].append(msg)

Consume flow:
  consumer → POST /queue/consume
               │
               └─ consume(queue_name)   [not Raft-replicated for speed]
                           │
                           ├─ pop from _queues[name]
                           └─ store in _in_flight[message_id]

Ack / Nack:
  consumer → POST /queue/ack
               └─ propose(CMD_ACK or CMD_NACK) → Raft
```

**At-least-once delivery** – Unacknowledged messages whose `ack_timeout` has elapsed are re-queued at the front of the queue by the `_redelivery_loop` background task.

### Distributed Cache (MESI)

Each cache entry tracks a **MESI state** that is replicated via Raft:

| State | Meaning |
|-------|---------|
| **M** Modified | Dirty; only this node holds valid data |
| **E** Exclusive | Clean; only this node has a copy |
| **S** Shared | Clean; multiple nodes may cache this key |
| **I** Invalid | Stale; must re-fetch from global store |

**Write path** – `CMD_WRITE` is proposed through Raft.  On apply, the writing node sets its local line to `M` or `E`; all other nodes receive the new value but mark their copy `I` (forcing them to serve from `_global_store` until they promote to `S` on next read).

**LRU eviction** – The `LRUCache` uses an `OrderedDict` to track access order.  When `capacity` is exceeded, the least-recently-used entry is evicted.

---

## Communication Layer

### MessageTransport

An `aiohttp` HTTP server + client pair.  Each node exposes a single `POST /rpc` endpoint.  Messages are JSON objects with a `type` discriminator.  The transport is intentionally protocol-agnostic; swapping HTTP for ZeroMQ or gRPC only requires replacing this class.

### FailureDetector

Implements the **Phi Accrual** algorithm (Hayashibara et al., 2004).  Instead of a binary "up / down" judgement, it emits a continuous suspicion level φ.  When φ exceeds the configurable threshold (default 8.0) the peer is *suspected*.  If a heartbeat later arrives, the suspicion is cleared automatically.

---

## Deployment

### Local (3 nodes, one terminal each)

```bash
# Terminal 1
NODE_ID=node-1 NODE_PORT=8001 NODE_TYPE=lock \
    PEERS="node-2:127.0.0.1:8002,node-3:127.0.0.1:8003" \
    python main.py

# Terminal 2
NODE_ID=node-2 NODE_PORT=8002 NODE_TYPE=lock \
    PEERS="node-1:127.0.0.1:8001,node-3:127.0.0.1:8003" \
    python main.py

# Terminal 3
NODE_ID=node-3 NODE_PORT=8003 NODE_TYPE=lock \
    PEERS="node-1:127.0.0.1:8001,node-2:127.0.0.1:8002" \
    python main.py
```

### Docker Compose

```bash
docker compose -f docker/docker-compose.yml up --build
```

This starts a 9-node cluster (3 × lock, 3 × queue, 3 × cache) plus Redis.

### Scaling

To add a fourth lock node without downtime:

1. Deploy the new container with its `NODE_ID` and `PEERS` pointing to the existing three.
2. Update `PEERS` on each existing node and send `SIGHUP` (or rolling-restart).

> **Note:** Dynamic membership changes (joint consensus) are not yet implemented. Static clusters are assumed for now. This is a known extension point.

---

## Fault Tolerance Matrix

| Scenario | Behaviour |
|----------|-----------|
| 1 of 3 nodes crashes | Remaining 2 form quorum; cluster continues normally |
| 2 of 3 nodes crash | No quorum; cluster suspends commits; reads may still be served stale |
| Leader crashes | Followers detect missing heartbeats; new election within 1 election timeout |
| Network partition (minority) | Minority nodes cannot commit; majority continues |
| Network partition (even split) | No side has majority; both halt until partition heals |
| Node restarts with stale log | Raft AppendEntries reconciles the log on reconnect |
| Node restarts after snapshot | InstallSnapshot brings it up to date |

---
