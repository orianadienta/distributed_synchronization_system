# Distributed Sync System

Implementasi sistem sinkronisasi terdistribusi berbasis **Raft Consensus** yang mencakup:

- **Distributed Lock Manager** – Shared/Exclusive locks dengan deadlock detection
- **Distributed Queue** – Consistent hashing + at-least-once delivery
- **Distributed Cache** – Protokol MESI coherence + LRU eviction

---

## Link Youtube



## Struktur Proyek

```
distributed-sync-system/
├── src/
│   ├── nodes/
│   │   ├── base_node.py        # Wiring: transport + failure detector + raft
│   │   ├── lock_manager.py     # LockManagerNode (state machine di atas Raft)
│   │   ├── queue_node.py       # QueueNode + consistent hash ring
│   │   └── cache_node.py       # CacheNode + MESI protocol
│   ├── consensus/
│   │   └── raft.py             # Implementasi Raft dari scratch
│   ├── communication/
│   │   ├── message_passing.py  # Async HTTP transport (aiohttp)
│   │   └── failure_detector.py # Phi Accrual Failure Detector
│   └── utils/
│       ├── config.py           # Konfigurasi via env vars / .env
│       └── metrics.py          # Counter, Gauge, Histogram registry
├── tests/
│   ├── unit/                   # Raft logic, failure detector, lock state machine
│   ├── integration/            # 3-node in-process cluster tests
│   └── performance/            # Election latency, throughput, commit latency
├── docker/
│   ├── Dockerfile.node
│   └── docker-compose.yml      # 9 nodes + Redis
├── docs/
│   ├── architecture.md
│   ├── api_spec.yaml           # OpenAPI 3.1
│   └── deployment_guide.md
├── benchmarks/
│   └── load_test_scenarios.py  # Locust scenarios
├── requirements.txt
├── .env.example
└── main.py                     # Entrypoint
```

---

## Instalasi & Menjalankan

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

### 3-node Lock Manager (lokal)

```bash
# Terminal 1
NODE_ID=node-1 NODE_PORT=8001 NODE_TYPE=lock \
    PEERS="node-2:127.0.0.1:8002,node-3:127.0.0.1:8003" python main.py

# Terminal 2
NODE_ID=node-2 NODE_PORT=8002 NODE_TYPE=lock \
    PEERS="node-1:127.0.0.1:8001,node-3:127.0.0.1:8003" python main.py

# Terminal 3
NODE_ID=node-3 NODE_PORT=8003 NODE_TYPE=lock \
    PEERS="node-1:127.0.0.1:8001,node-2:127.0.0.1:8002" python main.py
```

### Docker Compose (9 nodes)

```bash
docker compose -f docker/docker-compose.yml up --build
```

---

## Menjalankan Tests

```bash
pytest tests/unit/ -v                          # unit tests
pytest tests/integration/ -v --timeout=30      # integrasi (butuh network loopback)
pytest tests/performance/ -v -s --timeout=120  # performance benchmarks
```

---

## Arsitektur Raft

```
Follower ──(election timeout)──► Candidate ──(quorum votes)──► Leader
   ▲                                                               │
   └───────────────── (higher term seen) ────────────────────────┘
```

Setiap node menyimpan tiga field persisten:
- `currentTerm` – monotonically increasing election number
- `votedFor` – candidate yang mendapat suara pada term ini
- `log[]` – urutan perintah yang direplikasi ke semua node

Leader mengirim `AppendEntries` (termasuk heartbeat kosong) ke semua follower setiap `HEARTBEAT_INTERVAL` detik.  Jika follower tidak menerima heartbeat selama `ELECTION_TIMEOUT` acak, ia memulai pemilihan baru.

---

## Dokumentasi

- [Arsitektur](docs/architecture.md)
- [API Reference](docs/api_spec.yaml)
- [Deployment Guide](docs/deployment_guide.md)