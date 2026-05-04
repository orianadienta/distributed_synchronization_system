# Deployment Guide

## Prerequisites

| Tool | Minimum version | Notes |
|------|----------------|-------|
| Python | 3.8 | 3.11 recommended |
| pip | 23+ | |
| Docker | 24+ | For containerised deployment |
| Docker Compose | 2.20+ | Plugin version (`docker compose`) |
| Redis | 7+ | Only needed for persistence extensions |

---

## Quick Start (Local, No Docker)

### 1. Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Copy and edit environment file

```bash
cp .env.example .env
# edit .env if you need to change ports or Redis host
```

### 3. Start a 3-node Lock Manager cluster

Open **three separate terminals** from the project root.

**Terminal 1 – node-1**
```bash
NODE_ID=node-1 NODE_PORT=8001 NODE_TYPE=lock \
    PEERS="node-2:127.0.0.1:8002,node-3:127.0.0.1:8003" \
    python main.py
```

**Terminal 2 – node-2**
```bash
NODE_ID=node-2 NODE_PORT=8002 NODE_TYPE=lock \
    PEERS="node-1:127.0.0.1:8001,node-3:127.0.0.1:8003" \
    python main.py
```

**Terminal 3 – node-3**
```bash
NODE_ID=node-3 NODE_PORT=8003 NODE_TYPE=lock \
    PEERS="node-1:127.0.0.1:8001,node-2:127.0.0.1:8002" \
    python main.py
```

### 4. Verify the cluster

Within a few seconds one node will win the election.  Check with:

```bash
curl -s http://localhost:8001/status | python -m json.tool
# Look for "role": "leader" on one node and "role": "follower" on the others.
```

### 5. Try a lock acquire

```bash
# Acquire an exclusive lock (replace port with whichever node is leader)
curl -s -X POST http://localhost:8001/lock/acquire \
  -H "Content-Type: application/json" \
  -d '{"lock_name":"my-resource","mode":"exclusive","holder_id":"client-1","ttl":30}' \
  | python -m json.tool

# Release it
curl -s -X POST http://localhost:8001/lock/release \
  -H "Content-Type: application/json" \
  -d '{"lock_name":"my-resource","request_id":"<request_id from above>","holder_id":"client-1"}' \
  | python -m json.tool
```

---

## Docker Compose Deployment

This starts **9 nodes** (3 × lock, 3 × queue, 3 × cache) plus Redis.

```bash
# Build images and start
docker compose -f docker/docker-compose.yml up --build

# Run in background
docker compose -f docker/docker-compose.yml up -d --build

# Check logs
docker compose -f docker/docker-compose.yml logs -f lock-node-1

# Stop and clean up
docker compose -f docker/docker-compose.yml down -v
```

### Port mapping

| Service | External port |
|---------|--------------|
| lock-node-1 | 8001 |
| queue-node-1 | 8002 |
| cache-node-1 | 8003 |
| Redis | 6379 |

The remaining nodes in each cluster are reachable from within the Docker network but not exposed externally by default.  Add more `ports:` entries in `docker-compose.yml` if you need direct access.

---

## Running the Tests

### Unit tests (fast, no network)

```bash
pytest tests/unit/ -v
```

### Integration tests (3-node in-process cluster)

```bash
pytest tests/integration/ -v --timeout=30
```

### Performance tests

```bash
pytest tests/performance/ -v -s --timeout=120
```

### All tests with coverage

```bash
pytest tests/ --cov=src --cov-report=term-missing --timeout=60
```

### Parallel execution

```bash
pytest tests/unit/ tests/integration/ -n auto --timeout=30
```

---

## Load Testing with Locust

Start the relevant node cluster first (see Quick Start), then:

```bash
# Web UI at http://localhost:8089
locust -f benchmarks/load_test_scenarios.py \
    --host http://localhost:8001 \
    --users 50 --spawn-rate 5

# Headless (CI mode) – 60 second run
locust -f benchmarks/load_test_scenarios.py \
    --host http://localhost:8001 \
    --users 30 --spawn-rate 10 \
    --run-time 60s --headless \
    --csv=results/locust
```

Available user classes:
- `LockManagerUser` – targets the lock cluster
- `QueueProducerUser` / `QueueConsumerUser` – targets the queue cluster
- `CacheUser` – targets the cache cluster
- `MixedWorkloadUser` – realistic mixed scenario

---

## Configuration Reference

All settings can be provided via environment variables or the `.env` file (parsed by `python-dotenv`).

| Variable | Default | Description |
|----------|---------|-------------|
| `NODE_ID` | `node-1` | Unique node identifier |
| `NODE_HOST` | `0.0.0.0` | Bind address |
| `NODE_PORT` | `8000` | HTTP port |
| `NODE_TYPE` | `lock` | `lock` / `queue` / `cache` |
| `PEERS` | _(empty)_ | Comma-separated `id:host:port` |
| `DATA_DIR` | `./data` | Durable state directory |
| `LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` |
| `REDIS_HOST` | `127.0.0.1` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `ELECTION_TIMEOUT_MIN` | `0.15` | Minimum election timeout (s) |
| `ELECTION_TIMEOUT_MAX` | `0.30` | Maximum election timeout (s) |
| `HEARTBEAT_INTERVAL` | `0.05` | Leader heartbeat period (s) |
| `RPC_TIMEOUT` | `0.10` | RPC call timeout (s) |
| `LOCK_DEFAULT_TTL` | `30.0` | Default lock TTL (s) |
| `DEADLOCK_DETECTION_INTERVAL` | `5.0` | Deadlock scan period (s) |
| `QUEUE_VIRTUAL_NODES` | `150` | Virtual nodes in hash ring |
| `ACK_TIMEOUT` | `30.0` | Queue message ack timeout (s) |
| `CACHE_MAX_SIZE` | `1000` | LRU cache capacity (entries) |
| `CACHE_TTL` | `300.0` | Default cache entry TTL (s) |

---

## Troubleshooting

### "Not leader; current leader=…" (HTTP 503)

The node you contacted is a follower.  Find the leader:

```bash
for port in 8001 8002 8003; do
    role=$(curl -s http://localhost:$port/status | python -c "import sys,json; print(json.load(sys.stdin)['raft']['role'])")
    echo "port=$port role=$role"
done
```

Then send write requests to the leader port.

### No leader elected

- Ensure all nodes can reach each other on the configured ports.
- Check for firewall rules blocking TCP on those ports.
- Increase `ELECTION_TIMEOUT_MAX` if the network is slow.
- Run `curl http://localhost:800X/health` on each node to confirm they're up.

### "Connection refused" on startup

The HTTP server takes a moment to bind.  Add a `sleep 1` between starting nodes locally, or rely on the `depends_on` health-check in Docker Compose.

### Data directory permission errors

```bash
mkdir -p ./data && chmod 750 ./data
```

Inside Docker the `/data` volume is created automatically; ensure the container user has write access.

### Tests failing with "Address already in use"

The integration and performance tests bind to fixed ports (`19700+`, `19800+`).  If a previous test run crashed, wait a few seconds for the OS to reclaim the sockets, then retry.

---

## Monitoring

Every node exposes:

```
GET /health    → liveness probe (used by Docker HEALTHCHECK)
GET /status    → Raft state + failure-detector phi + all metrics
GET /metrics   → metrics-only (suitable for scraping)
```

To scrape metrics into Prometheus, add a job to your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: distributed_sync
    static_configs:
      - targets:
          - "localhost:8001"
          - "localhost:8002"
          - "localhost:8003"
    metrics_path: /metrics
```

> The `/metrics` endpoint returns JSON, not Prometheus text format.  To use the Prometheus text format, enable `prometheus-client` in `src/utils/metrics.py` (stub is already imported in `requirements.txt`).