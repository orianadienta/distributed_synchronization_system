# Panduan Deployment

## Prasyarat

| Alat | Versi Minimum | Catatan |
|------|---------------|---------|
| Python | 3.8 | 3.11 direkomendasikan |
| pip | 23+ | |
| Docker | 24+ | Untuk deployment terkontainerisasi |
| Docker Compose | 2.20+ | Versi plugin (`docker compose`) |
| Redis | 7+ | Hanya diperlukan untuk ekstensi persistensi |

---

## Mulai Cepat (Lokal, Tanpa Docker)

### 1. Instal dependensi

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Salin dan edit file environment

```bash
cp .env.example .env
# edit .env jika perlu mengubah port atau host Redis
```

### 3. Mulai kluster Lock Manager 3-node

Buka **tiga terminal terpisah** dari root proyek.

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

### 4. Verifikasi kluster

Dalam beberapa detik, satu node akan memenangkan pemilihan.  Periksa dengan:

```bash
curl -s http://localhost:8001/status | python -m json.tool
# Cari "role": "leader" di satu node dan "role": "follower" di node lainnya.
```

### 5. Coba acquire kunci

```bash
# Acquire kunci eksklusif (ganti port dengan node yang menjadi leader)
curl -s -X POST http://localhost:8001/lock/acquire \
  -H "Content-Type: application/json" \
  -d '{"lock_name":"my-resource","mode":"exclusive","holder_id":"client-1","ttl":30}' \
  | python -m json.tool

# Lepaskan kunci
curl -s -X POST http://localhost:8001/lock/release \
  -H "Content-Type: application/json" \
  -d '{"lock_name":"my-resource","request_id":"<request_id dari atas>","holder_id":"client-1"}' \
  | python -m json.tool
```

---

## Deployment Docker Compose

Ini memulai **9 node** (3 × lock, 3 × queue, 3 × cache) plus Redis.

```bash
# Build image dan mulai
docker compose -f docker/docker-compose.yml up --build

# Jalankan di latar belakang
docker compose -f docker/docker-compose.yml up -d --build

# Periksa log
docker compose -f docker/docker-compose.yml logs -f lock-node-1

# Stop dan bersihkan
docker compose -f docker/docker-compose.yml down -v
```

### Pemetaan port

| Layanan | Port Eksternal |
|---------|----------------|
| lock-node-1 | 8001 |
| queue-node-1 | 8002 |
| cache-node-1 | 8003 |
| Redis | 6379 |

Node lainnya di setiap kluster dapat dijangkau dari dalam jaringan Docker tetapi tidak diekspos secara eksternal secara default.  Tambahkan entri `ports:` lebih banyak di `docker-compose.yml` jika Anda memerlukan akses langsung.

---

## Menjalankan Tes

### Tes unit (cepat, tanpa jaringan)

```bash
pytest tests/unit/ -v
```

### Tes integrasi (kluster 3-node dalam proses)

```bash
pytest tests/integration/ -v --timeout=30
```

### Tes performa

```bash
pytest tests/performance/ -v -s --timeout=120
```

### Semua tes dengan coverage

```bash
pytest tests/ --cov=src --cov-report=term-missing --timeout=60
```

### Eksekusi paralel

```bash
pytest tests/unit/ tests/integration/ -n auto --timeout=30
```

---

## Load Testing dengan Locust

Mulai kluster node yang relevan terlebih dahulu (lihat Mulai Cepat), lalu:

```bash
# Web UI di http://localhost:8089
locust -f benchmarks/load_test_scenarios.py \
    --host http://localhost:8001 \
    --users 50 --spawn-rate 5

# Headless (mode CI) – jalankan 60 detik
locust -f benchmarks/load_test_scenarios.py \
    --host http://localhost:8001 \
    --users 30 --spawn-rate 10 \
    --run-time 60s --headless \
    --csv=results/locust
```

Kelas pengguna yang tersedia:
- `LockManagerUser` – menargetkan kluster lock
- `QueueProducerUser` / `QueueConsumerUser` – menargetkan kluster queue
- `CacheUser` – menargetkan kluster cache
- `MixedWorkloadUser` – skenario campuran realistis

---

## Referensi Konfigurasi

Semua pengaturan dapat diberikan melalui variabel environment atau file `.env` (diparse oleh `python-dotenv`).

| Variabel | Default | Deskripsi |
|----------|---------|-----------|
| `NODE_ID` | `node-1` | Pengenal node unik |
| `NODE_HOST` | `0.0.0.0` | Alamat bind |
| `NODE_PORT` | `8000` | Port HTTP |
| `NODE_TYPE` | `lock` | `lock` / `queue` / `cache` |
| `PEERS` | _(kosong)_ | Comma-separated `id:host:port` |
| `DATA_DIR` | `./data` | Direktori state tahan lama |
| `LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` |
| `REDIS_HOST` | `127.0.0.1` | Host Redis |
| `REDIS_PORT` | `6379` | Port Redis |
| `ELECTION_TIMEOUT_MIN` | `0.15` | Timeout pemilihan minimum (s) |
| `ELECTION_TIMEOUT_MAX` | `0.30` | Timeout pemilihan maksimum (s) |
| `HEARTBEAT_INTERVAL` | `0.05` | Periode heartbeat leader (s) |
| `RPC_TIMEOUT` | `0.10` | Timeout panggilan RPC (s) |
| `LOCK_DEFAULT_TTL` | `30.0` | TTL kunci default (s) |
| `DEADLOCK_DETECTION_INTERVAL` | `5.0` | Periode pemindaian deadlock (s) |
| `QUEUE_VIRTUAL_NODES` | `150` | Node virtual dalam hash ring |
| `ACK_TIMEOUT` | `30.0` | Timeout ack pesan antrian (s) |
| `CACHE_MAX_SIZE` | `1000` | Kapasitas cache LRU (entri) |
| `CACHE_TTL` | `300.0` | TTL entri cache default (s) |

---

## Pemecahan Masalah

### "Not leader; current leader=…" (HTTP 503)

Node yang Anda hubungi adalah follower.  Temukan leader:

```bash
for port in 8001 8002 8003; do
    role=$(curl -s http://localhost:$port/status | python -c "import sys,json; print(json.load(sys.stdin)['raft']['role'])")
    echo "port=$port role=$role"
done
```

Kemudian kirim permintaan tulis ke port leader.

### Tidak ada leader yang dipilih

- Pastikan semua node dapat saling menjangkau di port yang dikonfigurasi.
- Periksa aturan firewall yang memblokir TCP di port tersebut.
- Tingkatkan `ELECTION_TIMEOUT_MAX` jika jaringan lambat.
- Jalankan `curl http://localhost:800X/health` di setiap node untuk mengkonfirmasi node berjalan.

### "Connection refused" saat startup

Server HTTP membutuhkan waktu sejenak untuk bind.  Tambahkan `sleep 1` antara memulai node secara lokal, atau andalkan pemeriksaan `depends_on` di Docker Compose.

### Error izin direktori data

```bash
mkdir -p ./data && chmod 750 ./data
```

Di dalam Docker, volume `/data` dibuat secara otomatis; pastikan pengguna kontainer memiliki akses tulis.

### Tes gagal dengan "Address already in use"

Tes integrasi dan performa bind ke port tetap (`19700+`, `19800+`).  Jika jalankan tes sebelumnya crash, tunggu beberapa detik agar OS mengklaim kembali socket, lalu coba lagi.

---

## Monitoring

Setiap node mengekspos:

```
GET /health    → probe liveness (digunakan oleh Docker HEALTHCHECK)
GET /status    → status Raft + phi failure-detector + semua metrik
GET /metrics   → metrik saja (cocok untuk scraping)
```

Untuk mengambil metrik ke Prometheus, tambahkan job ke `prometheus.yml` Anda:

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

> Endpoint `/metrics` mengembalikan JSON, bukan format teks Prometheus.  Untuk menggunakan format teks Prometheus, aktifkan `prometheus-client` di `src/utils/metrics.py` (stub sudah diimpor di `requirements.txt`).