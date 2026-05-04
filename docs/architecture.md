# Arsitektur

## Gambaran Umum

Sistem sinkronisasi terdistribusi adalah kumpulan tiga subsistem independen yang semuanya berbagi lapisan konsensus yang sama: **Raft**.  Setiap subsistem berjalan sebagai kluster minimal tiga node sehingga sistem dapat mentoleransi kegagalan satu node mana pun sambil tetap memproses permintaan.

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

## Konsensus Raft

### Mengapa Raft?

Raft dipilih dibandingkan Paxos karena desainnya secara eksplisit mengutamakan keterbacaan.  Ketiga subsistem membutuhkan jaminan yang sama: **log yang terurut secara total dan linearisable** yang bertahan dari kegagalan node.  Raft menyediakan ini dengan model peran yang jelas (leader / follower / candidate) dan sekumpulan RPC yang terdefinisi dengan baik.

### Peran

| Peran | Tanggung Jawab |
|-------|----------------|
| **Leader** | Menerima proposal klien, mereplikasi entri log ke follower, memajukan commit index |
| **Follower** | Menerima dan menyimpan AppendEntries dari leader, mereset timer pemilihan saat menerima heartbeat |
| **Candidate** | Berkampanye untuk pemilihan dengan mengirim RequestVote ke semua peer |

### Pemilihan (Election)

1. Timer pemilihan follower aktif (acak dalam rentang `[ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX]`).
2. Node bertransisi menjadi **Candidate**, menambah term-nya, memilih dirinya sendiri, dan menyiarkan **RequestVote**.
3. Candidate menjadi **Leader** ketika menerima suara dari mayoritas mutlak kluster.
4. Split-vote? Timer aktif kembali dengan penundaan acak baru, memulai ulang pemilihan.

Timeout yang diacak (default 150–300 ms) membuat pemilihan terbagi menjadi sangat jarang secara statistik.

### Replikasi Log

```
Client → propose(cmd)
           │
           ▼
        Leader menambahkan ke log (memori + disk)
           │
           ├─── AppendEntries RPC → Follower-1 ──► ack
           ├─── AppendEntries RPC → Follower-2 ──► ack
           │
           ▼  (kuorum ack diterima)
        Leader memajukan commitIndex
           │
           ▼
        apply_command() dipanggil di setiap node saat lastApplied menyusul
           │
           ▼
        Client Future diselesaikan dengan hasil
```

Follower yang tertinggal menerima entri yang hilang pada panggilan `AppendEntries` berikutnya.  Leader memundurkan `nextIndex[peer]` satu slot per penolakan hingga menemukan titik divergensi.

### Persistensi

Tiga field harus bertahan dari crash (§5.4 dari paper):

| Field | Lokasi Penyimpanan |
|-------|--------------------|
| `currentTerm` | `<data_dir>/<node_id>-state.json` (penggantian nama atomik) |
| `votedFor` | file yang sama |
| `log[]` | file yang sama (serialisasi penuh; snapshot memangkasnya) |

### Snapshotting

Ketika log melebihi `SNAPSHOT_THRESHOLD` entri, leader dapat memadatkannya menjadi snapshot.  Snapshot menyimpan image state-machine yang telah diserialisasi hingga `lastIncludedIndex`.  Follower yang sangat tertinggal menerima snapshot melalui RPC **InstallSnapshot** alih-alih entri log individual.

---

## Desain Subsistem

### Manajer Kunci Terdistribusi

```
acquire_lock(name, mode, holder_id, ttl)
    │
    ├─ jika bukan leader → PermissionError (klien harus coba ulang di leader)
    │
    └─ propose({op: "acquire", ...})
              │
              ▼ Raft commits
          apply_command()
              │
              ├─ bisa acquire? → tambah ke holders, selesaikan Future (granted=True)
              └─ tidak        → tambah ke waiters, selesaikan Future (granted=False)
```

**Mode kunci**

| Mode | Semantik |
|------|----------|
| `shared` | Beberapa pemegang diizinkan; tidak ada penulis |
| `exclusive` | Hanya satu pemegang; tidak ada pembaca |

**Kedaluwarsa TTL** – Coroutine latar belakang di leader memindai setiap detik dan mengusulkan `CMD_EXPIRE` untuk pemegang yang `expires_at`-nya telah lewat.  Ini mencegah kunci yang kedaluwarsa memblokir sistem jika klien crash.

**Deteksi Deadlock** – Setiap kali `CMD_ACQUIRE` antri ke `waiters`, ID waiter ditambahkan ke graf wait-for.  Detektor siklus berbasis DFS berjalan setiap `DEADLOCK_DETECTION_INTERVAL` detik.  Jika siklus ditemukan, pemegang termuda dalam siklus dipilih sebagai korban dan kuncinya dilepas paksa.

### Antrian Terdistribusi

**Consistent hashing** memetakan nama antrian → node yang bertanggung jawab menggunakan hash ring dengan 150 node virtual per node nyata.  Ini mendistribusikan beban secara merata dan meminimalkan redistribusi ketika node bergabung atau pergi.

```
Alur Enqueue:
  producer → POST /queue/enqueue
               │
               └─ propose(CMD_ENQUEUE) → Raft
                           │
                           ▼
                      apply_command → _queues[name].append(msg)

Alur Consume:
  consumer → POST /queue/consume
               │
               └─ consume(queue_name)   [tidak direplikasi Raft demi kecepatan]
                           │
                           ├─ pop dari _queues[name]
                           └─ simpan di _in_flight[message_id]

Ack / Nack:
  consumer → POST /queue/ack
               └─ propose(CMD_ACK or CMD_NACK) → Raft
```

**Pengiriman at-least-once** – Pesan yang belum diakui yang `ack_timeout`-nya telah berlalu akan dimasukkan kembali ke awal antrian oleh task latar belakang `_redelivery_loop`.

### Cache Terdistribusi (MESI)

Setiap entri cache melacak **status MESI** yang direplikasi melalui Raft:

| Status | Makna |
|--------|-------|
| **M** Modified (Dimodifikasi) | Kotor; hanya node ini yang menyimpan data valid |
| **E** Exclusive (Eksklusif) | Bersih; hanya node ini yang memiliki salinan |
| **S** Shared (Dibagikan) | Bersih; beberapa node dapat menyimpan cache kunci ini |
| **I** Invalid (Tidak Valid) | Kedaluwarsa; harus diambil ulang dari penyimpanan global |

**Jalur tulis** – `CMD_WRITE` diusulkan melalui Raft.  Saat diterapkan, node penulis mengatur baris lokalnya ke `M` atau `E`; semua node lain menerima nilai baru tetapi menandai salinan mereka `I` (memaksa mereka untuk melayani dari `_global_store` sampai dipromosikan ke `S` saat pembacaan berikutnya).

**Eviksi LRU** – `LRUCache` menggunakan `OrderedDict` untuk melacak urutan akses.  Ketika `capacity` terlampaui, entri yang paling jarang diakses baru-baru ini dieviksi.

---

## Lapisan Komunikasi

### MessageTransport

Pasangan server + klien HTTP `aiohttp`.  Setiap node mengekspos satu endpoint `POST /rpc`.  Pesan adalah objek JSON dengan diskriminator `type`.  Transport ini secara sengaja agnostik terhadap protokol; mengganti HTTP dengan ZeroMQ atau gRPC hanya memerlukan penggantian kelas ini.

### FailureDetector

Mengimplementasikan algoritma **Phi Accrual** (Hayashibara et al., 2004).  Alih-alih penilaian biner "naik / turun", ini menghasilkan tingkat kecurigaan berkelanjutan φ.  Ketika φ melebihi ambang batas yang dapat dikonfigurasi (default 8.0), peer dicurigai.  Jika heartbeat datang kemudian, kecurigaan dibersihkan secara otomatis.

---

## Deployment

### Lokal (3 node, satu terminal masing-masing)

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

Ini memulai kluster 9 node (3 × lock, 3 × queue, 3 × cache) plus Redis.

### Penskalaan

Untuk menambahkan node lock keempat tanpa downtime:

1. Deploy kontainer baru dengan `NODE_ID` dan `PEERS`-nya yang menunjuk ke tiga yang sudah ada.
2. Perbarui `PEERS` di setiap node yang sudah ada dan kirim `SIGHUP` (atau rolling-restart).

> **Catatan:** Perubahan keanggotaan dinamis (joint consensus) belum diimplementasikan. Kluster statis diasumsikan untuk saat ini. Ini adalah titik ekstensi yang diketahui.

---

## Matriks Toleransi Kegagalan

| Skenario | Perilaku |
|----------|----------|
| 1 dari 3 node crash | 2 node yang tersisa membentuk kuorum; kluster berlanjut normal |
| 2 dari 3 node crash | Tidak ada kuorum; kluster menangguhkan commit; pembacaan mungkin masih dilayani dengan data basi |
| Leader crash | Follower mendeteksi heartbeat yang hilang; pemilihan baru dalam 1 timeout pemilihan |
| Partisi jaringan (minoritas) | Node minoritas tidak dapat commit; mayoritas berlanjut |
| Partisi jaringan (split genap) | Tidak ada sisi yang memiliki mayoritas; keduanya berhenti sampai partisi pulih |
| Node restart dengan log basi | Raft AppendEntries merekonsiliasi log saat terhubung kembali |
| Node restart setelah snapshot | InstallSnapshot memperbarui node hingga terkini |

---