# Campus CDN

A multi-phase content distribution platform for campus media delivery. Upload a video once, let many devices on the same LAN watch it together — with chunked transport, peer-aware scheduling, a Redis-backed edge cache, synchronized watch parties (browser or VLC), and a live analytics dashboard.

This README is a usage guide: what you can do with the system, how each piece works, and what is deliberately not in this version.

## Table of contents

- [What it does](#what-it-does)
- [Architecture](#architecture)
- [Quick start](#quick-start)
- [User guide](#user-guide)
  - [1. Upload a file](#1-upload-a-file)
  - [2. Download with the CLI](#2-download-with-the-cli)
  - [3. Watch a video together in the browser (Mac + phone)](#3-watch-a-video-together-in-the-browser-mac--phone)
  - [4. Watch a video together with VLC (desktop ↔ desktop)](#4-watch-a-video-together-with-vlc-desktop--desktop)
  - [5. See live analytics on the dashboard](#5-see-live-analytics-on-the-dashboard)
  - [6. Inspect peer-aware scheduling](#6-inspect-peer-aware-scheduling)
- [How it works](#how-it-works)
- [API reference](#api-reference)
- [What is NOT in this version](#what-is-not-in-this-version)
- [Tech stack](#tech-stack)
- [Project structure](#project-structure)
- [Running the tests](#running-the-tests)

## What it does

| Capability | What you get |
|---|---|
| **Chunked upload** | Files are split into 512 KB chunks at upload time; each chunk is SHA-256 hashed and stored under `chunks/<file_id>/`. |
| **Manifest-driven download** | Clients fetch a manifest of chunk hashes, then request each chunk individually — enabling parallel transfer, cache reuse, and integrity checks. |
| **Whole-file streaming** | `/stream/{file_id}` reassembles chunks on demand and supports HTTP Range, so any `<video>` element or VLC can seek. |
| **Peer discovery (LAN)** | Peers self-announce over UDP broadcast or `POST /peers/announce`. The registry tracks which chunks each peer holds and their bandwidth. |
| **Peer-aware scheduler** | Builds a download plan that prefers rare chunks, balances load across peers by bandwidth, and falls back to origin when no peer has a chunk. |
| **Edge cache** | Redis LFU cache for hot chunks. Every download response carries an `X-Cache-Hit: true/false` header. |
| **Watch party (browser)** | Open `/watchparty` on two devices on the same Wi-Fi, paste a file ID, play together. Play/pause/seek on one device drives the other in real time. |
| **Watch party (VLC)** | Each desktop client runs `client/watchparty_vlc.py`, which launches local VLC and bridges WebSocket sync ↔ VLC HTTP control. |
| **Analytics + dashboard** | Live counters for uploads, downloads, bytes served, cache hit ratio, peer contribution, 24-hour bandwidth chart. |

## Architecture

```text
                         +----------------------+
                         |   Browser / CLI / VLC|
                         | upload, download,    |
                         | watch party, metrics |
                         +----------+-----------+
                                    |
                                    v
                      +------------------------------+
                      |        FastAPI Server        |
                      |------------------------------|
                      | Upload / Download / Stream   |
                      | Peer Routes                  |
                      | Watch Party Routes + WS      |
                      | Analytics Routes             |
                      | Static: dashboard, watchparty|
                      +---+---------------+----------+
                          |               |
            +-------------+               +-------------------+
            |                                                     |
            v                                                     v
+--------------------------+                       +--------------------------+
| Chunking / Scheduling    |                       | Watch Party / Analytics  |
|--------------------------|                       |--------------------------|
| Chunker                  |                       | RoomManager              |
| Integrity Verification   |                       | ConnectionManager        |
| Connection Pool          |                       | AnalyticsCollector       |
| Peer Registry            |                       +--------------------------+
| Peer Discovery (UDP)     |
| Chunk Scheduler          |
| Edge Cache (Redis LFU)   |
+------------+-------------+
             |                           +----------------------+
             |                           | Redis                |
             |                           | cache + analytics    |
             |                           +----------------------+
             |
             v
   +---------------------+       +----------------------+
   | Local Chunk Storage |       | PostgreSQL           |
   | chunks/<file_id>/   |       | files + chunks       |
   +---------------------+       +----------------------+
```

## Quick start

### Prerequisites

- macOS with Homebrew
- Python 3.11+
- VLC (only needed for the desktop watch-party bridge)

### 1. Install services

```bash
brew install postgresql@16 redis
brew services start postgresql@16
brew services start redis
```

### 2. Create the database

The Homebrew Postgres install creates a superuser role matching your macOS username with no password.

```bash
createdb campuscdn
```

### 3. Install Python dependencies

```bash
cd campus-cdn
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 4. Configure environment

Copy `.env.example` to `.env` and replace the Docker hostnames with localhost:

```env
DATABASE_URL=postgresql+asyncpg://<your-mac-user>@localhost:5432/campuscdn
REDIS_URL=redis://localhost:6379
CHUNK_SIZE_BYTES=524288
CHUNK_STORAGE_PATH=chunks
MAX_POOL_CONNECTIONS=5
MAX_CACHE_SIZE_GB=2
LOG_LEVEL=INFO
```

### 5. Start the server

For local-only use:

```bash
uvicorn server.main:app --reload --port 8000
```

For LAN access (required for the phone or a second machine):

```bash
uvicorn server.main:app --reload --host 0.0.0.0 --port 8000
ipconfig getifaddr en0   # → your Mac's LAN IP, e.g. 192.168.29.34
```

API at `http://localhost:8000`. Dashboard at `/dashboard`. Watch party UI at `/watchparty`.

### Docker alternative

`.env.example` ships with the Docker URLs. `docker-compose up --build` brings up the app, Postgres, and Redis together. Use the Homebrew path above for development on macOS.

## User guide

### 1. Upload a file

```bash
curl -s -X POST http://localhost:8000/upload \
  -F "file=@./lecture.mp4" | python3 -m json.tool
```

Response:

```json
{
  "file_id": "0e4c965b-ed53-4fc5-b4f2-ba773b1ec4b0",
  "filename": "lecture.mp4",
  "total_chunks": 1280,
  "total_size": 670617211,
  "status": "ready",
  "uploaded_at": "2026-05-27T10:37:28.820553"
}
```

What happened under the hood:

1. The upload route received the multipart body, wrote it to a temp file, and handed it to the chunker.
2. The chunker split the file into 512 KB chunks under `chunks/<file_id>/<chunk_index>.bin`.
3. Each chunk was SHA-256 hashed; `FileRecord` + `ChunkRecord` rows were inserted into Postgres.
4. Status flipped to `ready`; the file is now available for download / streaming.

Keep the `file_id` — every other endpoint uses it.

### 2. Download with the CLI

```bash
python client/cli.py download \
  --file-id 0e4c965b-... \
  --server http://localhost:8000 \
  --output ./downloads/
```

The downloader:

1. Fetches `/manifest/{file_id}` (chunk count, ordered chunk hashes).
2. Calls `/peers` to see who else might serve chunks.
3. Builds a download plan with the chunk scheduler (rare chunks first, bandwidth-weighted assignment).
4. Issues parallel `GET /chunk/{file_id}/{index}` requests, throttled by the server's connection pool.
5. Verifies each chunk's SHA-256 against the manifest before writing.
6. Reassembles the chunks into the output file.

### 3. Watch a video together in the browser (Mac + phone)

Easiest demo. Works on any two devices with a browser on the same Wi-Fi.

**On the Mac**:

```bash
# Start the server bound to all interfaces
uvicorn server.main:app --reload --host 0.0.0.0 --port 8000
ipconfig getifaddr en0    # e.g. 192.168.29.34

# Upload the video
curl -s -X POST http://localhost:8000/upload -F "file=@lecture.mp4" | python3 -m json.tool
```

Open `http://192.168.29.34:8000/watchparty` in your browser. Fill in:
- **File ID** — from the upload response
- **Your name** — anything (e.g. `alice`)
- **Room ID** — leave blank

Click **Create new room**. The video starts streaming from `/stream/{file_id}`. A **Share link** appears under the player (`http://192.168.29.34:8000/watchparty?file_id=...&room_id=ABCDEF`).

**On the phone (Android Chrome)**: open the Share link. It pre-fills `file_id` and `room_id`. Type a name (e.g. `bob`), tap **Join**.

Both devices are now playing the same video. Play / pause / drag the seek bar on either side; the other follows within ~0.5 s. The bottom log shows `[send] PLAY t=12.34s` and `[recv] alice -> playing=true t=12.34s` lines as messages flow over the WebSocket.

**Troubleshooting**:
- Phone can't reach the Mac → check macOS firewall (System Settings → Network → Firewall → Allow incoming connections to `python3`).
- Video shows a broken icon → browser doesn't support the container. MKV works in Chrome/Firefox/Android Chrome but not Safari. Use an MP4/H.264/AAC file for full compatibility, or open in Chrome on the Mac.
- Verify reachability: open `http://192.168.29.34:8000/health` on the phone — should return `{"status":"ok",...}`.

### 4. Watch a video together with VLC (desktop ↔ desktop)

Useful when both clients are Mac/Windows/Linux desktops and you want native VLC playback instead of an HTML5 player.

Each client runs `client/watchparty_vlc.py`, which:
1. Launches VLC pointed at `http://<server>:8000/stream/{file_id}` with `--extraintf http` so we can drive it.
2. Connects to `/watchparty/ws/{room}/{peer}`.
3. Polls VLC every 500 ms; when the user plays/pauses/seeks, emits a WebSocket message.
4. Listens for `STATE` messages from other peers; applies them to local VLC via `pl_forceresume`, `pl_forcepause`, `seek`.

**On the host machine** (running the CDN):

```bash
uvicorn server.main:app --reload --host 0.0.0.0 --port 8000
ipconfig getifaddr en0     # e.g. 192.168.1.42
```

**On client A (host)**:

```bash
pip install httpx websockets        # one-time
python3 client/watchparty_vlc.py \
  --server http://192.168.1.42:8000 \
  --file-id 0e4c965b-... \
  --peer-id alice \
  --host
```

The script prints `Room: ABCDEF`. Share that room ID with client B.

**On client B**:

```bash
python3 client/watchparty_vlc.py \
  --server http://192.168.1.42:8000 \
  --file-id 0e4c965b-... \
  --peer-id bob \
  --room-id ABCDEF
```

Both VLC windows open and play in sync. To quit cleanly, `Ctrl+C` in each terminal — the script terminates VLC for you.

**Flags**:
- `--vlc-port` (default 8090) — local port for VLC's HTTP control. Override if two clients share a machine.
- `--vlc-password` (default `campus`) — basic-auth password for VLC's HTTP interface.
- `--vlc-path` — override VLC binary location.

### 5. See live analytics on the dashboard

Open `http://localhost:8000/dashboard`. Auto-refreshes every 10 seconds.

Shows:
- Total uploads, downloads, bytes served
- Cache hit ratio gauge
- Active peers, active watch parties
- 24-hour bandwidth chart (Chart.js)
- Transfer source breakdown (origin vs. cache vs. peer)
- Top downloaded files
- Peer contribution table (bytes contributed, chunks served, reliability)

All values come from `/analytics/summary`, `/analytics/bandwidth`, `/analytics/peers`, `/analytics/cache`, which are backed by Redis counters updated on every upload/download.

### 6. Inspect peer-aware scheduling

You can register a peer manually and watch the scheduler honor it.

```bash
# Get the first chunk's hash
CHUNK_HASH=$(curl -s "http://localhost:8000/manifest/$FILE_ID" | python3 -c "import json,sys; print(json.load(sys.stdin)['chunks'][0]['hash'])")

# Pretend a peer at 127.0.0.1:9101 holds it
curl -s -X POST http://localhost:8000/peers/announce \
  -H "Content-Type: application/json" \
  -d "{\"ip\":\"127.0.0.1\",\"port\":9101,\"chunks\":[\"$CHUNK_HASH\"],\"bandwidth_mbps\":50.0}"

# Confirm the registry sees it
curl -s http://localhost:8000/peers | python3 -m json.tool

# Resolve who has the chunk
curl -s "http://localhost:8000/peers/chunk/$CHUNK_HASH" | python3 -m json.tool
```

Mark a download as peer-sourced for the analytics breakdown:

```bash
curl -s -o /dev/null -D - \
  -H "X-CDN-Transfer-Mode: peer" \
  "http://localhost:8000/chunk/$FILE_ID/0" | grep -i "x-cache-hit\|http/"
```

First request: `X-Cache-Hit: false`. Second: `X-Cache-Hit: true` (served from Redis). The dashboard's source breakdown will show `peer` rising.

## How it works

### Chunk lifecycle

1. **Upload** writes the multipart body to a temp file, then the chunker reads it in 512 KB blocks, hashes each block (SHA-256), and persists `<file_id>/<index>.bin` to local disk. Metadata goes into Postgres.
2. **Download** for a single chunk: read from Redis (if cached) → verify hash → fallback to local disk → cache for next time → emit analytics.
3. **Stream** for the whole file: walk the chunk list, slice each chunk to the requested byte range (handles Range headers across chunk boundaries), yield bytes lazily so a single `<video>` element can seek to any point.

### Peer discovery + scheduling

- Each server instance starts a UDP discovery thread that listens for peer announcements on the broadcast address.
- The `PeerRegistry` keeps an in-memory map of `peer_id → {ip, port, available_chunks, bandwidth_mbps, last_seen}`.
- The `ChunkScheduler` builds a download plan: each chunk's *rarity* (fewer peers = higher priority) and each peer's *bandwidth* go into a score; assignments are spread so no single peer gets more than half the work. Chunks no peer has are tagged `origin`.

### Edge cache

- Redis stores chunks under `chunk:<sha256>` with an LFU policy (`maxmemory-policy allkeys-lfu` is set programmatically). Every fetch increments a hit/miss counter.
- The cache has a configurable size (`MAX_CACHE_SIZE_GB`). Once exceeded, Redis evicts the least-frequently used chunks.

### Watch party

- A room is a small in-memory object (`room_id`, `file_id`, `host_id`, `created_at`, `members`, `state`). `PlaybackState` carries `is_playing`, `timestamp_seconds`, `last_updated`, `updated_by`.
- The `ConnectionManager` owns the set of active WebSocket connections per room. Sending a `STATE` message broadcasts to every member.
- The browser and VLC clients both implement the same loop: on incoming `STATE`, ignore your own message, set an "apply grace" window (so the resulting local play/pause doesn't echo back), and adjust the player. On local play/pause/seek, emit a message.
- A background task in `ConnectionManager` re-broadcasts the current state every 5 seconds; if a member's reported timestamp drifts >2 s from the host, the server emits a corrective `SEEK`.

### Analytics

- `AnalyticsCollector` uses Redis `INCRBY` and hash maps for counters. Reads are cheap; the dashboard endpoint is one `MGET` per metric.
- Hourly bandwidth is bucketed by `bandwidth:hour:<YYYYMMDDHH>` keys with a 25-hour TTL.

## API reference

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/health` | Service health and active transfer count |
| `POST` | `/upload` | Multipart file upload |
| `GET` | `/manifest/{file_id}` | Manifest with chunk list and hashes |
| `GET` | `/chunk/{file_id}/{chunk_index}` | Single chunk, with `X-Cache-Hit` header |
| `GET` | `/stream/{file_id}` | Reassembled file; supports HTTP `Range` |
| `GET` | `/peers` | List active peers |
| `GET` | `/peers/{peer_id}` | Full peer info |
| `GET` | `/peers/chunk/{chunk_hash}` | Peers that hold a given chunk |
| `POST` | `/peers/announce` | Manual peer registration |
| `POST` | `/watchparty/create` | Create a watch-party room |
| `GET` | `/watchparty/{room_id}` | Room summary (member count, state) |
| `WS` | `/watchparty/ws/{room_id}/{peer_id}` | Watch-party sync channel |
| `GET` | `/analytics/summary` | Aggregate transfer + file analytics |
| `GET` | `/analytics/bandwidth?hours=24` | Hourly bandwidth buckets |
| `GET` | `/analytics/peers` | Peer contribution analytics |
| `GET` | `/analytics/cache` | Cache performance analytics |
| `GET` | `/dashboard` | Analytics dashboard (HTML) |
| `GET` | `/watchparty` | Watch-party browser UI (HTML) |

Watch-party WebSocket message types:

| Client → server | Payload |
|---|---|
| `JOIN` | `{type, room_id, peer_id}` |
| `PLAY` | `{type, peer_id, timestamp_seconds}` |
| `PAUSE` | `{type, peer_id, timestamp_seconds}` |
| `SEEK` | `{type, peer_id, timestamp_seconds}` |
| `PING` | `{type, client_time}` |

| Server → client | Payload |
|---|---|
| `STATE` | `{type, is_playing, timestamp_seconds, updated_by}` |
| `MEMBER_JOINED` / `MEMBER_LEFT` | `{type, peer_id, member_count}` |
| `PONG` | `{type, server_time, client_time}` |

## What is NOT in this version

Things the system intentionally does not do. These are scope choices for a campus-LAN demo, not missing implementation.

| Area | What's not there | Why it's out of scope |
|---|---|---|
| **Auth / accounts** | No user table, no login, no API keys | Campus-LAN trust model. Peer IDs are server-assigned UUIDs; chunks are content-addressed by SHA-256 — trust is in the bytes, not the identity. An `X-API-Key` middleware is a ~10-line addition if needed. |
| **Real peer-to-peer transfer** | Peers register chunk availability and the scheduler honors them, but chunk bytes still come from the origin server. The `X-CDN-Transfer-Mode` header tags the request for analytics. | A true P2P transport would need WebRTC data channels or a per-peer HTTP server with NAT traversal. The current design demonstrates the discovery + scheduling layer cleanly without that complexity. |
| **NAT traversal** | UDP discovery is broadcast-only | Single LAN by design |
| **HTTPS / TLS** | Plain HTTP | Behind a reverse proxy in any real deployment |
| **Transcoding** | Files are served as-uploaded | Use ffmpeg upstream; out of scope for a CDN |
| **Upload resume** | Interrupted uploads restart from zero | Acceptable for sub-GB files |
| **Streaming upload** | Multipart upload buffers the whole file in memory before chunking | Fine for the current sample sizes; would need spool-to-disk for multi-GB uploads |
| **Multi-origin replication** | Single FastAPI instance, single chunk store | Horizontal scaling is a separate problem |
| **Watch-party extras** | No chat, cursor sharing, or host-only permissions | Playback sync is the headline feature |
| **Access control on content** | Anyone with a `file_id` can download | Add `X-API-Key` middleware if needed |
| **Rate limiting / quotas** | None | Add nginx or a FastAPI middleware in front |
| **Persistent analytics** | Redis only; counters reset if Redis restarts | Periodic flush to Postgres if longer retention is needed |
| **Browser MKV in Safari** | Safari doesn't natively play MKV; the `/watchparty` page shows "operation not supported" | Use Chrome/Firefox/Android Chrome, or transcode the source to MP4/H.264/AAC |

## Tech stack

| Layer | Technology |
|---|---|
| API framework | FastAPI |
| ASGI server | Uvicorn |
| Database ORM | SQLAlchemy Async |
| Relational database | PostgreSQL 16 |
| Cache + analytics store | Redis 7 |
| Upload/download client | Python `httpx` + `tqdm` |
| Peer discovery | UDP broadcast |
| Watch party transport | WebSockets |
| Browser watch-party UI | HTML5 `<video>` + native WebSocket |
| Desktop watch-party | VLC HTTP control bridged by Python (`httpx` + `websockets`) |
| Container orchestration | Docker Compose (optional) |
| Testing | Pytest + Pytest AsyncIO + async-asgi-testclient |
| Dashboard frontend | HTML, CSS, JavaScript, Chart.js |

## Project structure

```text
campus-cdn/
├── client/
│   ├── cli.py                       # upload/download CLI
│   └── watchparty_vlc.py            # VLC bridge for desktop watch party
├── migrations/                      # SQL schema
├── server/
│   ├── analytics/collector.py
│   ├── cache/edge_cache.py
│   ├── chunks/
│   │   ├── chunker.py
│   │   ├── integrity.py
│   │   └── storage.py
│   ├── database/
│   │   ├── connection.py
│   │   └── models.py
│   ├── peers/
│   │   ├── discovery.py
│   │   └── registry.py
│   ├── routes/
│   │   ├── upload.py
│   │   ├── download.py              # /manifest, /chunk, /stream
│   │   ├── peers.py
│   │   ├── watchparty.py            # /watchparty/* + WebSocket
│   │   └── analytics.py
│   ├── scheduler/chunk_scheduler.py
│   ├── transfer/pool.py
│   ├── watchparty/
│   │   ├── room.py
│   │   └── sync.py
│   └── main.py
├── static/
│   ├── dashboard.html               # analytics dashboard
│   └── watchparty.html              # browser watch-party UI
├── tests/                           # 54 passing, 1 skipped
├── Dockerfile
├── docker-compose.yml
├── demo.sh
├── STATUS.md
└── README.md
```

## Running the tests

```bash
pytest tests/ --ignore=tests/load
```

Current state: **54 passing, 1 skipped**. The skipped test is a Redis hit-ratio test that hangs on the session-scoped event loop and is under investigation. Load tests are opt-in via `pytest tests/load`.

The suite covers the chunker, integrity verification, async connection pool, upload/download pipeline, peer discovery, peer-aware scheduler, edge cache, watch-party WebSocket sync, and end-to-end pipeline. WebSocket tests use `async-asgi-testclient` (httpx's ASGI transport doesn't speak WebSocket upgrades).

`DATABASE_URL` defaults to `postgresql+asyncpg://$(whoami)@localhost:5432/campuscdn` if unset, so tests work out of the box on a Homebrew install.

See [STATUS.md](STATUS.md) for the full current state, known issues, and scope limits.
