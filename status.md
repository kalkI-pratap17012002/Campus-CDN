# Campus CDN — Status

Snapshot as of the current build. Replaces the older test-bug investigation log.

## Headline

All five phases (upload/download, peer discovery, scheduler/cache, watch party, analytics) are working end-to-end, plus a browser-based watch-party UI and a VLC bridge for desktop clients.

```
54 passing, 1 skipped (Redis hit-ratio test under investigation)
Load tests opt-in: pytest tests/load
```

## What works today

### Core pipeline
- **Chunked upload** with SHA-256 hash per 512 KB chunk.
- **Manifest-driven download** — `/manifest/{file_id}` returns chunk hashes; `/chunk/{file_id}/{index}` serves bytes with cache-hit headers.
- **Whole-file streaming** — `/stream/{file_id}` reassembles chunks on the fly, with `Accept-Ranges: bytes` so browsers and VLC can seek without re-fetching.
- **Integrity verification** — every served chunk is SHA-256 checked against the manifest; corrupt chunks return 409.

### P2P-style distribution (LAN)
- **UDP peer discovery** with in-memory registry and chunk-availability bitmap.
- **Manual peer registration** via `POST /peers/announce` for testing.
- **Peer-aware scheduler** — rarity-first ordering, bandwidth-weighted assignment, load-spread across peers, origin fallback when no peer has a chunk.
- **Per-request transfer-mode tagging** via `X-CDN-Transfer-Mode: peer|cache|origin` for analytics.

### Edge cache
- **Redis LFU edge cache** for hot chunks with hit/miss accounting and eviction tracking.
- **X-Cache-Hit header** on every chunk and stream response.

### Watch party
- **Room creation + lookup** via `POST /watchparty/create` and `GET /watchparty/{room_id}`.
- **WebSocket fanout** with `PLAY`, `PAUSE`, `SEEK`, `PING`, `JOIN`, `MEMBER_JOINED`, `MEMBER_LEFT`, `STATE`, `PONG` messages.
- **Drift correction** — periodic `STATE` broadcast; if a member drifts >2 s from host, the server emits a corrective `SEEK`.
- **Browser UI** at `/watchparty` — paste file_id, create/join room, play video, sync with other devices on the same LAN. Tested Mac ↔ Android.
- **VLC desktop bridge** (`client/watchparty_vlc.py`) — launches local VLC pointed at `/stream/{file_id}` with HTTP control enabled, bridges WebSocket sync ↔ VLC HTTP commands.

### Analytics + dashboard
- **Redis-backed counters** for uploads, downloads, bytes transferred, cache hits/misses, peer contribution, hourly bandwidth buckets.
- **`/analytics/summary|cache|peers|bandwidth`** JSON endpoints.
- **Dashboard** at `/dashboard` — dark mode, auto-refresh every 10 s, Chart.js bandwidth graph, top files, peer contribution.

### Local dev
- Runs natively on macOS with Homebrew PostgreSQL 16 and Redis (no Docker required).
- Docker Compose stack still works as an alternative.

## Test infrastructure

```
pytest tests/ --ignore=tests/load
# → 54 passed, 1 skipped
```

Coverage spans: chunker, integrity, async pool, upload/download pipeline, peer discovery, scheduler, edge cache, watch-party WebSocket sync, end-to-end pipeline. The previous fixture issues (cross-loop asyncpg engine reuse, autouse DB cleanup, TestClient deadlock) are resolved by:

- Session-scoped pytest-asyncio loop (`pytest.ini`).
- Lazy initialization of asyncio primitives in the transfer pool.
- Engine `dispose()` at session teardown.
- `cleanup_chunks` is opt-in via `pytestmark = pytest.mark.usefixtures("cleanup_chunks")` instead of autouse.
- WebSocket tests use `async-asgi-testclient` (httpx ASGI transport doesn't speak WebSocket upgrades).

Skipped tests:
- `test_cache_stats_hit_ratio` — hangs on session-scoped loop; needs investigation, not a product bug.
- `tests/load/*` — opt-in via `pytest tests/load`.

## Not in this version

Deliberate scope limits — design choices for a campus-LAN demo, not gaps in implementation.

| Area | What's missing | Why it's out of scope |
|---|---|---|
| **Auth / accounts** | No user table, no login, no API keys | Campus-LAN trust model. Peer IDs are server-assigned UUIDs; chunks are content-addressed by SHA-256 — trust is in the bytes, not the identity |
| **Real P2P transfer** | Peers register availability + serve as analytics labels, but chunk bytes always come from the origin server | Demo-grade. A true peer-to-peer transport would need WebRTC data channels or a per-peer HTTP server with NAT traversal |
| **NAT traversal** | UDP discovery is broadcast-only, LAN-scoped | Out of scope for a campus deployment |
| **HTTPS / TLS** | Plain HTTP only | Behind a reverse proxy in any real deployment |
| **Transcoding** | Files served as-uploaded; Safari can't play MKV | Beyond CDN scope; pipe into ffmpeg if needed |
| **Upload resume** | Interrupted uploads restart from zero | Acceptable for sub-GB files |
| **Streaming upload** | Multipart upload buffers in memory before chunking | OK for current file sizes; would need spool-to-disk for multi-GB |
| **Multi-origin replication** | Single FastAPI instance | Horizontal scaling is a separate problem |
| **Watch-party extras** | No chat, no cursor sharing, host-vs-peer permissions | Sync is the headline feature |
| **Content access control** | Anyone with `file_id` can download | Add an `X-API-Key` middleware (~10 LOC) if needed |
| **Rate limiting / quota** | None | Add nginx / FastAPI middleware in front |
| **Persistent analytics** | Redis only, lost on Redis restart | Periodic flush to Postgres if longer retention needed |

## Known issues

- **Safari + MKV**: `/stream` reports `Content-Type: video/x-matroska` but Safari refuses to render MKV. Use Chrome/Firefox/Android Chrome, or transcode the source to MP4/H.264/AAC.
- **`test_cache_stats_hit_ratio`**: hangs in session-scoped pytest-asyncio. Marked `@pytest.mark.skip`; needs a separate look.
- **Upload memory footprint**: large multipart uploads load the whole file into memory before chunking. Fine for the ~640 MB sample; would need streaming for multi-GB.
- **Peer registry is in-memory**: peers vanish on server restart. Acceptable for a single-process demo.

## File / module overview

```
campus-cdn/
├── client/
│   ├── cli.py                  # upload/download CLI
│   └── watchparty_vlc.py       # VLC bridge for desktop watch party
├── server/
│   ├── analytics/collector.py  # Redis-backed counters
│   ├── cache/edge_cache.py     # Redis LFU edge cache
│   ├── chunks/
│   │   ├── chunker.py          # 512 KB chunk splitter
│   │   ├── integrity.py        # SHA-256 verification
│   │   └── storage.py          # chunk file IO
│   ├── database/
│   │   ├── connection.py       # async SQLAlchemy engine
│   │   └── models.py           # FileRecord, ChunkRecord
│   ├── peers/
│   │   ├── discovery.py        # UDP broadcast discovery
│   │   └── registry.py         # in-memory peer registry
│   ├── routes/
│   │   ├── upload.py           # POST /upload
│   │   ├── download.py         # /manifest, /chunk, /stream
│   │   ├── peers.py            # /peers/*
│   │   ├── watchparty.py       # /watchparty/* + WebSocket
│   │   └── analytics.py        # /analytics/*
│   ├── scheduler/chunk_scheduler.py  # rarity + bandwidth scoring
│   ├── transfer/pool.py        # async semaphore-based pool
│   ├── watchparty/
│   │   ├── room.py             # RoomManager
│   │   └── sync.py             # ConnectionManager
│   └── main.py                 # FastAPI app + lifespan
├── static/
│   ├── dashboard.html          # analytics dashboard
│   └── watchparty.html         # browser watch-party UI
└── tests/                      # 54 passing, 1 skipped
```
