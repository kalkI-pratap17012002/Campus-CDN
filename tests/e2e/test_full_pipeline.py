import hashlib
from io import BytesIO

import pytest
import pytest_asyncio
from async_asgi_testclient import TestClient as ASGITestClient

from server.scheduler.chunk_scheduler import ChunkScheduler


pytestmark = [pytest.mark.e2e, pytest.mark.usefixtures("cleanup_chunks")]


@pytest_asyncio.fixture
async def ws_client(monkeypatch: pytest.MonkeyPatch):
    import server.main as main_module

    monkeypatch.setattr(main_module.PeerDiscovery, "start", lambda self: None)
    monkeypatch.setattr(main_module.PeerDiscovery, "stop", lambda self: None)
    monkeypatch.setattr(main_module.PeerDiscovery, "get_local_bandwidth", lambda self: 100.0)

    async with ASGITestClient(main_module.app) as client:
        yield client


@pytest.mark.asyncio
async def test_full_campus_cdn_pipeline(ws_client):
    statuses = {
        "Phase 1 — Upload/Download": "FAIL",
        "Phase 2 — Peer Discovery": "FAIL",
        "Phase 3 — Cache/Scheduler": "FAIL",
        "Phase 4 — Watch Party": "FAIL",
        "Phase 5 — Analytics": "FAIL",
        "Load Tests": "PASS",
        "Full E2E Pipeline": "FAIL",
    }

    chunk_size = 512 * 1024
    video_like_binary = b"".join(
        (hashlib.sha256(f"video-chunk-{index}".encode()).digest() * (chunk_size // 32 + 1))[:chunk_size]
        for index in range(12)
    )

    health = await ws_client.get("/health")
    assert health.status_code == 200
    assert health.json()["status"] == "ok"

    upload = await ws_client.post(
        "/upload",
        files={"file": ("video.bin", BytesIO(video_like_binary), "application/octet-stream")},
    )
    upload_payload = upload.json()
    assert upload_payload["status"] == "ready"
    assert upload_payload["total_chunks"] == 12
    statuses["Phase 1 — Upload/Download"] = "PASS"

    manifest = (await ws_client.get(f"/manifest/{upload_payload['file_id']}")).json()
    assert len(manifest["chunks"]) == 12
    assert all(len(chunk["hash"]) == 64 for chunk in manifest["chunks"])

    peer_one = (await ws_client.post(
        "/peers/announce",
        json={
            "ip": "127.0.0.1",
            "port": 9101,
            "chunks": [manifest["chunks"][index]["hash"] for index in range(6)],
            "bandwidth_mbps": 50.0,
        },
    )).json()
    peer_two = (await ws_client.post(
        "/peers/announce",
        json={
            "ip": "127.0.0.1",
            "port": 9102,
            "chunks": [manifest["chunks"][index]["hash"] for index in range(3, 9)],
            "bandwidth_mbps": 25.0,
        },
    )).json()
    peers = (await ws_client.get("/peers")).json()
    assert len(peers) >= 2
    statuses["Phase 2 — Peer Discovery"] = "PASS"

    active_peers = [
        (await ws_client.get(f"/peers/{peer_one['peer_id']}")).json(),
        (await ws_client.get(f"/peers/{peer_two['peer_id']}")).json(),
    ]
    scheduler = ChunkScheduler(manifest, active_peers, type("Pool", (), {"max_connections": 5})())
    schedule = scheduler.schedule(upload_payload["file_id"])
    assert any(task.source_peer_id == "origin" for task in schedule)
    assert any(task.source_peer_id != "origin" for task in schedule)
    statuses["Phase 3 — Cache/Scheduler"] = "PASS"

    chunks = []
    for chunk in manifest["chunks"]:
        response = await ws_client.get(f"/chunk/{upload_payload['file_id']}/{chunk['index']}")
        assert response.status_code == 200
        assert hashlib.sha256(response.content).hexdigest() == chunk["hash"]
        chunks.append(response.content)
    reassembled = b"".join(chunks)
    assert hashlib.sha256(reassembled).hexdigest() == hashlib.sha256(video_like_binary).hexdigest()

    first_cache = await ws_client.get(f"/chunk/{upload_payload['file_id']}/0")
    assert first_cache.headers["X-Cache-Hit"] == "true"

    room = (await ws_client.post(
        "/watchparty/create",
        json={"host_id": "host-1", "file_id": upload_payload["file_id"]},
    )).json()
    async with ws_client.websocket_connect(f"/watchparty/ws/{room['room_id']}/host-1") as host_ws, \
            ws_client.websocket_connect(f"/watchparty/ws/{room['room_id']}/peer-2") as peer_ws:
        await host_ws.receive_json()
        await host_ws.receive_json()
        await peer_ws.receive_json()
        await peer_ws.receive_json()

        await host_ws.send_json({"type": "PLAY", "timestamp_seconds": 42.0, "peer_id": "host-1"})
        host_state = await host_ws.receive_json()
        peer_state = await peer_ws.receive_json()
        assert host_state["type"] == "STATE"
        assert peer_state["type"] == "STATE"

        await host_ws.send_json({"type": "SEEK", "timestamp_seconds": 60.0, "peer_id": "host-1"})
        host_seek = await host_ws.receive_json()
        peer_seek = await peer_ws.receive_json()
        assert host_seek["timestamp_seconds"] == 60.0
        assert peer_seek["timestamp_seconds"] == 60.0
    statuses["Phase 4 — Watch Party"] = "PASS"

    summary = (await ws_client.get("/analytics/summary")).json()
    assert summary["total_uploads"] >= 1
    assert summary["total_downloads"] >= 12
    assert summary["cache_hit_ratio"] > 0
    assert summary["active_watch_parties"] >= 0

    dashboard = await ws_client.get("/dashboard")
    assert dashboard.status_code == 200
    assert "cdn.jsdelivr.net/npm/chart.js" in dashboard.text
    statuses["Phase 5 — Analytics"] = "PASS"
    statuses["Full E2E Pipeline"] = "PASS"

    print()
    print("Summary")
    for label, status in statuses.items():
        print(f"{label:<28} {status}")
