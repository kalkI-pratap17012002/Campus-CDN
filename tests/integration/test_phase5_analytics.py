import time
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from server.analytics.collector import AnalyticsCollector
from server.cache.edge_cache import edge_cache
from server.database.models import FileRecord


pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("cleanup_chunks")]


@pytest.mark.asyncio
async def test_upload_recorded(async_client, test_file_small):
    await async_client.post(
        "/upload",
        files={"file": ("upload-record.bin", test_file_small, "application/octet-stream")},
    )
    summary = (await async_client.get("/analytics/summary")).json()

    assert summary["total_uploads"] == 1


@pytest.mark.asyncio
async def test_download_recorded(async_client, test_file_medium):
    upload = await async_client.post(
        "/upload",
        files={"file": ("download-record.bin", test_file_medium, "application/octet-stream")},
    )
    file_id = upload.json()["file_id"]
    manifest = (await async_client.get(f"/manifest/{file_id}")).json()

    for chunk in manifest["chunks"][:5]:
        response = await async_client.get(f"/chunk/{file_id}/{chunk['index']}")
        assert response.status_code == 200

    summary = (await async_client.get("/analytics/summary")).json()
    assert summary["total_downloads"] >= 5
    assert summary["total_bytes_transferred"] > 0


@pytest.mark.asyncio
async def test_cache_hit_ratio_tracked(async_client, redis_client):
    edge_cache.store_chunk("hit-key", b"payload")
    for _ in range(8):
        assert edge_cache.get_chunk("hit-key") == b"payload"
    for index in range(2):
        assert edge_cache.get_chunk(f"miss-key-{index}") is None

    cache = (await async_client.get("/analytics/cache")).json()
    assert cache["hit_ratio"] == pytest.approx(0.8, rel=1e-3)


@pytest.mark.asyncio
async def test_peer_vs_origin_ratio(async_client, app_instance):
    collector = app_instance.state.analytics_collector
    for index in range(7):
        collector.record_download(f"file-peer-{index}", f"chunk-{index}", 1024, "peer")
    for index in range(3):
        collector.record_download(f"file-origin-{index}", f"origin-{index}", 1024, "origin")

    summary = (await async_client.get("/analytics/summary")).json()
    assert summary["peer_transfer_ratio"] == pytest.approx(0.7, rel=1e-3)


@pytest.mark.asyncio
async def test_bandwidth_history_bucketed(async_client, app_instance, monkeypatch):
    collector = app_instance.state.analytics_collector
    now = datetime.now(UTC)
    timestamps = [
        (now - timedelta(hours=3)).timestamp(),
        (now - timedelta(hours=3, minutes=10)).timestamp(),
        (now - timedelta(hours=2)).timestamp(),
        (now - timedelta(hours=1)).timestamp(),
        (now - timedelta(hours=1, minutes=5)).timestamp(),
    ]

    for index, timestamp_value in enumerate(timestamps):
        monkeypatch.setattr("server.analytics.collector.time.time", lambda ts=timestamp_value: ts)
        collector.record_download("file-history", f"chunk-{index}", 1024 * (index + 1), "origin")

    history = (await async_client.get("/analytics/bandwidth?hours=24")).json()
    populated = [entry for entry in history if entry["bytes"] > 0]
    assert len(populated) >= 3


@pytest.mark.asyncio
async def test_top_files_sorted(async_client, db_session, app_instance):
    collector = app_instance.state.analytics_collector
    files = [
        (uuid.uuid4(), "file-a.bin", 50),
        (uuid.uuid4(), "file-b.bin", 20),
        (uuid.uuid4(), "file-c.bin", 80),
    ]
    for file_id, filename, downloads in files:
        db_session.add(
            FileRecord(
                id=file_id,
                filename=filename,
                total_size=1024,
                total_chunks=1,
                status="ready",
            )
        )
        for index in range(downloads):
            collector.record_download(str(file_id), f"{file_id}-chunk-{index}", 2048, "origin")
    await db_session.commit()

    summary = (await async_client.get("/analytics/summary")).json()
    assert summary["top_files"][0]["downloads"] == 80


@pytest.mark.asyncio
async def test_peer_contributions_tracked(async_client, redis_client):
    collectors = [
        AnalyticsCollector(
            redis_url="redis://localhost:6379/15",
            local_peer_id_provider=lambda peer_id=peer_id: peer_id,
            local_peer_ip_provider=lambda peer_ip=peer_ip: peer_ip,
        )
        for peer_id, peer_ip in [
            ("peer-a", "10.0.0.1"),
            ("peer-b", "10.0.0.2"),
            ("peer-c", "10.0.0.3"),
        ]
    ]
    for collector in collectors:
        collector.record_download("file-peer", "chunk-peer", 4096, "peer")
    peers = (await async_client.get("/analytics/peers")).json()

    try:
        assert {"peer-a", "peer-b", "peer-c"} <= {peer["peer_id"] for peer in peers}
        assert all(peer["bytes_contributed"] > 0 for peer in peers if peer["peer_id"].startswith("peer-"))
    finally:
        for collector in collectors:
            collector.stop()


@pytest.mark.asyncio
async def test_dashboard_loads(async_client):
    response = await async_client.get("/dashboard")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert "Campus CDN" in response.text
