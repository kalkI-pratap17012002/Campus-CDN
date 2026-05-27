from datetime import UTC, datetime

import pytest

from server.cache.edge_cache import EdgeCache
from server.peers.registry import PeerInfo
from server.scheduler.chunk_scheduler import ChunkScheduler


pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("cleanup_chunks")]


def _peer(peer_id: str, bandwidth: float, chunks: list[str]) -> PeerInfo:
    return PeerInfo(
        peer_id=peer_id,
        ip="127.0.0.1",
        port=8000,
        available_chunks=chunks,
        bandwidth_mbps=bandwidth,
        last_seen=datetime.now(UTC),
        is_active=True,
    )


def _manifest(chunk_count: int) -> dict:
    return {
        "chunks": [
            {"index": index, "size": 524288, "hash": f"hash-{index}"}
            for index in range(chunk_count)
        ]
    }


class _Pool:
    max_connections = 5


def test_rarity_score_sorts_correctly():
    manifest = _manifest(10)
    peers = [
        _peer("peer-a", 50.0, ["hash-0", "hash-1", "hash-2", "hash-3"]),
        _peer("peer-b", 50.0, ["hash-1", "hash-2", "hash-3"]),
        _peer("peer-c", 50.0, ["hash-2", "hash-3"]),
    ]

    schedule = ChunkScheduler(manifest, peers, _Pool()).schedule("file-1")
    positions = {task.chunk_index: position for position, task in enumerate(schedule)}

    assert positions[0] < positions[2]
    assert positions[1] < positions[2]


def test_bandwidth_aware_assignment():
    manifest = _manifest(10)
    shared_chunks = [chunk["hash"] for chunk in manifest["chunks"]]
    peers = [
        _peer("fast", 100.0, shared_chunks),
        _peer("slow", 1.0, shared_chunks),
    ]

    schedule = ChunkScheduler(manifest, peers, _Pool()).schedule("file-2")
    fast_assignments = sum(1 for task in schedule if task.source_peer_id == "fast")

    assert fast_assignments > len(schedule) // 2


def test_origin_fallback():
    manifest = _manifest(1)
    schedule = ChunkScheduler(manifest, [], _Pool()).schedule("file-3")
    assert schedule[0].source_peer_id == "origin"


def test_load_spread_across_peers():
    manifest = _manifest(30)
    shared_chunks = [chunk["hash"] for chunk in manifest["chunks"]]
    peers = [
        _peer("peer-1", 10.0, shared_chunks),
        _peer("peer-2", 10.0, shared_chunks),
        _peer("peer-3", 10.0, shared_chunks),
    ]

    schedule = ChunkScheduler(manifest, peers, _Pool()).schedule("file-4")
    counts = {}
    for task in schedule:
        counts[task.source_peer_id] = counts.get(task.source_peer_id, 0) + 1

    assert all(count <= 15 for peer_id, count in counts.items() if peer_id != "origin")


def test_schedule_stats_accuracy():
    manifest = _manifest(10)
    peers = [_peer("peer-1", 50.0, [f"hash-{index}" for index in range(7)])]
    scheduler = ChunkScheduler(manifest, peers, _Pool())
    scheduler.schedule("file-5")

    assert scheduler.get_schedule_stats()["from_peers"] == 7
    assert scheduler.get_schedule_stats()["from_origin"] == 3


def test_cache_store_and_retrieve(redis_client):
    cache = EdgeCache(redis_url="redis://localhost:6379/15")
    data = b"cache-data"
    cache.store_chunk("cache-hash", data)

    assert cache.get_chunk("cache-hash") == data


def test_cache_miss_returns_none():
    cache = EdgeCache(redis_url="redis://localhost:6379/15")
    assert cache.get_chunk("nonexistent_hash") is None


def test_lfu_eviction(redis_client):
    cache = EdgeCache(redis_url="redis://localhost:6379/15", max_cache_size_gb=0.000001)
    chunk_a = b"a" * 400
    chunk_b = b"b" * 400
    chunk_c = b"c" * 400

    cache.store_chunk("chunk-a", chunk_a)
    cache.store_chunk("chunk-b", chunk_b)
    for _ in range(10):
        assert cache.get_chunk("chunk-a") == chunk_a
    assert cache.get_chunk("chunk-b") == chunk_b

    cache.store_chunk("chunk-c", chunk_c)

    assert cache.is_cached("chunk-a") is True
    assert cache.is_cached("chunk-b") is False
    assert cache.is_cached("chunk-c") is True


@pytest.mark.asyncio
async def test_cache_hit_header(async_client, test_file_small):
    upload = await async_client.post(
        "/upload",
        files={"file": ("cache-header.bin", test_file_small, "application/octet-stream")},
    )
    file_id = upload.json()["file_id"]

    first = await async_client.get(f"/chunk/{file_id}/0")
    second = await async_client.get(f"/chunk/{file_id}/0")

    assert first.headers["X-Cache-Hit"] == "false"
    assert second.headers["X-Cache-Hit"] == "true"


@pytest.mark.skip(reason="hangs - needs investigation")
def test_cache_stats_hit_ratio():
    cache = EdgeCache(redis_url="redis://localhost:6379/15")
    cache.store_chunk("ratio-hit", b"payload")
    for _ in range(7):
        cache.get_chunk("ratio-hit")
    for index in range(3):
        cache.get_chunk(f"ratio-miss-{index}")

    stats = cache.get_stats()
    assert stats["hit_ratio"] == pytest.approx(0.7, rel=1e-3)
