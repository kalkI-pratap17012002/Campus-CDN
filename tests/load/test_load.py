import asyncio
import statistics
import time
from contextlib import ExitStack

import pytest

from server.cache.edge_cache import EdgeCache


pytestmark = [
    pytest.mark.load,
    pytest.mark.usefixtures("cleanup_chunks"),
    pytest.mark.skip(reason="load tests require manual run"),
]


async def _upload(async_client, filename: str, data: bytes) -> dict:
    response = await async_client.post(
        "/upload",
        files={"file": (filename, data, "application/octet-stream")},
    )
    assert response.status_code == 200
    return response.json()


@pytest.mark.asyncio
async def test_upload_throughput(async_client, test_file_medium):
    timings = []
    started = time.perf_counter()
    for index in range(10):
        upload_started = time.perf_counter()
        await _upload(async_client, f"seq-{index}.bin", test_file_medium)
        timings.append(time.perf_counter() - upload_started)
    total_elapsed = time.perf_counter() - started
    average_time = statistics.mean(timings)
    print(f"Average upload time per 3MB file: {average_time:.4f}s")

    assert total_elapsed < 30.0


@pytest.mark.asyncio
async def test_concurrent_uploads(async_client, test_file_medium):
    sequential_started = time.perf_counter()
    for index in range(5):
        await _upload(async_client, f"baseline-{index}.bin", test_file_medium)
    sequential_elapsed = time.perf_counter() - sequential_started

    concurrent_started = time.perf_counter()
    results = await asyncio.gather(
        *(_upload(async_client, f"parallel-{index}.bin", test_file_medium) for index in range(5))
    )
    concurrent_elapsed = time.perf_counter() - concurrent_started

    assert len(results) == 5
    assert concurrent_elapsed < sequential_elapsed


@pytest.mark.asyncio
async def test_chunk_download_throughput(async_client, test_file_large):
    upload = await _upload(async_client, "load-large.bin", test_file_large)
    manifest = (await async_client.get(f"/manifest/{upload['file_id']}")).json()

    started = time.perf_counter()
    responses = await asyncio.gather(
        *(async_client.get(f"/chunk/{upload['file_id']}/{chunk['index']}") for chunk in manifest["chunks"])
    )
    elapsed = time.perf_counter() - started
    bytes_per_second = len(test_file_large) / elapsed
    print(f"Chunk download throughput: {bytes_per_second:.2f} bytes/s")

    assert all(response.status_code == 200 for response in responses)
    assert elapsed < 10.0


def test_cache_performance(redis_client):
    cache = EdgeCache(redis_url="redis://localhost:6379/15")
    cache.store_chunk("load-hit", b"payload")
    latencies_ms = []

    started = time.perf_counter()
    for index in range(1000):
        op_started = time.perf_counter()
        if index % 2 == 0:
            cache.get_chunk("load-hit")
        else:
            cache.get_chunk(f"load-miss-{index}")
        latencies_ms.append((time.perf_counter() - op_started) * 1000)
    elapsed = time.perf_counter() - started
    p99 = sorted(latencies_ms)[int(len(latencies_ms) * 0.99) - 1]

    assert elapsed < 5.0
    assert p99 < 10.0


def test_websocket_fanout_speed(test_client):
    room = test_client.post("/watchparty/create", json={"host_id": "host-1", "file_id": "load-room"}).json()
    with ExitStack() as stack:
        sockets = [
            stack.enter_context(test_client.websocket_connect(f"/watchparty/ws/{room['room_id']}/{peer_id}"))
            for peer_id in ["host-1", *[f"peer-{index}" for index in range(2, 11)]]
        ]

        for websocket in sockets:
            for _ in range(10):
                websocket.receive_json()

        started = time.perf_counter()
        sockets[0].send_json({"type": "PLAY", "timestamp_seconds": 12.0, "peer_id": "host-1"})
        for websocket in sockets:
            message = websocket.receive_json()
            assert message["type"] == "STATE"
        elapsed_ms = (time.perf_counter() - started) * 1000

    assert elapsed_ms < 100.0


@pytest.mark.asyncio
async def test_pool_under_saturation(async_client, test_file_large, monkeypatch):
    import server.routes.download as download_module
    from server.transfer.pool import transfer_pool

    upload = await _upload(async_client, "saturation.bin", test_file_large)
    manifest = (await async_client.get(f"/manifest/{upload['file_id']}")).json()
    original_read_chunk = download_module.read_chunk
    max_seen = 0
    acquire_times = []
    original_acquire = transfer_pool.acquire

    async def tracked_acquire(file_id: str) -> int:
        nonlocal max_seen
        slot = await original_acquire(file_id)
        max_seen = max(max_seen, transfer_pool.get_active_count())
        acquire_times.append(time.perf_counter())
        return slot

    def slow_read_chunk(path: str) -> bytes:
        time.sleep(0.05)
        return original_read_chunk(path)

    monkeypatch.setattr(transfer_pool, "acquire", tracked_acquire)
    monkeypatch.setattr(download_module, "read_chunk", slow_read_chunk)

    started = time.perf_counter()
    responses = await asyncio.gather(
        *(async_client.get(f"/chunk/{upload['file_id']}/{chunk['index']}") for chunk in manifest["chunks"][:10])
    )
    elapsed = time.perf_counter() - started

    assert all(response.status_code == 200 for response in responses)
    assert max_seen <= 5
    assert elapsed < 2.0
