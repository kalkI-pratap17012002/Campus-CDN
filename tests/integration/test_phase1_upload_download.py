import asyncio
import hashlib
import time
import uuid
from pathlib import Path

import pytest
from sqlalchemy import select

from server.database.models import ChunkRecord
from server.transfer.pool import ConnectionPool, transfer_pool


pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("cleanup_chunks")]


async def _upload_file(async_client, filename: str, data: bytes) -> dict:
    response = await async_client.post(
        "/upload",
        files={"file": (filename, data, "application/octet-stream")},
    )
    assert response.status_code == 200, response.text
    return response.json()


@pytest.mark.asyncio
async def test_upload_small_file(async_client, test_file_small):
    payload = await _upload_file(async_client, "small.bin", test_file_small)

    assert "file_id" in payload
    assert payload["status"] == "ready"
    assert payload["total_chunks"] == 1


@pytest.mark.asyncio
async def test_upload_medium_file(async_client, db_session, test_file_medium):
    payload = await _upload_file(async_client, "medium.bin", test_file_medium)

    result = await db_session.execute(
        select(ChunkRecord).where(ChunkRecord.file_id == uuid.UUID(payload["file_id"]))
    )
    chunks = list(result.scalars().all())

    assert payload["total_chunks"] == 6
    assert len(chunks) == 6


@pytest.mark.asyncio
async def test_upload_large_file(async_client, test_file_large):
    payload = await _upload_file(async_client, "large.bin", test_file_large)

    assert payload["total_chunks"] == 24
    for chunk_index in range(24):
        assert Path(f"{Path.cwd() / 'chunks' / 'test-suite' / payload['file_id'] / f'{chunk_index}.bin'}").exists()


@pytest.mark.asyncio
async def test_manifest_returns_all_chunks(async_client, test_file_medium):
    payload = await _upload_file(async_client, "manifest.bin", test_file_medium)

    response = await async_client.get(f"/manifest/{payload['file_id']}")
    manifest = response.json()

    assert response.status_code == 200
    assert len(manifest["chunks"]) == payload["total_chunks"]
    assert all({"index", "size", "hash"} <= set(chunk) for chunk in manifest["chunks"])


@pytest.mark.asyncio
async def test_chunk_download_correct_bytes(async_client, test_file_small):
    payload = await _upload_file(async_client, "known.bin", test_file_small)

    response = await async_client.get(f"/chunk/{payload['file_id']}/0")

    assert response.status_code == 200
    assert response.content == test_file_small


@pytest.mark.asyncio
async def test_chunk_sha256_verification(async_client, db_session, test_file_small):
    payload = await _upload_file(async_client, "corrupt.bin", test_file_small)
    result = await db_session.execute(
        select(ChunkRecord).where(ChunkRecord.file_id == uuid.UUID(payload["file_id"]))
    )
    chunk_record = result.scalar_one()

    chunk_path = Path(chunk_record.storage_path)
    chunk_path.write_bytes(b"corrupted-bytes")

    response = await async_client.get(f"/chunk/{payload['file_id']}/0")
    assert response.status_code == 409


@pytest.mark.asyncio
async def test_concurrent_chunk_downloads(async_client, test_file_large, monkeypatch):
    payload = await _upload_file(async_client, "concurrent.bin", test_file_large)
    manifest = (await async_client.get(f"/manifest/{payload['file_id']}")).json()

    max_seen = 0
    original_acquire = transfer_pool.acquire

    async def tracked_acquire(file_id: str) -> int:
        nonlocal max_seen
        slot = await original_acquire(file_id)
        max_seen = max(max_seen, transfer_pool.get_active_count())
        return slot

    monkeypatch.setattr(transfer_pool, "acquire", tracked_acquire)

    responses = await asyncio.gather(
        *(async_client.get(f"/chunk/{payload['file_id']}/{chunk['index']}") for chunk in manifest["chunks"])
    )

    assert len(responses) == 24
    for response, chunk in zip(responses, manifest["chunks"], strict=True):
        assert response.status_code == 200
        assert hashlib.sha256(response.content).hexdigest() == chunk["hash"]
    assert max_seen <= 5


@pytest.mark.asyncio
async def test_pool_timeout():
    pool = ConnectionPool(max_connections=1, timeout_seconds=0.1)
    slot = await pool.acquire("file-1")

    started = time.perf_counter()
    with pytest.raises(TimeoutError):
        await pool.acquire("file-2")
    elapsed = time.perf_counter() - started

    assert elapsed < 1.0
    await pool.release("file-1", slot)


@pytest.mark.asyncio
async def test_upload_invalid_file_type(async_client):
    response = await async_client.post("/upload")
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_download_nonexistent_file(async_client):
    response = await async_client.get(f"/manifest/{uuid.uuid4()}")
    assert response.status_code == 404
