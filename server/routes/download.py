import logging
import mimetypes
import time
import uuid
from hashlib import sha256
from io import BytesIO
from typing import AsyncIterator

from fastapi import APIRouter, Depends, Header, HTTPException, Request, status
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from server.analytics.collector import AnalyticsCollector
from server.cache.edge_cache import edge_cache
from server.chunks.integrity import verify_chunk
from server.chunks.storage import read_chunk
from server.database.connection import get_db
from server.database.models import ChunkRecord, FileRecord
from server.transfer.pool import transfer_pool


logger = logging.getLogger(__name__)
router = APIRouter(tags=["download"])


@router.get("/manifest/{file_id}")
async def get_manifest(file_id: uuid.UUID, db: AsyncSession = Depends(get_db)) -> dict[str, object]:
    file_record = await db.get(FileRecord, file_id)
    if file_record is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found")

    result = await db.execute(
        select(ChunkRecord).where(ChunkRecord.file_id == file_id).order_by(ChunkRecord.chunk_index)
    )
    chunk_records = list(result.scalars().all())

    return {
        "file_id": str(file_record.id),
        "filename": file_record.filename,
        "total_size": file_record.total_size,
        "total_chunks": file_record.total_chunks,
        "status": file_record.status,
        "chunks": [
            {
                "index": chunk_record.chunk_index,
                "size": chunk_record.chunk_size,
                "hash": chunk_record.sha256_hash,
            }
            for chunk_record in chunk_records
        ],
    }


@router.get("/chunk/{file_id}/{chunk_index}")
async def download_chunk(
    request: Request,
    file_id: uuid.UUID,
    chunk_index: int,
    db: AsyncSession = Depends(get_db),
) -> StreamingResponse:
    slot: int | None = None
    start_time = time.perf_counter()

    try:
        slot = await transfer_pool.acquire(str(file_id))

        result = await db.execute(
            select(ChunkRecord).where(
                ChunkRecord.file_id == file_id,
                ChunkRecord.chunk_index == chunk_index,
            )
        )
        chunk_record = result.scalar_one_or_none()
        if chunk_record is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Chunk not found",
                headers={"X-Cache-Hit": "false"},
            )

        cache_hit = False
        chunk_bytes = edge_cache.get_chunk(chunk_record.sha256_hash)
        if chunk_bytes is not None and sha256(chunk_bytes).hexdigest() == chunk_record.sha256_hash:
            cache_hit = True
        else:
            chunk_bytes = read_chunk(chunk_record.storage_path)
            if not verify_chunk(chunk_record.storage_path, chunk_record.sha256_hash):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Chunk integrity verification failed",
                )
            edge_cache.store_chunk(chunk_record.sha256_hash, chunk_bytes)

        transfer_mode = request.headers.get("X-CDN-Transfer-Mode", "").strip().lower()
        source = "peer" if transfer_mode == "peer" else "cache" if cache_hit else "origin"
        analytics: AnalyticsCollector | None = getattr(request.app.state, "analytics_collector", None)
        if analytics is not None:
            analytics.record_download(
                str(file_id),
                chunk_record.sha256_hash,
                len(chunk_bytes),
                source=source,
            )

        elapsed = time.perf_counter() - start_time
        logger.info(
            "Served chunk file_id=%s chunk_index=%s size=%s bytes duration=%.4fs cache_hit=%s source=%s",
            file_id,
            chunk_index,
            len(chunk_bytes),
            elapsed,
            cache_hit,
            source,
        )

        stream = BytesIO(chunk_bytes)
        return StreamingResponse(
            stream,
            media_type="application/octet-stream",
            headers={
                "Content-Length": str(len(chunk_bytes)),
                "X-Cache-Hit": str(cache_hit).lower(),
            },
        )
    except TimeoutError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc
    finally:
        if slot is not None:
            await transfer_pool.release(str(file_id), slot)


def _parse_range_header(range_header: str, total_size: int) -> tuple[int, int]:
    units, _, range_spec = range_header.partition("=")
    if units.strip().lower() != "bytes" or "," in range_spec:
        raise HTTPException(status_code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE, detail="Invalid Range")
    start_str, _, end_str = range_spec.partition("-")
    if start_str == "":
        suffix = int(end_str)
        if suffix <= 0:
            raise HTTPException(status_code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE, detail="Invalid Range")
        start = max(0, total_size - suffix)
        end = total_size - 1
    else:
        start = int(start_str)
        end = int(end_str) if end_str else total_size - 1
    if start < 0 or end >= total_size or start > end:
        raise HTTPException(status_code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE, detail="Range out of bounds")
    return start, end


@router.get("/stream/{file_id}")
async def stream_file(
    file_id: uuid.UUID,
    range_header: str | None = Header(default=None, alias="Range"),
    db: AsyncSession = Depends(get_db),
) -> StreamingResponse:
    file_record = await db.get(FileRecord, file_id)
    if file_record is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found")

    result = await db.execute(
        select(ChunkRecord).where(ChunkRecord.file_id == file_id).order_by(ChunkRecord.chunk_index)
    )
    chunk_records = list(result.scalars().all())
    if not chunk_records:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No chunks available")

    chunk_offsets: list[int] = []
    running = 0
    for record in chunk_records:
        chunk_offsets.append(running)
        running += record.chunk_size
    total_size = running

    media_type, _ = mimetypes.guess_type(file_record.filename)
    if media_type is None:
        media_type = "application/octet-stream"

    start, end = (0, total_size - 1)
    is_partial = False
    if range_header:
        start, end = _parse_range_header(range_header, total_size)
        is_partial = True

    async def iter_range() -> AsyncIterator[bytes]:
        cursor = start
        for index, record in enumerate(chunk_records):
            chunk_start = chunk_offsets[index]
            chunk_end = chunk_start + record.chunk_size - 1
            if chunk_end < cursor:
                continue
            if chunk_start > end:
                break
            chunk_bytes = edge_cache.get_chunk(record.sha256_hash)
            if chunk_bytes is None or sha256(chunk_bytes).hexdigest() != record.sha256_hash:
                chunk_bytes = read_chunk(record.storage_path)
                edge_cache.store_chunk(record.sha256_hash, chunk_bytes)
            local_start = cursor - chunk_start
            local_end = min(end, chunk_end) - chunk_start + 1
            yield chunk_bytes[local_start:local_end]
            cursor = chunk_end + 1
            if cursor > end:
                break

    headers = {
        "Accept-Ranges": "bytes",
        "Content-Length": str(end - start + 1),
        "Content-Disposition": f'inline; filename="{file_record.filename}"',
    }
    if is_partial:
        headers["Content-Range"] = f"bytes {start}-{end}/{total_size}"

    return StreamingResponse(
        iter_range(),
        media_type=media_type,
        headers=headers,
        status_code=status.HTTP_206_PARTIAL_CONTENT if is_partial else status.HTTP_200_OK,
    )
