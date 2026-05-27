import logging
import os
import tempfile
from pathlib import Path

import aiofiles
from fastapi import APIRouter, Depends, File, HTTPException, Request, UploadFile, status
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.exc import ObjectDeletedError

from server.analytics.collector import AnalyticsCollector
from server.chunks.chunker import split_file
from server.chunks.integrity import compute_sha256, verify_chunk
from server.chunks.storage import read_chunk, save_chunk
from server.database.connection import get_db
from server.database.models import ChunkRecord, FileRecord


logger = logging.getLogger(__name__)
router = APIRouter(tags=["upload"])


@router.post("/upload")
async def upload_file(
    request: Request,
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
) -> dict[str, object]:
    if not file.filename:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Filename is required")

    temp_path: str | None = None
    file_record: FileRecord | None = None
    total_size = 0

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=Path(file.filename).suffix) as temp_file:
            temp_path = temp_file.name

        async with aiofiles.open(temp_path, "wb") as temp_handle:
            while True:
                data = await file.read(1024 * 1024)
                if not data:
                    break
                total_size += len(data)
                await temp_handle.write(data)

        file_record = FileRecord(
            filename=file.filename,
            total_size=total_size,
            total_chunks=0,
            status="uploading",
        )
        db.add(file_record)
        await db.commit()
        try:
            await db.refresh(file_record)
        except (ObjectDeletedError, InvalidRequestError):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="File record was removed during upload",
            )

        total_chunks = 0
        for chunk_metadata in split_file(temp_path, str(file_record.id)):
            chunk_bytes = read_chunk(chunk_metadata.storage_path)
            save_chunk(chunk_bytes, chunk_metadata.storage_path)

            computed_hash = compute_sha256(chunk_metadata.storage_path)
            if computed_hash != chunk_metadata.sha256_hash:
                raise ValueError(
                    f"Chunk hash mismatch for chunk {chunk_metadata.chunk_index}: "
                    f"{computed_hash} != {chunk_metadata.sha256_hash}"
                )
            if not verify_chunk(chunk_metadata.storage_path, computed_hash):
                raise ValueError(f"Chunk verification failed for chunk {chunk_metadata.chunk_index}")

            db.add(
                ChunkRecord(
                    file_id=file_record.id,
                    chunk_index=chunk_metadata.chunk_index,
                    chunk_size=chunk_metadata.size,
                    sha256_hash=computed_hash,
                    storage_path=chunk_metadata.storage_path,
                )
            )
            total_chunks += 1

        file_record.total_chunks = total_chunks
        file_record.status = "ready"
        await db.commit()
        try:
            await db.refresh(file_record)
        except (ObjectDeletedError, InvalidRequestError):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="File record was removed during upload",
            )

        logger.info(
            "Uploaded file_id=%s filename=%s total_chunks=%s total_size=%s",
            file_record.id,
            file_record.filename,
            file_record.total_chunks,
            file_record.total_size,
        )
        analytics: AnalyticsCollector | None = getattr(request.app.state, "analytics_collector", None)
        if analytics is not None:
            analytics.record_upload(str(file_record.id), int(file_record.total_size))

        return {
            "file_id": str(file_record.id),
            "filename": file_record.filename,
            "total_chunks": file_record.total_chunks,
            "total_size": file_record.total_size,
            "status": file_record.status,
            "uploaded_at": file_record.uploaded_at,
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Upload failed for filename=%s", file.filename)
        if file_record is not None:
            file_record.status = "corrupted"
            await db.commit()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Upload failed: {exc}",
        ) from exc
    finally:
        await file.close()
        if temp_path and os.path.exists(temp_path):
            os.unlink(temp_path)
