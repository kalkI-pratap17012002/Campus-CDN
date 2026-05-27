from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from server.chunks.integrity import compute_sha256
from server.chunks.storage import save_chunk
from server.config import settings


@dataclass(frozen=True)
class ChunkMetadata:
    chunk_index: int
    size: int
    sha256_hash: str
    storage_path: str


def split_file(file_path: str, file_id: str) -> Iterator[ChunkMetadata]:
    source_path = Path(file_path)
    output_dir = settings.chunk_storage_dir / str(file_id)
    output_dir.mkdir(parents=True, exist_ok=True)

    with source_path.open("rb", buffering=settings.CHUNK_SIZE_BYTES) as source_handle:
        chunk_index = 0
        while True:
            chunk_data = source_handle.read(settings.CHUNK_SIZE_BYTES)
            if not chunk_data:
                break

            destination = output_dir / f"{chunk_index}.bin"
            save_chunk(chunk_data, str(destination))
            chunk_hash = compute_sha256(str(destination))

            yield ChunkMetadata(
                chunk_index=chunk_index,
                size=len(chunk_data),
                sha256_hash=chunk_hash,
                storage_path=str(destination),
            )
            chunk_index += 1
