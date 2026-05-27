import hashlib
from typing import Any


def compute_sha256(file_path: str) -> str:
    digest = hashlib.sha256()
    with open(file_path, "rb") as file_handle:
        for block in iter(lambda: file_handle.read(8192), b""):
            digest.update(block)
    return digest.hexdigest()


def verify_chunk(chunk_path: str, expected_hash: str) -> bool:
    return compute_sha256(chunk_path) == expected_hash


def verify_file(file_id: str, chunk_records: list[Any]) -> dict[str, object]:
    corrupted_chunks: list[int] = []

    for chunk_record in chunk_records:
        if not verify_chunk(chunk_record.storage_path, chunk_record.sha256_hash):
            corrupted_chunks.append(chunk_record.chunk_index)

    return {
        "valid": len(corrupted_chunks) == 0,
        "corrupted_chunks": corrupted_chunks,
    }
