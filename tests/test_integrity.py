from types import SimpleNamespace

from server.chunks.integrity import compute_sha256, verify_chunk, verify_file


def test_sha256_matches_for_valid_chunk(tmp_path):
    chunk_path = tmp_path / "chunk.bin"
    chunk_path.write_bytes(b"campus-cdn")

    digest = compute_sha256(str(chunk_path))

    assert verify_chunk(str(chunk_path), digest) is True


def test_sha256_fails_after_corrupting_bytes(tmp_path):
    chunk_path = tmp_path / "chunk.bin"
    chunk_path.write_bytes(b"original-data")
    digest = compute_sha256(str(chunk_path))

    chunk_path.write_bytes(b"corrupted-data")

    assert verify_chunk(str(chunk_path), digest) is False


def test_verify_file_returns_correct_corrupted_list(tmp_path):
    valid_chunk = tmp_path / "valid.bin"
    corrupted_chunk = tmp_path / "corrupted.bin"
    valid_chunk.write_bytes(b"valid")
    corrupted_chunk.write_bytes(b"before")

    valid_hash = compute_sha256(str(valid_chunk))
    corrupted_hash = compute_sha256(str(corrupted_chunk))
    corrupted_chunk.write_bytes(b"after")

    result = verify_file(
        "file-789",
        [
            SimpleNamespace(chunk_index=0, storage_path=str(valid_chunk), sha256_hash=valid_hash),
            SimpleNamespace(chunk_index=1, storage_path=str(corrupted_chunk), sha256_hash=corrupted_hash),
        ],
    )

    assert result["valid"] is False
    assert result["corrupted_chunks"] == [1]
