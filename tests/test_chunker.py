from pathlib import Path

from server.chunks.chunker import split_file
from server.config import settings


def test_split_file_creates_expected_chunk_count_for_one_mb_file(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "CHUNK_STORAGE_PATH", str(tmp_path / "chunks"))
    source_path = tmp_path / "one_mb.bin"
    source_path.write_bytes(b"a" * (1024 * 1024))

    chunks = list(split_file(str(source_path), "file-123"))

    assert len(chunks) == 2
    assert [chunk.chunk_index for chunk in chunks] == [0, 1]


def test_split_file_last_chunk_has_correct_size_and_files_exist(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "CHUNK_STORAGE_PATH", str(tmp_path / "chunks"))
    source_path = tmp_path / "odd_size.bin"
    source_path.write_bytes(b"b" * (settings.CHUNK_SIZE_BYTES + 123))

    chunks = list(split_file(str(source_path), "file-456"))

    assert len(chunks) == 2
    assert chunks[-1].size == 123
    for chunk in chunks:
        assert Path(chunk.storage_path).exists()
