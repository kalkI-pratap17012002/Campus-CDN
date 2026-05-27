from pathlib import Path


def save_chunk(data: bytes, path: str) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("wb") as file_handle:
        file_handle.write(data)


def read_chunk(path: str) -> bytes:
    with open(path, "rb") as file_handle:
        return file_handle.read()


def delete_chunk(path: str) -> None:
    chunk_path = Path(path)
    if chunk_path.exists():
        chunk_path.unlink()


def chunk_exists(path: str) -> bool:
    return Path(path).exists()
