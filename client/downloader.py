import hashlib
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import httpx
from server.scheduler.chunk_scheduler import ChunkScheduler, ChunkTask
from tqdm import tqdm


@dataclass(frozen=True)
class PeerInfo:
    peer_id: str
    ip: str
    port: int
    available_chunks: list[str]
    bandwidth_mbps: float
    last_seen: str
    is_active: bool


@dataclass(frozen=True)
class ClientConnectionPool:
    max_connections: int = 5

    def get_active_count(self) -> int:
        return 0


def fetch_manifest(server_url: str, file_id: str) -> dict:
    response = httpx.get(f"{server_url.rstrip('/')}/manifest/{file_id}", timeout=30.0)
    response.raise_for_status()
    return response.json()


def download_chunk(server_url: str, file_id: str, chunk_index: int) -> bytes:
    chunk_data, _ = download_chunk_with_source(server_url, file_id, chunk_index)
    return chunk_data


def download_chunk_with_source(server_url: str, file_id: str, chunk_index: int) -> tuple[bytes, str]:
    response = httpx.get(f"{server_url.rstrip('/')}/chunk/{file_id}/{chunk_index}", timeout=60.0)
    response.raise_for_status()
    cache_hit = response.headers.get("X-Cache-Hit", "false").lower() == "true"
    return response.content, ("cache" if cache_hit else "origin")


def verify_chunk_data(data: bytes, expected_hash: str) -> bool:
    return hashlib.sha256(data).hexdigest() == expected_hash


def discover_peers(server_url: str) -> list[PeerInfo]:
    response = httpx.get(f"{server_url.rstrip('/')}/peers", timeout=15.0)
    response.raise_for_status()
    peer_summaries = response.json()

    peers: list[PeerInfo] = []
    for peer_summary in peer_summaries:
        detail_response = httpx.get(
            f"{server_url.rstrip('/')}/peers/{peer_summary['peer_id']}",
            timeout=15.0,
        )
        detail_response.raise_for_status()
        detail = detail_response.json()
        peers.append(
            PeerInfo(
                peer_id=detail["peer_id"],
                ip=detail["ip"],
                port=detail["port"],
                available_chunks=list(detail.get("available_chunks", [])),
                bandwidth_mbps=float(detail.get("bandwidth_mbps", 0.0)),
                last_seen=detail.get("last_seen", ""),
                is_active=bool(detail.get("is_active", True)),
            )
        )

    peers.sort(key=lambda peer: peer.bandwidth_mbps, reverse=True)
    return peers


def try_peer_download(peer: PeerInfo, file_id: str, chunk_index: int) -> bytes | None:
    try:
        response = httpx.get(
            f"http://{peer.ip}:{peer.port}/chunk/{file_id}/{chunk_index}",
            headers={"X-CDN-Transfer-Mode": "peer"},
            timeout=20.0,
        )
        response.raise_for_status()
        return response.content
    except Exception:
        return None


def download_file(server_url: str, file_id: str, output_path: str) -> Path:
    manifest = fetch_manifest(server_url, file_id)
    chunks = manifest["chunks"]
    try:
        peers = discover_peers(server_url)
    except Exception:
        peers = []
    scheduler = ChunkScheduler(manifest, peers, ClientConnectionPool(max_connections=5))
    schedule = scheduler.schedule(file_id)
    schedule_stats = scheduler.get_schedule_stats()
    print(f"Schedule stats: {json.dumps(schedule_stats)}")
    peer_lookup = {peer.peer_id: peer for peer in peers}
    chunk_lookup = {chunk["index"]: chunk for chunk in chunks}

    destination = Path(output_path)
    if destination.exists() and destination.is_dir():
        destination = destination / manifest["filename"]
    elif output_path.endswith(("/", "\\")):
        destination.mkdir(parents=True, exist_ok=True)
        destination = destination / manifest["filename"]
    elif destination.suffix == "":
        destination.mkdir(parents=True, exist_ok=True)
        destination = destination / manifest["filename"]
    else:
        destination.parent.mkdir(parents=True, exist_ok=True)

    downloaded_chunks: dict[int, bytes] = {}

    def fetch_with_retry(task: ChunkTask) -> tuple[int, bytes, str]:
        chunk_index = task.chunk_index
        chunk_info = chunk_lookup[chunk_index]
        expected_hash = chunk_info["hash"]
        scheduled_peer = peer_lookup.get(task.source_peer_id) if task.source_peer_id != "origin" else None
        last_error: Exception | None = None

        for attempt in range(1, 4):
            try:
                if scheduled_peer is not None:
                    chunk_data = try_peer_download(scheduled_peer, file_id, chunk_index)
                    if chunk_data is not None:
                        if not verify_chunk_data(chunk_data, expected_hash):
                            raise ValueError(f"Peer hash mismatch for chunk {chunk_index} on attempt {attempt}")
                        return (
                            chunk_index,
                            chunk_data,
                            f"peer:{scheduled_peer.ip}:{scheduled_peer.port}",
                        )

                chunk_data, source = download_chunk_with_source(server_url, file_id, chunk_index)
                if not verify_chunk_data(chunk_data, expected_hash):
                    raise ValueError(f"Server hash mismatch for chunk {chunk_index} on attempt {attempt}")
                return chunk_index, chunk_data, source
            except Exception as exc:
                last_error = exc

        raise RuntimeError(f"Failed to download chunk {chunk_index} after 3 attempts") from last_error

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_with_retry, task): task for task in schedule}
        with tqdm(total=len(chunks), desc="Downloading chunks", unit="chunk") as progress:
            for future in as_completed(futures):
                chunk_index, chunk_data, source = future.result()
                downloaded_chunks[chunk_index] = chunk_data
                progress.set_postfix_str(f"chunk={chunk_index} source={source}")
                print(f"Completed chunk {chunk_index} from {source}")
                progress.update(1)

    with destination.open("wb") as output_handle:
        for chunk_info in sorted(chunks, key=lambda item: item["index"]):
            output_handle.write(downloaded_chunks[chunk_info["index"]])

    actual_size = destination.stat().st_size
    expected_size = manifest["total_size"]
    integrity_ok = actual_size == expected_size and len(downloaded_chunks) == manifest["total_chunks"]
    print(
        f"Final verification: {'passed' if integrity_ok else 'failed'} "
        f"(size={actual_size}, expected={expected_size})"
    )

    if not integrity_ok:
        raise RuntimeError("Downloaded file failed final verification")

    return destination
