from __future__ import annotations

import json
import logging
import os
import socket
import tempfile
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from server.chunks.integrity import compute_sha256
from server.config import settings
from server.peers.registry import PeerInfo, PeerRegistry


logger = logging.getLogger(__name__)


class PeerDiscovery:
    BROADCAST_PORT = 5005
    ANNOUNCE_INTERVAL_SECONDS = 10

    def __init__(self, registry: PeerRegistry, app_port: int) -> None:
        self.registry = registry
        self.app_port = app_port
        self.peer_id = str(uuid.uuid4())
        self.ip = self._get_local_ip()
        self._bandwidth_mbps = self.get_local_bandwidth()
        self._stop_event = threading.Event()
        self._announce_thread: threading.Thread | None = None
        self._listener_thread: threading.Thread | None = None

    def start(self) -> None:
        if self._announce_thread and self._announce_thread.is_alive():
            return

        self._stop_event.clear()
        self._announce_thread = threading.Thread(
            target=self._announce_loop,
            name="peer-discovery-announce",
            daemon=True,
        )
        self._listener_thread = threading.Thread(
            target=self._listen_loop,
            name="peer-discovery-listener",
            daemon=True,
        )
        self._announce_thread.start()
        self._listener_thread.start()
        logger.info("Peer discovery started peer_id=%s ip=%s port=%s", self.peer_id, self.ip, self.app_port)

    def stop(self) -> None:
        self._stop_event.set()
        if self._announce_thread is not None:
            self._announce_thread.join(timeout=2.0)
        if self._listener_thread is not None:
            self._listener_thread.join(timeout=2.0)
        logger.info("Peer discovery stopped peer_id=%s", self.peer_id)

    def get_local_bandwidth(self) -> float:
        data = os.urandom(10 * 1024 * 1024)
        start = time.perf_counter()
        with tempfile.NamedTemporaryFile(delete=True) as temp_file:
            temp_file.write(data)
            temp_file.flush()
            os.fsync(temp_file.fileno())
        duration = max(time.perf_counter() - start, 0.001)
        return round((len(data) * 8) / duration / 1_000_000, 2)

    def _announce_loop(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            while not self._stop_event.is_set():
                payload = self._build_announce_payload()
                message = json.dumps(payload).encode("utf-8")
                try:
                    sock.sendto(message, ("255.255.255.255", self.BROADCAST_PORT))
                    sock.sendto(message, ("127.0.0.1", self.BROADCAST_PORT))
                    logger.debug("Broadcasted ANNOUNCE for peer_id=%s", self.peer_id)
                except OSError as exc:
                    logger.warning("Failed to send peer announcement: %s", exc)

                if self._stop_event.wait(self.ANNOUNCE_INTERVAL_SECONDS):
                    break

    def _listen_loop(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except (AttributeError, OSError):
                pass
            sock.bind(("", self.BROADCAST_PORT))
            sock.settimeout(1.0)

            while not self._stop_event.is_set():
                try:
                    payload_bytes, _ = sock.recvfrom(65535)
                except socket.timeout:
                    continue
                except OSError as exc:
                    if not self._stop_event.is_set():
                        logger.warning("Peer discovery listener stopped unexpectedly: %s", exc)
                    break

                try:
                    payload = json.loads(payload_bytes.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    continue

                if payload.get("type") != "ANNOUNCE":
                    continue
                if payload.get("peer_id") == self.peer_id:
                    continue

                try:
                    peer = PeerInfo(
                        peer_id=str(payload["peer_id"]),
                        ip=str(payload["ip"]),
                        port=int(payload["port"]),
                        available_chunks=list(payload.get("chunks", [])),
                        bandwidth_mbps=float(payload.get("bandwidth_mbps", 0.0)),
                        last_seen=self._parse_timestamp(payload.get("timestamp")),
                        is_active=True,
                    )
                except (KeyError, TypeError, ValueError):
                    continue

                self.registry.register_peer(peer)
                logger.debug("Registered discovered peer peer_id=%s", peer.peer_id)

    def _build_announce_payload(self) -> dict[str, object]:
        return {
            "type": "ANNOUNCE",
            "peer_id": self.peer_id,
            "ip": self.ip,
            "port": self.app_port,
            "chunks": self._collect_local_chunk_hashes(),
            "bandwidth_mbps": self._bandwidth_mbps,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def _collect_local_chunk_hashes(self) -> list[str]:
        chunk_hashes: list[str] = []
        root = settings.chunk_storage_dir
        if not root.exists():
            return chunk_hashes

        for chunk_path in sorted(root.rglob("*.bin")):
            if chunk_path.is_file():
                chunk_hashes.append(compute_sha256(str(chunk_path)))
        return chunk_hashes

    @staticmethod
    def _get_local_ip() -> str:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            try:
                sock.connect(("8.8.8.8", 80))
                return str(sock.getsockname()[0])
            except OSError:
                return "127.0.0.1"

    @staticmethod
    def _parse_timestamp(timestamp: object) -> datetime:
        if isinstance(timestamp, str):
            try:
                parsed = datetime.fromisoformat(timestamp)
                if parsed.tzinfo is None:
                    return parsed.replace(tzinfo=timezone.utc)
                return parsed
            except ValueError:
                return datetime.now(timezone.utc)
        return datetime.now(timezone.utc)
