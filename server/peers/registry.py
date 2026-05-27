from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


@dataclass
class PeerInfo:
    peer_id: str
    ip: str
    port: int
    available_chunks: list[str]
    bandwidth_mbps: float
    last_seen: datetime
    is_active: bool = True


class PeerRegistry:
    def __init__(self, cleanup_interval_seconds: int = 10, stale_timeout_seconds: int = 30) -> None:
        self._cleanup_interval_seconds = cleanup_interval_seconds
        self._stale_timeout_seconds = stale_timeout_seconds
        self._peers: dict[str, PeerInfo] = {}
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_loop,
            name="peer-registry-cleanup",
            daemon=True,
        )
        self._cleanup_thread.start()

    def register_peer(self, peer_info: PeerInfo) -> None:
        now = datetime.now(timezone.utc)
        with self._lock:
            existing = self._peers.get(peer_info.peer_id)
            if existing is None:
                peer_info.last_seen = now
                peer_info.is_active = True
                self._peers[peer_info.peer_id] = peer_info
                return

            existing.ip = peer_info.ip
            existing.port = peer_info.port
            existing.available_chunks = list(peer_info.available_chunks)
            existing.bandwidth_mbps = peer_info.bandwidth_mbps
            existing.last_seen = now
            existing.is_active = True

    def update_peer(self, peer_id: str, chunks: list[str], bandwidth: float) -> None:
        with self._lock:
            peer = self._peers.get(peer_id)
            if peer is None:
                return

            peer.available_chunks = list(chunks)
            peer.bandwidth_mbps = bandwidth
            peer.last_seen = datetime.now(timezone.utc)
            peer.is_active = True

    def get_active_peers(self) -> list[PeerInfo]:
        self.cleanup_stale(timeout_seconds=self._stale_timeout_seconds)
        with self._lock:
            return [self._clone_peer(peer) for peer in self._peers.values() if peer.is_active]

    def get_all_peers(self) -> list[PeerInfo]:
        with self._lock:
            return [self._clone_peer(peer) for peer in self._peers.values()]

    def get_peers_with_chunk(self, chunk_hash: str) -> list[PeerInfo]:
        self.cleanup_stale(timeout_seconds=self._stale_timeout_seconds)
        with self._lock:
            return [
                self._clone_peer(peer)
                for peer in self._peers.values()
                if peer.is_active and chunk_hash in peer.available_chunks
            ]

    def get_peer(self, peer_id: str) -> PeerInfo | None:
        self.cleanup_stale(timeout_seconds=self._stale_timeout_seconds)
        with self._lock:
            peer = self._peers.get(peer_id)
            if peer is None:
                return None
            return self._clone_peer(peer)

    def mark_inactive(self, peer_id: str) -> None:
        with self._lock:
            peer = self._peers.get(peer_id)
            if peer is not None:
                peer.is_active = False

    def cleanup_stale(self, timeout_seconds: int = 30) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=timeout_seconds)
        with self._lock:
            for peer in self._peers.values():
                if peer.last_seen < cutoff:
                    peer.is_active = False

    def stop(self) -> None:
        self._stop_event.set()
        self._cleanup_thread.join(timeout=1.0)

    def _cleanup_loop(self) -> None:
        while not self._stop_event.wait(self._cleanup_interval_seconds):
            self.cleanup_stale(timeout_seconds=self._stale_timeout_seconds)

    @staticmethod
    def _clone_peer(peer: PeerInfo) -> PeerInfo:
        return PeerInfo(
            peer_id=peer.peer_id,
            ip=peer.ip,
            port=peer.port,
            available_chunks=list(peer.available_chunks),
            bandwidth_mbps=peer.bandwidth_mbps,
            last_seen=peer.last_seen,
            is_active=peer.is_active,
        )
