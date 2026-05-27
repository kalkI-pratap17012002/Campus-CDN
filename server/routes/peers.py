from __future__ import annotations

import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field

from server.peers.registry import PeerInfo, PeerRegistry


router = APIRouter(tags=["peers"])


def get_peer_registry(request: Request) -> PeerRegistry:
    registry = getattr(request.app.state, "peer_registry", None)
    if registry is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Peer registry is not available",
        )
    return registry


class ManualAnnouncement(BaseModel):
    ip: str
    port: int
    chunks: list[str] = Field(default_factory=list)
    bandwidth_mbps: float


@router.get("/peers")
async def list_peers(registry: PeerRegistry = Depends(get_peer_registry)) -> list[dict[str, object]]:
    peers = registry.get_active_peers()
    return [
        {
            "peer_id": peer.peer_id,
            "ip": peer.ip,
            "port": peer.port,
            "chunk_count": len(peer.available_chunks),
            "bandwidth_mbps": peer.bandwidth_mbps,
            "last_seen": peer.last_seen.isoformat(),
        }
        for peer in peers
    ]


@router.get("/peers/chunk/{chunk_hash}")
async def peers_with_chunk(
    chunk_hash: str,
    registry: PeerRegistry = Depends(get_peer_registry),
) -> list[dict[str, object]]:
    peers = registry.get_peers_with_chunk(chunk_hash)
    return [
        {
            "peer_id": peer.peer_id,
            "ip": peer.ip,
            "port": peer.port,
            "chunk_count": len(peer.available_chunks),
            "bandwidth_mbps": peer.bandwidth_mbps,
            "last_seen": peer.last_seen.isoformat(),
        }
        for peer in peers
    ]


@router.get("/peers/{peer_id}")
async def get_peer(peer_id: uuid.UUID, registry: PeerRegistry = Depends(get_peer_registry)) -> dict[str, object]:
    peer = registry.get_peer(str(peer_id))
    if peer is None or not peer.is_active:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Peer not found")

    return {
        "peer_id": peer.peer_id,
        "ip": peer.ip,
        "port": peer.port,
        "available_chunks": peer.available_chunks,
        "bandwidth_mbps": peer.bandwidth_mbps,
        "last_seen": peer.last_seen.isoformat(),
        "is_active": peer.is_active,
    }


@router.post("/peers/announce")
async def announce_peer(
    announcement: ManualAnnouncement,
    registry: PeerRegistry = Depends(get_peer_registry),
) -> dict[str, object]:
    peer_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{announcement.ip}:{announcement.port}"))
    peer = PeerInfo(
        peer_id=peer_id,
        ip=announcement.ip,
        port=announcement.port,
        available_chunks=list(announcement.chunks),
        bandwidth_mbps=announcement.bandwidth_mbps,
        last_seen=datetime.now(timezone.utc),
        is_active=True,
    )
    registry.register_peer(peer)
    return {
        "peer_id": peer.peer_id,
        "ip": peer.ip,
        "port": peer.port,
        "chunk_count": len(peer.available_chunks),
        "bandwidth_mbps": peer.bandwidth_mbps,
        "last_seen": peer.last_seen.isoformat(),
        "is_active": peer.is_active,
    }
