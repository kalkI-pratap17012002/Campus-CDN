from __future__ import annotations

import uuid
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from server.analytics.collector import AnalyticsCollector
from server.cache.edge_cache import edge_cache
from server.database.connection import get_db
from server.database.models import FileRecord
from server.peers.registry import PeerRegistry
from server.watchparty.room import RoomManager


router = APIRouter(tags=["analytics"])


def get_analytics_collector(request: Request) -> AnalyticsCollector:
    collector = getattr(request.app.state, "analytics_collector", None)
    if collector is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Analytics collector is not available",
        )
    return collector


def get_peer_registry(request: Request) -> PeerRegistry:
    registry = getattr(request.app.state, "peer_registry", None)
    if registry is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Peer registry is not available",
        )
    return registry


def get_room_manager(request: Request) -> RoomManager:
    room_manager = getattr(request.app.state, "room_manager", None)
    if room_manager is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Room manager is not available",
        )
    return room_manager


@router.get("/analytics/summary")
async def analytics_summary(
    request: Request,
    collector: AnalyticsCollector = Depends(get_analytics_collector),
    registry: PeerRegistry = Depends(get_peer_registry),
    room_manager: RoomManager = Depends(get_room_manager),
    db: AsyncSession = Depends(get_db),
) -> dict[str, object]:
    summary = collector.get_summary()
    top_files_raw = collector.get_top_files(n=5)
    top_files: list[dict[str, object]] = []

    for item in top_files_raw:
        filename = str(item["file_id"])
        try:
            file_record = await db.get(FileRecord, uuid.UUID(str(item["file_id"])))
        except ValueError:
            file_record = None
        if file_record is not None:
            filename = file_record.filename
        top_files.append(
            {
                "file_id": item["file_id"],
                "filename": filename,
                "downloads": int(item["downloads"]),
                "bytes_served": int(item["bytes_served"]),
            }
        )

    return {
        "total_uploads": int(summary["total_uploads"]),
        "total_downloads": int(summary["total_downloads"]),
        "total_bytes_transferred": int(summary["total_bytes_transferred"]),
        "cache_hit_ratio": float(summary["cache_hit_ratio"]),
        "peer_transfer_ratio": float(summary["peer_transfer_ratio"]),
        "active_peers": len(registry.get_active_peers()),
        "active_watch_parties": room_manager.get_active_room_count(),
        "top_files": top_files,
        "cache_transfers": int(summary["cache_transfers"]),
        "peer_transfers": int(summary["peer_transfers"]),
        "origin_transfers": int(summary["origin_transfers"]),
    }


@router.get("/analytics/bandwidth")
async def analytics_bandwidth(
    hours: int = Query(default=24, ge=1, le=168),
    collector: AnalyticsCollector = Depends(get_analytics_collector),
) -> list[dict[str, object]]:
    return collector.get_bandwidth_history(hours=hours)


@router.get("/analytics/peers")
async def analytics_peers(
    collector: AnalyticsCollector = Depends(get_analytics_collector),
    registry: PeerRegistry = Depends(get_peer_registry),
) -> list[dict[str, object]]:
    contributions = collector.get_peer_contributions()
    peers_by_id = {peer.peer_id: peer for peer in registry.get_all_peers()}
    contribution_by_peer = {str(item["peer_id"]): item for item in contributions}
    peer_rows: list[dict[str, object]] = []

    for peer_id in sorted(set(peers_by_id) | set(contribution_by_peer)):
        item = contribution_by_peer.get(
            peer_id,
            {
                "peer_id": peer_id,
                "ip": "",
                "bytes_contributed": 0,
                "chunks_served": 0,
            },
        )
        peer = peers_by_id.get(peer_id)
        if peer is not None:
            age_seconds = max((datetime.now(UTC) - peer.last_seen).total_seconds(), 0.0)
            reliability_score = max(0.0, 1.0 - min(age_seconds, 30.0) / 30.0) if peer.is_active else 0.0
            ip = peer.ip
        else:
            reliability_score = 0.0
            ip = str(item.get("ip", "")) or "unknown"
        peer_rows.append(
            {
                "peer_id": item["peer_id"],
                "ip": ip,
                "bytes_contributed": int(item["bytes_contributed"]),
                "chunks_served": int(item["chunks_served"]),
                "reliability_score": round(reliability_score, 2),
            }
        )

    return peer_rows


@router.get("/analytics/cache")
async def analytics_cache(
    collector: AnalyticsCollector = Depends(get_analytics_collector),
) -> dict[str, object]:
    cache_stats = edge_cache.get_stats()
    return {
        "hit_ratio": float(cache_stats["hit_ratio"]),
        "total_cached_chunks": int(cache_stats["cached_chunks"]),
        "cache_size_mb": float(cache_stats["total_size_mb"]),
        "evictions_today": edge_cache.get_evictions_today(),
    }
