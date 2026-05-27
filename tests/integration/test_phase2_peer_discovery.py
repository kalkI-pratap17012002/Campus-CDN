import json
import socket
import time
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from server.peers.discovery import PeerDiscovery
from server.peers.registry import PeerInfo, PeerRegistry


pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("cleanup_chunks")]


@pytest.mark.asyncio
async def test_peer_registration(async_client):
    payload = {
        "ip": "127.0.0.1",
        "port": 9001,
        "chunks": ["hash-a", "hash-b"],
        "bandwidth_mbps": 42.5,
    }
    response = await async_client.post("/peers/announce", json=payload)
    peers = (await async_client.get("/peers")).json()

    assert response.status_code == 200
    assert any(peer["ip"] == payload["ip"] and peer["port"] == payload["port"] for peer in peers)


@pytest.mark.asyncio
async def test_peer_appears_in_chunk_lookup(async_client):
    payload = {
        "ip": "127.0.0.1",
        "port": 9002,
        "chunks": ["chunk-lookup-hash"],
        "bandwidth_mbps": 55.0,
    }
    await async_client.post("/peers/announce", json=payload)
    response = await async_client.get("/peers/chunk/chunk-lookup-hash")

    assert response.status_code == 200
    assert any(peer["ip"] == payload["ip"] for peer in response.json())


@pytest.mark.asyncio
async def test_stale_peer_cleanup(async_client, app_instance):
    peer_id = str(uuid.uuid4())
    registry = app_instance.state.peer_registry
    registry.register_peer(
        PeerInfo(
            peer_id=peer_id,
            ip="127.0.0.1",
            port=9003,
            available_chunks=["old-hash"],
            bandwidth_mbps=10.0,
            last_seen=datetime.now(UTC),
            is_active=True,
        )
    )
    registry._peers[peer_id].last_seen = datetime.now(UTC) - timedelta(seconds=60)
    registry.cleanup_stale(timeout_seconds=30)

    response = await async_client.get("/peers")
    assert all(peer["peer_id"] != peer_id for peer in response.json())


@pytest.mark.asyncio
async def test_peer_info_endpoint(async_client):
    payload = {
        "ip": "127.0.0.1",
        "port": 9004,
        "chunks": ["c1", "c2", "c3"],
        "bandwidth_mbps": 88.0,
    }
    created = (await async_client.post("/peers/announce", json=payload)).json()
    response = await async_client.get(f"/peers/{created['peer_id']}")
    peer = response.json()

    assert response.status_code == 200
    assert peer["ip"] == payload["ip"]
    assert peer["port"] == payload["port"]
    assert peer["bandwidth_mbps"] == payload["bandwidth_mbps"]
    assert peer["available_chunks"] == payload["chunks"]


@pytest.mark.asyncio
async def test_inactive_peer_excluded(async_client, app_instance):
    payload = {
        "ip": "127.0.0.1",
        "port": 9005,
        "chunks": ["inactive-chunk"],
        "bandwidth_mbps": 12.0,
    }
    created = (await async_client.post("/peers/announce", json=payload)).json()
    app_instance.state.peer_registry.mark_inactive(created["peer_id"])

    peers = (await async_client.get("/peers")).json()
    assert all(peer["peer_id"] != created["peer_id"] for peer in peers)


def test_bandwidth_measurement():
    registry = PeerRegistry()
    discovery = PeerDiscovery(registry, app_port=8123)

    started = time.perf_counter()
    bandwidth = discovery.get_local_bandwidth()
    elapsed = time.perf_counter() - started

    assert isinstance(bandwidth, float)
    assert bandwidth > 0.0
    assert elapsed < 5.0
    registry.stop()


def test_udp_announce_received():
    registry = PeerRegistry()
    discovery = PeerDiscovery(registry, app_port=8124)
    discovery.start()

    announced_peer_id = str(uuid.uuid4())
    payload = {
        "type": "ANNOUNCE",
        "peer_id": announced_peer_id,
        "ip": "127.0.0.1",
        "port": 8125,
        "chunks": ["udp-hash"],
        "bandwidth_mbps": 75.5,
        "timestamp": datetime.now(UTC).isoformat(),
    }

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.sendto(json.dumps(payload).encode("utf-8"), ("127.0.0.1", 5005))

    deadline = time.time() + 2.0
    try:
        while time.time() < deadline:
            if any(peer.peer_id == announced_peer_id for peer in registry.get_active_peers()):
                break
            time.sleep(0.05)
        assert any(peer.peer_id == announced_peer_id for peer in registry.get_active_peers())
    finally:
        discovery.stop()
        registry.stop()


def test_own_broadcast_ignored():
    registry = PeerRegistry()
    discovery = PeerDiscovery(registry, app_port=8126)
    discovery.start()

    payload = {
        "type": "ANNOUNCE",
        "peer_id": discovery.peer_id,
        "ip": discovery.ip,
        "port": 8126,
        "chunks": ["self-hash"],
        "bandwidth_mbps": 90.0,
        "timestamp": datetime.now(UTC).isoformat(),
    }

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.sendto(json.dumps(payload).encode("utf-8"), ("127.0.0.1", 5005))

    time.sleep(0.5)
    try:
        assert all(peer.peer_id != discovery.peer_id for peer in registry.get_active_peers())
    finally:
        discovery.stop()
        registry.stop()
