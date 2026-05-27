import asyncio
import time

import pytest
import pytest_asyncio
from async_asgi_testclient import TestClient as ASGITestClient


pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("cleanup_chunks")]


@pytest_asyncio.fixture
async def ws_client(monkeypatch: pytest.MonkeyPatch):
    import server.main as main_module

    monkeypatch.setattr(main_module.PeerDiscovery, "start", lambda self: None)
    monkeypatch.setattr(main_module.PeerDiscovery, "stop", lambda self: None)
    monkeypatch.setattr(main_module.PeerDiscovery, "get_local_bandwidth", lambda self: 100.0)

    async with ASGITestClient(main_module.app) as client:
        yield client


async def _drain_messages(websocket, expected_count: int) -> list[dict]:
    return [await websocket.receive_json() for _ in range(expected_count)]


async def _receive_until_type(websocket, message_type: str, attempts: int = 5) -> dict:
    for _ in range(attempts):
        message = await websocket.receive_json()
        if message.get("type") == message_type:
            return message
    raise AssertionError(f"Did not receive message type {message_type}")


@pytest.mark.asyncio
async def test_create_room(ws_client):
    response = await ws_client.post(
        "/watchparty/create", json={"host_id": "host-1", "file_id": "file-123"}
    )
    payload = response.json()

    assert response.status_code == 200
    assert len(payload["room_id"]) == 6
    assert payload["room_id"].isalnum()
    assert payload["file_id"] == "file-123"


@pytest.mark.asyncio
async def test_get_room_info(ws_client):
    room = (await ws_client.post(
        "/watchparty/create", json={"host_id": "host-1", "file_id": "file-abc"}
    )).json()
    response = await ws_client.get(f"/watchparty/{room['room_id']}")
    payload = response.json()

    assert response.status_code == 200
    assert payload["member_count"] == 0
    assert "state" in payload


@pytest.mark.asyncio
async def test_websocket_join_broadcast(ws_client):
    room = (await ws_client.post(
        "/watchparty/create", json={"host_id": "host-1", "file_id": "file-x"}
    )).json()
    async with ws_client.websocket_connect(f"/watchparty/ws/{room['room_id']}/host-1") as host_ws:
        await host_ws.receive_json()
        async with ws_client.websocket_connect(f"/watchparty/ws/{room['room_id']}/peer-2") as peer_ws:
            await peer_ws.receive_json()
            joined = await _receive_until_type(peer_ws, "MEMBER_JOINED")
            assert joined["peer_id"] == "host-1"


@pytest.mark.asyncio
async def test_play_syncs_to_all(ws_client):
    room = (await ws_client.post(
        "/watchparty/create", json={"host_id": "host-1", "file_id": "file-play"}
    )).json()
    async with ws_client.websocket_connect(f"/watchparty/ws/{room['room_id']}/host-1") as host_ws, \
            ws_client.websocket_connect(f"/watchparty/ws/{room['room_id']}/peer-2") as peer2_ws, \
            ws_client.websocket_connect(f"/watchparty/ws/{room['room_id']}/peer-3") as peer3_ws:
        await _drain_messages(host_ws, 3)
        await _drain_messages(peer2_ws, 3)
        await _drain_messages(peer3_ws, 3)

        started = time.perf_counter()
        await host_ws.send_json({"type": "PLAY", "timestamp_seconds": 42.0, "peer_id": "host-1"})
        state2 = await _receive_until_type(peer2_ws, "STATE")
        state3 = await _receive_until_type(peer3_ws, "STATE")
        latency_ms = (time.perf_counter() - started) * 1000

        assert state2["is_playing"] is True
        assert state2["timestamp_seconds"] == 42.0
        assert state3["is_playing"] is True
        assert state3["timestamp_seconds"] == 42.0
        assert latency_ms < 500.0


@pytest.mark.asyncio
async def test_pause_syncs_to_all(ws_client):
    room = (await ws_client.post(
        "/watchparty/create", json={"host_id": "host-1", "file_id": "file-pause"}
    )).json()
    async with ws_client.websocket_connect(f"/watchparty/ws/{room['room_id']}/host-1") as host_ws, \
            ws_client.websocket_connect(f"/watchparty/ws/{room['room_id']}/peer-2") as peer2_ws, \
            ws_client.websocket_connect(f"/watchparty/ws/{room['room_id']}/peer-3") as peer3_ws:
        await _drain_messages(host_ws, 3)
        await _drain_messages(peer2_ws, 3)
        await _drain_messages(peer3_ws, 3)

        await host_ws.send_json({"type": "PAUSE", "timestamp_seconds": 30.0, "peer_id": "host-1"})
        state2 = await _receive_until_type(peer2_ws, "STATE")
        state3 = await _receive_until_type(peer3_ws, "STATE")

        assert state2["is_playing"] is False
        assert state2["timestamp_seconds"] == 30.0
        assert state3["is_playing"] is False
        assert state3["timestamp_seconds"] == 30.0


@pytest.mark.asyncio
async def test_seek_syncs_to_all(ws_client):
    room = (await ws_client.post(
        "/watchparty/create", json={"host_id": "host-1", "file_id": "file-seek"}
    )).json()
    async with ws_client.websocket_connect(f"/watchparty/ws/{room['room_id']}/host-1") as host_ws, \
            ws_client.websocket_connect(f"/watchparty/ws/{room['room_id']}/peer-2") as peer2_ws:
        await _drain_messages(host_ws, 2)
        await _drain_messages(peer2_ws, 2)

        await host_ws.send_json({"type": "SEEK", "timestamp_seconds": 120.0, "peer_id": "host-1"})
        state = await _receive_until_type(peer2_ws, "STATE")

        assert state["timestamp_seconds"] == 120.0


@pytest.mark.asyncio
async def test_ping_pong_latency(ws_client):
    room = (await ws_client.post(
        "/watchparty/create", json={"host_id": "host-1", "file_id": "file-ping"}
    )).json()
    async with ws_client.websocket_connect(f"/watchparty/ws/{room['room_id']}/host-1") as host_ws:
        await host_ws.receive_json()
        started = time.perf_counter()
        await host_ws.send_json({"type": "PING", "client_time": 123456789})
        pong = await _receive_until_type(host_ws, "PONG")
        latency_ms = (time.perf_counter() - started) * 1000

        assert pong["client_time"] == 123456789
        assert latency_ms < 500.0


@pytest.mark.asyncio
async def test_drift_correction(ws_client):
    import server.main as main_module

    room = (await ws_client.post(
        "/watchparty/create", json={"host_id": "host-1", "file_id": "file-drift"}
    )).json()
    room_manager = main_module.app.state.room_manager

    async with ws_client.websocket_connect(f"/watchparty/ws/{room['room_id']}/host-1") as host_ws, \
            ws_client.websocket_connect(f"/watchparty/ws/{room['room_id']}/peer-2") as peer_ws:
        await _drain_messages(host_ws, 2)
        await _drain_messages(peer_ws, 2)

        await host_ws.send_json({"type": "PLAY", "timestamp_seconds": 100.0, "peer_id": "host-1"})
        await _receive_until_type(host_ws, "STATE")
        await _receive_until_type(peer_ws, "STATE")

        member_state = room_manager.get_state(room["room_id"])
        room_manager.update_state(
            room["room_id"],
            type(member_state)(
                is_playing=True,
                timestamp_seconds=105.5,
                last_updated=member_state.last_updated,
                updated_by="peer-2",
            ),
            "peer-2",
        )

        await asyncio.sleep(5.5)
        seek_message = await _receive_until_type(peer_ws, "SEEK", attempts=3)
        assert seek_message["timestamp_seconds"] >= 100.0


@pytest.mark.asyncio
async def test_member_left_broadcast(ws_client):
    room = (await ws_client.post(
        "/watchparty/create", json={"host_id": "host-1", "file_id": "file-left"}
    )).json()
    async with ws_client.websocket_connect(f"/watchparty/ws/{room['room_id']}/host-1") as host_ws, \
            ws_client.websocket_connect(f"/watchparty/ws/{room['room_id']}/peer-2") as peer2_ws:
        await _drain_messages(host_ws, 2)
        await _drain_messages(peer2_ws, 2)
        async with ws_client.websocket_connect(f"/watchparty/ws/{room['room_id']}/peer-3") as peer3_ws:
            await _drain_messages(host_ws, 1)
            await _drain_messages(peer2_ws, 1)
            await _drain_messages(peer3_ws, 3)

        left_host = await _receive_until_type(host_ws, "MEMBER_LEFT")
        left_peer2 = await _receive_until_type(peer2_ws, "MEMBER_LEFT")
        assert left_host["peer_id"] == "peer-3"
        assert left_host["member_count"] == 2
        assert left_peer2["peer_id"] == "peer-3"


@pytest.mark.asyncio
async def test_periodic_state_sync(ws_client):
    room = (await ws_client.post(
        "/watchparty/create", json={"host_id": "host-1", "file_id": "file-periodic"}
    )).json()
    async with ws_client.websocket_connect(f"/watchparty/ws/{room['room_id']}/host-1") as host_ws:
        await host_ws.receive_json()
        await asyncio.sleep(5.5)
        state = await _receive_until_type(host_ws, "STATE", attempts=2)
        assert state["type"] == "STATE"
