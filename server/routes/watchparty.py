from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, HTTPException, Request, WebSocket, status
from pydantic import BaseModel
from starlette.websockets import WebSocketDisconnect

from server.watchparty.room import PlaybackState, RoomManager
from server.watchparty.sync import ConnectionManager


router = APIRouter(tags=["watchparty"])


class CreateRoomRequest(BaseModel):
    host_id: str
    file_id: str


def get_room_manager_from_app(request: Request) -> RoomManager:
    room_manager = getattr(request.app.state, "room_manager", None)
    if room_manager is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Watch party room manager is not available",
        )
    return room_manager


def get_ws_managers(websocket: WebSocket) -> tuple[RoomManager, ConnectionManager]:
    room_manager = getattr(websocket.app.state, "room_manager", None)
    connection_manager = getattr(websocket.app.state, "watchparty_connection_manager", None)
    if room_manager is None or connection_manager is None:
        raise RuntimeError("Watch party services are not available")
    return room_manager, connection_manager


@router.post("/watchparty/create")
async def create_watchparty_room(
    payload: CreateRoomRequest,
    request: Request,
) -> dict[str, object]:
    room_manager = get_room_manager_from_app(request)
    room_id = room_manager.create_room(payload.host_id, payload.file_id)
    room = room_manager.get_room(room_id)
    if room is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create room")

    return {
        "room_id": room.room_id,
        "file_id": room.file_id,
        "created_at": room.created_at.isoformat(),
    }


@router.get("/watchparty/{room_id}")
async def get_watchparty_room(room_id: str, request: Request) -> dict[str, object]:
    room_manager = get_room_manager_from_app(request)
    room = room_manager.get_room(room_id)
    if room is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Room not found")

    state = room_manager.get_state(room_id)
    return {
        "room_id": room.room_id,
        "file_id": room.file_id,
        "member_count": room_manager.get_member_count(room_id),
        "state": {
            "is_playing": state.is_playing,
            "timestamp": room_manager.get_effective_timestamp(room_id),
        },
    }


@router.websocket("/watchparty/ws/{room_id}/{peer_id}")
async def watchparty_websocket(websocket: WebSocket, room_id: str, peer_id: str) -> None:
    room_manager, connection_manager = get_ws_managers(websocket)
    room = room_manager.get_room(room_id)
    if room is None:
        await websocket.close(code=4404)
        return

    await connection_manager.connect(websocket, room_id, peer_id)
    try:
        existing_members = room_manager.get_members(room_id)
        room_manager.add_member(room_id, peer_id)
        current_state = room_manager.get_state(room_id)
        await connection_manager.send_to_peer(
            peer_id,
            {
                "type": "STATE",
                "is_playing": current_state.is_playing,
                "timestamp_seconds": room_manager.get_effective_timestamp(room_id),
                "updated_by": current_state.updated_by,
            },
        )
        for existing_member in existing_members:
            await connection_manager.send_to_peer(
                peer_id,
                {
                    "type": "MEMBER_JOINED",
                    "peer_id": existing_member,
                    "member_count": room_manager.get_member_count(room_id),
                },
            )
        await connection_manager.broadcast_to_room(
            room_id,
            {
                "type": "MEMBER_JOINED",
                "peer_id": peer_id,
                "member_count": room_manager.get_member_count(room_id),
            },
        )

        while True:
            payload = await websocket.receive_json()
            if not isinstance(payload, dict):
                await connection_manager.send_to_peer(
                    peer_id,
                    {"type": "ERROR", "message": "Invalid message format"},
                )
                continue

            message_type = str(payload.get("type", "")).upper()
            if message_type == "JOIN":
                room_manager.add_member(room_id, peer_id)
                state = room_manager.get_state(room_id)
                await connection_manager.send_to_peer(
                    peer_id,
                    {
                        "type": "STATE",
                        "is_playing": state.is_playing,
                        "timestamp_seconds": room_manager.get_effective_timestamp(room_id),
                        "updated_by": state.updated_by,
                    },
                )
                continue

            if message_type in {"PLAY", "PAUSE", "SEEK"}:
                timestamp_seconds = float(payload.get("timestamp_seconds", 0.0))
                current_state = room_manager.get_state(room_id)
                next_state = PlaybackState(
                    is_playing=True if message_type == "PLAY" else False if message_type == "PAUSE" else current_state.is_playing,
                    timestamp_seconds=timestamp_seconds,
                    last_updated=datetime.now(timezone.utc),
                    updated_by=peer_id,
                )
                room_manager.update_state(room_id, next_state, peer_id)
                state_message = {
                    "type": "STATE",
                    "is_playing": next_state.is_playing,
                    "timestamp_seconds": next_state.timestamp_seconds,
                    "updated_by": peer_id,
                }
                await connection_manager.send_to_peer(peer_id, state_message)
                await connection_manager.broadcast_to_room(room_id, state_message)
                continue

            if message_type == "PING":
                await connection_manager.send_to_peer(
                    peer_id,
                    {
                        "type": "PONG",
                        "server_time": int(time.time() * 1000),
                        "client_time": int(payload.get("client_time", 0)),
                    },
                )
                continue

            await connection_manager.send_to_peer(
                peer_id,
                {"type": "ERROR", "message": f"Unsupported message type: {message_type}"},
            )
    except WebSocketDisconnect:
        pass
    finally:
        await connection_manager.disconnect(websocket, room_id, peer_id)
        room_manager.remove_member(room_id, peer_id)
        member_count = room_manager.get_member_count(room_id)
        if member_count == 0:
            room_manager.delete_room(room_id)
        else:
            await connection_manager.broadcast_to_room(
                room_id,
                {
                    "type": "MEMBER_LEFT",
                    "peer_id": peer_id,
                    "member_count": member_count,
                },
            )
