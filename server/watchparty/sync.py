from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from typing import Any

from fastapi import WebSocket
from starlette.websockets import WebSocketDisconnect, WebSocketState

from server.watchparty.room import RoomManager


logger = logging.getLogger(__name__)


class ConnectionManager:
    def __init__(self, room_manager: RoomManager) -> None:
        self.room_manager = room_manager
        self._room_connections: dict[str, dict[str, WebSocket]] = defaultdict(dict)
        self._peer_rooms: dict[str, str] = {}
        self._room_tasks: dict[str, asyncio.Task[None]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket, room_id: str, peer_id: str) -> None:
        await ws.accept()
        async with self._lock:
            self._room_connections[room_id][peer_id] = ws
            self._peer_rooms[peer_id] = room_id
            if room_id not in self._room_tasks:
                self._room_tasks[room_id] = asyncio.create_task(self._state_sync_loop(room_id))

    async def disconnect(self, ws: WebSocket, room_id: str, peer_id: str) -> None:
        async with self._lock:
            room_connections = self._room_connections.get(room_id, {})
            current = room_connections.get(peer_id)
            if current is ws:
                room_connections.pop(peer_id, None)
            self._peer_rooms.pop(peer_id, None)
            if not room_connections:
                self._room_connections.pop(room_id, None)
                task = self._room_tasks.pop(room_id, None)
                if task is not None:
                    task.cancel()

    async def broadcast_to_room(self, room_id: str, message: dict[str, Any]) -> None:
        sender_id = self._get_sender_id(message)
        async with self._lock:
            room_connections = list(self._room_connections.get(room_id, {}).items())

        for peer_id, ws in room_connections:
            if sender_id is not None and peer_id == sender_id:
                continue
            await self._safe_send(ws, message, room_id=room_id, peer_id=peer_id)

    async def broadcast_all_to_room(self, room_id: str, message: dict[str, Any]) -> None:
        async with self._lock:
            room_connections = list(self._room_connections.get(room_id, {}).items())

        for peer_id, ws in room_connections:
            await self._safe_send(ws, message, room_id=room_id, peer_id=peer_id)

    async def send_to_peer(self, peer_id: str, message: dict[str, Any]) -> None:
        async with self._lock:
            room_id = self._peer_rooms.get(peer_id)
            if room_id is None:
                return
            ws = self._room_connections.get(room_id, {}).get(peer_id)

        if ws is not None:
            await self._safe_send(ws, message, room_id=room_id, peer_id=peer_id)

    async def stop(self) -> None:
        async with self._lock:
            tasks = list(self._room_tasks.values())
            self._room_tasks.clear()
            room_connections = [
                (room_id, peer_id, ws)
                for room_id, peers in self._room_connections.items()
                for peer_id, ws in peers.items()
            ]
            self._room_connections.clear()
            self._peer_rooms.clear()

        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        for _, _, ws in room_connections:
            try:
                if ws.application_state == WebSocketState.CONNECTED:
                    await ws.close()
            except RuntimeError:
                continue

    async def _state_sync_loop(self, room_id: str) -> None:
        try:
            while True:
                await asyncio.sleep(5)
                room = self.room_manager.get_room(room_id)
                if room is None:
                    break

                state = self.room_manager.get_state(room_id)
                state_message = {
                    "type": "STATE",
                    "is_playing": state.is_playing,
                    "timestamp_seconds": self.room_manager.get_effective_timestamp(room_id),
                    "updated_by": state.updated_by,
                }
                await self.broadcast_all_to_room(room_id, state_message)
                await self._send_drift_corrections(room_id, room.host_id)
        except asyncio.CancelledError:
            logger.debug("Stopped watchparty sync loop for room_id=%s", room_id)
            raise

    async def _send_drift_corrections(self, room_id: str, host_id: str) -> None:
        room = self.room_manager.get_room(room_id)
        if room is None:
            return

        try:
            host_timestamp = self.room_manager.get_effective_timestamp(room_id, host_id)
        except KeyError:
            host_timestamp = self.room_manager.get_effective_timestamp(room_id)

        for member_id in self.room_manager.get_members(room_id):
            if member_id == host_id:
                continue
            try:
                member_timestamp = self.room_manager.get_effective_timestamp(room_id, member_id)
            except KeyError:
                continue
            if abs(member_timestamp - host_timestamp) > 2.0:
                await self.send_to_peer(
                    member_id,
                    {
                        "type": "SEEK",
                        "timestamp_seconds": host_timestamp,
                        "updated_by": host_id,
                    },
                )

    async def _safe_send(self, ws: WebSocket, message: dict[str, Any], room_id: str, peer_id: str) -> None:
        try:
            await ws.send_json(message)
        except (RuntimeError, WebSocketDisconnect):
            await self.disconnect(ws, room_id, peer_id)
            self.room_manager.remove_member(room_id, peer_id)
            if self.room_manager.get_member_count(room_id) == 0:
                self.room_manager.delete_room(room_id)

    @staticmethod
    def _get_sender_id(message: dict[str, Any]) -> str | None:
        sender = message.get("peer_id")
        if isinstance(sender, str):
            return sender
        updated_by = message.get("updated_by")
        if isinstance(updated_by, str):
            return updated_by
        return None
