from __future__ import annotations

import random
import string
import threading
from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass
class WatchPartyRoom:
    room_id: str
    host_id: str
    file_id: str
    created_at: datetime


@dataclass
class PlaybackState:
    is_playing: bool
    timestamp_seconds: float
    last_updated: datetime
    updated_by: str


class RoomManager:
    def __init__(self) -> None:
        self._rooms: dict[str, WatchPartyRoom] = {}
        self._states: dict[str, PlaybackState] = {}
        self._members: dict[str, set[str]] = {}
        self._member_states: dict[str, dict[str, PlaybackState]] = {}
        self._lock = threading.RLock()

    def create_room(self, host_id: str, file_id: str) -> str:
        with self._lock:
            room_id = self._generate_room_id()
            created_at = datetime.now(timezone.utc)
            room = WatchPartyRoom(
                room_id=room_id,
                host_id=host_id,
                file_id=file_id,
                created_at=created_at,
            )
            initial_state = PlaybackState(
                is_playing=False,
                timestamp_seconds=0.0,
                last_updated=created_at,
                updated_by=host_id,
            )
            self._rooms[room_id] = room
            self._states[room_id] = initial_state
            self._members[room_id] = set()
            self._member_states[room_id] = {}
            return room_id

    def get_room(self, room_id: str) -> WatchPartyRoom | None:
        with self._lock:
            room = self._rooms.get(room_id)
            if room is None:
                return None
            return WatchPartyRoom(
                room_id=room.room_id,
                host_id=room.host_id,
                file_id=room.file_id,
                created_at=room.created_at,
            )

    def get_state(self, room_id: str) -> PlaybackState:
        with self._lock:
            state = self._states.get(room_id)
            if state is None:
                raise KeyError(f"Room not found: {room_id}")
            return PlaybackState(
                is_playing=state.is_playing,
                timestamp_seconds=state.timestamp_seconds,
                last_updated=state.last_updated,
                updated_by=state.updated_by,
            )

    def update_state(self, room_id: str, state: PlaybackState, peer_id: str) -> None:
        with self._lock:
            if room_id not in self._rooms:
                raise KeyError(f"Room not found: {room_id}")
            normalized_state = PlaybackState(
                is_playing=state.is_playing,
                timestamp_seconds=float(state.timestamp_seconds),
                last_updated=state.last_updated,
                updated_by=peer_id,
            )
            self._states[room_id] = normalized_state
            self._members.setdefault(room_id, set()).add(peer_id)
            self._member_states.setdefault(room_id, {})[peer_id] = PlaybackState(
                is_playing=normalized_state.is_playing,
                timestamp_seconds=normalized_state.timestamp_seconds,
                last_updated=normalized_state.last_updated,
                updated_by=peer_id,
            )

    def get_members(self, room_id: str) -> list[str]:
        with self._lock:
            members = self._members.get(room_id)
            if members is None:
                return []
            return sorted(members)

    def add_member(self, room_id: str, peer_id: str) -> None:
        with self._lock:
            if room_id not in self._rooms:
                raise KeyError(f"Room not found: {room_id}")
            self._members.setdefault(room_id, set()).add(peer_id)
            current_state = self._states[room_id]
            self._member_states.setdefault(room_id, {})
            self._member_states[room_id].setdefault(
                peer_id,
                PlaybackState(
                    is_playing=current_state.is_playing,
                    timestamp_seconds=current_state.timestamp_seconds,
                    last_updated=datetime.now(timezone.utc),
                    updated_by=current_state.updated_by,
                ),
            )

    def remove_member(self, room_id: str, peer_id: str) -> None:
        with self._lock:
            members = self._members.get(room_id)
            if members is not None:
                members.discard(peer_id)
            member_states = self._member_states.get(room_id)
            if member_states is not None:
                member_states.pop(peer_id, None)

    def delete_room(self, room_id: str) -> None:
        with self._lock:
            self._rooms.pop(room_id, None)
            self._states.pop(room_id, None)
            self._members.pop(room_id, None)
            self._member_states.pop(room_id, None)

    def get_member_state(self, room_id: str, peer_id: str) -> PlaybackState | None:
        with self._lock:
            room_states = self._member_states.get(room_id, {})
            state = room_states.get(peer_id)
            if state is None:
                return None
            return PlaybackState(
                is_playing=state.is_playing,
                timestamp_seconds=state.timestamp_seconds,
                last_updated=state.last_updated,
                updated_by=state.updated_by,
            )

    def get_effective_timestamp(self, room_id: str, peer_id: str | None = None) -> float:
        with self._lock:
            if peer_id is None:
                state = self._states.get(room_id)
            else:
                state = self._member_states.get(room_id, {}).get(peer_id)
            if state is None:
                raise KeyError(f"State not found for room={room_id} peer={peer_id}")
            return self._calculate_effective_timestamp(state)

    def get_member_count(self, room_id: str) -> int:
        return len(self.get_members(room_id))

    def get_active_room_count(self) -> int:
        with self._lock:
            return len(self._rooms)

    @staticmethod
    def _calculate_effective_timestamp(state: PlaybackState) -> float:
        if not state.is_playing:
            return float(state.timestamp_seconds)
        elapsed = (datetime.now(timezone.utc) - state.last_updated).total_seconds()
        return float(state.timestamp_seconds + max(elapsed, 0.0))

    def _generate_room_id(self) -> str:
        alphabet = string.ascii_uppercase + string.digits
        while True:
            room_id = "".join(random.choices(alphabet, k=6))
            if room_id not in self._rooms:
                return room_id
