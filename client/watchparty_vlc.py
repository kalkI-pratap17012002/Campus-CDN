from __future__ import annotations

import argparse
import asyncio
import base64
import json
import platform
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import httpx
import websockets


DEFAULT_VLC_BINARIES = [
    "/Applications/VLC.app/Contents/MacOS/VLC",
    "vlc",
]
VLC_POLL_INTERVAL = 0.5
SEEK_TOLERANCE_SECONDS = 1.5
STATE_DEBOUNCE_SECONDS = 0.4
APPLY_GRACE_SECONDS = 1.0


def resolve_vlc_binary() -> str:
    for candidate in DEFAULT_VLC_BINARIES:
        resolved = shutil.which(candidate) or (candidate if Path(candidate).exists() else None)
        if resolved:
            return resolved
    sys.exit("VLC not found. Install VLC (https://www.videolan.org/) or pass --vlc-path.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Campus CDN watch party + VLC bridge")
    parser.add_argument("--server", default="http://localhost:8000", help="CDN base URL")
    parser.add_argument("--file-id", required=True, help="File UUID returned by /upload")
    parser.add_argument("--peer-id", required=True, help="Unique id for this client in the room")
    parser.add_argument("--room-id", help="Existing room id to join. Omit + use --host to create one")
    parser.add_argument("--host", action="store_true", help="Create a new room and print the room id")
    parser.add_argument("--vlc-path", default=None, help="Override VLC binary path")
    parser.add_argument("--vlc-port", type=int, default=8090, help="Local VLC HTTP control port")
    parser.add_argument("--vlc-password", default="campus", help="VLC HTTP interface password")
    return parser.parse_args()


def launch_vlc(vlc_binary: str, stream_url: str, port: int, password: str) -> subprocess.Popen[bytes]:
    args = [
        vlc_binary,
        stream_url,
        "--extraintf", "http",
        "--http-host", "127.0.0.1",
        "--http-port", str(port),
        "--http-password", password,
        "--no-video-title-show",
        "--play-and-exit" if False else "--repeat",
    ]
    if platform.system() == "Darwin":
        args.append("--no-macosx-show-playback-buttons-in-fullscreen")
    return subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


class VlcController:
    def __init__(self, port: int, password: str) -> None:
        token = base64.b64encode(f":{password}".encode()).decode()
        self._client = httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{port}",
            headers={"Authorization": f"Basic {token}"},
            timeout=2.0,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def wait_ready(self, attempts: int = 40) -> None:
        for _ in range(attempts):
            try:
                response = await self._client.get("/requests/status.json")
                if response.status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.5)
        raise RuntimeError("VLC HTTP control never came up")

    async def status(self) -> dict[str, Any]:
        response = await self._client.get("/requests/status.json")
        response.raise_for_status()
        return response.json()

    async def play(self) -> None:
        await self._client.get("/requests/status.json", params={"command": "pl_forceresume"})

    async def pause(self) -> None:
        await self._client.get("/requests/status.json", params={"command": "pl_forcepause"})

    async def seek(self, seconds: float) -> None:
        await self._client.get("/requests/status.json", params={"command": "seek", "val": str(int(seconds))})


def state_from_vlc(payload: dict[str, Any]) -> tuple[bool, float]:
    is_playing = payload.get("state") == "playing"
    timestamp = float(payload.get("time", 0))
    return is_playing, timestamp


async def create_or_join_room(server: str, host: bool, peer_id: str, file_id: str, room_id: str | None) -> str:
    if host and not room_id:
        async with httpx.AsyncClient(base_url=server) as http:
            response = await http.post(
                "/watchparty/create",
                json={"host_id": peer_id, "file_id": file_id},
            )
            response.raise_for_status()
            payload = response.json()
        return payload["room_id"]
    if not room_id:
        sys.exit("--room-id is required unless --host is set")
    return room_id


async def listen_for_state(ws, vlc: VlcController, apply_grace: dict[str, float]) -> None:
    async for raw in ws:
        try:
            message = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if message.get("type") != "STATE":
            if message.get("type") in {"MEMBER_JOINED", "MEMBER_LEFT"}:
                print(f"[room] {message['type']} peer={message.get('peer_id')} members={message.get('member_count')}")
            continue
        timestamp = float(message.get("timestamp_seconds", 0.0))
        is_playing = bool(message.get("is_playing"))
        updated_by = message.get("updated_by")
        print(f"[recv] {updated_by} -> playing={is_playing} t={timestamp:.2f}s")

        try:
            current = await vlc.status()
            current_playing, current_time = state_from_vlc(current)
            if abs(current_time - timestamp) > SEEK_TOLERANCE_SECONDS:
                await vlc.seek(timestamp)
            if is_playing and not current_playing:
                await vlc.play()
            elif not is_playing and current_playing:
                await vlc.pause()
        except httpx.HTTPError as exc:
            print(f"[vlc] error applying state: {exc}")
        apply_grace["until"] = time.monotonic() + APPLY_GRACE_SECONDS


async def watch_vlc(ws, vlc: VlcController, peer_id: str, apply_grace: dict[str, float]) -> None:
    last_playing: bool | None = None
    last_timestamp: float | None = None
    last_emit_at = 0.0
    while True:
        await asyncio.sleep(VLC_POLL_INTERVAL)
        try:
            payload = await vlc.status()
        except httpx.HTTPError:
            continue
        is_playing, timestamp = state_from_vlc(payload)
        if time.monotonic() < apply_grace["until"]:
            last_playing = is_playing
            last_timestamp = timestamp
            continue
        if last_playing is None:
            last_playing = is_playing
            last_timestamp = timestamp
            continue

        events: list[tuple[str, float]] = []
        if is_playing != last_playing:
            events.append(("PLAY" if is_playing else "PAUSE", timestamp))
        elif last_timestamp is not None and abs((timestamp - last_timestamp) - VLC_POLL_INTERVAL) > SEEK_TOLERANCE_SECONDS:
            events.append(("SEEK", timestamp))

        now = time.monotonic()
        for event_type, ts in events:
            if now - last_emit_at < STATE_DEBOUNCE_SECONDS:
                continue
            message = {"type": event_type, "timestamp_seconds": ts, "peer_id": peer_id}
            await ws.send(json.dumps(message))
            print(f"[send] {event_type} t={ts:.2f}s")
            last_emit_at = now

        last_playing = is_playing
        last_timestamp = timestamp


async def main() -> None:
    args = parse_args()
    vlc_binary = args.vlc_path or resolve_vlc_binary()
    stream_url = f"{args.server.rstrip('/')}/stream/{args.file_id}"
    ws_base = args.server.rstrip("/").replace("http://", "ws://").replace("https://", "wss://")

    room_id = await create_or_join_room(args.server, args.host, args.peer_id, args.file_id, args.room_id)
    print(f"Room: {room_id}")
    print(f"Stream URL: {stream_url}")
    print(f"Launching VLC ({vlc_binary})...")

    vlc_process = launch_vlc(vlc_binary, stream_url, args.vlc_port, args.vlc_password)
    vlc = VlcController(args.vlc_port, args.vlc_password)
    try:
        await vlc.wait_ready()
        print(f"VLC HTTP ready on 127.0.0.1:{args.vlc_port}")
        ws_url = f"{ws_base}/watchparty/ws/{room_id}/{args.peer_id}"
        async with websockets.connect(ws_url) as ws:
            print(f"Joined watch party as {args.peer_id}. Use VLC controls to play/pause/seek.")
            apply_grace = {"until": 0.0}
            await asyncio.gather(
                listen_for_state(ws, vlc, apply_grace),
                watch_vlc(ws, vlc, args.peer_id, apply_grace),
            )
    finally:
        await vlc.close()
        vlc_process.terminate()
        try:
            vlc_process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            vlc_process.kill()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nLeaving watch party.")
