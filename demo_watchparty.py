import asyncio
import json
import sys

import httpx
import websockets


SERVER_HTTP = "http://localhost:8000"
SERVER_WS = "ws://localhost:8000"


async def receive_messages(name: str, ws, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=0.5)
        except asyncio.TimeoutError:
            continue
        except websockets.ConnectionClosed:
            return
        message = json.loads(raw)
        print(f"  [{name} received] {json.dumps(message)}")


async def main(file_id: str) -> None:
    async with httpx.AsyncClient() as http:
        response = await http.post(
            f"{SERVER_HTTP}/watchparty/create",
            json={"host_id": "alice", "file_id": file_id},
        )
        room = response.json()
    room_id = room["room_id"]
    print(f"Created room {room_id} for file {file_id}\n")

    host_url = f"{SERVER_WS}/watchparty/ws/{room_id}/alice"
    peer_url = f"{SERVER_WS}/watchparty/ws/{room_id}/bob"

    async with websockets.connect(host_url) as host_ws, websockets.connect(peer_url) as peer_ws:
        stop = asyncio.Event()
        listeners = asyncio.gather(
            receive_messages("alice", host_ws, stop),
            receive_messages("bob", peer_ws, stop),
        )

        print("--- alice and bob joined ---")
        await asyncio.sleep(0.5)

        print("\n--- alice sends PLAY @ 42.0s ---")
        await host_ws.send(json.dumps({"type": "PLAY", "timestamp_seconds": 42.0, "peer_id": "alice"}))
        await asyncio.sleep(0.5)

        print("\n--- alice sends PAUSE @ 50.0s ---")
        await host_ws.send(json.dumps({"type": "PAUSE", "timestamp_seconds": 50.0, "peer_id": "alice"}))
        await asyncio.sleep(0.5)

        print("\n--- alice sends SEEK to 120.0s ---")
        await host_ws.send(json.dumps({"type": "SEEK", "timestamp_seconds": 120.0, "peer_id": "alice"}))
        await asyncio.sleep(0.5)

        print("\n--- alice sends PING ---")
        await host_ws.send(json.dumps({"type": "PING", "client_time": 1234567890}))
        await asyncio.sleep(0.5)

        print("\n--- room state (members still connected) ---")
        async with httpx.AsyncClient() as http:
            info = (await http.get(f"{SERVER_HTTP}/watchparty/{room_id}")).json()
        print(json.dumps(info, indent=2))

        stop.set()
        await listeners


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: python3 demo_watchparty.py <FILE_ID>")
        sys.exit(1)
    asyncio.run(main(sys.argv[1]))
