from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Callable

import redis

from server.config import settings


logger = logging.getLogger(__name__)


class AnalyticsCollector:
    def __init__(
        self,
        redis_url: str | None = None,
        active_peers_provider: Callable[[], int] | None = None,
        local_peer_id_provider: Callable[[], str | None] | None = None,
        local_peer_ip_provider: Callable[[], str | None] | None = None,
    ) -> None:
        self.redis_url = redis_url or settings.REDIS_URL
        self.active_peers_provider = active_peers_provider
        self.local_peer_id_provider = local_peer_id_provider
        self.local_peer_ip_provider = local_peer_ip_provider
        self._client: redis.Redis | None = None
        self._enabled = False
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._sampler_thread: threading.Thread | None = None
        self._connect()
        if self.active_peers_provider is not None:
            self._sampler_thread = threading.Thread(
                target=self._active_peers_sampler_loop,
                name="analytics-active-peers-sampler",
                daemon=True,
            )
            self._sampler_thread.start()

    def record_upload(self, file_id: str, size_bytes: int) -> None:
        client = self._get_client()
        if client is None:
            return

        pipeline = client.pipeline()
        pipeline.incr("total_uploads")
        pipeline.hset(f"file:{file_id}", mapping={"uploaded_bytes": int(size_bytes)})
        pipeline.execute()

    def record_download(self, file_id: str, chunk_hash: str, size_bytes: int, source: str) -> None:
        client = self._get_client()
        if client is None:
            return

        normalized_source = source.lower()
        if normalized_source not in {"cache", "peer", "origin"}:
            normalized_source = "origin"

        timestamp = time.time()
        bandwidth_member = json.dumps(
            {
                "id": uuid.uuid4().hex,
                "timestamp": timestamp,
                "bytes": int(size_bytes),
                "chunk_hash": chunk_hash,
                "source": normalized_source,
            }
        )

        pipeline = client.pipeline()
        pipeline.incr("total_downloads")
        pipeline.incrby("total_bytes_transferred", int(size_bytes))
        if normalized_source == "cache":
            pipeline.incr("cache_hits")
        else:
            pipeline.incr("cache_misses")
        if normalized_source == "peer":
            pipeline.incr("peer_transfers")
        elif normalized_source == "origin":
            pipeline.incr("origin_transfers")
        pipeline.zadd("bandwidth_usage", {bandwidth_member: timestamp})
        pipeline.hincrby(f"file:{file_id}", "downloads", 1)
        pipeline.hincrby(f"file:{file_id}", "bytes_served", int(size_bytes))
        pipeline.execute()

        if normalized_source == "peer":
            self._record_local_peer_contribution(int(size_bytes))

    def get_summary(self) -> dict[str, float | int]:
        client = self._get_client()
        if client is None:
            return {
                "total_uploads": 0,
                "total_downloads": 0,
                "total_bytes_transferred": 0,
                "cache_hit_ratio": 0.0,
                "peer_transfer_ratio": 0.0,
                "cache_transfers": 0,
                "peer_transfers": 0,
                "origin_transfers": 0,
            }

        values = client.mget(
            [
                "total_uploads",
                "total_downloads",
                "total_bytes_transferred",
                "cache_hits",
                "cache_misses",
                "peer_transfers",
                "origin_transfers",
            ]
        )
        total_uploads, total_downloads, total_bytes, cache_hits, cache_misses, peer_transfers, origin_transfers = (
            int(value or 0) for value in values
        )
        total_transfer_events = max(total_downloads, 1)
        cache_transfers = cache_hits
        return {
            "total_uploads": total_uploads,
            "total_downloads": total_downloads,
            "total_bytes_transferred": total_bytes,
            "cache_hit_ratio": round(cache_hits / total_transfer_events, 4) if total_downloads else 0.0,
            "peer_transfer_ratio": round(peer_transfers / total_transfer_events, 4) if total_downloads else 0.0,
            "cache_transfers": cache_transfers,
            "peer_transfers": peer_transfers,
            "origin_transfers": origin_transfers,
            "cache_misses": cache_misses,
        }

    def get_top_files(self, n: int = 10) -> list[dict[str, int | str]]:
        client = self._get_client()
        if client is None:
            return []

        file_stats: list[dict[str, int | str]] = []
        for key in client.scan_iter(match="file:*"):
            key_str = self._decode(key)
            if key_str.count(":") != 1:
                continue
            data = client.hgetall(key_str)
            downloads = int(data.get(b"downloads", b"0"))
            bytes_served = int(data.get(b"bytes_served", b"0"))
            file_stats.append(
                {
                    "file_id": key_str.split("file:", 1)[1],
                    "downloads": downloads,
                    "bytes_served": bytes_served,
                }
            )

        file_stats.sort(key=lambda item: (int(item["downloads"]), int(item["bytes_served"])), reverse=True)
        return file_stats[:n]

    def get_peer_contributions(self) -> list[dict[str, int | str]]:
        client = self._get_client()
        if client is None:
            return []

        peer_stats: list[dict[str, int | str]] = []
        for key in client.scan_iter(match="peer:*"):
            key_str = self._decode(key)
            if key_str.count(":") != 1:
                continue
            data = client.hgetall(key_str)
            peer_stats.append(
                {
                    "peer_id": key_str.split("peer:", 1)[1],
                    "ip": self._decode(data.get(b"ip", b"")),
                    "bytes_contributed": int(data.get(b"bytes_contributed", b"0")),
                    "chunks_served": int(data.get(b"chunks_served", b"0")),
                }
            )

        peer_stats.sort(
            key=lambda item: (int(item["bytes_contributed"]), int(item["chunks_served"])),
            reverse=True,
        )
        return peer_stats

    def get_bandwidth_history(self, hours: int = 24) -> list[dict[str, int | str]]:
        client = self._get_client()
        if client is None:
            return []

        now = datetime.now(UTC)
        start_time = now - timedelta(hours=hours)
        entries = client.zrangebyscore("bandwidth_usage", start_time.timestamp(), now.timestamp())

        by_hour: dict[str, int] = defaultdict(int)
        for entry in entries:
            try:
                payload = json.loads(self._decode(entry))
            except json.JSONDecodeError:
                continue
            entry_timestamp = datetime.fromtimestamp(float(payload["timestamp"]), UTC)
            hour_key = entry_timestamp.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:00")
            by_hour[hour_key] += int(payload["bytes"])

        history: list[dict[str, int | str]] = []
        current = start_time.replace(minute=0, second=0, microsecond=0)
        end = now.replace(minute=0, second=0, microsecond=0)
        while current <= end:
            hour_key = current.strftime("%Y-%m-%dT%H:00")
            history.append({"hour": hour_key, "bytes": by_hour.get(hour_key, 0)})
            current += timedelta(hours=1)
        return history

    def get_cache_hit_ratio(self) -> float:
        summary = self.get_summary()
        return float(summary["cache_hit_ratio"])

    def stop(self) -> None:
        self._stop_event.set()
        if self._sampler_thread is not None:
            self._sampler_thread.join(timeout=1.0)

    def _record_local_peer_contribution(self, size_bytes: int) -> None:
        client = self._get_client()
        if client is None or self.local_peer_id_provider is None:
            return

        peer_id = self.local_peer_id_provider()
        if not peer_id:
            return

        mapping: dict[str, int | str] = {}
        if self.local_peer_ip_provider is not None:
            peer_ip = self.local_peer_ip_provider()
            if peer_ip:
                mapping["ip"] = peer_ip

        pipeline = client.pipeline()
        pipeline.hincrby(f"peer:{peer_id}", "bytes_contributed", int(size_bytes))
        pipeline.hincrby(f"peer:{peer_id}", "chunks_served", 1)
        if mapping:
            pipeline.hset(f"peer:{peer_id}", mapping=mapping)
        pipeline.execute()

    def _active_peers_sampler_loop(self) -> None:
        self._record_active_peers_count()
        while not self._stop_event.wait(30):
            self._record_active_peers_count()

    def _record_active_peers_count(self) -> None:
        client = self._get_client()
        if client is None or self.active_peers_provider is None:
            return

        try:
            count = int(self.active_peers_provider())
        except Exception as exc:
            logger.warning("Failed to sample active peers count: %s", exc)
            return

        timestamp = time.time()
        member = json.dumps({"id": uuid.uuid4().hex, "timestamp": timestamp, "count": count})
        client.zadd("active_peers_count", {member: timestamp})

    def _connect(self) -> None:
        try:
            self._client = redis.Redis.from_url(self.redis_url)
            self._client.ping()
            self._enabled = True
        except redis.RedisError as exc:
            self._client = None
            self._enabled = False
            logger.warning("Redis analytics unavailable at %s: %s", self.redis_url, exc)

    def _get_client(self) -> redis.Redis | None:
        if self._enabled and self._client is not None:
            return self._client

        self._connect()
        if self._enabled and self._client is not None:
            return self._client
        return None

    @staticmethod
    def _decode(value: bytes | str | None) -> str:
        if value is None:
            return ""
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return value
