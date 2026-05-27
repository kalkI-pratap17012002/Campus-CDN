from __future__ import annotations

import logging
import threading
from datetime import UTC, datetime

import redis

from server.config import settings


logger = logging.getLogger(__name__)


class EdgeCache:
    def __init__(self, redis_url: str | None = None, max_cache_size_gb: int | None = None, ttl_seconds: int = 86400) -> None:
        self.redis_url = redis_url or settings.REDIS_URL
        self.max_cache_size_bytes = int((max_cache_size_gb or settings.MAX_CACHE_SIZE_GB) * (1024 ** 3))
        self.ttl_seconds = ttl_seconds
        self.hit_count = 0
        self.miss_count = 0
        self._lock = threading.RLock()
        self._client: redis.Redis | None = None
        self._enabled = False
        self._connect()

    def store_chunk(self, chunk_hash: str, data: bytes) -> None:
        client = self._get_client()
        if client is None:
            return

        chunk_key = self._chunk_key(chunk_hash)
        freq_key = self._freq_key(chunk_hash)
        size_key = self._size_key(chunk_hash)
        size_bytes = len(data)

        if size_bytes > self.max_cache_size_bytes:
            logger.warning("Skipping cache for chunk_hash=%s because it exceeds cache capacity", chunk_hash)
            return

        with self._lock:
            previous_size = int(client.get(size_key) or 0)
            projected_size = self._current_size_bytes(client) - previous_size + size_bytes
            while projected_size > self.max_cache_size_bytes and self._cached_chunk_count(client) > 0:
                self.evict_lfu()
                projected_size = self._current_size_bytes(client) - previous_size + size_bytes

            pipeline = client.pipeline()
            pipeline.set(chunk_key, data, ex=self.ttl_seconds)
            pipeline.set(size_key, size_bytes, ex=self.ttl_seconds)
            if previous_size == 0:
                pipeline.set(freq_key, 1, ex=self.ttl_seconds)
            else:
                pipeline.expire(freq_key, self.ttl_seconds)
            pipeline.execute()

    def get_chunk(self, chunk_hash: str) -> bytes | None:
        client = self._get_client()
        if client is None:
            self._record_miss()
            return None

        chunk_key = self._chunk_key(chunk_hash)
        freq_key = self._freq_key(chunk_hash)

        with self._lock:
            chunk_data = client.get(chunk_key)
            if chunk_data is None:
                self._record_miss()
                return None

            pipeline = client.pipeline()
            pipeline.incr(freq_key)
            pipeline.expire(freq_key, self.ttl_seconds)
            pipeline.expire(chunk_key, self.ttl_seconds)
            pipeline.expire(self._size_key(chunk_hash), self.ttl_seconds)
            pipeline.execute()
            self._record_hit()
            return bytes(chunk_data)

    def evict_lfu(self) -> None:
        client = self._get_client()
        if client is None:
            return

        with self._lock:
            lowest_hash: str | None = None
            lowest_frequency: int | None = None

            for freq_key in client.scan_iter(match="freq:*"):
                freq_key_str = self._decode_key(freq_key)
                frequency = int(client.get(freq_key_str) or 0)
                chunk_hash = freq_key_str.split("freq:", 1)[1]
                if lowest_frequency is None or frequency < lowest_frequency:
                    lowest_frequency = frequency
                    lowest_hash = chunk_hash

            if lowest_hash is None:
                return

            pipeline = client.pipeline()
            pipeline.delete(self._chunk_key(lowest_hash))
            pipeline.delete(self._freq_key(lowest_hash))
            pipeline.delete(self._size_key(lowest_hash))
            pipeline.incr(self._eviction_key_for_today())
            pipeline.expire(self._eviction_key_for_today(), 172800)
            pipeline.execute()

    def get_stats(self) -> dict[str, float | int]:
        client = self._get_client()
        cached_chunks = 0
        total_size_mb = 0.0
        if client is not None:
            cached_chunks = self._cached_chunk_count(client)
            total_size_mb = round(self._current_size_bytes(client) / (1024 * 1024), 2)

        total_requests = self.hit_count + self.miss_count
        hit_ratio = round((self.hit_count / total_requests) if total_requests else 0.0, 4)
        return {
            "cached_chunks": cached_chunks,
            "total_size_mb": total_size_mb,
            "hit_count": self.hit_count,
            "miss_count": self.miss_count,
            "hit_ratio": hit_ratio,
        }

    def is_cached(self, chunk_hash: str) -> bool:
        client = self._get_client()
        if client is None:
            return False
        return bool(client.exists(self._chunk_key(chunk_hash)))

    def get_evictions_today(self) -> int:
        client = self._get_client()
        if client is None:
            return 0
        return int(client.get(self._eviction_key_for_today()) or 0)

    def _connect(self) -> None:
        try:
            self._client = redis.Redis.from_url(self.redis_url)
            self._client.ping()
            self._enabled = True
        except redis.RedisError as exc:
            self._client = None
            self._enabled = False
            logger.warning("Redis edge cache unavailable at %s: %s", self.redis_url, exc)

    def _get_client(self) -> redis.Redis | None:
        if self._enabled and self._client is not None:
            return self._client

        self._connect()
        if self._enabled and self._client is not None:
            return self._client
        return None

    def _current_size_bytes(self, client: redis.Redis) -> int:
        total = 0
        for size_key in client.scan_iter(match="size:*"):
            total += int(client.get(self._decode_key(size_key)) or 0)
        return total

    def _cached_chunk_count(self, client: redis.Redis) -> int:
        return sum(1 for _ in client.scan_iter(match="chunk:*"))

    def _record_hit(self) -> None:
        self.hit_count += 1

    def _record_miss(self) -> None:
        self.miss_count += 1

    @staticmethod
    def _chunk_key(chunk_hash: str) -> str:
        return f"chunk:{chunk_hash}"

    @staticmethod
    def _freq_key(chunk_hash: str) -> str:
        return f"freq:{chunk_hash}"

    @staticmethod
    def _size_key(chunk_hash: str) -> str:
        return f"size:{chunk_hash}"

    @staticmethod
    def _eviction_key_for_today() -> str:
        return f"cache:evictions:{datetime.now(UTC).strftime('%Y-%m-%d')}"

    @staticmethod
    def _decode_key(key: bytes | str) -> str:
        if isinstance(key, bytes):
            return key.decode("utf-8")
        return key


edge_cache = EdgeCache()
