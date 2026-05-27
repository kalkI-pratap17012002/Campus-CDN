import asyncio
import logging

from server.config import settings


logger = logging.getLogger(__name__)


class ConnectionPool:
    def __init__(self, max_connections: int | None = None, timeout_seconds: float = 30.0) -> None:
        self.max_connections = max_connections or settings.MAX_POOL_CONNECTIONS
        self.timeout_seconds = timeout_seconds
        self._semaphore: asyncio.Semaphore | None = None
        self._lock: asyncio.Lock | None = None
        self._slot_counter = 0
        self._active_slots: dict[int, str] = {}

    def _get_semaphore(self) -> asyncio.Semaphore:
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self.max_connections)
        return self._semaphore

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def acquire(self, file_id: str) -> int:
        logger.info("Waiting for transfer slot for file_id=%s", file_id)
        semaphore = self._get_semaphore()
        try:
            await asyncio.wait_for(semaphore.acquire(), timeout=self.timeout_seconds)
        except asyncio.TimeoutError as exc:
            logger.warning(
                "Timed out waiting for transfer slot for file_id=%s after %.2fs",
                file_id,
                self.timeout_seconds,
            )
            raise TimeoutError(f"Timed out acquiring transfer slot for file {file_id}") from exc

        async with self._get_lock():
            self._slot_counter += 1
            slot = self._slot_counter
            self._active_slots[slot] = file_id

        logger.info(
            "Acquired transfer slot=%s for file_id=%s active=%s",
            slot,
            file_id,
            self.get_active_count(),
        )
        return slot

    async def release(self, file_id: str, slot: int) -> None:
        async with self._get_lock():
            owner = self._active_slots.get(slot)
            if owner is not None:
                self._active_slots.pop(slot, None)
                self._get_semaphore().release()

        logger.info(
            "Released transfer slot=%s for file_id=%s active=%s",
            slot,
            file_id,
            self.get_active_count(),
        )

    def get_active_count(self) -> int:
        return len(self._active_slots)


transfer_pool = ConnectionPool()
