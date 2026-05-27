import asyncio

import pytest

from server.transfer.pool import ConnectionPool


@pytest.mark.asyncio
async def test_five_concurrent_acquires_succeed():
    pool = ConnectionPool(max_connections=5, timeout_seconds=0.5)

    slots = await asyncio.gather(*(pool.acquire(f"file-{index}") for index in range(5)))

    assert len(slots) == 5
    assert pool.get_active_count() == 5

    await asyncio.gather(*(pool.release(f"file-{index}", slot) for index, slot in enumerate(slots)))
    assert pool.get_active_count() == 0


@pytest.mark.asyncio
async def test_sixth_acquire_blocks_until_one_releases():
    pool = ConnectionPool(max_connections=5, timeout_seconds=1.0)
    slots = [await pool.acquire(f"file-{index}") for index in range(5)]

    pending_acquire = asyncio.create_task(pool.acquire("file-6"))
    await asyncio.sleep(0.05)
    assert pending_acquire.done() is False

    await pool.release("file-0", slots[0])
    sixth_slot = await pending_acquire

    assert isinstance(sixth_slot, int)
    assert pool.get_active_count() == 5

    for index, slot in enumerate(slots[1:], start=1):
        await pool.release(f"file-{index}", slot)
    await pool.release("file-6", sixth_slot)
    assert pool.get_active_count() == 0


@pytest.mark.asyncio
async def test_timeout_raises_after_pool_stays_full():
    pool = ConnectionPool(max_connections=1, timeout_seconds=0.1)
    first_slot = await pool.acquire("file-1")

    with pytest.raises(TimeoutError):
        await pool.acquire("file-2")

    await pool.release("file-1", first_slot)
