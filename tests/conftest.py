import asyncio
import getpass
import os
import shutil
import sys
import threading
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
import redis
from fastapi.testclient import TestClient
from sqlalchemy import text


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

_local_db_user = getpass.getuser()
os.environ.setdefault("DATABASE_URL", f"postgresql+asyncpg://{_local_db_user}@localhost:5432/campuscdn")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/15")
os.environ.setdefault("CHUNK_STORAGE_PATH", str(PROJECT_ROOT / "chunks" / "test-suite"))
os.environ.setdefault("APP_PORT", "8000")

from server.cache.edge_cache import edge_cache
from server.config import settings
from server.database.connection import async_session_factory, create_tables, dispose_engine, engine
from server.database.models import ChunkRecord, FileRecord


_cache_counter_lock = threading.Lock()


def _pattern_bytes(size: int, seed: int) -> bytes:
    return bytes((seed + index) % 256 for index in range(size))


async def _truncate_database() -> None:
    async with async_session_factory() as session:
        await session.execute(text("DELETE FROM chunks"))
        await session.execute(text("DELETE FROM files"))
        await session.commit()


def _reset_cache_counters() -> None:
    with _cache_counter_lock:
        edge_cache.hit_count = 0
        edge_cache.miss_count = 0


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session")
async def init_db(event_loop):
    await create_tables()
    yield
    await dispose_engine()


@pytest.fixture(scope="function")
def test_file_small() -> bytes:
    return _pattern_bytes(512 * 1024, 7)


@pytest.fixture(scope="function")
def test_file_medium() -> bytes:
    return _pattern_bytes(3 * 1024 * 1024, 13)


@pytest.fixture(scope="function")
def test_file_large() -> bytes:
    return _pattern_bytes(12 * 1024 * 1024, 29)


@pytest.fixture(scope="function")
def redis_client() -> redis.Redis:
    client = redis.Redis.from_url(os.environ["REDIS_URL"])
    client.flushdb()
    yield client
    client.flushdb()


@pytest_asyncio.fixture(scope="function")
async def db_session(init_db):
    async with async_session_factory() as session:
        yield session


@pytest_asyncio.fixture(scope="function")
async def cleanup_chunks(redis_client: redis.Redis, init_db):
    shutil.rmtree(settings.chunk_storage_dir, ignore_errors=True)
    settings.chunk_storage_dir.mkdir(parents=True, exist_ok=True)
    await _truncate_database()
    _reset_cache_counters()
    yield
    shutil.rmtree(settings.chunk_storage_dir, ignore_errors=True)
    settings.chunk_storage_dir.mkdir(parents=True, exist_ok=True)
    redis_client.flushdb()
    await _truncate_database()
    _reset_cache_counters()


@pytest_asyncio.fixture(scope="function")
async def app_instance(monkeypatch: pytest.MonkeyPatch):
    import server.main as main_module

    monkeypatch.setattr(main_module.PeerDiscovery, "start", lambda self: None)
    monkeypatch.setattr(main_module.PeerDiscovery, "stop", lambda self: None)
    monkeypatch.setattr(main_module.PeerDiscovery, "get_local_bandwidth", lambda self: 100.0)

    async with main_module.app.router.lifespan_context(main_module.app):
        yield main_module.app


@pytest_asyncio.fixture(scope="function")
async def async_client(app_instance):
    transport = httpx.ASGITransport(app=app_instance)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client


@pytest.fixture(scope="function")
def test_client(monkeypatch: pytest.MonkeyPatch):
    import server.main as main_module

    monkeypatch.setattr(main_module.PeerDiscovery, "start", lambda self: None)
    monkeypatch.setattr(main_module.PeerDiscovery, "stop", lambda self: None)
    monkeypatch.setattr(main_module.PeerDiscovery, "get_local_bandwidth", lambda self: 100.0)

    with TestClient(main_module.app) as client:
        yield client
