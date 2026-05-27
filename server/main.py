import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from server.analytics.collector import AnalyticsCollector
from server.config import settings
from server.database.connection import create_tables
from server.routes.analytics import router as analytics_router
from server.routes.download import router as download_router
from server.routes.peers import router as peers_router
from server.routes.upload import router as upload_router
from server.routes.watchparty import router as watchparty_router
from server.peers.discovery import PeerDiscovery
from server.peers.registry import PeerRegistry
from server.transfer.pool import transfer_pool
from server.watchparty.room import RoomManager
from server.watchparty.sync import ConnectionManager


def configure_logging() -> None:
    log_path = Path("logs/app.log")
    log_path.parent.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(settings.log_level_value())
    root_logger.handlers.clear()
    root_logger.addHandler(file_handler)
    root_logger.addHandler(stream_handler)


configure_logging()
BASE_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = BASE_DIR / "static"


def get_application_port() -> int:
    return int(os.getenv("APP_PORT", "8000"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    await create_tables()
    app.state.peer_registry = PeerRegistry()
    app.state.peer_discovery = PeerDiscovery(app.state.peer_registry, app_port=get_application_port())
    app.state.room_manager = RoomManager()
    app.state.watchparty_connection_manager = ConnectionManager(app.state.room_manager)
    app.state.analytics_collector = AnalyticsCollector(
        active_peers_provider=lambda: len(app.state.peer_registry.get_active_peers()),
        local_peer_id_provider=lambda: app.state.peer_discovery.peer_id,
        local_peer_ip_provider=lambda: app.state.peer_discovery.ip,
    )
    app.state.peer_discovery.start()
    yield
    app.state.analytics_collector.stop()
    await app.state.watchparty_connection_manager.stop()
    app.state.peer_discovery.stop()
    app.state.peer_registry.stop()


app = FastAPI(title="Campus Content Distribution Platform", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
app.include_router(upload_router)
app.include_router(download_router)
app.include_router(peers_router)
app.include_router(watchparty_router)
app.include_router(analytics_router)


@app.get("/health")
async def health() -> dict[str, object]:
    return {"status": "ok", "active_transfers": transfer_pool.get_active_count()}


@app.get("/dashboard", include_in_schema=False)
async def dashboard() -> FileResponse:
    return FileResponse(STATIC_DIR / "dashboard.html")


@app.get("/watchparty", include_in_schema=False)
async def watchparty_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "watchparty.html")
