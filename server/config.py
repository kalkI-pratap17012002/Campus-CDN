import logging
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


load_dotenv()


@dataclass
class Settings:
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL",
        "postgresql+asyncpg://user:password@localhost:5432/campuscdn",
    )
    CHUNK_SIZE_BYTES: int = int(os.getenv("CHUNK_SIZE_BYTES", "524288"))
    CHUNK_STORAGE_PATH: str = os.getenv("CHUNK_STORAGE_PATH", "chunks")
    MAX_POOL_CONNECTIONS: int = int(os.getenv("MAX_POOL_CONNECTIONS", "5"))
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379")
    MAX_CACHE_SIZE_GB: int = int(os.getenv("MAX_CACHE_SIZE_GB", "2"))
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    @property
    def chunk_storage_dir(self) -> Path:
        return Path(self.CHUNK_STORAGE_PATH)

    def ensure_directories(self) -> None:
        self.chunk_storage_dir.mkdir(parents=True, exist_ok=True)
        Path("logs").mkdir(parents=True, exist_ok=True)

    def log_level_value(self) -> int:
        return getattr(logging, self.LOG_LEVEL.upper(), logging.INFO)


settings = Settings()
settings.ensure_directories()
