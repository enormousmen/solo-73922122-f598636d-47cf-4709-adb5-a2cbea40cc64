"""Shared pytest fixtures."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import Settings  # noqa: E402
from app.main import create_app  # noqa: E402


@pytest.fixture()
def settings(tmp_path: Path) -> Settings:
    return Settings(
        data_dir=tmp_path / "data",
        remap_attempts=1,
        debug_endpoints=True,
    )


@pytest.fixture()
def client(settings: Settings) -> TestClient:
    return TestClient(create_app(settings))
