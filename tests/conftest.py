"""Shared fixtures: a demo repository exposed over an in-process ASGI transport.

The harvester's HTTP client is pointed at the real demo FastAPI app through
``httpx.ASGITransport``.  That exercises the actual OAI-PMH XML, tokens, slow
pages and fault injection without needing a network port.
"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
from pathlib import Path

import httpx
import pytest
import pytest_asyncio

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Set before importing the applications: both create their SQLite store at
# import time.  Keep test artefacts out of the repository working tree.
os.environ.setdefault("DATA_DIR", tempfile.mkdtemp(prefix="harvester-tests-"))

import demo_oai.main as demo_main  # noqa: E402
from app.harvest import HarvestConfig, Harvester  # noqa: E402
from app.oai import OaiClient  # noqa: E402
from app.store import Store  # noqa: E402
from demo_oai.repository import DemoRepo  # noqa: E402


@pytest.fixture
def repo(tmp_path: Path) -> DemoRepo:
    instance = DemoRepo(str(tmp_path / "demo.sqlite3"), page_size=3)
    demo_main.repo = instance
    return instance


@pytest.fixture
def demo_app(repo: DemoRepo):
    return demo_main.app


@pytest.fixture
def store(tmp_path: Path) -> Store:
    s = Store(tmp_path / "harvester.sqlite3")
    yield s
    s.close()


@pytest.fixture
def harvester(store: Store, demo_app, repo: DemoRepo) -> Harvester:
    def factory() -> OaiClient:
        return OaiClient(
            "http://demo.local/oai",
            transport=httpx.ASGITransport(app=demo_app),
            read_timeout=30,
            connect_timeout=5,
        )

    return Harvester(
        store,
        HarvestConfig(oai_url="http://demo.local/oai", max_attempts=3, retry_backoff_ms=0),
        client_factory=factory,
    )


@pytest_asyncio.fixture(autouse=True)
async def _shutdown_harvesters():
    """Ensure no fixture-created harvester task leaks between tests."""
    yield


async def wait_for_outcome(control, timeout: float = 20.0) -> str:
    """Wait for a run to settle without requiring the task not to be cancelled."""
    try:
        await asyncio.wait_for(control.task, timeout)
    except asyncio.CancelledError:
        pass
    await asyncio.wait_for(control.done.wait(), timeout)
    return control.outcome or "unknown"
