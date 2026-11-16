"""HTTP-level tests for the harvester web app (pages and APIs)."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from app.oai import OaiClient


@pytest.fixture
def web_app(tmp_path: Path, demo_app, repo):
    def factory() -> OaiClient:
        return OaiClient(
            "http://demo.local/oai",
            transport=httpx.ASGITransport(app=demo_app),
        )

    app = create_app(
        db_path=tmp_path / "web.sqlite3",
        oai_url="http://demo.local/oai",
        client_factory=factory,
    )
    return app


def poll_until(client: TestClient, predicate, attempts: int = 100) -> dict:
    for _ in range(attempts):
        payload = client.get("/api/status").json()
        if predicate(payload):
            return payload
        import time

        time.sleep(0.05)
    raise AssertionError(f"condition not met; last={payload}")


def test_pages_render_and_are_empty_initially(web_app):
    with TestClient(web_app) as client:
        response = client.get("/")
        assert response.status_code == 200
        assert "已发布目录" in response.text
        assert "尚未成功采集" in response.text
        assert client.get("/runs").status_code == 200
        assert client.get("/status").status_code == 200
        status = client.get("/api/status").json()
        assert status["active"] is None
        assert status["published"]["watermark"] is None


def test_start_harvest_via_api_and_see_catalogue(web_app):
    with TestClient(web_app) as client:
        response = client.post("/api/harvest")
        assert response.status_code == 202
        payload = poll_until(client, lambda p: p["active"] is None and p["published"]["watermark"])
        assert payload["published"]["catalog_size"] > 0

        page = client.get("/")
        assert "数字人文中的文本挖掘方法" in page.text
        assert payload["published"]["watermark"] in page.text

        search = client.get("/", params={"q": "数据治理"})
        assert "高校图书馆数据治理实践" in search.text
        assert "数字人文中的文本挖掘方法" not in search.text

        catalog = client.get("/api/catalog", params={"q": "OAI-PMH"}).json()
        assert catalog["total"] == 1


def test_second_start_while_running_conflicts(web_app, repo):
    repo.set_meta("slow_seconds", "1.0")
    with TestClient(web_app) as client:
        assert client.post("/api/harvest").status_code == 202
        second = client.post("/api/harvest")
        assert second.status_code == 409
        client.post("/api/harvest/cancel")
        poll_until(client, lambda p: p["active"] is None)


def test_cancel_api_is_immediate(web_app, repo):
    repo.set_meta("slow_seconds", "5")
    with TestClient(web_app) as client:
        assert client.post("/api/harvest").status_code == 202
        poll_until(client, lambda p: p["active"] is not None)
        import time

        started = time.monotonic()
        response = client.post("/api/harvest/cancel")
        assert response.status_code == 200
        assert time.monotonic() - started < 2.0
        status = poll_until(client, lambda p: p["active"] is None)
        assert status["published"]["watermark"] is None


def test_cancel_when_idle_is_conflict(web_app):
    with TestClient(web_app) as client:
        assert client.post("/api/harvest/cancel").status_code == 409


def test_restart_while_slow_starts_new_round_immediately(web_app, repo):
    repo.set_meta("slow_seconds", "5")
    with TestClient(web_app) as client:
        first = client.post("/api/harvest")
        assert first.status_code == 202
        first_id = first.json()["run_id"]
        poll_until(client, lambda p: p["active"] is not None)

        import time

        started = time.monotonic()
        second = client.post("/api/harvest/restart")
        assert second.status_code == 202
        assert time.monotonic() - started < 2.0
        assert second.json()["run_id"] != first_id

        # The slow old round must not block the new one: turn slowness off.
        repo.set_meta("slow_seconds", "0")
        status = poll_until(
            client,
            lambda p: p["active"] is None and p["published"]["watermark"],
        )
        assert status["published"]["catalog_size"] > 0
        runs = client.get("/runs").text
        assert "cancelled" in runs


def test_runs_page_lists_attempts_and_token_restarts(web_app, repo):
    with TestClient(web_app) as client:
        client.post("/api/harvest")
        poll_until(client, lambda p: p["active"] is None and p["published"]["watermark"])
        page = client.get("/runs")
        assert page.status_code == 200
        assert "succeeded" in page.text
        assert "分页重取" in page.text
