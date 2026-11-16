"""Cancellation, supersession and late-response behaviour.

These tests deliberately make the demo source slow.  They verify:

* cancelling marks the round cancelled at once (the API call does not wait for
  a slow page) and publishes nothing;
* a new round can start immediately after cancellation;
* a late response from the cancelled round can neither publish nor change the
  new round's state;
* a failed round never moves the published watermark / catalogue.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from conftest import wait_for_outcome


async def test_cancel_returns_immediately_and_publishes_nothing(harvester, store, repo):
    # Seed a successful baseline first.
    first = await harvester.start_run()
    await wait_for_outcome(first)
    baseline = store.published_state()
    baseline_items = store.catalog_page(page_size=200)["records"]

    await asyncio.sleep(1.05)
    repo.add_record(title="本应被取消的改动")
    repo.set_meta("slow_seconds", "5")

    control = await harvester.start_run()
    await asyncio.sleep(0.2)  # let it enter the slow HTTP read
    assert store.get_run(control.run_id)["status"] == "running"

    started = time.monotonic()
    await harvester.cancel_run(control.run_id, "测试取消", "user")
    elapsed = time.monotonic() - started
    assert elapsed < 1.0, f"cancel waited for the slow page ({elapsed:.2f}s)"

    run = store.get_run(control.run_id)
    assert run["status"] == "cancelled"
    assert run["staged_count"] == 0

    # Give the abandoned request time to "arrive late".
    await asyncio.sleep(0.4)
    after = store.published_state()
    assert after["watermark"] == baseline["watermark"]
    assert after["success_count"] == baseline["success_count"]
    assert store.catalog_page(page_size=200)["records"] == baseline_items


async def test_new_round_starts_immediately_after_cancel_and_old_late_response_is_ignored(
    harvester, store, repo
):
    await asyncio.sleep(0)
    first = await harvester.start_run()
    await wait_for_outcome(first)

    await asyncio.sleep(1.05)
    repo.set_meta("slow_seconds", "3")
    old = await harvester.start_run()
    await asyncio.sleep(0.2)

    # Cancel old and start a new round in one API-like sequence.
    await harvester.cancel_run(old.run_id, "被新一轮取代", "superseded")
    repo.set_meta("slow_seconds", "0")
    new = await harvester.start_run()
    assert new.run_id != old.run_id
    assert await wait_for_outcome(new) == "succeeded"

    new_run = store.get_run(new.run_id)
    assert new_run["status"] == "succeeded"
    watermark = store.published_state()["watermark"]
    assert watermark == new_run["window_until"]

    # Wait past the old round's slow response; it must not alter anything.
    await asyncio.sleep(3.5)
    assert store.published_state()["watermark"] == watermark
    assert store.get_run(old.run_id)["status"] == "cancelled"
    assert store.get_run(new.run_id)["status"] == "succeeded"


async def test_commit_is_rejected_for_cancelled_round(store, harvester):
    """Even if a cancelled run reached the commit step, it cannot publish."""
    run = store.create_run()
    run_id = run["id"]
    store.set_window(run_id, "2026-01-01T00:00:00Z")
    store.stage_records(
        run_id,
        [
            {
                "identifier": "oai:late:1",
                "title": "迟到记录",
                "creator": None,
                "datestamp": "2025-12-31T23:59:59Z",
                "deleted": False,
            }
        ],
        0,
    )
    assert store.cancel_run(run_id, "cancelled before commit")
    result = store.commit_run(run_id)
    assert result["committed"] is False
    assert store.published_state()["watermark"] is None
    assert store.catalog_page(page_size=10)["records"] == []


async def test_no_concurrent_round_can_start_while_one_runs(harvester, repo):
    repo.set_meta("slow_seconds", "1")
    control = await harvester.start_run()
    with pytest.raises(RuntimeError):
        await harvester.start_run()
    await harvester.cancel_run(control.run_id)
    await wait_for_outcome(control)


async def test_source_failure_keeps_published_content(harvester, store, demo_app, monkeypatch):
    first = await harvester.start_run()
    await wait_for_outcome(first)
    baseline = store.published_state()

    # Break the source URL: every request fails.
    harvester.config.oai_url = "http://127.0.0.1:9/oai"
    from app.oai import OaiClient
    import httpx

    harvester._client_factory = lambda: OaiClient(
        "http://127.0.0.1:9/oai", transport=httpx.ASGITransport(app=demo_app)
    )

    class Boom(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            raise httpx.ConnectError("boom", request=request)

    harvester._client_factory = lambda: OaiClient(
        "http://127.0.0.1:9/oai", transport=Boom()
    )
    await asyncio.sleep(1.05)
    control = await harvester.start_run()
    assert await wait_for_outcome(control) == "failed"
    state = store.published_state()
    assert state["watermark"] == baseline["watermark"]
    assert state["catalog_size"] == baseline["catalog_size"]
    run = store.get_run(control.run_id)
    assert run["status"] == "failed"
    assert run["staged_count"] == 0
