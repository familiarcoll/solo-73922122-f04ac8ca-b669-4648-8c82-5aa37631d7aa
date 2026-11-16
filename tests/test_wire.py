"""Wire-level checks: what the harvester actually requests from the source.

A recording transport wraps the demo ASGI app so the tests can assert the
exact ``from``/``until`` parameters and token behaviour seen by the source,
rather than only the end state in SQLite.
"""

from __future__ import annotations

import asyncio
from urllib.parse import parse_qsl
from urllib.parse import urlparse

import httpx

from app.harvest import HarvestConfig, Harvester
from app.oai import OaiClient


class RecordingTransport(httpx.AsyncBaseTransport):
    def __init__(self, app) -> None:
        self._inner = httpx.ASGITransport(app=app)
        self.requests: list[dict[str, str]] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(dict(parse_qsl(urlparse(str(request.url)).query)))
        return await self._inner.handle_async_request(request)


def make_harvester(store, app):
    transport = RecordingTransport(app)

    def factory() -> OaiClient:
        return OaiClient("http://demo.local/oai", transport=transport)

    harvester = Harvester(
        store,
        HarvestConfig(oai_url="http://demo.local/oai", max_attempts=3, retry_backoff_ms=0),
        client_factory=factory,
    )
    return harvester, transport


async def wait(control):
    try:
        await asyncio.wait_for(control.task, 20)
    except asyncio.CancelledError:
        pass
    await asyncio.wait_for(control.done.wait(), 20)
    return control.outcome


async def test_first_round_has_no_from_and_closed_until(store, demo_app, repo):
    harvester, wire = make_harvester(store, demo_app)
    control = await harvester.start_run()
    assert await wait(control) == "succeeded"

    first_list = next(
        r for r in wire.requests if r.get("verb") == "ListRecords" and "resumptionToken" not in r
    )
    assert "from" not in first_list
    assert first_list["metadataPrefix"] == "oai_dc"
    assert first_list["until"].endswith("Z")
    assert store.published_state()["watermark"] == first_list["until"]

    # Tokens carry no from/until; pagination is tied to the original window.
    token_requests = [r for r in wire.requests if "resumptionToken" in r]
    assert token_requests
    for request in token_requests:
        assert "from" not in request and "until" not in request


async def test_incremental_round_uses_previous_watermark_as_from(store, demo_app, repo):
    harvester, wire = make_harvester(store, demo_app)
    first = await harvester.start_run()
    await wait(first)
    watermark = store.published_state()["watermark"]

    await asyncio.sleep(1.05)
    repo.add_record(title="增量论文")
    wire.requests.clear()

    second = await harvester.start_run()
    assert await wait(second) == "succeeded"
    listing = next(
        r for r in wire.requests if r.get("verb") == "ListRecords" and "resumptionToken" not in r
    )
    assert listing["from"] == watermark
    assert listing["until"] == store.published_state()["watermark"]
    assert listing["until"] >= watermark


async def test_token_expiry_refetches_original_window_exactly(store, demo_app, repo):
    # Establish a watermark first so the retried window is an incremental one.
    baseline_harvester, _ = make_harvester(store, demo_app)
    baseline = await baseline_harvester.start_run()
    assert await wait(baseline) == "succeeded"

    await asyncio.sleep(1.05)
    repo.add_record(title="令牌失效前的新记录 A")
    repo.add_record(title="令牌失效前的新记录 B")
    repo.add_record(title="令牌失效前的新记录 C")

    repo.set_meta("page_size", "1")
    repo.set_meta("token_failures", "1")
    harvester, wire = make_harvester(store, demo_app)
    control = await harvester.start_run()
    assert await wait(control) == "succeeded"

    initial_lists = [
        r for r in wire.requests if r.get("verb") == "ListRecords" and "resumptionToken" not in r
    ]
    assert len(initial_lists) == 2, "one aborted attempt and one clean restart"
    assert initial_lists[0]["from"] == initial_lists[1]["from"]
    assert initial_lists[0]["until"] == initial_lists[1]["until"]
    # The restarted attempt uses a fresh token chain (no stale token carried over).
    tokens = [r["resumptionToken"] for r in wire.requests if "resumptionToken" in r]
    assert len(tokens) == len(set(tokens))
    assert store.get_run(control.run_id)["token_restarts"] == 1
