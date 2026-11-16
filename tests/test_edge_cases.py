"""Protocol edge cases with a scripted source (no demo repository)."""

from __future__ import annotations

import asyncio

from app.harvest import HarvestConfig, Harvester
from app.oai import OaiPage, OaiRecord


class ScriptedClient:
    def __init__(self, pages, source_now="2026-01-01T00:00:10Z"):
        self.pages = list(pages)
        self.source_now = source_now
        self.calls: list[dict] = []
        self._index = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False

    async def identify_time(self):
        return self.source_now

    async def list_records(self, *, from_, until, token=None):
        self.calls.append({"from": from_, "until": until, "token": token})
        page = self.pages[self._index]
        self._index += 1
        if isinstance(page, Exception):
            raise page
        return page


def _harvester(store, pages, source_now="2026-01-01T00:00:10Z"):
    client = ScriptedClient(pages, source_now)
    harvester = Harvester(
        store,
        HarvestConfig(oai_url="scripted", max_attempts=3, retry_backoff_ms=0),
        client_factory=lambda: client,
    )
    return harvester, client


async def _wait(control):
    try:
        await asyncio.wait_for(control.task, 10)
    except asyncio.CancelledError:
        pass
    await asyncio.wait_for(control.done.wait(), 10)
    return control.outcome


def record(identifier, datestamp, title="标题", deleted=False):
    return OaiRecord(
        identifier=identifier,
        datestamp=datestamp,
        deleted=deleted,
        title=None if deleted else title,
    )


async def test_out_of_window_record_fails_round_without_publishing(store):
    harvester, client = _harvester(
        store,
        [
            OaiPage(
                records=[
                    record("oai:x:1", "2026-01-01T00:00:05Z"),
                    record("oai:x:2", "2026-01-02T00:00:00Z"),  # after `until`
                ]
            )
        ],
    )
    control = await harvester.start_run()
    assert await _wait(control) == "failed"
    assert "越窗" in (store.get_run(control.run_id)["last_error"] or "")
    assert store.published_state()["watermark"] is None
    assert store.published_state()["catalog_size"] == 0


async def test_lower_bound_is_inclusive(store):
    # First seed a watermark one second before the record.
    harvester, client = _harvester(
        store,
        [
            OaiPage(token="t1", records=[record("oai:a", "2025-12-31T23:59:58Z")]),
            OaiPage(records=[record("oai:b", "2025-12-31T23:59:59Z")]),
        ],
        source_now="2026-01-01T00:00:00Z",
    )
    control = await harvester.start_run()
    assert await _wait(control) == "succeeded"
    watermark = store.published_state()["watermark"]
    assert watermark == "2026-01-01T00:00:00Z"

    # Next round's from is the watermark; a record exactly at `from` is allowed.
    harvester2, client2 = _harvester(
        store,
        [OaiPage(records=[record("oai:c", watermark)])],
        source_now="2026-01-01T00:00:05Z",
    )
    control2 = await harvester2.start_run()
    assert await _wait(control2) == "succeeded"
    assert client2.calls[0]["from"] == watermark
    assert "oai:c" in {
        r["identifier"] for r in store.catalog_page(page_size=50)["records"]
    }


async def test_deleted_record_without_metadata_is_accepted(store):
    harvester, _ = _harvester(
        store,
        [
            OaiPage(
                records=[
                    record("oai:del", "2026-01-01T00:00:01Z", deleted=True),
                ]
            )
        ],
    )
    control = await harvester.start_run()
    assert await _wait(control) == "succeeded"
    # A deletion for an unknown identifier is a no-op, not a crash.
    assert store.catalog_page(page_size=10)["total"] == 0
    assert store.published_state()["watermark"] == "2026-01-01T00:00:10Z"


async def test_duplicate_token_fails_instead_of_looping(store):
    harvester, _ = _harvester(
        store,
        [
            OaiPage(token="same", records=[record("oai:a", "2026-01-01T00:00:01Z")]),
            OaiPage(token="same", records=[record("oai:b", "2026-01-01T00:00:02Z")]),
        ],
    )
    control = await harvester.start_run()
    assert await _wait(control) == "failed"
    assert "重复" in (store.get_run(control.run_id)["last_error"] or "")
    assert store.published_state()["watermark"] is None


async def test_result_set_change_mid_pagination_restarts_window(store):
    """A growing result set makes cursor pagination skip records; retry."""

    class ChangingClient(ScriptedClient):
        async def list_records(self, *, from_, until, token=None):
            self.calls.append({"from": from_, "until": until, "token": token})
            if len(self.calls) == 1:
                return OaiPage(
                    token="t1",
                    complete_list_size=2,
                    records=[record("oai:chg:1", "2026-01-01T00:00:01Z")],
                )
            if len(self.calls) == 2:
                return OaiPage(
                    token="t2",
                    complete_list_size=3,  # grew between pages
                    records=[record("oai:chg:2", "2026-01-01T00:00:02Z")],
                )
            if len(self.calls) == 3:  # retry from page 1
                return OaiPage(
                    token="t3",
                    complete_list_size=3,
                    records=[record("oai:chg:1", "2026-01-01T00:00:01Z")],
                )
            if len(self.calls) == 4:
                return OaiPage(
                    token="t4",
                    complete_list_size=3,
                    records=[record("oai:chg:2", "2026-01-01T00:00:02Z")],
                )
            return OaiPage(
                complete_list_size=3,
                records=[record("oai:chg:3", "2026-01-01T00:00:03Z")],
            )

    changing = ChangingClient([])
    harvester = Harvester(
        store,
        HarvestConfig(oai_url="scripted", max_attempts=3, retry_backoff_ms=0),
        client_factory=lambda: changing,
    )
    control = await harvester.start_run()
    assert await _wait(control) == "succeeded"
    run = store.get_run(control.run_id)
    assert run["token_restarts"] == 1
    # Retry restarted at page 1 with the same window.
    assert changing.calls[2]["from"] == changing.calls[0]["from"]
    assert changing.calls[2]["token"] is None
    assert run["records_seen"] == 3


async def test_inverted_source_clock_fails(store):
    from app.store import Store

    # Pretend a previous round published a watermark in the future.
    run = store.create_run()
    store.set_window(run["id"], "2026-01-01T00:00:00Z")
    store.stage_records(
        run["id"],
        [
            {
                "identifier": "seed",
                "title": "seed",
                "creator": None,
                "datestamp": "2025-12-31T00:00:00Z",
                "deleted": False,
            }
        ],
        0,
    )
    assert store.commit_run(run["id"])["committed"]

    harvester, client = _harvester(store, [OaiPage(records=[])], source_now="2025-01-01T00:00:00Z")
    control = await harvester.start_run()
    assert await _wait(control) == "failed"
    assert "倒置" in (store.get_run(control.run_id)["last_error"] or "")
    assert client.calls == []  # never listed anything
