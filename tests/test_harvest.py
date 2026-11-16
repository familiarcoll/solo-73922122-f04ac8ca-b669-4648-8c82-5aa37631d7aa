"""End-to-end behaviour of a harvest round against the demo repository."""

from __future__ import annotations

import asyncio

from conftest import wait_for_outcome


def catalog_item(store, identifier: str) -> dict | None:
    for item in store.catalog_page(page_size=200)["records"]:
        if item["identifier"] == identifier:
            return item
    return None


async def test_first_round_is_full_and_sets_watermark(harvester, store, repo):
    expected = len(repo.all_records())
    control = await harvester.start_run()
    assert await wait_for_outcome(control) == "succeeded"

    run = store.get_run(control.run_id)
    assert run["status"] == "succeeded"
    assert run["window_from"] is None  # first round has no lower bound
    assert run["window_until"]
    assert run["pages_fetched"] == 4  # 10 records at page_size=3
    assert run["staged_count"] == 0  # staging cleaned up after publish
    assert store.published_state()["watermark"] == run["window_until"]
    assert len(store.catalog_page(page_size=200)["records"]) == expected


async def test_incremental_window_is_inclusive_and_starts_at_watermark(harvester, store, repo):
    first = await harvester.start_run()
    await wait_for_outcome(first)
    watermark = store.published_state()["watermark"]

    # A new record arrives.  Wait until a later second so it is inside the
    # window that starts at the previous watermark.
    await asyncio.sleep(1.05)
    repo.add_record(title="新增的论文", creator="新作者")

    second = await harvester.start_run()
    assert await wait_for_outcome(second) == "succeeded"
    run = store.get_run(second.run_id)
    assert run["window_from"] == watermark  # inclusive lower bound
    assert catalog_item(store, "oai:demo:0001") is not None  # nothing lost


async def test_unchanged_catalog_yields_empty_window_but_advances_watermark(harvester, store):
    first = await harvester.start_run()
    await wait_for_outcome(first)
    watermark = store.published_state()["watermark"]

    await asyncio.sleep(1.05)
    second = await harvester.start_run()
    assert await wait_for_outcome(second) == "succeeded"
    run = store.get_run(second.run_id)
    assert run["records_seen"] == 0
    assert store.published_state()["watermark"] > watermark
    assert store.published_state()["success_count"] == 2


async def test_deleted_marker_removes_old_entry(harvester, store, repo):
    first = await harvester.start_run()
    await wait_for_outcome(first)
    assert catalog_item(store, "oai:demo:0003") is not None

    await asyncio.sleep(1.05)
    repo.withdraw("oai:demo:0003")
    second = await harvester.start_run()
    assert await wait_for_outcome(second) == "succeeded"

    assert catalog_item(store, "oai:demo:0003") is None
    # other records untouched
    assert catalog_item(store, "oai:demo:0001") is not None


async def test_correction_updates_existing_entry(harvester, store, repo):
    first = await harvester.start_run()
    await wait_for_outcome(first)

    await asyncio.sleep(1.05)
    repo.touch_record("oai:demo:0002", title="更正后的标题")
    second = await harvester.start_run()
    assert await wait_for_outcome(second) == "succeeded"

    item = catalog_item(store, "oai:demo:0002")
    assert item is not None
    assert item["title"] == "更正后的标题"
    # exactly one row per identifier
    ids = [i["identifier"] for i in store.catalog_page(page_size=200)["records"]]
    assert len(ids) == len(set(ids))


async def test_token_expiry_restarts_original_window_without_mixing(harvester, store, repo):
    """A bad token discards the whole attempt and re-requests [from, until]."""
    repo.set_meta("token_failures", "1")

    # page size 1 forces several pages; the failure happens on the first token.
    repo.set_meta("page_size", "1")
    total = len(repo.all_records())

    control = await harvester.start_run()
    assert await wait_for_outcome(control) == "succeeded"

    run = store.get_run(control.run_id)
    assert run["token_restarts"] == 1
    # A full clean re-fetch: exactly one page per record, no duplicates mixed
    # from the abandoned attempt.
    assert run["pages_fetched"] == total
    assert len(store.catalog_page(page_size=200)["records"]) == total


async def test_repeated_token_failures_fail_round_without_publishing(harvester, store, repo):
    first = await harvester.start_run()
    await wait_for_outcome(first)
    published_before = store.published_state()
    catalog_before = store.catalog_page(page_size=200)

    repo.set_meta("page_size", "1")
    repo.set_meta("token_failures", "10")
    await asyncio.sleep(1.05)
    repo.add_record(title="这条不会被发布")
    repo.add_record(title="这条也不会被发布")
    repo.add_record(title="这条同样不会")

    control = await harvester.start_run()
    assert await wait_for_outcome(control) == "failed"

    run = store.get_run(control.run_id)
    assert run["status"] == "failed"
    assert "令牌" in (run["last_error"] or "")
    published_after = store.published_state()
    assert published_after["watermark"] == published_before["watermark"]
    assert published_after["catalog_size"] == published_before["catalog_size"]
    assert published_after["success_count"] == published_before["success_count"]
    assert store.catalog_page(page_size=200)["records"] == catalog_before["records"]


async def test_duplicate_identifier_keeps_newest_single_row(store, harvester):
    """Staging keeps exactly one row per identifier, newest datestamp wins."""
    run_id = store.create_run()["id"]
    store.set_window(run_id, "2026-01-01T00:00:00Z")
    store.stage_records(
        run_id,
        [
            {
                "identifier": "dup",
                "title": "旧标题",
                "creator": None,
                "datestamp": "2026-01-01T00:00:00Z",
                "deleted": False,
            }
        ],
        0,
    )
    store.stage_records(
        run_id,
        [
            {
                "identifier": "dup",
                "title": "新标题",
                "creator": None,
                "datestamp": "2026-01-01T00:00:01Z",
                "deleted": False,
            }
        ],
        100,
    )
    rows = store._conn.execute(
        "SELECT * FROM run_records WHERE run_id = ?", (run_id,)
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["title"] == "新标题"
    store.finish_failed(run_id, "test cleanup")
