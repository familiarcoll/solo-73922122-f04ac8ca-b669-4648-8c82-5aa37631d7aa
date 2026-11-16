"""Catalogue revisions: field-level three-way merge, conflicts and durability.

These tests drive :class:`app.store.Store` directly so the source's datestamps
can be chosen exactly; that is what makes same-second corrections testable.
"""

from __future__ import annotations

import asyncio

from app.store import RevisionRejected, Store


def publish(store: Store, records: list[dict], until: str) -> dict:
    run = store.create_run()
    store.set_window(run["id"], until)
    store.stage_records(run["id"], records, 0)
    return store.commit_run(run["id"])


def rec(identifier="oai:rev:1", title="源标题", creator="源作者",
        datestamp="2026-01-01T00:00:00Z"):
    return {
        "identifier": identifier,
        "title": title,
        "creator": creator,
        "datestamp": datestamp,
        "deleted": False,
    }


def edit(store: Store, identifier="oai:rev:1", *, title=None, creator=None,
         fields=("title",)):
    view = store.record_editor_view(identifier)
    revision = view["revision"]
    return store.save_revision(
        identifier,
        title=title,
        creator=creator,
        fields=list(fields),
        base_source_version=view["source_version"],
        base_revision_version=revision["version"] if revision else 0,
        expect_revision=revision is not None,
    )


def test_source_values_are_stored_separately_and_catalogue_shows_merged(store):
    publish(store, [rec()], "2026-01-01T00:00:00Z")
    view = store.record_editor_view("oai:rev:1")
    assert view["source"] == {"title": "源标题", "creator": "源作者"}
    assert view["title"] == "源标题"  # current catalogue = initial source

    merged = edit(store, title="校订标题")
    assert merged["title"] == "校订标题"
    assert merged["source"]["title"] == "源标题"
    assert merged["revision"]["title"] == "校订标题"

    item = store.catalog_page(page_size=10)["records"][0]
    assert item["title"] == "校订标题" and item["source_title"] == "源标题"
    api_page = store.catalog_editor_page()
    assert api_page["records"][0]["title"] == "校订标题"
    assert api_page["revision_total"] == 1


def test_only_revision_changed_uses_revision(store):
    publish(store, [rec()], "2026-01-01T00:00:00Z")
    edit(store, title="校订标题")
    # Re-publishing the unchanged source (inclusive boundary) keeps revision.
    result = publish(store, [rec()], "2026-01-01T00:00:01Z")
    assert result["new_conflicts"] == 0
    view = store.record_editor_view("oai:rev:1")
    assert view["title"] == "校订标题"
    assert view["conflicts"] == {"title": False, "creator": False}
    assert view["origins"]["title"] == "revision"


def test_only_source_changed_uses_source(store):
    publish(store, [rec()], "2026-01-01T00:00:00Z")
    # A revision that merely repeats the source value...
    edit(store, title="源标题")
    # ...must not block a later source correction.
    publish(store, [rec(title="源端新标题")], "2026-01-01T00:00:02Z")
    view = store.record_editor_view("oai:rev:1")
    assert view["title"] == "源端新标题"
    assert view["source"]["title"] == "源端新标题"
    assert view["conflicts"]["title"] is False
    # The empty revision row is cleaned up.
    assert view["revision"] is None
    assert store.catalog_editor_page()["revision_total"] == 0


def test_both_changed_to_same_value_merges_without_conflict(store):
    publish(store, [rec()], "2026-01-01T00:00:00Z")
    edit(store, title="共同新标题")
    result = publish(store, [rec(title="共同新标题")], "2026-01-01T00:00:03Z")
    assert result["new_conflicts"] == 0
    view = store.record_editor_view("oai:rev:1")
    assert view["title"] == "共同新标题"
    assert view["conflicts"]["title"] is False
    assert view["origins"]["title"] == "merged"


def test_both_changed_differently_keeps_revision_and_flags_conflict(store):
    publish(store, [rec()], "2026-01-01T00:00:00Z")
    edit(store, title="校订标题")
    result = publish(store, [rec(title="源端更正标题")], "2026-01-01T00:00:04Z")
    assert result["new_conflicts"] == 1
    view = store.record_editor_view("oai:rev:1")
    assert view["title"] == "校订标题"  # revision kept
    assert view["source"]["title"] == "源端更正标题"
    assert view["conflicts"] == {"title": True, "creator": False}
    assert store.catalog_page(page_size=10)["records"][0]["title"] == "校订标题"
    page = store.catalog_editor_page()
    assert page["conflict_total"] == 1


def test_other_fields_and_record_update_normally_around_a_conflict(store):
    publish(store, [rec()], "2026-01-01T00:00:00Z")
    edit(store, title="校订标题", fields=("title",))
    result = publish(
        store,
        [rec(title="源端更正标题", creator="源端新作者")],
        "2026-01-01T00:00:05Z",
    )
    assert result["new_conflicts"] == 1
    view = store.record_editor_view("oai:rev:1")
    assert view["title"] == "校订标题"
    assert view["conflicts"]["title"] is True
    # The creator had no revision, so the new source value is shown.
    assert view["creator"] == "源端新作者"
    assert view["source"]["creator"] == "源端新作者"


def test_repeated_identical_source_creates_no_new_conflict(store):
    publish(store, [rec()], "2026-01-01T00:00:00Z")
    edit(store, title="校订标题")
    first = publish(store, [rec(title="源端更正标题")], "2026-01-01T00:00:06Z")
    assert first["new_conflicts"] == 1
    before = store.record_editor_view("oai:rev:1")

    # The next window starts at the previous watermark, so the boundary
    # second is read again with identical content.
    second = publish(store, [rec(title="源端更正标题")], "2026-01-01T00:00:07Z")
    assert second["new_conflicts"] == 0
    after = store.record_editor_view("oai:rev:1")
    assert after["conflicts"] == before["conflicts"]
    assert after["source_version"] == before["source_version"]
    assert after["revision"]["version"] == before["revision"]["version"]


def test_same_second_correction_invalidates_submission(store):
    publish(store, [rec(title="A")], "2026-01-01T00:00:00Z")
    opened = store.record_editor_view("oai:rev:1")
    # Source content changes but keeps the SAME OAI datestamp (one-second
    # granularity).  Content versioning must still detect it.
    publish(store, [rec(title="B")], "2026-01-01T00:00:00Z")
    assert (
        store.record_editor_view("oai:rev:1")["source_version"]
        > opened["source_version"]
    )

    try:
        store.save_revision(
            "oai:rev:1",
            title="我的校订",
            creator="",
            fields=["title"],
            base_source_version=opened["source_version"],
            base_revision_version=0,
            expect_revision=False,
            expect_source=opened["source"],
        )
    except RevisionRejected as exc:
        assert exc.status_code == 409
        assert exc.diff["source"]["title"] == "B"
    else:  # pragma: no cover
        raise AssertionError("stale submission was accepted")

    # Nothing was written.
    view = store.record_editor_view("oai:rev:1")
    assert view["title"] == "B"
    assert view["revision"] is None


def test_revision_change_invalidates_submission(store):
    publish(store, [rec()], "2026-01-01T00:00:00Z")
    opened = store.record_editor_view("oai:rev:1")
    edit(store, title="别人的校订")
    try:
        store.save_revision(
            "oai:rev:1",
            title="我的校订",
            creator="",
            fields=["title"],
            base_source_version=opened["source_version"],
            base_revision_version=0,
            expect_revision=False,
        )
    except RevisionRejected as exc:
        assert exc.diff["revision"]["title"] == "别人的校订"
    else:  # pragma: no cover
        raise AssertionError("stale revision submission was accepted")
    assert store.record_editor_view("oai:rev:1")["title"] == "别人的校订"


def test_withdraw_abolishes_revisions_and_restore_starts_from_source(store):
    publish(store, [rec()], "2026-01-01T00:00:00Z")
    edit(store, title="校订标题")
    # Produce a conflict too.
    publish(store, [rec(title="源端更正")], "2026-01-01T00:00:08Z")
    assert store.record_editor_view("oai:rev:1")["conflicts"]["title"] is True

    withdrawn = {"identifier": "oai:rev:1", "title": None, "creator": None,
                 "datestamp": "2026-01-01T00:00:09Z", "deleted": True}
    publish(store, [withdrawn], "2026-01-01T00:00:09Z")
    assert store.record_editor_view("oai:rev:1") is None
    with store._lock:
        assert store._conn.execute(
            "SELECT COUNT(*) AS n FROM revisions WHERE identifier = ?",
            ("oai:rev:1",),
        ).fetchone()["n"] == 0

    # Restore: the old revision must not come back.
    publish(store, [rec(title="恢复后的源标题")], "2026-01-01T00:00:10Z")
    view = store.record_editor_view("oai:rev:1")
    assert view["title"] == "恢复后的源标题"
    assert view["revision"] is None
    assert view["conflicts"] == {"title": False, "creator": False}


def test_resolve_conflict_per_field_choosing_source_or_revision(store):
    publish(store, [rec()], "2026-01-01T00:00:00Z")
    edit(store, title="校订标题", creator="校订作者", fields=("title", "creator"))
    publish(
        store,
        [rec(title="源端更正标题", creator="源端更正作者")],
        "2026-01-01T00:00:11Z",
    )
    view = store.record_editor_view("oai:rev:1")
    assert view["conflicts"] == {"title": True, "creator": True}

    merged = store.save_revision(
        "oai:rev:1",
        title=view["revision"]["title"],
        creator=view["revision"]["creator"],
        fields=[],
        choices={"title": "source", "creator": "revision"},
        base_source_version=view["source_version"],
        base_revision_version=view["revision"]["version"],
        expect_revision=True,
    )
    assert merged["title"] == "源端更正标题"
    assert merged["creator"] == "校订作者"
    assert merged["conflicts"] == {"title": False, "creator": False}
    assert merged["origins"] == {"title": "source", "creator": "revision"}

    # Choosing the source for the only remaining edited field drops the
    # revision row entirely.
    store.save_revision(
        "oai:rev:1",
        title="",
        creator="",
        fields=[],
        choices={"creator": "source"},
        base_source_version=merged["source_version"],
        base_revision_version=merged["revision"]["version"],
        expect_revision=True,
    )
    final = store.record_editor_view("oai:rev:1")
    assert final["revision"] is None
    assert final["creator"] == "源端更正作者"


def test_failed_and_cancelled_rounds_leave_revisions_conflicts_and_watermark(store):
    publish(store, [rec()], "2026-01-01T00:00:00Z")
    edit(store, title="校订标题")
    publish(store, [rec(title="源端更正")], "2026-01-01T00:00:12Z")
    baseline_view = store.record_editor_view("oai:rev:1")
    baseline_state = store.published_state()
    baseline_items = store.catalog_page(page_size=20)["records"]

    failed = store.create_run()
    store.set_window(failed["id"], "2026-01-01T00:00:13Z")
    store.stage_records(failed["id"], [rec(title="不会发布")], 0)
    store.finish_failed(failed["id"], "boom")
    assert store.record_editor_view("oai:rev:1") == baseline_view
    assert store.published_state() == baseline_state
    assert store.catalog_page(page_size=20)["records"] == baseline_items

    cancelled = store.create_run()
    store.set_window(cancelled["id"], "2026-01-01T00:00:14Z")
    store.stage_records(cancelled["id"], [rec(title="也不会发布")], 0)
    store.cancel_run(cancelled["id"], "test")
    assert store.record_editor_view("oai:rev:1") == baseline_view
    assert store.published_state() == baseline_state


def test_revisions_and_conflicts_survive_restart(tmp_path):
    db = tmp_path / "persist.sqlite3"
    first = Store(db)
    publish(first, [rec()], "2026-01-01T00:00:00Z")
    edit(first, title="校订标题")
    publish(first, [rec(title="源端更正")], "2026-01-01T00:00:15Z")
    before = first.record_editor_view("oai:rev:1")
    first.close()

    second = Store(db)
    after = second.record_editor_view("oai:rev:1")
    assert after == before
    assert after["title"] == "校订标题"
    assert after["conflicts"]["title"] is True
    second.close()


def test_legacy_catalogue_is_migrated_with_published_values_as_source(tmp_path):
    """An existing database (no revision columns) keeps working unchanged."""
    import sqlite3

    db = tmp_path / "legacy.sqlite3"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE catalog (
            identifier TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            creator TEXT,
            source_datestamp TEXT NOT NULL,
            harvested_at TEXT NOT NULL
        );
        INSERT INTO catalog VALUES
            ('oai:legacy:1', '旧目录标题', '旧作者',
             '2025-12-31T00:00:00Z', '2026-01-01T00:00:00Z');
        """
    )
    conn.commit()
    conn.close()

    store = Store(db)
    view = store.record_editor_view("oai:legacy:1")
    assert view["title"] == "旧目录标题"
    assert view["source"]["title"] == "旧目录标题"
    assert view["source"]["creator"] == "旧作者"
    assert view["revision"] is None
    assert view["conflicts"] == {"title": False, "creator": False}

    # The upgraded record can immediately take a revision and merge normally.
    edit(store, "oai:legacy:1", title="迁移后的校订")
    publish(
        store,
        [rec("oai:legacy:1", title="源端更正")],
        "2026-01-01T00:00:00Z",
    )
    after = store.record_editor_view("oai:legacy:1")
    assert after["title"] == "迁移后的校订"
    assert after["conflicts"]["title"] is True
    store.close()


def test_same_second_demo_correction_is_detected(repo):
    """The demo source can change content without moving its datestamp."""
    from datetime import datetime, timezone

    before = repo.raw_record("oai:demo:0001")
    repo.touch_record(
        "oai:demo:0001",
        title="同秒更正标题",
        datestamp=datetime.strptime(
            before["datestamp"], "%Y-%m-%dT%H:%M:%SZ"
        ).replace(tzinfo=timezone.utc),
    )
    after = repo.raw_record("oai:demo:0001")
    assert after["datestamp"] == before["datestamp"]
    assert after["title"] == "同秒更正标题"


async def test_same_second_source_correction_is_harvested_and_rejects_stale_form(store):
    """A content change within the same OAI second is still a change.

    The window is inclusive at both ends, so re-requesting
    ``[watermark, watermark]`` returns the record again.  Publishing it bumps
    the content version even though the datestamp did not move.
    """
    from app.harvest import HarvestConfig, Harvester
    from app.oai import OaiPage, OaiRecord

    class Client:
        def __init__(self) -> None:
            self.title = "同秒前标题"
            self.now = "2026-01-01T00:00:10Z"

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc_info):
            return False

        async def identify_time(self):
            return self.now

        async def list_records(self, *, from_, until, token=None):
            return OaiPage(
                records=[
                    OaiRecord(
                        identifier="oai:same:1",
                        datestamp=self.now,
                        deleted=False,
                        title=self.title,
                        creator="作者",
                    )
                ]
            )

    client = Client()
    harvester = Harvester(
        store,
        HarvestConfig(oai_url="scripted", max_attempts=3, retry_backoff_ms=0),
        client_factory=lambda: client,
    )

    async def run() -> None:
        control = await harvester.start_run()
        try:
            await asyncio.wait_for(control.task, 10)
        except asyncio.CancelledError:
            pass
        await asyncio.wait_for(control.done.wait(), 10)
        assert control.outcome == "succeeded", store.get_run(control.run_id)

    await run()
    opened = store.record_editor_view("oai:same:1")
    assert opened["source"]["title"] == "同秒前标题"

    # Same second, new content.
    client.title = "同秒更正标题"
    await run()
    current = store.record_editor_view("oai:same:1")
    assert current["source"]["title"] == "同秒更正标题"
    assert current["source_version"] > opened["source_version"]
    assert current["source_datestamp"] == opened["source_datestamp"]

    try:
        store.save_revision(
            "oai:same:1",
            title="馆员校订",
            creator="",
            fields=["title"],
            base_source_version=opened["source_version"],
            base_revision_version=0,
            expect_revision=False,
            expect_source=opened["source"],
        )
    except RevisionRejected:
        pass
    else:  # pragma: no cover
        raise AssertionError("stale form was accepted")

    # A freshly opened form works.
    assert edit(store, "oai:same:1", title="馆员校订")["title"] == "馆员校订"


async def test_revision_saved_during_harvest_is_not_overwritten(
    harvester, store, repo
):
    first = await harvester.start_run()
    from conftest import wait_for_outcome

    await wait_for_outcome(first)
    baseline_source = store.record_editor_view("oai:demo:0001")["source"]

    # Correct one record at the source and make the next round slow enough to
    # submit a revision while it is fetching.
    await asyncio.sleep(1.05)
    repo.touch_record("oai:demo:0001", title="源端更正标题")
    repo.set_meta("slow_seconds", "2")
    control = await harvester.start_run()
    await asyncio.sleep(0.2)
    view = store.record_editor_view("oai:demo:0001")
    assert view["source"] == baseline_source  # the new value is not published yet
    store.save_revision(
        "oai:demo:0001",
        title="采集中提交的校订",
        creator="",
        fields=["title"],
        base_source_version=view["source_version"],
        base_revision_version=0,
        expect_revision=False,
    )
    repo.set_meta("slow_seconds", "0")
    assert await wait_for_outcome(control) == "succeeded"

    after = store.record_editor_view("oai:demo:0001")
    # Source changed and the revision changed differently: revision kept,
    # source visible separately, conflict flagged.
    assert after["title"] == "采集中提交的校订"
    assert after["source"]["title"] == "源端更正标题"
    assert after["conflicts"]["title"] is True


def test_empty_title_revision_is_rejected(store):
    publish(store, [rec()], "2026-01-01T00:00:00Z")
    view = store.record_editor_view("oai:rev:1")
    try:
        store.save_revision(
            "oai:rev:1",
            title="   ",
            creator="",
            fields=["title"],
            base_source_version=view["source_version"],
            base_revision_version=0,
            expect_revision=False,
        )
    except RevisionRejected as exc:
        assert exc.status_code == 400
    else:  # pragma: no cover
        raise AssertionError("empty title accepted")
    assert store.record_editor_view("oai:rev:1")["title"] == "源标题"


async def test_withdrawal_in_the_same_commit_abolishes_an_in_flight_revision(
    harvester, store, repo
):
    """A revision submitted while a round runs dies with the withdrawal."""
    from conftest import wait_for_outcome

    first = await harvester.start_run()
    await wait_for_outcome(first)
    view = store.record_editor_view("oai:demo:0002")

    await asyncio.sleep(1.05)
    repo.withdraw("oai:demo:0002")
    repo.set_meta("slow_seconds", "2")
    control = await harvester.start_run()
    await asyncio.sleep(0.2)
    store.save_revision(
        "oai:demo:0002",
        title="采集中提交但即将被撤回的校订",
        creator="",
        fields=["title"],
        base_source_version=view["source_version"],
        base_revision_version=0,
        expect_revision=False,
    )
    repo.set_meta("slow_seconds", "0")
    assert await wait_for_outcome(control) == "succeeded"

    assert store.record_editor_view("oai:demo:0002") is None
    with store._lock:
        assert store._conn.execute(
            "SELECT COUNT(*) AS n FROM revisions WHERE identifier = ?",
            ("oai:demo:0002",),
        ).fetchone()["n"] == 0


def test_restore_after_revision_does_not_reuse_it(store):
    publish(store, [rec()], "2026-01-01T00:00:00Z")
    edit(store, title="旧校订")
    publish(
        store,
        [{"identifier": "oai:rev:1", "title": None, "creator": None,
          "datestamp": "2026-01-01T00:00:20Z", "deleted": True}],
        "2026-01-01T00:00:20Z",
    )
    # Restore brings back a *different* source value; the old revision must
    # not shadow it.
    publish(store, [rec(title="恢复后的新源值")], "2026-01-01T00:00:21Z")
    view = store.record_editor_view("oai:rev:1")
    assert view["title"] == "恢复后的新源值"
    assert view["revision"] is None
    assert view["conflicts"] == {"title": False, "creator": False}
