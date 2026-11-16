"""HTTP/UI tests for the revision workspace."""

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

    return create_app(
        db_path=tmp_path / "web_revisions.sqlite3",
        oai_url="http://demo.local/oai",
        client_factory=factory,
    )


def poll_until(client: TestClient, predicate, attempts: int = 150) -> dict:
    import time

    payload = {}
    for _ in range(attempts):
        payload = client.get("/api/status").json()
        if predicate(payload):
            return payload
        time.sleep(0.05)
    raise AssertionError(f"condition not met; last={payload}")


def harvest(client: TestClient) -> None:
    assert client.post("/api/harvest").status_code == 202
    poll_until(client, lambda p: p["active"] is None and p["published"]["watermark"])


def test_revision_pages_and_api(web_app):
    with TestClient(web_app) as client:
        harvest(client)
        page = client.get("/revisions")
        assert page.status_code == 200
        assert "书目校订" in page.text
        assert "冲突" in page.text

        record = client.get("/api/revisions/oai:demo:0001").json()
        assert record["source"]["title"] == "数字人文中的文本挖掘方法"
        assert record["title"] == record["source"]["title"]
        assert record["revision"] is None

        editor = client.get("/revisions/oai:demo:0001")
        assert editor.status_code == 200
        assert "数字人文中的文本挖掘方法" in editor.text
        assert "保存校订" in editor.text

        response = client.post(
            "/api/revisions/oai:demo:0001",
            data={
                "title": "校订后的标题",
                "creator": "",
                "fields": ["title"],
                "base_source_version": record["source_version"],
                "base_revision_version": 0,
                "expect_revision": "0",
            },
        )
        assert response.status_code == 200, response.text
        saved = response.json()
        assert saved["title"] == "校订后的标题"
        assert saved["source"]["title"] == "数字人文中的文本挖掘方法"
        assert saved["revision"]["title"] == "校订后的标题"

        # The catalogue API exposes both values.
        item = client.get("/api/revisions/oai:demo:0001").json()
        assert item["title"] == "校订后的标题"
        assert item["source"]["title"] == "数字人文中的文本挖掘方法"

        listing = client.get("/api/revisions").json()
        assert listing["revision_total"] == 1
        text = client.get("/revisions").text
        assert "已校订" in text


def test_stale_submission_is_rejected_as_a_whole_and_shows_diff(web_app, repo):
    with TestClient(web_app) as client:
        harvest(client)
        record = client.get("/api/revisions/oai:demo:0002").json()
        # Someone else saves a revision after this form was opened.
        other = client.post(
            "/api/revisions/oai:demo:0002",
            data={
                "title": "别人的标题",
                "creator": "",
                "fields": ["title"],
                "base_source_version": record["source_version"],
                "base_revision_version": 0,
                "expect_revision": "0",
            },
        )
        assert other.status_code == 200

        stale = client.post(
            "/api/revisions/oai:demo:0002",
            data={
                "title": "我的标题",
                "creator": "",
                "fields": ["title"],
                "base_source_version": record["source_version"],
                "base_revision_version": 0,
                "expect_revision": "0",
            },
        )
        assert stale.status_code == 409
        body = stale.json()
        assert body["rejected"] is True
        assert body["diff"]["revision"]["title"] == "别人的标题"
        assert client.get("/api/revisions/oai:demo:0002").json()["title"] == "别人的标题"

        # The HTML form also shows the new diff and rejects the whole edit.
        html = client.post(
            "/revisions/oai:demo:0002",
            data={
                "title": "我的标题",
                "creator": "我的作者",
                "fields": ["title", "creator"],
                "base_source_version": record["source_version"],
                "base_revision_version": 0,
                "expect_revision": "0",
                "action": "save",
            },
        )
        assert html.status_code == 409
        assert "整次提交已拒绝" in html.text
        assert "别人的标题" in html.text
        current = client.get("/api/revisions/oai:demo:0002").json()
        assert current["title"] == "别人的标题"
        assert not current["revision"]["edited"]["creator"]


def test_conflict_visible_in_page_and_resolved_field_by_field(web_app, repo):
    with TestClient(web_app) as client:
        harvest(client)
        record = client.get("/api/revisions/oai:demo:0003").json()
        client.post(
            "/api/revisions/oai:demo:0003",
            data={
                "title": "馆员校订",
                "creator": "馆员作者",
                "fields": ["title", "creator"],
                "base_source_version": record["source_version"],
                "base_revision_version": 0,
                "expect_revision": "0",
            },
        )
        import time

        time.sleep(1.05)
        repo.touch_record("oai:demo:0003", title="源端更正")
        harvest(client)

        conflicted = client.get("/api/revisions/oai:demo:0003").json()
        assert conflicted["title"] == "馆员校订"
        assert conflicted["source"]["title"] == "源端更正"
        assert conflicted["conflicts"]["title"] is True
        assert client.get("/revisions").text.count("冲突") >= 1
        editor = client.get("/revisions/oai:demo:0003").text
        assert "采用源值" in editor and "采用校订值" in editor

        resolved = client.post(
            "/api/revisions/oai:demo:0003/resolve",
            data={
                "title_choice": "source",
                "creator_choice": "revision",
                "base_source_version": conflicted["source_version"],
                "base_revision_version": conflicted["revision"]["version"],
            },
        )
        assert resolved.status_code == 200, resolved.text
        final = resolved.json()
        assert final["title"] == "源端更正"
        assert final["creator"] == "馆员作者"
        assert final["conflicts"] == {"title": False, "creator": False}


def test_withdraw_then_restore_drops_revision(web_app, repo):
    with TestClient(web_app) as client:
        harvest(client)
        record = client.get("/api/revisions/oai:demo:0004").json()
        client.post(
            "/api/revisions/oai:demo:0004",
            data={
                "title": "将被撤回的校订",
                "creator": "",
                "fields": ["title"],
                "base_source_version": record["source_version"],
                "base_revision_version": 0,
                "expect_revision": "0",
            },
        )
        import time

        time.sleep(1.05)
        repo.withdraw("oai:demo:0004")
        harvest(client)
        assert client.get("/api/revisions/oai:demo:0004").status_code == 404
        assert client.get("/revisions/oai:demo:0004").status_code == 404

        time.sleep(1.05)
        repo.restore("oai:demo:0004")
        harvest(client)
        restored = client.get("/api/revisions/oai:demo:0004").json()
        assert restored["title"] == "已恢复记录 oai:demo:0004"
        assert restored["revision"] is None


def test_html_editor_saves_revision_and_take_source(web_app):
    with TestClient(web_app) as client:
        harvest(client)
        record = client.get("/api/revisions/oai:demo:0007").json()
        response = client.post(
            "/revisions/oai:demo:0007",
            data={
                "title": "网页保存的校订",
                "creator": "网页作者",
                "fields": ["title", "creator"],
                "base_source_version": record["source_version"],
                "base_revision_version": 0,
                "expect_revision": "0",
                "expect_source_title": record["source"]["title"],
                "expect_source_creator": record["source"]["creator"],
                "expect_revision_title": "",
                "expect_revision_creator": "",
                "action": "save",
            },
            follow_redirects=False,
        )
        assert response.status_code == 303
        saved = client.get("/api/revisions/oai:demo:0007").json()
        assert saved["title"] == "网页保存的校订"
        assert saved["creator"] == "网页作者"
        assert saved["source"]["title"] == "科研数据管理服务的组织模式"

        # "Give up the revision and take all source values".
        discarded = client.post(
            "/revisions/oai:demo:0007",
            data={
                "base_source_version": saved["source_version"],
                "base_revision_version": saved["revision"]["version"],
                "expect_revision": "1",
                "expect_source_title": saved["source"]["title"],
                "expect_source_creator": saved["source"]["creator"],
                "expect_revision_title": saved["revision"]["title"],
                "expect_revision_creator": saved["revision"]["creator"],
                "action": "take_source",
            },
            follow_redirects=False,
        )
        assert discarded.status_code == 303
        final = client.get("/api/revisions/oai:demo:0007").json()
        assert final["title"] == final["source"]["title"]
        assert final["revision"] is None


def test_empty_title_is_rejected_by_api(web_app):
    with TestClient(web_app) as client:
        harvest(client)
        record = client.get("/api/revisions/oai:demo:0008").json()
        response = client.post(
            "/api/revisions/oai:demo:0008",
            data={
                "title": "   ",
                "creator": "",
                "fields": ["title"],
                "base_source_version": record["source_version"],
                "base_revision_version": 0,
                "expect_revision": "0",
            },
        )
        assert response.status_code == 400
        assert client.get("/api/revisions/oai:demo:0008").json()["revision"] is None
