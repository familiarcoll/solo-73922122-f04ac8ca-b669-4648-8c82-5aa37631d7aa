"""In-memory OAI-PMH demo repository used for local end-to-end demos.

It supports the subsets of OAI-PMH that the harvester exercises:

* ``Identify`` returns the *repository's current UTC time* as ``responseDate``;
* ``ListRecords`` with ``metadataPrefix=oai_dc`` honours inclusive
  ``from``/``until`` (second precision) and pages through ``resumptionToken``;
* records can be added, corrected or withdrawn at runtime, from a small
  control panel.

Fault injection (to exercise the harvester's edge cases):

* a configurable slow-page delay (``slow_seconds``) on every response;
* a configurable number of upcoming ``badResumptionToken`` failures
  (``token_failures``), which also invalidates all outstanding tokens;
* configurable page size and token TTL.

The demo repository persists to its own SQLite database so a restart does not
lose demonstrations.
"""

from __future__ import annotations

import asyncio
import base64
import re
import secrets
import sqlite3
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape, quoteattr

import oaitime

OAI_NS = "http://www.openarchives.org/OAI/2.0/"
DC_NS = "http://purl.org/dc/elements/1.1/"
OAI_DC_NS = "http://www.openarchives.org/OAI/2.0/oai_dc/"
XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"


def _fmt(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse(value: str, name: str) -> datetime:
    return oaitime.parse_datestamp(value)


def _identified(value: str, name: str) -> datetime:
    """Validate a bare ``YYYY-MM-DD`` or full OAI timestamp (inclusive)."""
    text = (value or "").strip()
    if not text:
        raise ValueError(f"{name} 不能为空")
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return _parse(text, name)


class DemoRepo:
    def __init__(self, db_path: str, *, page_size: int = 3) -> None:
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            db_path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS records (
                identifier TEXT PRIMARY KEY,
                title TEXT,
                creator TEXT,
                datestamp TEXT NOT NULL,
                deleted INTEGER NOT NULL DEFAULT 0,
                origin TEXT NOT NULL DEFAULT 'demo'
            )
            """
        )
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tokens (
                token TEXT PRIMARY KEY,
                window_from TEXT,
                window_until TEXT,
                cursor INTEGER NOT NULL,
                page_size INTEGER NOT NULL,
                failed INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            )
            """
        )
        self._conn.execute("INSERT OR IGNORE INTO meta(key, value) VALUES('page_size', ?)", (str(page_size),))
        self._conn.execute("INSERT OR IGNORE INTO meta(key, value) VALUES('slow_seconds', '0')")
        self._conn.execute("INSERT OR IGNORE INTO meta(key, value) VALUES('token_failures', '0')")
        self._conn.execute("INSERT OR IGNORE INTO meta(key, value) VALUES('token_ttl_seconds', '3600')")
        self._conn.execute("INSERT OR IGNORE INTO meta(key, value) VALUES('repository_name', 'Library Demo Repository')")
        self._seed()

    # ------------------------------------------------------------------ seed
    def _seed(self) -> None:
        with self._lock:
            count = self._conn.execute("SELECT COUNT(*) AS n FROM records").fetchone()["n"]
            if count:
                return
            now = datetime.now(timezone.utc).replace(microsecond=0)
            seeds = [
                ("oai:demo:0001", "数字人文中的文本挖掘方法", "王小明; 李静", 3600 * 24 * 40),
                ("oai:demo:0002", "高校图书馆数据治理实践", "陈立", 3600 * 24 * 35),
                ("oai:demo:0003", "学术机构知识库的长期保存策略", "赵敏", 3600 * 24 * 30),
                ("oai:demo:0004", "基于 OAI-PMH 的元数据互操作", "孙浩", 3600 * 24 * 25),
                ("oai:demo:0005", "开放获取期刊的引用优势分析", "周雨", 3600 * 24 * 20),
                ("oai:demo:0006", "图书馆参考咨询服务的智能化转型", "吴涛", 3600 * 24 * 15),
                ("oai:demo:0007", "科研数据管理服务的组织模式", "郑洁", 3600 * 24 * 10),
                ("oai:demo:0008", "古籍数字化中的图像检索", "钱程", 3600 * 24 * 5),
                ("oai:demo:0009", "下一代图书馆服务平台的架构设计", "冯雪", 3600 * 24 * 2),
                ("oai:demo:0010", "Altmetrics 在机构库评价中的应用", "何川", 3600),
            ]
            for identifier, title, creator, age in seeds:
                self._conn.execute(
                    """
                    INSERT INTO records(identifier, title, creator, datestamp,
                                        deleted, origin)
                    VALUES (?, ?, ?, ?, 0, 'seed')
                    """,
                    (identifier, title, creator, _fmt(now - timedelta(seconds=age))),
                )

    # ------------------------------------------------------------------ meta
    def get_meta(self, key: str, default: str | None = None) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key = ?", (key,)
            ).fetchone()
            return row["value"] if row else default

    def set_meta(self, key: str, value: str) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO meta(key, value) VALUES(?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )

    def state(self) -> dict[str, Any]:
        with self._lock:
            meta = {
                row["key"]: row["value"]
                for row in self._conn.execute("SELECT key, value FROM meta")
            }
            counts = self._conn.execute(
                """
                SELECT COUNT(*) AS total,
                       SUM(deleted) AS deleted
                  FROM records
                """
            ).fetchone()
            return {
                "page_size": int(meta.get("page_size", "3")),
                "slow_seconds": float(meta.get("slow_seconds", "0")),
                "token_failures": int(meta.get("token_failures", "0")),
                "token_ttl_seconds": int(meta.get("token_ttl_seconds", "3600")),
                "repository_name": meta.get("repository_name", "Library Demo Repository"),
                "record_total": int(counts["total"] or 0),
                "deleted_total": int(counts["deleted"] or 0),
                "now": _fmt(datetime.now(timezone.utc)),
            }

    def all_records(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT identifier, title, creator, datestamp, deleted, origin
                  FROM records ORDER BY identifier
                """
            ).fetchall()
            return [dict(r) for r in rows]

    # ------------------------------------------------------------- mutations
    def add_record(
        self,
        *,
        title: str,
        creator: str | None = None,
        identifier: str | None = None,
        datestamp: datetime | None = None,
    ) -> dict[str, Any]:
        now = datestamp or datetime.now(timezone.utc).replace(microsecond=0)
        ident = identifier or f"oai:demo:{uuid.uuid4().hex[:10]}"
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO records(identifier, title, creator, datestamp,
                                    deleted, origin)
                VALUES(?, ?, ?, ?, 0, 'demo')
                ON CONFLICT(identifier) DO UPDATE SET
                    title = excluded.title,
                    creator = excluded.creator,
                    datestamp = excluded.datestamp,
                    deleted = 0
                """,
                (ident, title, creator, _fmt(now)),
            )
            return dict(
                self._conn.execute(
                    "SELECT * FROM records WHERE identifier = ?", (ident,)
                ).fetchone()
            )

    def touch_record(self, identifier: str, *, title: str | None = None) -> dict[str, Any]:
        """Correct a record (newer datestamp), so the next harvest updates it."""
        now = _fmt(datetime.now(timezone.utc).replace(microsecond=0))
        with self._lock:
            existing = self._conn.execute(
                "SELECT * FROM records WHERE identifier = ?", (identifier,)
            ).fetchone()
            if existing is None:
                raise KeyError(identifier)
            self._conn.execute(
                """
                UPDATE records
                   SET title = COALESCE(?, title),
                       datestamp = ?
                 WHERE identifier = ?
                """,
                (title, now, identifier),
            )
            return dict(
                self._conn.execute(
                    "SELECT * FROM records WHERE identifier = ?", (identifier,)
                ).fetchone()
            )

    def withdraw(self, identifier: str) -> dict[str, Any]:
        now = _fmt(datetime.now(timezone.utc).replace(microsecond=0))
        with self._lock:
            existing = self._conn.execute(
                "SELECT * FROM records WHERE identifier = ?", (identifier,)
            ).fetchone()
            if existing is None:
                raise KeyError(identifier)
            self._conn.execute(
                """
                UPDATE records
                   SET deleted = 1,
                       datestamp = ?,
                       title = NULL,
                       creator = NULL
                 WHERE identifier = ?
                """,
                (now, identifier),
            )
            return dict(
                self._conn.execute(
                    "SELECT * FROM records WHERE identifier = ?", (identifier,)
                ).fetchone()
            )

    def restore(self, identifier: str) -> dict[str, Any]:
        with self._lock:
            existing = self._conn.execute(
                "SELECT * FROM records WHERE identifier = ?", (identifier,)
            ).fetchone()
            if existing is None:
                raise KeyError(identifier)
            self._conn.execute(
                """
                UPDATE records
                   SET deleted = 0,
                       title = COALESCE(title, '已恢复记录 ' || identifier),
                       datestamp = ?
                 WHERE identifier = ?
                """,
                (_fmt(datetime.now(timezone.utc).replace(microsecond=0)), identifier),
            )
            return dict(
                self._conn.execute(
                    "SELECT * FROM records WHERE identifier = ?", (identifier,)
                ).fetchone()
            )

    def withdraw_all(self) -> int:
        with self._lock:
            rows = self._conn.execute(
                "SELECT identifier FROM records WHERE deleted = 0"
            ).fetchall()
            for row in rows:
                self.withdraw(row["identifier"])
            return len(rows)

    # ------------------------------------------------------------------- OAI
    def _select_window(
        self, window_from: str | None, window_until: str
    ) -> list[sqlite3.Row]:
        until_dt = _identified(window_until, "until")
        lower_dt = _identified(window_from, "from") if window_from else None
        rows = self._conn.execute(
            """
            SELECT identifier, title, creator, datestamp, deleted
              FROM records
             ORDER BY identifier
            """
        ).fetchall()
        selected: list[sqlite3.Row] = []
        for row in rows:
            dt = datetime.strptime(row["datestamp"], "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=timezone.utc
            )
            if dt > until_dt:
                continue
            if lower_dt is not None and dt < lower_dt:
                continue
            selected.append(row)
        return selected

    def _issue_token(
        self,
        *,
        window_from: str | None,
        window_until: str,
        cursor: int,
        page_size: int,
    ) -> str:
        token = base64.urlsafe_b64encode(secrets.token_bytes(18)).decode().rstrip("=")
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO tokens(token, window_from, window_until, cursor,
                                   page_size, failed, created_at)
                VALUES(?, ?, ?, ?, ?, 0, ?)
                """,
                (
                    token,
                    window_from,
                    window_until,
                    cursor,
                    page_size,
                    _fmt(datetime.now(timezone.utc)),
                ),
            )
        return token

    def _consume_token(self, token: str) -> sqlite3.Row | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tokens WHERE token = ?", (token,)
            ).fetchone()
            if row is None:
                return None
            failures = int(self.get_meta("token_failures", "0") or 0)
            if failures > 0:
                # Expire every outstanding token and consume one failure.
                self._conn.execute("DELETE FROM tokens")
                self.set_meta("token_failures", str(failures - 1))
                return "failed"
            created = datetime.strptime(row["created_at"], "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=timezone.utc
            )
            ttl = int(self.get_meta("token_ttl_seconds", "3600") or 3600)
            age = (datetime.now(timezone.utc) - created).total_seconds()
            if age > ttl:
                self._conn.execute("DELETE FROM tokens WHERE token = ?", (token,))
                return None
            return row

    # ------------------------------------------------------------- XML render
    def _render_record(self, row: sqlite3.Row) -> str:
        if row["deleted"]:
            header = (
                f"<header status=\"deleted\">"
                f"<identifier>{escape(row['identifier'])}</identifier>"
                f"<datestamp>{escape(row['datestamp'])}</datestamp>"
                f"</header>"
            )
            return f"<record>{header}</record>"
        title = escape(row["title"] or "")
        creator = row["creator"]
        creator_xml = (
            f"<dc:creator>{escape(creator)}</dc:creator>" if creator else ""
        )
        return (
            "<record><header>"
            f"<identifier>{escape(row['identifier'])}</identifier>"
            f"<datestamp>{escape(row['datestamp'])}</datestamp>"
            "</header><metadata>"
            f'<oai_dc:dc xmlns:dc="{DC_NS}" xmlns:oai_dc="{OAI_DC_NS}" '
            f'xmlns:xsi="{XSI_NS}" '
            'xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ '
            'http://www.openarchives.org/OAI/2.0/oai_dc.xsd">'
            f"<dc:title>{title}</dc:title>{creator_xml}"
            "<dc:type>article</dc:type>"
            "</oai_dc:dc></metadata></record>"
        )

    def _envelope(self, body: str) -> str:
        return (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            f'<OAI-PMH xmlns="{OAI_NS}" xmlns:xsi="{XSI_NS}" '
            f'xsi:schemaLocation="{OAI_NS} '
            'http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">'
            f"<responseDate>{_fmt(datetime.now(timezone.utc))}</responseDate>"
            f"{body}</OAI-PMH>"
        )

    def _error(self, code: str, message: str, request: str | None = None) -> str:
        req = self._request(request or "ListRecords", [])
        return self._envelope(
            f"{req}<error code={quoteattr(code)}>{escape(message)}</error>"
        )

    def _request(self, verb: str, params: list[tuple[str, str]]) -> str:
        query = "&amp;".join(
            f"{escape(k)}={escape(v)}" for k, v in params if v
        )
        url = f"http://demo.local/oai?verb={escape(verb)}" + (
            f"&amp;{query}" if query else ""
        )
        return f"<request verb={quoteattr(verb)}>{url}</request>"

    # ---------------------------------------------------------------- verbs
    def identify(self) -> str:
        with self._lock:
            name = escape(self.get_meta("repository_name", "Library Demo Repository") or "")
        body = (
            self._request("Identify", [])
            + "<Identify>"
            f"<repositoryName>{name}</repositoryName>"
            "<baseURL>http://demo.local/oai</baseURL>"
            "<protocolVersion>2.0</protocolVersion>"
            "<adminEmail>demo@example.invalid</adminEmail>"
            "<earliestDatestamp>2000-01-01T00:00:00Z</earliestDatestamp>"
            "<deletedRecord>persistent</deletedRecord>"
            "<granularity>YYYY-MM-DDThh:mm:ssZ</granularity>"
            "</Identify>"
        )
        return self._envelope(body)

    def list_records(self, params: dict[str, str]) -> str:
        token = (params.get("resumptionToken") or "").strip()
        if token:
            if params.get("metadataPrefix") or params.get("from") or params.get("until"):
                return self._error(
                    "badArgument",
                    "resumptionToken 不能与 metadataPrefix/from/until 同时使用",
                )
            token_row = self._consume_token(token)
            if token_row == "failed":
                return self._error("badResumptionToken", "resumptionToken 已失效（演示故障注入）")
            if token_row is None:
                return self._error("badResumptionToken", "resumptionToken 不存在或已过期")
            window_from = token_row["window_from"]
            window_until = token_row["window_until"]
            cursor = int(token_row["cursor"])
            page_size = int(token_row["page_size"])
        else:
            metadata_prefix = params.get("metadataPrefix")
            if metadata_prefix != "oai_dc":
                return self._error(
                    "cannotDisseminateFormat",
                    "演示源仅支持 metadataPrefix=oai_dc",
                )
            try:
                if params.get("until"):
                    window_until = _identified(params["until"], "until").strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    )
                else:
                    window_until = _fmt(datetime.now(timezone.utc))
            except Exception:
                return self._error("badArgument", "until 参数不是合法时间戳")
            window_from = None
            if params.get("from"):
                try:
                    window_from = _identified(params["from"], "from").strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    )
                except Exception:
                    return self._error("badArgument", "from 参数不是合法时间戳")
                if window_from > window_until:
                    return self._error("badArgument", "from 晚于 until")
            page_size = int(self.get_meta("page_size", "3") or 3)
            cursor = 0

        selected = self._select_window(window_from, window_until)
        if not selected:
            return self._error("noRecordsMatch", "时间窗内没有记录")
        page = selected[cursor : cursor + page_size]
        records_xml = "".join(self._render_record(row) for row in page)
        next_cursor = cursor + len(page)
        token_xml = ""
        if next_cursor < len(selected):
            new_token = self._issue_token(
                window_from=window_from,
                window_until=window_until,
                cursor=next_cursor,
                page_size=page_size,
            )
            token_xml = (
                f"<resumptionToken completeListSize=\"{len(selected)}\" "
                f"cursor=\"{cursor}\">{escape(new_token)}</resumptionToken>"
            )
        req = self._request(
            "ListRecords",
            [
                ("metadataPrefix", "oai_dc"),
                ("from", window_from or ""),
                ("until", window_until),
            ],
        )
        return self._envelope(
            f"{req}<ListRecords>{records_xml}{token_xml}</ListRecords>"
        )

    def dispatch(self, verb: str, params: dict[str, str]) -> str:
        if verb == "Identify":
            return self.identify()
        if verb == "ListRecords":
            return self.list_records(params)
        if verb == "ListMetadataFormats":
            return self._envelope(
                self._request("ListMetadataFormats", [])
                + "<ListMetadataFormats><metadataFormat>"
                "<metadataPrefix>oai_dc</metadataPrefix>"
                f"<schema>{OAI_DC_NS}oai_dc.xsd</schema>"
                f"<metadataNamespace>{DC_NS}</metadataNamespace>"
                "</metadataFormat></ListMetadataFormats>"
            )
        return self._error("badVerb", f"不支持的 verb: {verb}")


async def apply_slow_delay(repo: DemoRepo) -> None:
    delay = float(repo.get_meta("slow_seconds", "0") or 0)
    if delay > 0:
        await asyncio.sleep(delay)
