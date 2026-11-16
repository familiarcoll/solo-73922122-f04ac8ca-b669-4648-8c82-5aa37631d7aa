"""SQLite persistence for the harvester.

The published catalogue and the harvest watermark live in the same SQLite
database.  A harvest round writes into ``run_records`` (a per-run staging
area) and only the final commit transaction copies staging into ``catalog``
and advances the watermark.  This is what makes a failed or interrupted
round a no-op for readers of the published catalogue.

Cataloguer revisions live in ``revisions``, keyed by identifier and
cascade-deleted with the catalogue row (withdrawal).  ``catalog`` keeps both
the latest source values and the merged, cataloguer-visible values plus
per-field conflict flags.  A harvest commit merges field by field (see
:mod:`app.reconcile`) inside the same transaction that advances the
watermark, so a revision submitted while a round was running is never simply
overwritten.
"""

from __future__ import annotations

import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from .reconcile import clean, merge_field

REVISION_FIELDS = ("title", "creator")


class RevisionRejected(Exception):
    """A revision submission was refused as a whole.

    ``status_code`` is 409 for concurrent changes and 400 for invalid input.
    ``diff`` carries the current record so the caller can show what changed.
    """

    def __init__(
        self,
        message: str,
        *,
        status_code: int = 409,
        diff: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.diff = diff or {}


def utc_now_millis() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


class Store:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            str(self.db_path), check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._init_schema()
        self._recover_interrupted_runs()
        self._ensure_invariants()

    # ------------------------------------------------------------------ schema
    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );

                CREATE TABLE IF NOT EXISTS published_state (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    watermark TEXT,
                    updated_at TEXT,
                    success_count INTEGER NOT NULL DEFAULT 0,
                    last_run_id TEXT
                );

                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY,
                    status TEXT NOT NULL CHECK (
                        status IN ('running', 'succeeded', 'failed', 'cancelled')
                    ),
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    window_from TEXT,
                    window_until TEXT,
                    pages_fetched INTEGER NOT NULL DEFAULT 0,
                    records_seen INTEGER NOT NULL DEFAULT 0,
                    token_restarts INTEGER NOT NULL DEFAULT 0,
                    attempt INTEGER NOT NULL DEFAULT 1,
                    complete_list_size INTEGER,
                    phase TEXT,
                    last_error TEXT,
                    cancel_reason TEXT,
                    watermark_after TEXT
                );

                CREATE TABLE IF NOT EXISTS run_records (
                    run_id TEXT NOT NULL,
                    identifier TEXT NOT NULL,
                    title TEXT,
                    creator TEXT,
                    datestamp TEXT NOT NULL,
                    deleted INTEGER NOT NULL DEFAULT 0,
                    seq INTEGER NOT NULL,
                    PRIMARY KEY (run_id, identifier),
                    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS catalog (
                    identifier TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    creator TEXT,
                    source_datestamp TEXT NOT NULL,
                    harvested_at TEXT NOT NULL,
                    source_title TEXT NOT NULL DEFAULT '',
                    source_creator TEXT NOT NULL DEFAULT '',
                    origin_title TEXT NOT NULL DEFAULT 'source',
                    origin_creator TEXT NOT NULL DEFAULT 'source',
                    conflict_title INTEGER NOT NULL DEFAULT 0,
                    conflict_creator INTEGER NOT NULL DEFAULT 0,
                    source_version INTEGER NOT NULL DEFAULT 1
                );

                CREATE INDEX IF NOT EXISTS idx_catalog_title ON catalog(title);

                CREATE TABLE IF NOT EXISTS revisions (
                    identifier TEXT PRIMARY KEY,
                    base_title TEXT NOT NULL DEFAULT '',
                    base_creator TEXT NOT NULL DEFAULT '',
                    title_edited INTEGER NOT NULL DEFAULT 0,
                    creator_edited INTEGER NOT NULL DEFAULT 0,
                    title TEXT NOT NULL DEFAULT '',
                    creator TEXT NOT NULL DEFAULT '',
                    version INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (identifier)
                        REFERENCES catalog(identifier) ON DELETE CASCADE
                );

                INSERT OR IGNORE INTO published_state
                    (id, watermark, updated_at, success_count, last_run_id)
                VALUES (1, NULL, NULL, 0, NULL);
                """
            )
            self._migrate_schema()

    def _migrate_schema(self) -> None:
        """Bring an existing catalogue database up to the revision schema.

        The already-published title/creator become the initial source values,
        so an existing catalogue keeps behaving exactly as before until a
        cataloguer saves a revision.
        """
        columns = {
            row["name"] for row in self._conn.execute("PRAGMA table_info(catalog)")
        }
        additions = {
            "source_title": "TEXT NOT NULL DEFAULT ''",
            "source_creator": "TEXT NOT NULL DEFAULT ''",
            "origin_title": "TEXT NOT NULL DEFAULT 'source'",
            "origin_creator": "TEXT NOT NULL DEFAULT 'source'",
            "conflict_title": "INTEGER NOT NULL DEFAULT 0",
            "conflict_creator": "INTEGER NOT NULL DEFAULT 0",
            "source_version": "INTEGER NOT NULL DEFAULT 1",
        }
        added_source = False
        for name, ddl in additions.items():
            if name not in columns:
                self._conn.execute(f"ALTER TABLE catalog ADD COLUMN {name} {ddl}")
                if name == "source_title":
                    added_source = True
        if added_source:
            # Initial source values are the already published values.
            self._conn.execute(
                "UPDATE catalog SET source_title = title WHERE source_title = ''"
            )
            self._conn.execute(
                "UPDATE catalog SET source_creator = COALESCE(creator, '')"
                " WHERE source_creator = ''"
            )

    def _recover_interrupted_runs(self) -> None:
        """A previous process died.  No in-memory task can own these runs."""
        with self._lock:
            now = utc_now_millis()
            rows = self._conn.execute(
                "SELECT id FROM runs WHERE status = 'running'"
            ).fetchall()
            for row in rows:
                self._conn.execute(
                    """
                    UPDATE runs
                       SET status = 'failed',
                           finished_at = ?,
                           phase = 'failed',
                           last_error = COALESCE(
                               last_error,
                               '采集进程中断，本轮未提交（已发布目录与水位保持不变）'
                           )
                     WHERE id = ? AND status = 'running'
                    """,
                    (now, row["id"]),
                )
                self._conn.execute(
                    "DELETE FROM run_records WHERE run_id = ?", (row["id"],)
                )
            self._conn.execute(
                "DELETE FROM meta WHERE key = 'active_run_id'"
            )

    def _ensure_invariants(self) -> None:
        """At most one round may be ``running``, even across processes.

        Created after recovery so a legacy database with stray ``running``
        rows cannot make startup fail.
        """
        with self._lock:
            self._conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_one_active
                    ON runs(status) WHERE status = 'running'
                """
            )

    # ----------------------------------------------------------------- helpers
    def _meta_get(self, key: str) -> str | None:
        row = self._conn.execute(
            "SELECT value FROM meta WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row else None

    def _meta_set(self, key: str, value: str | None) -> None:
        if value is None:
            self._conn.execute("DELETE FROM meta WHERE key = ?", (key,))
        else:
            self._conn.execute(
                """
                INSERT INTO meta(key, value) VALUES(?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )

    # -------------------------------------------------------------------- runs
    def create_run(self) -> dict[str, Any]:
        with self._lock:
            run_id = uuid.uuid4().hex
            now = utc_now_millis()
            state = self._conn.execute(
                "SELECT watermark FROM published_state WHERE id = 1"
            ).fetchone()
            watermark = state["watermark"] if state else None
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    """
                    INSERT INTO runs
                        (id, status, started_at, window_from, phase)
                    VALUES (?, 'running', ?, ?, 'connecting')
                    """,
                    (run_id, now, watermark),
                )
                self._meta_set("active_run_id", run_id)
                self._conn.execute("COMMIT")
            except sqlite3.IntegrityError as exc:
                self._conn.execute("ROLLBACK")
                raise RuntimeError("已有采集轮次在运行") from exc
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            run = self.get_run(run_id)
            assert run is not None
            return run

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM runs WHERE id = ?", (run_id,)
            ).fetchone()
            if row is None:
                return None
            run = dict(row)
            staged = self._conn.execute(
                "SELECT COUNT(*) AS n FROM run_records WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            run["staged_count"] = int(staged["n"])
            return run

    def get_active_run(self) -> dict[str, Any] | None:
        with self._lock:
            run_id = self._meta_get("active_run_id")
            if run_id:
                run = self.get_run(run_id)
                if run and run["status"] == "running":
                    return run
            row = self._conn.execute(
                """
                SELECT * FROM runs WHERE status = 'running'
                 ORDER BY started_at DESC LIMIT 1
                """
            ).fetchone()
            return self.get_run(row["id"]) if row else None

    def list_runs(self, limit: int = 15) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM runs ORDER BY started_at DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

    def is_run_running(self, run_id: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM runs WHERE id = ? AND status = 'running'",
                (run_id,),
            ).fetchone()
            return row is not None

    def set_window(self, run_id: str, until: str) -> bool:
        with self._lock:
            cur = self._conn.execute(
                """
                UPDATE runs SET window_until = ?, phase = 'fetching'
                 WHERE id = ? AND status = 'running'
                """,
                (until, run_id),
            )
            return cur.rowcount > 0

    def update_progress(
        self,
        run_id: str,
        *,
        pages_fetched: int,
        records_seen: int,
        token_restarts: int,
        attempt: int,
        complete_list_size: int | None,
        phase: str | None = None,
    ) -> bool:
        with self._lock:
            if phase is None:
                cur = self._conn.execute(
                    """
                    UPDATE runs
                       SET pages_fetched = ?,
                           records_seen = ?,
                           token_restarts = ?,
                           attempt = ?,
                           complete_list_size = ?
                     WHERE id = ? AND status = 'running'
                    """,
                    (
                        pages_fetched,
                        records_seen,
                        token_restarts,
                        attempt,
                        complete_list_size,
                        run_id,
                    ),
                )
            else:
                cur = self._conn.execute(
                    """
                    UPDATE runs
                       SET pages_fetched = ?,
                           records_seen = ?,
                           token_restarts = ?,
                           attempt = ?,
                           complete_list_size = ?,
                           phase = ?
                     WHERE id = ? AND status = 'running'
                    """,
                    (
                        pages_fetched,
                        records_seen,
                        token_restarts,
                        attempt,
                        complete_list_size,
                        phase,
                        run_id,
                    ),
                )
            return cur.rowcount > 0

    def set_last_error(self, run_id: str, message: str) -> None:
        with self._lock:
            self._conn.execute(
                """
                UPDATE runs SET last_error = ?
                 WHERE id = ? AND status = 'running'
                """,
                (message[:4000], run_id),
            )

    def finish_cancelled(self, run_id: str, reason: str) -> bool:
        """Finalise a cancelled run; safe to call twice."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                cur = self._conn.execute(
                    """
                    UPDATE runs
                       SET status = 'cancelled',
                           finished_at = COALESCE(finished_at, ?),
                           phase = 'cancelled',
                           cancel_reason = COALESCE(cancel_reason, ?)
                     WHERE id = ? AND status = 'running'
                    """,
                    (utc_now_millis(), reason[:1000], run_id),
                )
                changed = cur.rowcount > 0
                self._delete_stage(run_id)
                if changed:
                    self._clear_active_if(run_id)
                self._conn.execute("COMMIT")
                return changed
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def finish_failed(self, run_id: str, error: str) -> bool:
        """Finalise a failed run.  Staging is discarded, published data not touched."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                cur = self._conn.execute(
                    """
                    UPDATE runs
                       SET status = 'failed',
                           finished_at = COALESCE(finished_at, ?),
                           phase = 'failed',
                           last_error = ?
                     WHERE id = ? AND status = 'running'
                    """,
                    (utc_now_millis(), error[:4000], run_id),
                )
                changed = cur.rowcount > 0
                self._delete_stage(run_id)
                if changed:
                    self._clear_active_if(run_id)
                self._conn.execute("COMMIT")
                return changed
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def reset_stage(self, run_id: str) -> bool:
        """Discard all pages collected for the current attempt."""
        with self._lock:
            if not self.is_run_running(run_id):
                return False
            self._conn.execute(
                "DELETE FROM run_records WHERE run_id = ?", (run_id,)
            )
            return True

    def stage_records(
        self, run_id: str, records: Sequence[dict[str, Any]], seq_start: int
    ) -> int:
        """Insert one page into staging, keeping one row per identifier.

        If the same identifier occurs again (whether because of a malformed
        source or a repeated page), the record with the newer OAI datestamp
        wins; the later observation wins a tie.
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                if not self.is_run_running(run_id):
                    self._conn.execute("ROLLBACK")
                    return 0
                for offset, record in enumerate(records):
                    seq = seq_start + offset
                    self._conn.execute(
                        """
                        INSERT INTO run_records
                            (run_id, identifier, title, creator, datestamp,
                             deleted, seq)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(run_id, identifier) DO UPDATE SET
                            title = excluded.title,
                            creator = excluded.creator,
                            datestamp = excluded.datestamp,
                            deleted = excluded.deleted,
                            seq = excluded.seq
                        WHERE excluded.datestamp > run_records.datestamp
                           OR (excluded.datestamp = run_records.datestamp
                               AND excluded.seq > run_records.seq)
                        """,
                        (
                            run_id,
                            record["identifier"],
                            record.get("title"),
                            record.get("creator"),
                            record["datestamp"],
                            1 if record.get("deleted") else 0,
                            seq,
                        ),
                    )
                self._conn.execute("COMMIT")
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            return len(records)

    def _delete_stage(self, run_id: str) -> None:
        self._conn.execute("DELETE FROM run_records WHERE run_id = ?", (run_id,))

    def _clear_active_if(self, run_id: str) -> None:
        if self._meta_get("active_run_id") == run_id:
            self._meta_set("active_run_id", None)

    def commit_run(self, run_id: str) -> dict[str, Any]:
        """Atomically publish staging, merge revisions and advance the watermark.

        The check for ownership (``active_run_id``) happens inside the same
        IMMEDIATE transaction as the catalogue writes.  A cancelled or
        superseded round can therefore never publish, even if its HTTP
        response arrives late.

        Source values, the merged (cataloguer-visible) values, conflict flags
        and the watermark all become visible in this one transaction.  A
        revision committed while the round was fetching is never overwritten:
        it is either kept, merged when both sides now agree, or kept with its
        field flagged as a conflict.
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                run = self._conn.execute(
                    "SELECT * FROM runs WHERE id = ?", (run_id,)
                ).fetchone()
                active = self._meta_get("active_run_id")
                if (
                    run is None
                    or run["status"] != "running"
                    or active != run_id
                    or not run["window_until"]
                ):
                    self._conn.execute("ROLLBACK")
                    return {"committed": False, "reason": "run is not active"}

                rows = self._conn.execute(
                    """
                    SELECT identifier, title, creator, datestamp, deleted
                      FROM run_records WHERE run_id = ?
                     ORDER BY seq
                    """,
                    (run_id,),
                ).fetchall()

                now = utc_now_millis()
                upserts = 0
                deletions = 0
                new_conflicts = 0
                for row in rows:
                    if row["deleted"]:
                        # Withdrawing removes the catalogue entry.  The
                        # revision (and its conflicts) is removed with it, so
                        # a later restore starts from the source values only.
                        self._conn.execute(
                            "DELETE FROM catalog WHERE identifier = ?",
                            (row["identifier"],),
                        )
                        deletions += 1
                    else:
                        conflicts = self._publish_source_record(
                            identifier=row["identifier"],
                            source_title=row["title"] or "",
                            source_creator=row["creator"] or "",
                            source_datestamp=row["datestamp"],
                            harvested_at=now,
                        )
                        new_conflicts += conflicts
                        upserts += 1

                watermark = run["window_until"]
                self._conn.execute(
                    """
                    UPDATE published_state
                       SET watermark = ?,
                           updated_at = ?,
                           success_count = success_count + 1,
                           last_run_id = ?
                     WHERE id = 1
                    """,
                    (watermark, now, run_id),
                )
                self._conn.execute(
                    """
                    UPDATE runs
                       SET status = 'succeeded',
                           finished_at = ?,
                           phase = 'published',
                           watermark_after = ?,
                           last_error = NULL
                     WHERE id = ?
                    """,
                    (now, watermark, run_id),
                )
                self._clear_active_if(run_id)
                self._delete_stage(run_id)
                self._conn.execute("COMMIT")
                return {
                    "committed": True,
                    "upserts": upserts,
                    "deletions": deletions,
                    "new_conflicts": new_conflicts,
                    "watermark": watermark,
                }
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def _publish_source_record(
        self,
        *,
        identifier: str,
        source_title: str,
        source_creator: str,
        source_datestamp: str,
        harvested_at: str,
    ) -> int:
        """Merge one source record into the catalogue inside a transaction.

        Returns the number of fields newly flagged as conflicts.
        """
        existing = self._conn.execute(
            "SELECT * FROM catalog WHERE identifier = ?", (identifier,)
        ).fetchone()
        revision = self._conn.execute(
            "SELECT * FROM revisions WHERE identifier = ?", (identifier,)
        ).fetchone()

        if revision is None:
            title = clean(source_title)
            creator = clean(source_creator)
            origin_title = origin_creator = "source"
            conflict_title = conflict_creator = 0
        else:
            title_merge = merge_field(
                base=revision["base_title"],
                revision=revision["title"],
                source=source_title,
                edited=bool(revision["title_edited"]),
            )
            creator_merge = merge_field(
                base=revision["base_creator"],
                revision=revision["creator"],
                source=source_creator,
                edited=bool(revision["creator_edited"]),
            )
            title = title_merge.value
            creator = creator_merge.value
            origin_title = title_merge.origin
            origin_creator = creator_merge.origin
            conflict_title = 1 if title_merge.conflict else 0
            conflict_creator = 1 if creator_merge.conflict else 0

            # A field whose revision no longer differs from the source basis
            # has nothing left to express, so the revision follows the source.
            # Conflicting fields keep their revision value and original basis
            # until a cataloguer resolves them.
            updates: dict[str, Any] = {}
            if revision["title_edited"] and not conflict_title:
                if (
                    revision["base_title"] != clean(source_title)
                    or revision["title"] != title
                ):
                    updates["base_title"] = clean(source_title)
                    updates["title"] = title
            if revision["creator_edited"] and not conflict_creator:
                if (
                    revision["base_creator"] != clean(source_creator)
                    or revision["creator"] != creator
                ):
                    updates["base_creator"] = clean(source_creator)
                    updates["creator"] = creator
            if updates:
                updates["version"] = revision["version"] + 1
                updates["updated_at"] = utc_now_millis()
                assignments = ", ".join(f"{key} = ?" for key in updates)
                self._conn.execute(
                    f"UPDATE revisions SET {assignments} WHERE identifier = ?",
                    [*updates.values(), identifier],
                )

        if existing is None:
            self._conn.execute(
                """
                INSERT INTO catalog
                    (identifier, title, creator, source_datestamp,
                     harvested_at, source_title, source_creator,
                     origin_title, origin_creator, conflict_title,
                     conflict_creator, source_version)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                """,
                (
                    identifier,
                    title,
                    creator or None,
                    source_datestamp,
                    harvested_at,
                    clean(source_title),
                    clean(source_creator),
                    origin_title,
                    origin_creator,
                    conflict_title,
                    conflict_creator,
                ),
            )
            return conflict_title + conflict_creator

        already_title = bool(existing["conflict_title"])
        already_creator = bool(existing["conflict_creator"])
        # Content version, not timestamp: an OAI datestamp has one-second
        # precision, so a same-second correction must still invalidate a
        # revision form that was opened before it.  Re-publishing identical
        # content (the inclusive window always re-reads its lower bound) does
        # not advance the version.
        source_changed = (
            clean(existing["source_title"]) != clean(source_title)
            or clean(existing["source_creator"]) != clean(source_creator)
        )
        self._conn.execute(
            """
            UPDATE catalog
               SET title = ?,
                   creator = ?,
                   source_datestamp = ?,
                   harvested_at = ?,
                   source_title = ?,
                   source_creator = ?,
                   origin_title = ?,
                   origin_creator = ?,
                   conflict_title = ?,
                   conflict_creator = ?,
                   source_version = source_version + ?
             WHERE identifier = ?
            """,
            (
                title,
                creator or None,
                source_datestamp,
                harvested_at,
                clean(source_title),
                clean(source_creator),
                origin_title,
                origin_creator,
                conflict_title,
                conflict_creator,
                1 if source_changed else 0,
                identifier,
            ),
        )
        new_title = conflict_title and not already_title
        new_creator = conflict_creator and not already_creator
        return int(new_title) + int(new_creator)

    def fail_run(self, run_id: str, error: str) -> bool:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                cur = self._conn.execute(
                    """
                    UPDATE runs
                       SET status = 'failed',
                           finished_at = ?,
                           phase = 'failed',
                           last_error = ?
                     WHERE id = ? AND status = 'running'
                    """,
                    (utc_now_millis(), error[:4000], run_id),
                )
                self._delete_stage(run_id)
                self._clear_active_if(run_id)
                self._conn.execute("COMMIT")
                return cur.rowcount > 0
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    # --------------------------------------------------------------- revisions
    def catalog_item(self, identifier: str) -> dict[str, Any] | None:
        """One catalogue entry with its source values and conflict flags."""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM catalog WHERE identifier = ?", (identifier,)
            ).fetchone()
            if row is None:
                return None
            item = dict(row)
            revision = self._conn.execute(
                "SELECT * FROM revisions WHERE identifier = ?", (identifier,)
            ).fetchone()
            item["revision"] = dict(revision) if revision else None
            return item

    def record_editor_view(self, identifier: str) -> dict[str, Any] | None:
        """Everything the edit page needs for one identifier."""
        item = self.catalog_item(identifier)
        if item is None:
            return None
        revision = item["revision"]
        return {
            "identifier": identifier,
            "title": item["title"],
            "creator": item["creator"] or "",
            "source": {
                "title": item["source_title"],
                "creator": item["source_creator"],
            },
            "source_datestamp": item["source_datestamp"],
            "source_version": item["source_version"],
            "conflicts": {
                "title": bool(item["conflict_title"]),
                "creator": bool(item["conflict_creator"]),
            },
            "origins": {
                "title": item["origin_title"],
                "creator": item["origin_creator"],
            },
            "revision": (
                {
                    "title": revision["title"],
                    "creator": revision["creator"],
                    "base": {
                        "title": revision["base_title"],
                        "creator": revision["base_creator"],
                    },
                    "edited": {
                        "title": bool(revision["title_edited"]),
                        "creator": bool(revision["creator_edited"]),
                    },
                    "version": revision["version"],
                    "updated_at": revision["updated_at"],
                }
                if revision
                else None
            ),
        }

    def catalog_editor_page(
        self, query: str = "", page: int = 1, page_size: int = 25
    ) -> dict[str, Any]:
        result = self.catalog_page(query, page, page_size)
        with self._lock:
            by_identifier = {
                row["identifier"]: dict(row)
                for row in self._conn.execute(
                    "SELECT identifier, title, creator, title_edited,"
                    " creator_edited FROM revisions"
                )
            }
        for item in result["records"]:
            revision = by_identifier.get(item["identifier"])
            item["has_revision"] = revision is not None and (
                revision["title_edited"] or revision["creator_edited"]
            )
            item["revision_title"] = revision["title"] if revision else None
            item["revision_creator"] = revision["creator"] if revision else None
        with self._lock:
            result["conflict_total"] = int(
                self._conn.execute(
                    "SELECT COUNT(*) AS n FROM catalog"
                    " WHERE conflict_title = 1 OR conflict_creator = 1"
                ).fetchone()["n"]
            )
            result["revision_total"] = int(
                self._conn.execute(
                    "SELECT COUNT(*) AS n FROM revisions"
                    " WHERE title_edited = 1 OR creator_edited = 1"
                ).fetchone()["n"]
            )
        return result

    def save_revision(
        self,
        identifier: str,
        *,
        title: str | None,
        creator: str | None,
        fields: Sequence[str],
        base_source_version: int,
        base_revision_version: int,
        expect_revision: bool,
        expect_source: dict[str, str] | None = None,
        choices: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Create or replace a cataloguer revision, atomically.

        The whole submission is rejected when either the source content or the
        revision changed after the form was rendered.  The check and the write
        happen under one lock/transaction, so a concurrent harvest commit can
        be neither lost nor overwrite the revision.

        ``choices`` optionally resolves conflicted fields (``source`` /
        ``revision``) as part of the same submission, so a combined edit is
        all-or-nothing too.
        """
        fields = [f for f in fields if f in REVISION_FIELDS]
        choices = {
            f: choice
            for f, choice in (choices or {}).items()
            if f in REVISION_FIELDS
        }
        if "title" in fields and not clean(title):
            raise RevisionRejected("标题不能为空", status_code=400)
        for field, choice in choices.items():
            if choice not in ("source", "revision"):
                raise RevisionRejected(
                    f"字段 {field} 的解冲突选项无效", status_code=400
                )
        if not fields and not choices:
            raise RevisionRejected(
                "请至少勾选一个要校订的字段，或为冲突字段选择源值/校订值",
                status_code=400,
            )
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                item = self._conn.execute(
                    "SELECT * FROM catalog WHERE identifier = ?", (identifier,)
                ).fetchone()
                if item is None:
                    self._conn.execute("ROLLBACK")
                    raise RevisionRejected(
                        "书目不存在（可能已被撤回）", status_code=404
                    )
                revision = self._conn.execute(
                    "SELECT * FROM revisions WHERE identifier = ?", (identifier,)
                ).fetchone()
                for field, choice in choices.items():
                    if choice == "revision" and (
                        revision is None or not revision[f"{field}_edited"]
                    ):
                        self._conn.execute("ROLLBACK")
                        raise RevisionRejected(
                            f"字段 {field} 没有可保留的校订值", status_code=400
                        )

                diff = self._revision_diff(item, revision)
                stale: list[str] = []
                if item["source_version"] != int(base_source_version):
                    stale.append("source")
                if expect_revision and revision is None:
                    stale.append("revision")
                if revision is not None and (
                    revision["version"] != int(base_revision_version)
                ):
                    stale.append("revision")
                # The textual expectation is a second guard for the HTML form:
                # identical content re-observed at a later datestamp does not
                # bump the version, but if the content itself differs from what
                # the cataloguer saw, the submission is stale too.
                if expect_source is not None and (
                    clean(expect_source.get("title")) != clean(item["source_title"])
                    or clean(expect_source.get("creator"))
                    != clean(item["source_creator"])
                ):
                    stale.append("source")
                if stale:
                    self._conn.execute("ROLLBACK")
                    raise RevisionRejected(
                        "提交前源值或校订已变化，本次提交已整次拒绝",
                        diff=diff,
                    )

                source = {
                    "title": clean(item["source_title"]),
                    "creator": clean(item["source_creator"]),
                }
                now = utc_now_millis()
                if revision is None:
                    self._conn.execute(
                        """
                        INSERT INTO revisions
                            (identifier, base_title, base_creator,
                             title_edited, creator_edited, title, creator,
                             version, created_at, updated_at)
                        VALUES (?, ?, ?, 0, 0, ?, ?, 0, ?, ?)
                        """,
                        (
                            identifier,
                            source["title"],
                            source["creator"],
                            clean(item["title"]),
                            clean(item["creator"]),
                            now,
                            now,
                        ),
                    )
                else:
                    revision = self._conn.execute(
                        "SELECT * FROM revisions WHERE identifier = ?",
                        (identifier,),
                    ).fetchone()

                submitted = {
                    "title": clean(title) if "title" in fields else None,
                    "creator": clean(creator) if "creator" in fields else None,
                }
                column_assignments: list[str] = []
                column_values: list[Any] = []
                for field in fields:
                    value = submitted[field] or ""
                    edited = value != source[field]
                    self._conn.execute(
                        f"""
                        UPDATE revisions
                           SET base_{field} = ?,
                               {field} = ?,
                               {field}_edited = ?
                         WHERE identifier = ?
                        """,
                        (source[field], value, int(edited), identifier),
                    )
                    column_assignments.append(f"{field} = ?")
                    column_values.append(value if field == "title" else (value or None))
                    column_assignments.append(f"origin_{field} = ?")
                    column_values.append("revision" if edited else "source")
                    column_assignments.append(f"conflict_{field} = 0")

                for field, choice in choices.items():
                    source_value = source[field]
                    if choice == "source":
                        self._conn.execute(
                            f"UPDATE revisions SET {field} = ?,"
                            f" base_{field} = ?, {field}_edited = 0"
                            " WHERE identifier = ?",
                            (source_value, source_value, identifier),
                        )
                        column_assignments += [
                            f"{field} = ?",
                            "origin_{} = ?".format(field),
                            f"conflict_{field} = 0",
                        ]
                        column_values += [
                            source_value if field == "title" else (source_value or None),
                            "source",
                        ]
                    else:
                        # Keep the revision value, but re-base it on the
                        # current source value so it tracks future changes.
                        keep = self._conn.execute(
                            f"SELECT {field} FROM revisions WHERE identifier = ?",
                            (identifier,),
                        ).fetchone()
                        revision_value = clean(keep[field]) if keep else ""
                        self._conn.execute(
                            f"UPDATE revisions SET base_{field} = ?"
                            " WHERE identifier = ?",
                            (source_value, identifier),
                        )
                        column_assignments += [
                            f"{field} = ?",
                            "origin_{} = ?".format(field),
                            f"conflict_{field} = 0",
                        ]
                        column_values += [
                            revision_value if field == "title" else (revision_value or None),
                            "revision",
                        ]
                self._conn.execute(
                    "UPDATE revisions SET version = version + 1, updated_at = ?"
                    " WHERE identifier = ?",
                    (now, identifier),
                )
                # Fields not submitted keep their current visible value (which
                # may be a revision value or an unresolved conflict); they are
                # deliberately not reset to the source.
                if column_assignments:
                    self._conn.execute(
                        f"UPDATE catalog SET {', '.join(column_assignments)}"
                        " WHERE identifier = ?",
                        [*column_values, identifier],
                    )
                # A field accepted at its source value carries no revision, so
                # its provenance is "source" even if an older revision row
                # survives because another field is still edited.
                for field in fields:
                    if not int(
                        self._conn.execute(
                            f"SELECT {field}_edited FROM revisions"
                            " WHERE identifier = ?",
                            (identifier,),
                        ).fetchone()[f"{field}_edited"]
                    ):
                        self._conn.execute(
                            f"UPDATE catalog SET origin_{field} = 'source'"
                            " WHERE identifier = ?",
                            (identifier,),
                        )
                self._drop_empty_revision(identifier)
                self._conn.execute("COMMIT")
                return self.record_editor_view(identifier) or {}
            except RevisionRejected:
                raise
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def _drop_empty_revision(self, identifier: str) -> None:
        """Remove a revision row that no longer edits any field."""
        self._conn.execute(
            "DELETE FROM revisions WHERE identifier = ?"
            " AND title_edited = 0 AND creator_edited = 0",
            (identifier,),
        )

    def _revision_diff(
        self, item: sqlite3.Row, revision: sqlite3.Row | None
    ) -> dict[str, Any]:
        return {
            "identifier": item["identifier"],
            "source": {
                "title": item["source_title"],
                "creator": item["source_creator"],
                "datestamp": item["source_datestamp"],
                "version": item["source_version"],
            },
            "catalog": {
                "title": item["title"],
                "creator": item["creator"] or "",
            },
            "conflicts": {
                "title": bool(item["conflict_title"]),
                "creator": bool(item["conflict_creator"]),
            },
            "revision": (
                {
                    "title": revision["title"],
                    "creator": revision["creator"],
                    "edited": {
                        "title": bool(revision["title_edited"]),
                        "creator": bool(revision["creator_edited"]),
                    },
                    "version": revision["version"],
                }
                if revision
                else None
            ),
            "changed_at": item["harvested_at"],
        }

    def cancel_run(self, run_id: str, reason: str) -> bool:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                cur = self._conn.execute(
                    """
                    UPDATE runs
                       SET status = 'cancelled',
                           finished_at = ?,
                           phase = 'cancelled',
                           cancel_reason = ?
                     WHERE id = ? AND status = 'running'
                    """,
                    (utc_now_millis(), reason[:1000], run_id),
                )
                self._delete_stage(run_id)
                self._clear_active_if(run_id)
                self._conn.execute("COMMIT")
                return cur.rowcount > 0
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    # --------------------------------------------------------------- published
    def published_state(self) -> dict[str, Any]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM published_state WHERE id = 1"
            ).fetchone()
            result = dict(row) if row else {
                "watermark": None,
                "updated_at": None,
                "success_count": 0,
                "last_run_id": None,
            }
            count = self._conn.execute(
                "SELECT COUNT(*) AS n FROM catalog"
            ).fetchone()
            result["catalog_size"] = int(count["n"])
            result["revision_count"] = int(
                self._conn.execute(
                    "SELECT COUNT(*) AS n FROM revisions"
                    " WHERE title_edited = 1 OR creator_edited = 1"
                ).fetchone()["n"]
            )
            result["conflict_count"] = int(
                self._conn.execute(
                    "SELECT COUNT(*) AS n FROM catalog"
                    " WHERE conflict_title = 1 OR conflict_creator = 1"
                ).fetchone()["n"]
            )
            return result

    def catalog_page(
        self, query: str = "", page: int = 1, page_size: int = 25
    ) -> dict[str, Any]:
        query = (query or "").strip()
        page = max(1, int(page or 1))
        page_size = min(200, max(1, int(page_size or 25)))
        like = f"%{query}%"
        where = ""
        params: list[Any] = []
        if query:
            where = (
                "WHERE identifier LIKE ? OR title LIKE ? "
                "OR COALESCE(creator, '') LIKE ? "
                "OR source_title LIKE ? OR source_creator LIKE ?"
            )
            params = [like, like, like, like, like]
        with self._lock:
            total = self._conn.execute(
                f"SELECT COUNT(*) AS n FROM catalog {where}", params
            ).fetchone()["n"]
            total = int(total)
            pages = max(1, (total + page_size - 1) // page_size)
            if page > pages:
                page = pages
            rows = self._conn.execute(
                f"""
                SELECT identifier, title, creator, source_datestamp,
                       harvested_at, source_title, source_creator,
                       origin_title, origin_creator,
                       conflict_title, conflict_creator, source_version
                  FROM catalog {where}
                 ORDER BY title COLLATE NOCASE, identifier
                 LIMIT ? OFFSET ?
                """,
                [*params, page_size, (page - 1) * page_size],
            ).fetchall()
            return {
                "records": [dict(r) for r in rows],
                "total": total,
                "page": page,
                "page_size": page_size,
                "pages": pages,
                "query": query,
            }

    def close(self) -> None:
        with self._lock:
            self._conn.close()
