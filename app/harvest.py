"""Harvest round orchestration.

Contract implemented here:

* The window of a round is ``[previous committed watermark, source's current
  time]``.  The first round has no lower bound (full load).  Both ends are
  inclusive: ``from`` is the watermark and ``until`` is the source's current
  UTC time read from ``Identify``.  Re-reading the boundary second is harmless
  because staging keeps one row per identifier and the commit is idempotent.
* Every round starts at page 1 of that window and continues until
  ``ListRecords`` stops returning a resumption token.  If the source rejects a
  token (expired), the whole staged attempt is discarded and the *original*
  window is requested again from the beginning, up to ``max_attempts`` times.
  Pages from the discarded attempt are never mixed into the published result.
* Publication and watermark advance happen in a single SQLite transaction (see
  :meth:`Store.commit_run`).  Any failure leaves published data untouched.
* Cancellation is enforced between pages, while parsing, and again inside the
  commit transaction.  The API cancels the asyncio task so a slow page is not
  waited for; the task's late response can neither publish nor touch another
  round's state because every write is keyed by ``run_id`` and the commit checks
  that this run is still the active one.
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass, field
from typing import Any, Callable

from .oai import (
    BadResumptionToken,
    NoRecordsMatch,
    OaiClient,
    OaiError,
    OaiPage,
    parse_datestamp,
)
from .store import Store


class HarvestCancelled(Exception):
    pass


class HarvestFailed(Exception):
    pass


class PaginationChanged(Exception):
    """The source's result set changed size while a window was being paged."""


@dataclass
class HarvestConfig:
    oai_url: str
    max_attempts: int = 3
    retry_backoff_ms: int = 300
    retry_backoff_jitter_ms: int = 200
    read_timeout: float = 20.0
    connect_timeout: float = 10.0


@dataclass
class AttemptStats:
    pages: int = 0
    records: int = 0
    complete_list_size: int | None = None


@dataclass
class RunControl:
    """Handle shared between the API and a running round."""

    run_id: str
    store: Store
    task: "asyncio.Task[None] | None" = None
    cancel_requested: bool = False
    cancel_source: str | None = None
    cancel_reason: str | None = None
    done: asyncio.Event = field(default_factory=asyncio.Event)
    outcome: str | None = None

    def request_cancel(self, reason: str, source: str) -> None:
        if self.cancel_requested:
            return
        self.cancel_requested = True
        self.cancel_source = source
        self.cancel_reason = reason

    @property
    def superseded(self) -> bool:
        return self.cancel_source == "superseded"

    def resolve(self, outcome: str) -> None:
        if self.outcome is None:
            self.outcome = outcome
        self.done.set()


class Harvester:
    def __init__(
        self,
        store: Store,
        config: HarvestConfig,
        *,
        client_factory: Callable[[], Any] | None = None,
    ) -> None:
        self.store = store
        self.config = config
        self._client_factory = client_factory or self._default_client
        self._controls: dict[str, RunControl] = {}
        self._lock = asyncio.Lock()

    def _default_client(self) -> OaiClient:
        return OaiClient(
            self.config.oai_url,
            read_timeout=self.config.read_timeout,
            connect_timeout=self.config.connect_timeout,
        )

    # ------------------------------------------------------------- lifecycle
    async def start_run(self) -> RunControl:
        async with self._lock:
            active = self.store.get_active_run()
            if active is not None:
                raise RuntimeError(f"已有采集轮次在运行: {active['id']}")
            run = self.store.create_run()
            control = RunControl(run_id=run["id"], store=self.store)
            self._controls[run["id"]] = control
            control.task = asyncio.create_task(
                self._run(control), name=f"harvest-{run['id']}"
            )
            control.task.add_done_callback(
                lambda task, c=control: self._on_task_done(c, task)
            )
            return control

    def _on_task_done(self, control: RunControl, task: asyncio.Task) -> None:
        """Belt-and-braces finalisation.

        ``_run`` normally resolves the control itself.  The one path it cannot
        cover is a task cancelled before its first step ever executed.  This
        callback guarantees that such a run is still marked cancelled, never
        left as ``running`` and never able to publish.
        """
        if control.done.is_set():
            return
        try:
            if task.cancelled():
                self.store.finish_cancelled(
                    control.run_id,
                    control.cancel_reason or "采集任务在开始前被取消",
                )
                control.resolve("cancelled")
            else:
                exc = task.exception()
                if exc is not None:
                    self.store.finish_failed(
                        control.run_id, f"采集异常: {exc!r}"
                    )
                control.resolve("failed")
        except Exception:
            control.resolve("failed")

    async def cancel_run(
        self, run_id: str, reason: str = "用户取消", source: str = "user"
    ) -> bool:
        """Cancel without waiting for an in-flight page.

        The run is marked cancelled in SQLite immediately (a fast local write),
        which closes the commit path for it.  The asyncio task is then
        cancelled so the HTTP read is abandoned now; its eventual unwinding is
        harmless.
        """
        control = self._controls.get(run_id)
        marked = self.store.cancel_run(run_id, reason)
        if control is not None:
            control.request_cancel(reason, source)
            if control.task is not None and not control.task.done():
                control.task.cancel()
        return marked

    def control(self, run_id: str) -> RunControl | None:
        return self._controls.get(run_id)

    async def shutdown(self) -> None:
        controls = list(self._controls.values())
        for control in controls:
            control.request_cancel("服务停止", "shutdown")
            self.store.cancel_run(control.run_id, "服务停止")
            if control.task is not None and not control.task.done():
                control.task.cancel()
        for control in controls:
            if control.task is not None:
                try:
                    await control.task
                except (asyncio.CancelledError, Exception):
                    pass

    def _check_cancelled(self, control: RunControl) -> None:
        if control.cancel_requested or not self.store.is_run_running(control.run_id):
            raise HarvestCancelled(control.cancel_reason or "本轮已取消")

    async def _run(self, control: RunControl) -> None:
        run_id = control.run_id
        try:
            await self._harvest(control)
            control.resolve("succeeded")
        except asyncio.CancelledError:
            # cancel_run() has already persisted 'cancelled'; if the task was
            # cancelled for another reason (e.g. event loop shutdown), make
            # sure the run does not stay 'running'.
            self.store.finish_cancelled(
                run_id, control.cancel_reason or "采集任务被中断"
            )
            control.resolve("cancelled")
        except HarvestCancelled as exc:
            self.store.finish_cancelled(run_id, str(exc))
            control.resolve("cancelled")
        except HarvestFailed as exc:
            self.store.finish_failed(run_id, str(exc))
            control.resolve("failed")
        except Exception as exc:  # unexpected bug or I/O error
            self.store.finish_failed(run_id, f"采集异常: {exc!r}")
            control.resolve("failed")
        finally:
            async with self._lock:
                if len(self._controls) > 50:
                    for key, value in list(self._controls.items()):
                        if value.done.is_set() and key != run_id:
                            del self._controls[key]

    # --------------------------------------------------------------- one run
    async def _harvest(self, control: RunControl) -> None:
        run_id = control.run_id
        store = self.store

        async with self._client_factory() as client:
            self._check_cancelled(control)
            try:
                source_now = await client.identify_time()
            except OaiError as exc:
                raise HarvestFailed(f"读取源端当前时刻失败: {exc}") from exc

            self._check_cancelled(control)
            run = store.get_run(run_id)
            if run is None:
                raise HarvestFailed("运行记录不存在")
            window_from = run["window_from"]
            window_until = source_now
            if window_from:
                try:
                    if parse_datestamp(window_from) > parse_datestamp(window_until):
                        raise HarvestFailed(
                            "源端当前时刻早于已发布水位，拒绝构造倒置时间窗"
                        )
                except OaiError as exc:
                    raise HarvestFailed(f"已发布水位格式异常: {exc}") from exc
            if not store.set_window(run_id, window_until):
                raise HarvestCancelled(control.cancel_reason or "本轮已取消")

            total_pages = 0
            total_records = 0
            complete_list_size: int | None = None
            token_restarts = 0
            restarts = 0
            last_token_error: str | None = None

            while True:
                self._check_cancelled(control)
                attempt = restarts + 1
                store.update_progress(
                    run_id,
                    pages_fetched=total_pages,
                    records_seen=total_records,
                    token_restarts=token_restarts,
                    attempt=attempt,
                    complete_list_size=complete_list_size,
                    phase="fetching" if restarts == 0 else "retrying",
                )
                try:
                    stats = await self._fetch_window(
                        client,
                        control,
                        window_from=window_from,
                        window_until=window_until,
                        attempt=attempt,
                        token_restarts=token_restarts,
                        pages_base=total_pages,
                        records_base=total_records,
                        complete_list_size=complete_list_size,
                    )
                except (BadResumptionToken, PaginationChanged) as exc:
                    last_token_error = str(exc)
                    token_restarts += 1
                    # Drop every page of this attempt.  The retry re-requests
                    # the original window from page 1, with no stale token.
                    # The failed attempt's page/record counters are not carried
                    # into the retry: they describe work that was discarded.
                    store.reset_stage(run_id)
                    total_pages = 0
                    total_records = 0
                    complete_list_size = None
                    store.update_progress(
                        run_id,
                        pages_fetched=0,
                        records_seen=0,
                        token_restarts=token_restarts,
                        attempt=restarts + 1,
                        complete_list_size=None,
                        phase="retrying",
                    )
                    store.set_last_error(
                        run_id,
                        "本轮分页作废并从原时间窗重取: " + last_token_error,
                    )
                    restarts += 1
                    if restarts >= self.config.max_attempts:
                        raise HarvestFailed(
                            f"连续 {restarts} 次分页重取均失败，本轮放弃"
                            f"（未发布）: {last_token_error}"
                        ) from exc
                    self._check_cancelled(control)
                    await self._backoff(control)
                    continue

                total_pages += stats.pages
                total_records += stats.records
                if stats.complete_list_size is not None:
                    complete_list_size = stats.complete_list_size
                break

            self._check_cancelled(control)
            store.update_progress(
                run_id,
                pages_fetched=total_pages,
                records_seen=total_records,
                token_restarts=token_restarts,
                attempt=restarts + 1,
                complete_list_size=complete_list_size,
                phase="committing",
            )
            result = store.commit_run(run_id)
            if not result.get("committed"):
                if not store.is_run_running(run_id):
                    raise HarvestCancelled(
                        control.cancel_reason or "本轮在提交前被取消"
                    )
                raise HarvestFailed(f"提交被拒绝: {result.get('reason')}")

    async def _backoff(self, control: RunControl) -> None:
        delay_ms = self.config.retry_backoff_ms + random.randint(
            0, max(0, self.config.retry_backoff_jitter_ms)
        )
        remaining = delay_ms / 1000
        while remaining > 0:
            self._check_cancelled(control)
            step = min(0.05, remaining)
            await asyncio.sleep(step)
            remaining -= step

    async def _fetch_window(
        self,
        client: Any,
        control: RunControl,
        *,
        window_from: str | None,
        window_until: str,
        attempt: int,
        token_restarts: int,
        pages_base: int,
        records_base: int,
        complete_list_size: int | None,
    ) -> AttemptStats:
        run_id = control.run_id
        store = self.store
        stats = AttemptStats(complete_list_size=complete_list_size)
        token: str | None = None
        seen_tokens: set[str] = set()
        seq = 0
        lower_dt = parse_datestamp(window_from) if window_from else None
        upper_dt = parse_datestamp(window_until)

        while True:
            self._check_cancelled(control)
            try:
                page: OaiPage = await client.list_records(
                    from_=window_from if token is None else None,
                    until=window_until,
                    token=token,
                )
            except NoRecordsMatch as exc:
                if token is not None:
                    raise HarvestFailed(
                        f"分页中途返回 noRecordsMatch: {exc}"
                    ) from exc
                # An empty window is not an error; the commit advances the
                # watermark so the next round starts after this instant.
                store.update_progress(
                    run_id,
                    pages_fetched=pages_base + stats.pages,
                    records_seen=records_base + stats.records,
                    token_restarts=token_restarts,
                    attempt=attempt,
                    complete_list_size=stats.complete_list_size,
                )
                return stats
            except BadResumptionToken:
                raise
            except OaiError as exc:
                raise HarvestFailed(f"取数失败: {exc}") from exc
            self._check_cancelled(control)

            stats.pages += 1
            if page.complete_list_size is not None:
                if (
                    stats.complete_list_size is not None
                    and page.complete_list_size != stats.complete_list_size
                ):
                    # A shifting result set makes cursor-based pagination
                    # unreliable (records can be skipped).  Discard the whole
                    # attempt and page the original window again.
                    raise PaginationChanged(
                        "分页期间源端结果集大小变化 "
                        f"({stats.complete_list_size} → "
                        f"{page.complete_list_size})，作废本次分页"
                    )
                stats.complete_list_size = page.complete_list_size

            page_records: list[dict[str, Any]] = []
            for record in page.records:
                try:
                    record_dt = parse_datestamp(record.datestamp)
                except OaiError as exc:
                    raise HarvestFailed(f"记录时间戳无效: {exc}") from exc
                if (lower_dt is not None and record_dt < lower_dt) or (
                    record_dt > upper_dt
                ):
                    # A conforming source only returns records inside the
                    # requested window.  Keeping an out-of-window record would
                    # corrupt the published catalogue, so fail the round.
                    raise HarvestFailed(
                        f"源端返回越窗记录 {record.identifier} "
                        f"({record.datestamp})，期望 "
                        f"[{window_from or '-∞'}, {window_until}]"
                    )
                page_records.append(
                    {
                        "identifier": record.identifier,
                        "title": record.title,
                        "creator": record.creator,
                        "datestamp": record.datestamp,
                        "deleted": record.deleted,
                    }
                )

            seq += 1
            if not store.stage_records(run_id, page_records, seq * 1_000_000):
                raise HarvestCancelled(control.cancel_reason or "本轮已取消")

            stats.records += len(page_records)
            store.update_progress(
                run_id,
                pages_fetched=pages_base + stats.pages,
                records_seen=records_base + stats.records,
                token_restarts=token_restarts,
                attempt=attempt,
                complete_list_size=stats.complete_list_size,
            )

            token = page.token
            if not token:
                return stats
            if token in seen_tokens:
                raise HarvestFailed(
                    "源端返回了重复的 resumptionToken，拒绝死循环"
                )
            seen_tokens.add(token)
