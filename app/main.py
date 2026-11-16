"""Web application for the institutional-repository harvester.

Pages
-----
``/``                          catalogue browser, watermark, run controls
``/runs``                      round history (windows, attempts, restarts, errors)
``/revisions``                 catalogue revision workspace (edit / conflicts)
``/revisions/<identifier>``    per-record field editor
``/status``                    statically rendered status JSON
``GET  /api/status``           polling endpoint used by the page
``GET  /api/catalog``          catalogue JSON (search + pagination)
``GET  /api/revisions``        revision workspace JSON
``GET  /api/revisions/<id>``   one record's source/revision/conflict state
``POST /api/revisions/<id>``   save a revision (rejects the whole submission
                               if source or revision changed meanwhile)
``POST /api/revisions/<id>/resolve``  resolve conflicted fields
``POST /api/harvest``          start a round
``POST /api/harvest/cancel``   cancel the active round without waiting for it
``POST /api/harvest/restart``  cancel (if any) and immediately start a new round
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal
from urllib.parse import quote

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .harvest import HarvestConfig, Harvester
from .store import REVISION_FIELDS, RevisionRejected, Store

BASE_DIR = Path(__file__).resolve().parent


def _data_dir() -> Path:
    return Path(os.environ.get("DATA_DIR", "./data"))


def _oai_url() -> str:
    return os.environ.get("OAI_URL", "http://demo-oai:8000/oai")


def create_app(
    *,
    db_path: str | Path | None = None,
    oai_url: str | None = None,
    read_timeout: float | None = None,
    max_attempts: int | None = None,
    client_factory: Any = None,
) -> FastAPI:
    data_dir = _data_dir()
    data_dir.mkdir(parents=True, exist_ok=True)
    store = Store(db_path or (data_dir / "harvester.sqlite3"))
    config = HarvestConfig(
        oai_url=oai_url or _oai_url(),
        read_timeout=float(
            read_timeout
            if read_timeout is not None
            else os.environ.get("OAI_READ_TIMEOUT", "20")
        ),
        connect_timeout=float(os.environ.get("OAI_CONNECT_TIMEOUT", "10")),
        max_attempts=int(
            max_attempts
            if max_attempts is not None
            else os.environ.get("HARVEST_MAX_ATTEMPTS", "3")
        ),
        retry_backoff_ms=int(os.environ.get("HARVEST_RETRY_BACKOFF_MS", "300")),
    )
    harvester = Harvester(store, config, client_factory=client_factory)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        # A fresh process owns no in-flight rounds; Store already marked any
        # 'running' rows from a previous process as failed without publishing.
        yield
        await harvester.shutdown()
        store.close()

    app = FastAPI(title="机构库采集控制台", lifespan=lifespan)
    app.state.store = store
    app.state.harvester = harvester
    app.state.config = config
    templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
    static_dir = BASE_DIR / "static"
    static_dir.mkdir(exist_ok=True)
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    def status_payload() -> dict[str, Any]:
        active = store.get_active_run()
        published = store.published_state()
        last = store.list_runs(limit=1)
        return {
            "active": active,
            "published": published,
            "last_run": last[0] if last else None,
            "config": {
                "oai_url": config.oai_url,
                "max_attempts": config.max_attempts,
                "read_timeout": config.read_timeout,
            },
        }

    @app.get("/", response_class=HTMLResponse)
    async def index(
        request: Request,
        q: str = "",
        page: int = 1,
        page_size: int = 20,
        message: str | None = None,
    ) -> HTMLResponse:
        catalogue = store.catalog_editor_page(q, page, page_size)
        return templates.TemplateResponse(
            request,
            "index.html",
            {
                "catalogue": catalogue,
                "revisions": {
                    "revision_total": catalogue["revision_total"],
                    "conflict_total": catalogue["conflict_total"],
                },
                "published": store.published_state(),
                "status": status_payload(),
                "runs": store.list_runs(limit=10),
                "message": message,
            },
        )

    @app.get("/runs", response_class=HTMLResponse)
    async def runs_page(request: Request) -> HTMLResponse:
        return templates.TemplateResponse(
            request,
            "runs.html",
            {
                "runs": store.list_runs(limit=50),
                "published": store.published_state(),
                "status": status_payload(),
            },
        )

    @app.get("/api/status")
    async def api_status() -> JSONResponse:
        return JSONResponse(status_payload())

    @app.get("/status")
    async def status_page(request: Request) -> HTMLResponse:
        return templates.TemplateResponse(
            request, "status.html", {"status": status_payload()}
        )

    @app.post("/api/harvest")
    async def start_harvest() -> JSONResponse:
        try:
            control = await harvester.start_run()
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return JSONResponse(
            {"run_id": control.run_id, "status": "running"}, status_code=202
        )

    @app.post("/api/harvest/cancel")
    async def cancel_harvest() -> JSONResponse:
        active = store.get_active_run()
        if active is None:
            raise HTTPException(status_code=409, detail="当前没有正在运行的采集轮次")
        await harvester.cancel_run(active["id"], "用户在网页取消", "user")
        return JSONResponse({"run_id": active["id"], "status": "cancelled"})

    @app.post("/api/harvest/restart")
    async def restart_harvest() -> JSONResponse:
        """Cancel the active round (if any) and immediately start a new one.

        ``cancel_run`` returns as soon as SQLite has recorded the cancellation
        and the old asyncio task has been asked to stop; it never waits for a
        slow HTTP page.  The new run gets a new ``run_id`` and its own window,
        and the commit ownership check makes the old round unable to publish.
        """
        active = store.get_active_run()
        if active is not None:
            await harvester.cancel_run(
                active["id"], "用户取消了本轮以开始新一轮", "superseded"
            )
        try:
            control = await harvester.start_run()
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return JSONResponse(
            {"run_id": control.run_id, "status": "running"}, status_code=202
        )

    @app.get("/api/catalog")
    async def api_catalog(q: str = "", page: int = 1, page_size: int = 20) -> JSONResponse:
        return JSONResponse(store.catalog_page(q, page, page_size))

    # ------------------------------------------------------------- revisions
    def _revision_or_404(identifier: str) -> dict[str, Any]:
        view = store.record_editor_view(identifier)
        if view is None:
            raise HTTPException(status_code=404, detail="书目不存在（可能已被撤回）")
        return view

    @app.get("/api/revisions")
    async def api_revisions(
        q: str = "", page: int = 1, page_size: int = 20
    ) -> JSONResponse:
        return JSONResponse(store.catalog_editor_page(q, page, page_size))

    @app.get("/api/revisions/{identifier:path}")
    async def api_revision(identifier: str) -> JSONResponse:
        return JSONResponse(_revision_or_404(identifier))

    @app.post("/api/revisions/{identifier:path}/resolve")
    async def api_resolve_conflicts(
        identifier: str,
        base_source_version: int = Form(...),
        base_revision_version: int = Form(...),
        title_choice: Literal["", "source", "revision"] = Form(""),
        creator_choice: Literal["", "source", "revision"] = Form(""),
    ) -> JSONResponse:
        choices = {
            field: choice
            for field, choice in (
                ("title", title_choice),
                ("creator", creator_choice),
            )
            if choice
        }
        if not choices:
            return JSONResponse(
                {"detail": "请至少为一个冲突字段选择源值或校订值", "rejected": True},
                status_code=400,
            )
        view = _revision_or_404(identifier)
        revision = view["revision"]
        try:
            merged = store.save_revision(
                identifier,
                title=view["revision"]["title"] if revision else None,
                creator=view["revision"]["creator"] if revision else None,
                fields=[],
                choices=choices,
                base_source_version=base_source_version,
                base_revision_version=base_revision_version,
                expect_revision=False,
            )
        except RevisionRejected as exc:
            return JSONResponse(
                {"detail": exc.message, "diff": exc.diff, "rejected": True},
                status_code=exc.status_code,
            )
        return JSONResponse(merged)

    @app.post("/api/revisions/{identifier:path}")
    async def api_save_revision(
        identifier: str,
        title: str = Form(""),
        creator: str = Form(""),
        fields: list[str] = Form(default=[]),
        base_source_version: int = Form(...),
        base_revision_version: int = Form(...),
        expect_revision: str = Form("0"),
        expect_source_title: str | None = Form(None),
        expect_source_creator: str | None = Form(None),
    ) -> JSONResponse:
        expect_source = None
        if expect_source_title is not None or expect_source_creator is not None:
            expect_source = {
                "title": expect_source_title or "",
                "creator": expect_source_creator or "",
            }
        try:
            view = store.save_revision(
                identifier,
                title=title,
                creator=creator,
                fields=fields,
                base_source_version=base_source_version,
                base_revision_version=base_revision_version,
                expect_revision=expect_revision not in ("", "0", "false", "False"),
                expect_source=expect_source,
            )
        except RevisionRejected as exc:
            return JSONResponse(
                {"detail": exc.message, "diff": exc.diff, "rejected": True},
                status_code=exc.status_code,
            )
        return JSONResponse(view)

    @app.post("/revisions/{identifier:path}")
    async def revision_editor_submit(
        request: Request,
        identifier: str,
        title: str = Form(""),
        creator: str = Form(""),
        fields: list[str] = Form(default=[]),
        base_source_version: int = Form(...),
        base_revision_version: int = Form(...),
        expect_revision: str = Form("0"),
        expect_source_title: str = Form(""),
        expect_source_creator: str = Form(""),
        expect_revision_title: str = Form(""),
        expect_revision_creator: str = Form(""),
        title_choice: Literal["", "source", "revision"] = Form(""),
        creator_choice: Literal["", "source", "revision"] = Form(""),
        action: str = Form("save"),
    ) -> HTMLResponse:
        # ``fields`` may arrive as checkbox values or as explicit per-field
        # "edit this field" toggles; normalise to the two editable fields.
        selected = [f for f in fields if f in REVISION_FIELDS]
        if action == "resolve":
            choices = {
                field: choice
                for field, choice in (
                    ("title", title_choice),
                    ("creator", creator_choice),
                )
                if choice
            }
            selected = []
        elif action == "take_source":
            # Explicit "use the latest source" button: no revision at all for
            # this record, conflicts disappear with the revision.
            choices = {field: "source" for field in REVISION_FIELDS}
            selected = []
        else:
            choices = {}
        try:
            store.save_revision(
                identifier,
                title=title,
                creator=creator,
                fields=selected,
                choices=choices,
                base_source_version=base_source_version,
                base_revision_version=base_revision_version,
                expect_revision=expect_revision not in ("", "0", "false", "False"),
                expect_source={
                    "title": expect_source_title,
                    "creator": expect_source_creator,
                },
            )
        except RevisionRejected as exc:
            view = store.record_editor_view(identifier)
            if view is None:
                raise HTTPException(
                    status_code=404, detail="书目不存在（可能已被撤回）"
                ) from exc
            return templates.TemplateResponse(
                request,
                "revision_edit.html",
                {
                    "record": view,
                    "error": exc.message,
                    "diff": exc.diff,
                    "expect_source": {
                        "title": expect_source_title,
                        "creator": expect_source_creator,
                    },
                    "expect_revision": {
                        "title": expect_revision_title,
                        "creator": expect_revision_creator,
                    },
                    "message": None,
                },
                status_code=exc.status_code,
            )
        label = "冲突取舍已保存" if choices else "校订已保存"
        target = (
            f"/revisions/{quote(identifier, safe='')}"
            "?message=" + quote(label)
        )
        return RedirectResponse(target, status_code=303)

    @app.get("/revisions", response_class=HTMLResponse)
    async def revisions_page(
        request: Request,
        q: str = "",
        page: int = 1,
        page_size: int = 20,
        message: str | None = None,
    ) -> HTMLResponse:
        return templates.TemplateResponse(
            request,
            "revisions.html",
            {
                "catalogue": store.catalog_editor_page(q, page, page_size),
                "message": message,
                "fields": REVISION_FIELDS,
            },
        )

    @app.get("/revisions/{identifier:path}", response_class=HTMLResponse)
    async def revision_editor_page(
        request: Request,
        identifier: str,
        message: str | None = None,
        error: str | None = None,
    ) -> HTMLResponse:
        view = store.record_editor_view(identifier)
        if view is None:
            raise HTTPException(status_code=404, detail="书目不存在（可能已被撤回）")
        return templates.TemplateResponse(
            request,
            "revision_edit.html",
            {"record": view, "message": message, "error": error},
        )

    return app


app = create_app()
