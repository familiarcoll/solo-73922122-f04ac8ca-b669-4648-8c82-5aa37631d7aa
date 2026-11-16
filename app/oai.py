"""Minimal OAI-PMH 2.0 client for harvesting ``ListRecords`` / ``oai_dc``.

Only the pieces the harvester needs are implemented:

* ``Identify`` – used to read the repository's current UTC time, which becomes
  the inclusive upper bound of the harvest window;
* ``ListRecords`` with ``metadataPrefix=oai_dc`` and resumption tokens.

The client never follows a token into a different window.  A stale token is
surfaced as :class:`BadResumptionToken` so the caller can discard the whole
staged attempt and request the original ``[from, until]`` window again.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import httpx

import oaitime

OAI_NS = "http://www.openarchives.org/OAI/2.0/"
DC_NS = "http://purl.org/dc/elements/1.1/"
OAI_DC_NS = "http://www.openarchives.org/OAI/2.0/oai_dc/"

MAX_RESPONSE_BYTES = 16 * 1024 * 1024


class OaiError(Exception):
    """The source returned an unusable OAI-PMH response."""


class BadResumptionToken(OaiError):
    """The resumption token is expired, unknown or otherwise invalid."""


class NoRecordsMatch(OaiError):
    """The source reported an empty result set for the requested window."""


@dataclass(frozen=True)
class OaiRecord:
    identifier: str
    datestamp: str
    deleted: bool
    title: str | None = None
    creator: str | None = None


@dataclass
class OaiPage:
    records: list[OaiRecord] = field(default_factory=list)
    token: str | None = None
    complete_list_size: int | None = None
    cursor: int | None = None
    response_date: str | None = None


def normalize_datestamp(value: str | None, what: str = "datestamp") -> str:
    """Canonical second-precision UTC OAI timestamp, raising :class:`OaiError`."""
    try:
        return oaitime.normalize_datestamp(value, what)
    except oaitime.TimestampError as exc:
        raise OaiError(str(exc)) from exc


def parse_datestamp(value: str) -> datetime:
    try:
        return oaitime.parse_datestamp(value)
    except oaitime.TimestampError as exc:
        raise OaiError(str(exc)) from exc


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _find_child(element: ET.Element, name: str) -> ET.Element | None:
    for child in element:
        if _local(child.tag) == name:
            return child
    return None


def _find_children(element: ET.Element, name: str) -> list[ET.Element]:
    return [child for child in element if _local(child.tag) == name]


def _text(element: ET.Element | None) -> str | None:
    if element is None or element.text is None:
        return None
    value = element.text.strip()
    return value or None


def _parse_oai_error(root: ET.Element) -> OaiError | None:
    for error in _find_children(root, "error"):
        code = (error.get("code") or "").strip()
        message = (error.text or "").strip() or code or "unknown error"
        if code == "badResumptionToken":
            return BadResumptionToken(f"源端令牌失效: {message}")
        if code == "noRecordsMatch":
            return NoRecordsMatch(f"时间窗内无记录: {message}")
        return OaiError(f"源端 OAI 错误 {code or '(无代码)'}: {message}")
    return None


def parse_oai_response(content: bytes) -> tuple[ET.Element, str | None]:
    if b"<!DOCTYPE" in content.upper() or b"<!ENTITY" in content.upper():
        raise OaiError("源端响应包含 DTD/实体声明，已拒绝解析")
    try:
        root = ET.fromstring(content)
    except ET.ParseError as exc:
        raise OaiError(f"源端返回了非法 XML: {exc}") from exc
    if _local(root.tag) != "OAI-PMH":
        raise OaiError("源端响应根元素不是 OAI-PMH")
    response_date = None
    response_date_el = _find_child(root, "responseDate")
    if response_date_el is not None:
        response_date = normalize_datestamp(
            _text(response_date_el), "responseDate"
        )
    return root, response_date


def _parse_record(record_el: ET.Element) -> OaiRecord:
    header = _find_child(record_el, "header")
    if header is None:
        raise OaiError("记录缺少 header")
    identifier = _text(_find_child(header, "identifier"))
    if not identifier:
        raise OaiError("记录缺少标识")
    datestamp = normalize_datestamp(
        _text(_find_child(header, "datestamp")), f"记录 {identifier} 的 datestamp"
    )
    status = (header.get("status") or "").strip().lower()
    deleted = status == "deleted"

    if deleted:
        return OaiRecord(
            identifier=identifier,
            datestamp=datestamp,
            deleted=True,
        )

    metadata = _find_child(record_el, "metadata")
    if metadata is None:
        raise OaiError(f"记录 {identifier} 非删除态但缺少 metadata")
    dc = _find_child(metadata, "dc")
    if dc is None:
        raise OaiError(f"记录 {identifier} 的 metadata 不是 oai_dc")

    titles = [
        text
        for el in _find_children(dc, "title")
        if (text := _text(el)) is not None
    ]
    if not titles:
        raise OaiError(f"记录 {identifier} 缺少标题且没有删除标记")
    creators = [
        text
        for el in _find_children(dc, "creator")
        if (text := _text(el)) is not None
    ]
    return OaiRecord(
        identifier=identifier,
        datestamp=datestamp,
        deleted=False,
        title=" / ".join(titles),
        creator="; ".join(creators) if creators else None,
    )


def parse_list_records(content: bytes) -> OaiPage:
    root, response_date = parse_oai_response(content)
    error = _parse_oai_error(root)
    if error is not None:
        raise error
    list_records = _find_child(root, "ListRecords")
    if list_records is None:
        raise OaiError("OAI-PMH 响应中没有 ListRecords")
    records = [
        _parse_record(record_el)
        for record_el in _find_children(list_records, "record")
    ]
    token_el = _find_child(list_records, "resumptionToken")
    token = _text(token_el)
    complete_list_size = None
    cursor = None
    if token_el is not None:
        raw_size = token_el.get("completeListSize")
        if raw_size and raw_size.strip().isdigit():
            complete_list_size = int(raw_size.strip())
        raw_cursor = token_el.get("cursor")
        if raw_cursor and raw_cursor.strip().isdigit():
            cursor = int(raw_cursor.strip())
    return OaiPage(
        records=records,
        token=token,
        complete_list_size=complete_list_size,
        cursor=cursor,
        response_date=response_date,
    )


def parse_identify(content: bytes) -> str:
    root, response_date = parse_oai_response(content)
    error = _parse_oai_error(root)
    if error is not None:
        raise error
    identify = _find_child(root, "Identify")
    if identify is None:
        raise OaiError("OAI-PMH 响应中没有 Identify")
    repository_time = _text(_find_child(identify, "responseDate"))
    if repository_time:
        return normalize_datestamp(repository_time, "Identify 的 responseDate")
    if response_date:
        return response_date
    raise OaiError("Identify 响应缺少 responseDate，无法确定源端当前时刻")


class OaiClient:
    def __init__(
        self,
        base_url: str,
        *,
        read_timeout: float = 20.0,
        connect_timeout: float = 10.0,
        max_response_bytes: int = MAX_RESPONSE_BYTES,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.base_url = base_url
        self.max_response_bytes = max_response_bytes
        self._timeout = httpx.Timeout(
            read_timeout, connect=connect_timeout, write=10.0, pool=10.0
        )
        self._transport = transport
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "OaiClient":
        self._client = httpx.AsyncClient(
            timeout=self._timeout,
            follow_redirects=True,
            transport=self._transport,
            headers={"User-Agent": "library-harvester/1.0 (OAI-PMH)"},
        )
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _get(self, params: dict[str, str]) -> bytes:
        if self._client is None:
            raise RuntimeError("OaiClient must be used as an async context manager")
        try:
            async with self._client.stream(
                "GET", self.base_url, params=params
            ) as response:
                if response.status_code != 200:
                    preview = (await response.aread())[:300]
                    raise OaiError(
                        f"源端 HTTP {response.status_code}: "
                        f"{preview.decode('utf-8', 'replace')}"
                    )
                chunks: list[bytes] = []
                total = 0
                async for chunk in response.aiter_bytes():
                    total += len(chunk)
                    if total > self.max_response_bytes:
                        raise OaiError("源端响应超过大小上限")
                    chunks.append(chunk)
                return b"".join(chunks)
        except httpx.TimeoutException as exc:
            raise OaiError(f"访问源端超时: {exc}") from exc
        except httpx.HTTPError as exc:
            raise OaiError(f"访问源端失败: {exc}") from exc

    async def identify_time(self) -> str:
        content = await self._get({"verb": "Identify"})
        return parse_identify(content)

    async def list_records(
        self,
        *,
        from_: str | None,
        until: str,
        token: str | None = None,
    ) -> OaiPage:
        if token:
            params = {"verb": "ListRecords", "resumptionToken": token}
        else:
            params = {"verb": "ListRecords", "metadataPrefix": "oai_dc", "until": until}
            if from_:
                params["from"] = from_
        content = await self._get(params)
        return parse_list_records(content)
