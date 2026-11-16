"""Unit tests for OAI-PMH parsing and timestamps."""

from __future__ import annotations

import pytest

from app.oai import (
    BadResumptionToken,
    NoRecordsMatch,
    OaiError,
    normalize_datestamp,
    parse_identify,
    parse_list_records,
)

OAI_ENVELOPE = """<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <responseDate>2026-01-02T03:04:05Z</responseDate>
  {body}
</OAI-PMH>"""

RECORD_LIVE = """
<record>
  <header>
    <identifier>oai:test:1</identifier>
    <datestamp>2026-01-02T03:04:05Z</datestamp>
  </header>
  <metadata>
    <oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
               xmlns:dc="http://purl.org/dc/elements/1.1/">
      <dc:title>标题一</dc:title>
      <dc:creator>作者甲</dc:creator>
      <dc:creator>作者乙</dc:creator>
    </oai_dc:dc>
  </metadata>
</record>"""

RECORD_DELETED = """
<record>
  <header status="deleted">
    <identifier>oai:test:2</identifier>
    <datestamp>2026-01-02T03:04:06Z</datestamp>
  </header>
</record>"""


def test_normalize_datestamp_second_precision():
    assert normalize_datestamp("2026-01-02T03:04:05Z") == "2026-01-02T03:04:05Z"
    assert normalize_datestamp("2026-01-02T03:04:05.987Z") == "2026-01-02T03:04:05Z"
    # offsets are converted to UTC
    assert normalize_datestamp("2026-01-02T05:04:05+02:00") == "2026-01-02T03:04:05Z"
    assert normalize_datestamp("2026-01-01T22:04:05-05:00") == "2026-01-02T03:04:05Z"


@pytest.mark.parametrize(
    "bad",
    ["2026-01-02", "2026-01-02T03:04Z", "not-a-date", "", None],
)
def test_normalize_datestamp_rejects(bad):
    with pytest.raises(OaiError):
        normalize_datestamp(bad)


def test_parse_live_record_takes_first_page():
    body = f"<ListRecords>{RECORD_LIVE}{RECORD_DELETED}</ListRecords>"
    page = parse_list_records((OAI_ENVELOPE.format(body=body)).encode())
    assert len(page.records) == 2
    live = page.records[0]
    assert live.identifier == "oai:test:1"
    assert live.deleted is False
    assert live.title == "标题一"
    assert live.creator == "作者甲; 作者乙"
    assert page.records[1].deleted is True
    assert page.records[1].title is None
    assert page.token is None


def test_parse_token_metadata_and_response_date():
    body = (
        "<ListRecords>"
        + RECORD_LIVE
        + '<resumptionToken completeListSize="42" cursor="0">tok-1</resumptionToken>'
        "</ListRecords>"
    )
    page = parse_list_records((OAI_ENVELOPE.format(body=body)).encode())
    assert page.token == "tok-1"
    assert page.complete_list_size == 42
    assert page.cursor == 0
    assert page.response_date == "2026-01-02T03:04:05Z"


def test_bad_resumption_token_is_specific_error():
    body = '<error code="badResumptionToken">expired</error>'
    with pytest.raises(BadResumptionToken):
        parse_list_records((OAI_ENVELOPE.format(body=body)).encode())


def test_no_records_match_is_specific_error():
    body = '<error code="noRecordsMatch">empty</error>'
    with pytest.raises(NoRecordsMatch):
        parse_list_records((OAI_ENVELOPE.format(body=body)).encode())


def test_missing_title_without_deleted_marker_fails():
    body = """
    <ListRecords><record>
      <header><identifier>oai:test:3</identifier>
        <datestamp>2026-01-02T03:04:05Z</datestamp></header>
      <metadata><oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
        xmlns:dc="http://purl.org/dc/elements/1.1/"><dc:creator>无名</dc:creator></oai_dc:dc></metadata>
    </record></ListRecords>"""
    with pytest.raises(OaiError, match="标题"):
        parse_list_records((OAI_ENVELOPE.format(body=body)).encode())


def test_doctype_is_rejected():
    with pytest.raises(OaiError, match="DTD"):
        parse_list_records(
            b'<?xml version="1.0"?><!DOCTYPE x [<!ENTITY a "b">]><OAI-PMH/>'
        )


def test_parse_identify_uses_repository_time():
    body = """
    <Identify>
      <repositoryName>demo</repositoryName>
      <responseDate>2026-02-03T04:05:06Z</responseDate>
    </Identify>"""
    assert parse_identify(OAI_ENVELOPE.format(body=body).encode()) == "2026-02-03T04:05:06Z"
