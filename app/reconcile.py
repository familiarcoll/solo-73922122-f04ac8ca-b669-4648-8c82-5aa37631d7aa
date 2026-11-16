"""Field-level three-way reconciliation for catalogue revisions.

A cataloguer edits a record against the source value they saw (the *basis*).
Later harvests may bring a newer source value.  For every field the three
values are compared independently:

* no revision for the field          -> the source value wins;
* only the revision changed          -> the revision value wins;
* only the source changed            -> the source value wins;
* both changed to the same value     -> that value wins, no conflict;
* both changed to different values   -> keep the revision value and flag a
  conflict, so a cataloguer can resolve it explicitly.

The comparison is by value, never by timestamp.  OAI datestamps only have
second precision, so two corrections inside one second would otherwise be
indistinguishable; content versions (see :mod:`app.store`) carry that
information instead.
"""

from __future__ import annotations

from dataclasses import dataclass


def clean(value: str | None) -> str:
    """Canonical text used for comparison and storage."""
    return "" if value is None else str(value).strip()


@dataclass(frozen=True)
class FieldMerge:
    value: str
    conflict: bool
    origin: str


def merge_field(
    *,
    base: str | None,
    revision: str | None,
    source: str | None,
    edited: bool,
) -> FieldMerge:
    """Merge one field.

    ``edited`` says whether the revision actually covers this field.  When it
    does not, the source is authoritative and no conflict can exist.
    """
    source_value = clean(source)
    if not edited:
        return FieldMerge(value=source_value, conflict=False, origin="source")

    base_value = clean(base)
    revision_value = clean(revision)

    if source_value == revision_value:
        # Both sides agree (this includes the case where the source caught up
        # with the revision, or both changed to the same new value).
        return FieldMerge(value=revision_value, conflict=False, origin="merged")
    if source_value == base_value:
        # The source has not moved since the revision was made.
        return FieldMerge(value=revision_value, conflict=False, origin="revision")
    if revision_value == base_value:
        # The revision did not actually change anything; the source moved.
        return FieldMerge(value=source_value, conflict=False, origin="source")
    # Both sides moved, in different directions.  The revision value stays
    # visible and the field is marked as conflicted.
    return FieldMerge(value=revision_value, conflict=True, origin="revision")
