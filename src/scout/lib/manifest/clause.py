# src/scout/lib/manifest/clause.py
"""Table-agnostic SQL pieces shared across the manifest package.
Author: Marcus
Created: 2026-09-30
License: AGPL-3.0-or-later
"""

from typing import NamedTuple


class SqlWhere(NamedTuple):
    """One SQL predicate with its bound values; the form a fragment takes
    when it leaves the repo that owns it. Unpacks as (where, params)."""

    sql: str
    params: tuple[str, ...]
