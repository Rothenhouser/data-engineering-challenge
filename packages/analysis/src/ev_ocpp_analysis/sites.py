"""Charger -> site mapping (a nullable analytical dimension).

OCPP frames carry no site information — a *site* is an operator grouping of
chargers, external metadata. We thread an optional ``charger_id -> site_id`` map
through the derived layer so fleet rollups can later aggregate per site. With no
map, ``site_id`` is simply ``None`` everywhere (the iteration-one default).

The default map is read once from the ``OCPP_SITE_MAP`` env var as JSON, e.g.
``{"charger1": "site-a", "charger2": "site-a"}``.
"""

from __future__ import annotations

import json
import os

_DEFAULT_MAP: dict[str, str] = {}
_env = os.environ.get("OCPP_SITE_MAP")
if _env:
    try:
        _DEFAULT_MAP = json.loads(_env)
    except ValueError, TypeError:
        _DEFAULT_MAP = {}


def site_for(charger_id: str, mapping: dict[str, str] | None = None) -> str | None:
    """Return the site for a charger, or ``None`` if unmapped."""
    return (mapping if mapping is not None else _DEFAULT_MAP).get(charger_id)
