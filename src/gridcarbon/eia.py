"""Client for the EIA Open Data API v2 (EIA-930 Hourly Electric Grid Monitor).

Docs: https://www.eia.gov/opendata/  (register for a free key there)

Routes used:
  electricity/rto/fuel-type-data  -> hourly net generation by fuel type (MWh)
  electricity/rto/region-data     -> hourly demand (type D), forecast (DF), net generation (NG), interchange (TI)
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import requests

BASE_URL = "https://api.eia.gov/v2"
PAGE_SIZE = 5000  # EIA's maximum rows per request
log = logging.getLogger(__name__)


class EIAError(RuntimeError):
    pass


def hour_str(dt: datetime) -> str:
    """EIA hourly period format, in UTC: 2026-11-17T06"""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H")


def lookback_window(hours: int, now: datetime | None = None) -> tuple[str, str]:
    now = now or datetime.now(timezone.utc)
    return hour_str(now - timedelta(hours=hours)), hour_str(now)


def build_params(
    api_key: str,
    regions: Iterable[str],
    start: str,
    end: str,
    offset: int = 0,
    extra_facets: dict[str, Iterable[str]] | None = None,
) -> list[tuple[str, Any]]:
    """EIA uses repeated query keys for facets, so params must be a list of tuples."""
    params: list[tuple[str, Any]] = [
        ("api_key", api_key),
        ("frequency", "hourly"),
        ("data[0]", "value"),
        ("start", start),
        ("end", end),
        ("sort[0][column]", "period"),
        ("sort[0][direction]", "desc"),
        ("length", PAGE_SIZE),
        ("offset", offset),
    ]
    for r in regions:
        params.append(("facets[respondent][]", r))
    for facet, values in (extra_facets or {}).items():
        for v in values:
            params.append((f"facets[{facet}][]", v))
    return params


def _get_with_retry(session: requests.Session, url: str, params, retries: int = 4) -> dict:
    for attempt in range(retries + 1):
        resp = session.get(url, params=params, timeout=60)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code in (429, 500, 502, 503, 504) and attempt < retries:
            wait = 2 ** attempt
            log.warning("EIA returned %s, retrying in %ss", resp.status_code, wait)
            time.sleep(wait)
            continue
        # Never log the URL: it contains the API key.
        raise EIAError(f"EIA request failed with HTTP {resp.status_code}: {resp.text[:300]}")
    raise EIAError("unreachable")


def fetch_route(
    route: str,
    api_key: str,
    regions: Iterable[str],
    start: str,
    end: str,
    extra_facets: dict[str, Iterable[str]] | None = None,
    session: requests.Session | None = None,
) -> list[dict]:
    """Fetch every row for a route and window, following EIA's offset pagination."""
    if not api_key:
        raise EIAError("EIA_API_KEY is not set. Register at https://www.eia.gov/opendata/ or use --mock.")
    session = session or requests.Session()
    url = f"{BASE_URL}/{route}/data/"
    rows: list[dict] = []
    offset = 0
    while True:
        payload = _get_with_retry(session, url, build_params(api_key, regions, start, end, offset, extra_facets))
        response = payload.get("response", {})
        page = response.get("data", [])
        rows.extend(page)
        total = int(response.get("total", len(rows)) or 0)
        offset += len(page)
        if not page or offset >= total:
            break
    return rows


def _to_float(value) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_fuel_rows(rows: list[dict], ingested_at: str) -> list[dict]:
    """Map raw EIA fuel-type rows to our event schema. Drops rows with no value."""
    out = []
    for r in rows:
        mwh = _to_float(r.get("value"))
        if mwh is None:
            continue
        out.append(
            {
                "source": "eia",
                "dataset": "fuel_mix",
                "respondent": r["respondent"],
                "respondent_name": r.get("respondent-name"),
                "period": r["period"],
                "fueltype": r["fueltype"],
                "mwh": mwh,
                "ingested_at": ingested_at,
            }
        )
    return out


def normalize_region_rows(rows: list[dict], ingested_at: str) -> list[dict]:
    out = []
    for r in rows:
        mwh = _to_float(r.get("value"))
        if mwh is None:
            continue
        out.append(
            {
                "source": "eia",
                "dataset": "demand",
                "respondent": r["respondent"],
                "respondent_name": r.get("respondent-name"),
                "period": r["period"],
                "type": r["type"],
                "mwh": mwh,
                "ingested_at": ingested_at,
            }
        )
    return out


def fetch_fuel_mix(api_key: str, regions, start: str, end: str, ingested_at: str) -> list[dict]:
    rows = fetch_route("electricity/rto/fuel-type-data", api_key, regions, start, end)
    return normalize_fuel_rows(rows, ingested_at)


def fetch_demand(api_key: str, regions, start: str, end: str, ingested_at: str) -> list[dict]:
    # D = demand, DF = day-ahead demand forecast
    rows = fetch_route(
        "electricity/rto/region-data", api_key, regions, start, end, extra_facets={"type": ["D", "DF"]}
    )
    return normalize_region_rows(rows, ingested_at)
