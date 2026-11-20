"""DuckDB storage: idempotent upserts of raw events + carbon-intensity calculation.

Design notes
- Every table has a natural primary key, and writes use INSERT OR REPLACE.
  EIA revises recent hours and the producer re-sends a lookback window on
  every poll, so the same row arrives many times. Upserts make that harmless.
- Carbon intensity is recomputed only for (region, hour) pairs touched by
  the current batch, not the whole table.
- DuckDB allows one writer process at a time. The consumer opens a
  connection per batch and closes it, so the dashboard can read in between.
"""
from __future__ import annotations

import time
from contextlib import contextmanager
from pathlib import Path

import duckdb

from .config import CLEAN_FUELS, EMISSION_FACTORS_KG_PER_MWH, FOSSIL_FUELS

SCHEMA = """
CREATE TABLE IF NOT EXISTS fuel_mix (
    respondent      VARCHAR   NOT NULL,
    respondent_name VARCHAR,
    period          TIMESTAMP NOT NULL,   -- hour beginning, UTC
    fueltype        VARCHAR   NOT NULL,
    mwh             DOUBLE    NOT NULL,
    source          VARCHAR,
    ingested_at     TIMESTAMPTZ,
    PRIMARY KEY (respondent, period, fueltype)
);

CREATE TABLE IF NOT EXISTS demand (
    respondent      VARCHAR   NOT NULL,
    respondent_name VARCHAR,
    period          TIMESTAMP NOT NULL,
    type            VARCHAR   NOT NULL,   -- D = demand, DF = day-ahead forecast
    mwh             DOUBLE    NOT NULL,
    source          VARCHAR,
    ingested_at     TIMESTAMPTZ,
    PRIMARY KEY (respondent, period, type)
);

CREATE TABLE IF NOT EXISTS emission_factors (
    fueltype   VARCHAR PRIMARY KEY,
    kg_per_mwh DOUBLE  NOT NULL,
    category   VARCHAR NOT NULL           -- fossil | clean | other
);

CREATE TABLE IF NOT EXISTS carbon_intensity (
    respondent      VARCHAR   NOT NULL,
    period          TIMESTAMP NOT NULL,
    generation_mwh  DOUBLE,
    co2_tonnes      DOUBLE,
    g_co2_per_kwh   DOUBLE,               -- kg/MWh is numerically equal to g/kWh
    fossil_share    DOUBLE,
    clean_share     DOUBLE,
    fuels_reported  INTEGER,
    unknown_fuels   VARCHAR,              -- fuel codes with no emission factor (data-quality flag)
    computed_at     TIMESTAMPTZ,
    PRIMARY KEY (respondent, period)
);
"""


def _category(fuel: str) -> str:
    if fuel in FOSSIL_FUELS:
        return "fossil"
    if fuel in CLEAN_FUELS:
        return "clean"
    return "other"


@contextmanager
def connect(path: Path | str, read_only: bool = False, retries: int = 20, wait: float = 0.25):
    """Open DuckDB, retrying briefly if another process holds the write lock."""
    path = Path(path)
    if not read_only:
        path.parent.mkdir(parents=True, exist_ok=True)
    last_err = None
    for _ in range(retries):
        try:
            con = duckdb.connect(str(path), read_only=read_only)
            break
        except duckdb.IOException as e:  # file is locked by the other process
            last_err = e
            time.sleep(wait)
    else:
        raise last_err
    try:
        yield con
    finally:
        con.close()


def init_db(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(SCHEMA)
    con.executemany(
        "INSERT OR REPLACE INTO emission_factors VALUES (?, ?, ?)",
        [(f, kg, _category(f)) for f, kg in EMISSION_FACTORS_KG_PER_MWH.items()],
    )


def upsert_fuel_mix(con, events: list[dict]) -> int:
    if not events:
        return 0
    con.executemany(
        """INSERT OR REPLACE INTO fuel_mix
           VALUES (?, ?, strptime(?, '%Y-%m-%dT%H'), ?, ?, ?, ?)""",
        [
            (e["respondent"], e.get("respondent_name"), e["period"], e["fueltype"], e["mwh"],
             e.get("source"), e.get("ingested_at"))
            for e in events
        ],
    )
    return len(events)


def upsert_demand(con, events: list[dict]) -> int:
    if not events:
        return 0
    con.executemany(
        """INSERT OR REPLACE INTO demand
           VALUES (?, ?, strptime(?, '%Y-%m-%dT%H'), ?, ?, ?, ?)""",
        [
            (e["respondent"], e.get("respondent_name"), e["period"], e["type"], e["mwh"],
             e.get("source"), e.get("ingested_at"))
            for e in events
        ],
    )
    return len(events)


def recompute_intensity(con, keys: set[tuple[str, str]]) -> int:
    """Recompute carbon intensity for the given (respondent, 'YYYY-MM-DDTHH') pairs.

    intensity = sum(MWh_fuel * kg_per_MWh_fuel) / sum(MWh_fuel)
    Negative generation (battery or pumped-storage charging) is excluded from
    both sides: it is consumption, not generation.
    """
    if not keys:
        return 0
    con.execute("CREATE OR REPLACE TEMP TABLE touched (respondent VARCHAR, period TIMESTAMP)")
    con.executemany(
        "INSERT INTO touched VALUES (?, strptime(?, '%Y-%m-%dT%H'))", sorted(keys)
    )
    con.execute(
        """
        INSERT OR REPLACE INTO carbon_intensity
        SELECT
            f.respondent,
            f.period,
            SUM(f.mwh)                                                         AS generation_mwh,
            SUM(f.mwh * COALESCE(ef.kg_per_mwh, 0)) / 1000.0                   AS co2_tonnes,
            SUM(f.mwh * COALESCE(ef.kg_per_mwh, 0)) / NULLIF(SUM(f.mwh), 0)    AS g_co2_per_kwh,
            SUM(f.mwh) FILTER (WHERE ef.category = 'fossil') / NULLIF(SUM(f.mwh), 0) AS fossil_share,
            SUM(f.mwh) FILTER (WHERE ef.category = 'clean')  / NULLIF(SUM(f.mwh), 0) AS clean_share,
            COUNT(*)                                                           AS fuels_reported,
            string_agg(f.fueltype, ',') FILTER (WHERE ef.fueltype IS NULL)     AS unknown_fuels,
            now()                                                              AS computed_at
        FROM fuel_mix f
        JOIN touched t USING (respondent, period)
        LEFT JOIN emission_factors ef USING (fueltype)
        WHERE f.mwh > 0
        GROUP BY f.respondent, f.period
        """
    )
    return len(keys)


def write_batch(path: Path | str, events: list[dict]) -> dict:
    """Upsert a mixed batch of events and refresh intensity. Returns counts."""
    fuel = [e for e in events if e.get("dataset") == "fuel_mix"]
    dem = [e for e in events if e.get("dataset") == "demand"]
    keys = {(e["respondent"], e["period"]) for e in fuel}
    with connect(path) as con:
        init_db(con)
        con.execute("BEGIN")
        try:
            n_fuel = upsert_fuel_mix(con, fuel)
            n_dem = upsert_demand(con, dem)
            n_ci = recompute_intensity(con, keys)
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    return {"fuel_mix": n_fuel, "demand": n_dem, "intensity_hours": n_ci}
