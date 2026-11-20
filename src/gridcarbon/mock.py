"""Synthetic EIA-shaped data so the whole pipeline runs before you have an API key.

The numbers are plausible, not real: each region gets a fixed fuel profile,
a daily demand curve, a solar curve that peaks at local noon, and some noise.
Output uses exactly the same event schema as eia.normalize_*_rows.
"""
from __future__ import annotations

import math
import random
from datetime import datetime, timedelta, timezone

from .eia import hour_str

# Rough average-load (MW) and fuel shares per region, plus UTC offset for the solar curve.
PROFILES = {
    "ERCO": {"load": 50000, "utc": -6, "mix": {"NG": 0.42, "WND": 0.26, "COL": 0.13, "NUC": 0.09, "SUN": 0.09, "OTH": 0.01}},
    "PJM":  {"load": 90000, "utc": -5, "mix": {"NG": 0.44, "NUC": 0.33, "COL": 0.15, "WND": 0.04, "SUN": 0.02, "WAT": 0.01, "OTH": 0.01}},
    "MISO": {"load": 75000, "utc": -6, "mix": {"COL": 0.32, "NG": 0.30, "WND": 0.18, "NUC": 0.15, "SUN": 0.03, "WAT": 0.01, "OTH": 0.01}},
    "CISO": {"load": 28000, "utc": -8, "mix": {"NG": 0.38, "SUN": 0.28, "WAT": 0.12, "WND": 0.09, "NUC": 0.08, "OTH": 0.05}},
    "NYIS": {"load": 17000, "utc": -5, "mix": {"NG": 0.47, "WAT": 0.23, "NUC": 0.21, "WND": 0.05, "SUN": 0.02, "OIL": 0.01, "OTH": 0.01}},
    "ISNE": {"load": 13000, "utc": -5, "mix": {"NG": 0.53, "NUC": 0.24, "WAT": 0.07, "SUN": 0.06, "WND": 0.04, "OTH": 0.06}},
    "SWPP": {"load": 33000, "utc": -6, "mix": {"WND": 0.38, "NG": 0.25, "COL": 0.25, "NUC": 0.06, "WAT": 0.04, "SUN": 0.01, "OTH": 0.01}},
}
DEFAULT_PROFILE = {"load": 20000, "utc": -6, "mix": {"NG": 0.5, "COL": 0.2, "NUC": 0.2, "WND": 0.1}}


def _hour_events(region: str, period: datetime, ingested_at: str, rng: random.Random) -> tuple[list[dict], list[dict]]:
    p = PROFILES.get(region, DEFAULT_PROFILE)
    local_hour = (period.hour + p["utc"]) % 24
    # Demand: low ~4am, high ~6pm local
    demand_shape = 1 + 0.18 * math.sin((local_hour - 10) / 24 * 2 * math.pi)
    demand = p["load"] * demand_shape * rng.uniform(0.97, 1.03)
    solar_shape = max(0.0, math.sin((local_hour - 6) / 12 * math.pi)) if 6 <= local_hour <= 18 else 0.0
    wind_shape = rng.uniform(0.5, 1.5)

    # Variable renewables move; gas fills whatever is left so supply matches demand.
    gen = {}
    for fuel, share in p["mix"].items():
        if fuel == "SUN":
            gen[fuel] = demand * share * 2.2 * solar_shape
        elif fuel == "WND":
            gen[fuel] = demand * share * wind_shape
        elif fuel != "NG":
            gen[fuel] = demand * share * rng.uniform(0.95, 1.05)
    gen["NG"] = max(demand * 0.05, demand - sum(gen.values()))

    period_s = hour_str(period)
    fuel_events = [
        {
            "source": "mock", "dataset": "fuel_mix", "respondent": region, "respondent_name": f"{region} (mock)",
            "period": period_s, "fueltype": fuel, "mwh": round(mwh, 1), "ingested_at": ingested_at,
        }
        for fuel, mwh in gen.items()
    ]
    demand_events = [
        {
            "source": "mock", "dataset": "demand", "respondent": region, "respondent_name": f"{region} (mock)",
            "period": period_s, "type": t, "mwh": round(v, 1), "ingested_at": ingested_at,
        }
        for t, v in (("D", demand), ("DF", demand * rng.uniform(0.97, 1.03)))
    ]
    return fuel_events, demand_events


def generate(regions, start: datetime, hours: int, seed: int | None = None) -> tuple[list[dict], list[dict]]:
    """Generate `hours` hours of data starting at `start` (UTC, truncated to the hour)."""
    rng = random.Random(seed)
    start = start.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    ingested_at = datetime.now(timezone.utc).isoformat()
    fuel, demand = [], []
    for h in range(hours):
        period = start + timedelta(hours=h)
        for region in regions:
            f, d = _hour_events(region, period, ingested_at, rng)
            fuel.extend(f)
            demand.extend(d)
    return fuel, demand
