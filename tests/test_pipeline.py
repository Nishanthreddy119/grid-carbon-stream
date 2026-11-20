import json
from datetime import datetime, timezone

import pytest

from gridcarbon import eia, mock, storage
from gridcarbon.consumer import decode
from gridcarbon.producer import event_key

TS = "2026-11-18T00:00:00+00:00"


def fuel(region, period, fueltype, mwh):
    return {"source": "test", "dataset": "fuel_mix", "respondent": region, "respondent_name": region,
            "period": period, "fueltype": fueltype, "mwh": mwh, "ingested_at": TS}


def intensity(db, region, period):
    with storage.connect(db, read_only=True) as con:
        return con.execute(
            """SELECT g_co2_per_kwh, fossil_share, clean_share, fuels_reported, unknown_fuels
               FROM carbon_intensity WHERE respondent = ? AND period = strptime(?, '%Y-%m-%dT%H')""",
            [region, period],
        ).fetchone()


# ---------- EIA client ----------------------------------------------------------------------

def test_build_params_repeats_facets():
    p = eia.build_params("KEY", ["PJM", "MISO"], "2026-11-17T00", "2026-11-18T00",
                         extra_facets={"type": ["D", "DF"]})
    assert p.count(("facets[respondent][]", "PJM")) == 1
    assert ("facets[respondent][]", "MISO") in p
    assert ("facets[type][]", "DF") in p
    assert ("frequency", "hourly") in p


def test_normalize_drops_null_and_parses_strings():
    rows = [
        {"period": "2026-11-17T06", "respondent": "PJM", "respondent-name": "PJM", "fueltype": "NG", "value": "1234.5"},
        {"period": "2026-11-17T06", "respondent": "PJM", "respondent-name": "PJM", "fueltype": "SUN", "value": None},
    ]
    out = eia.normalize_fuel_rows(rows, TS)
    assert len(out) == 1 and out[0]["mwh"] == 1234.5 and out[0]["dataset"] == "fuel_mix"


class FakeResp:
    def __init__(self, payload, status=200):
        self.payload, self.status_code, self.text = payload, status, ""

    def json(self):
        return self.payload


class FakeSession:
    """Serves two pages to check offset pagination."""

    def __init__(self):
        self.calls = []

    def get(self, url, params, timeout):
        offset = dict(params)["offset"]
        self.calls.append(offset)
        rows = [{"period": "2026-11-17T06", "respondent": "PJM", "fueltype": "NG", "value": 1}] * (
            eia.PAGE_SIZE if offset == 0 else 3)
        return FakeResp({"response": {"total": str(eia.PAGE_SIZE + 3), "data": rows}})


def test_fetch_route_paginates():
    s = FakeSession()
    rows = eia.fetch_route("electricity/rto/fuel-type-data", "KEY", ["PJM"], "a", "b", session=s)
    assert len(rows) == eia.PAGE_SIZE + 3
    assert s.calls == [0, eia.PAGE_SIZE]


def test_fetch_requires_key():
    with pytest.raises(eia.EIAError):
        eia.fetch_route("x", "", ["PJM"], "a", "b")


# ---------- Storage / carbon math -------------------------------------------------------------

def test_intensity_math(tmp_path):
    db = tmp_path / "t.duckdb"
    # 600 MWh coal (1000 kg/MWh) + 400 MWh wind (0) -> 600,000 kg / 1000 MWh = 600 g/kWh
    storage.write_batch(db, [fuel("PJM", "2026-11-17T06", "COL", 600), fuel("PJM", "2026-11-17T06", "WND", 400)])
    g, fossil, clean, n, unknown = intensity(db, "PJM", "2026-11-17T06")
    assert g == pytest.approx(600)
    assert fossil == pytest.approx(0.6) and clean == pytest.approx(0.4)
    assert n == 2 and unknown is None


def test_upserts_are_idempotent_and_revisions_win(tmp_path):
    db = tmp_path / "t.duckdb"
    batch = [fuel("PJM", "2026-11-17T06", "NG", 1000)]
    storage.write_batch(db, batch)
    storage.write_batch(db, batch)  # duplicate delivery
    storage.write_batch(db, [fuel("PJM", "2026-11-17T06", "WND", 1000)])  # late-arriving fuel
    with storage.connect(db, read_only=True) as con:
        assert con.execute("SELECT count(*) FROM fuel_mix").fetchone()[0] == 2
    assert intensity(db, "PJM", "2026-11-17T06")[0] == pytest.approx(205)  # (1000*410)/2000
    storage.write_batch(db, [fuel("PJM", "2026-11-17T06", "WND", 3000)])  # EIA revision
    assert intensity(db, "PJM", "2026-11-17T06")[0] == pytest.approx(102.5)  # 410000/4000


def test_negative_storage_and_unknown_fuel(tmp_path):
    db = tmp_path / "t.duckdb"
    storage.write_batch(db, [
        fuel("CISO", "2026-11-17T20", "NG", 1000),
        fuel("CISO", "2026-11-17T20", "BAT", -200),   # battery charging: excluded
        fuel("CISO", "2026-11-17T20", "GEO", 500),    # no emission factor configured
    ])
    g, _, _, n, unknown = intensity(db, "CISO", "2026-11-17T20")
    assert n == 2
    assert unknown == "GEO"
    assert g == pytest.approx(1000 * 410 / 1500)


# ---------- Mock + end to end through the Kafka message format ---------------------------------

class FakeMsg:
    def __init__(self, value):
        self._v = value

    def error(self):
        return None

    def value(self):
        return self._v


def test_end_to_end_mock_through_message_format(tmp_path):
    db = tmp_path / "t.duckdb"
    regions = ["ERCO", "PJM", "CISO"]
    f, d = mock.generate(regions, datetime(2026, 11, 17, tzinfo=timezone.utc), 24, seed=7)
    keys = {event_key(e) for e in f}
    assert len(keys) == len(f)  # keys are unique per logical row

    msgs = [FakeMsg(json.dumps(e).encode()) for e in f + d] + [FakeMsg(b"not json")]
    events, bad = decode(msgs)
    assert bad == 1
    counts = storage.write_batch(db, events)
    assert counts["intensity_hours"] == 24 * len(regions)

    with storage.connect(db, read_only=True) as con:
        rows = con.execute(
            "SELECT respondent, min(g_co2_per_kwh), max(g_co2_per_kwh) FROM carbon_intensity GROUP BY 1"
        ).fetchall()
    for _, lo, hi in rows:
        assert 0 < lo < hi < 1000  # plausible range, and it varies over the day
