"""Live grid carbon-intensity dashboard.

Run:  streamlit run app/dashboard.py
"""
from __future__ import annotations

from datetime import timedelta

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

from gridcarbon.config import get_settings
from gridcarbon.storage import connect

st.set_page_config(page_title="Grid Carbon Intensity", page_icon="⚡", layout="wide")
settings = get_settings()

FUEL_COLORS = {
    "COL": "#5b5b5b", "NG": "#e8833a", "OIL": "#8c564b", "NUC": "#9467bd",
    "WAT": "#1f77b4", "WND": "#2ca02c", "SUN": "#f2c12e", "OTH": "#bcbd22",
}


def query(sql: str, params=None) -> pd.DataFrame:
    try:
        with connect(settings.duckdb_path, read_only=True) as con:
            return con.execute(sql, params or []).df()
    except (duckdb.IOException, duckdb.CatalogException):
        return pd.DataFrame()


st.title("⚡ Grid carbon intensity")
st.caption(
    "Hourly CO₂ intensity of electricity generation by U.S. grid region, computed from the "
    "EIA-930 Hourly Electric Grid Monitor. Lower is cleaner. Emission factors are approximate."
)

with st.sidebar:
    hours = st.slider("Window (hours)", 12, 168, 48, step=12)
    refresh = st.selectbox("Auto-refresh", ["30s", "60s", "off"], index=0)


@st.fragment(run_every=None if refresh == "off" else refresh)
def live_view() -> None:
    ci = query(
        """
        SELECT ci.*, f.source
        FROM carbon_intensity ci
        LEFT JOIN (SELECT respondent, period, any_value(source) AS source
                   FROM fuel_mix GROUP BY ALL) f USING (respondent, period)
        WHERE ci.period >= (SELECT max(period) FROM carbon_intensity) - to_hours(?::INT)
        ORDER BY ci.period
        """,
        [hours],
    )
    if ci.empty:
        st.info("No data yet. Start the producer and consumer (see README), then this page fills in.")
        return

    if (ci["source"] == "mock").any():
        st.warning("Showing **synthetic mock data**. Run the producer without `--mock` for real EIA data.")

    regions = sorted(ci["respondent"].unique())
    latest_period = ci["period"].max()

    # --- KPI cards: latest intensity per region and change vs 24h earlier -----------------
    cols = st.columns(len(regions))
    for col, region in zip(cols, regions):
        r = ci[ci["respondent"] == region].set_index("period")["g_co2_per_kwh"]
        now_val = r.iloc[-1]
        prev = r.get(r.index[-1] - timedelta(hours=24))
        col.metric(
            region,
            f"{now_val:,.0f} g/kWh",
            None if prev is None or pd.isna(prev) else f"{now_val - prev:+,.0f} vs 24h ago",
            delta_color="inverse",  # going down is good
        )
    st.caption(f"Latest hour with data: {latest_period:%Y-%m-%d %H:00} UTC")

    # --- Intensity over time -------------------------------------------------------------
    line = (
        alt.Chart(ci)
        .mark_line(strokeWidth=2)
        .encode(
            x=alt.X("period:T", title="Hour (UTC)"),
            y=alt.Y("g_co2_per_kwh:Q", title="g CO₂ / kWh"),
            color=alt.Color("respondent:N", title="Region"),
            tooltip=["respondent", alt.Tooltip("period:T", format="%b %d %H:00"),
                     alt.Tooltip("g_co2_per_kwh:Q", format=",.0f", title="g/kWh"),
                     alt.Tooltip("clean_share:Q", format=".0%", title="clean share")],
        )
        .properties(height=340)
    )
    st.altair_chart(line, width="stretch")

    left, right = st.columns([2, 3])

    # --- Cleanest hour per region in the last 24h -----------------------------------------
    with left:
        st.subheader("Cleanest hour, last 24h")
        last24 = ci[ci["period"] > latest_period - timedelta(hours=24)]
        best = last24.loc[last24.groupby("respondent")["g_co2_per_kwh"].idxmin()].copy()
        best["period"] = best["period"].dt.strftime("%b %d, %H:00")
        st.dataframe(
            best[["respondent", "period", "g_co2_per_kwh", "clean_share"]]
            .rename(columns={"respondent": "Region", "period": "Hour (UTC)",
                             "g_co2_per_kwh": "g/kWh", "clean_share": "Clean share"})
            .style.format({"g/kWh": "{:,.0f}", "Clean share": "{:.0%}"}),
            hide_index=True, width="stretch",
        )
        flagged = ci.dropna(subset=["unknown_fuels"])
        if not flagged.empty:
            st.caption(f"⚠️ Fuel codes without an emission factor: "
                       f"{', '.join(sorted(set(','.join(flagged['unknown_fuels']).split(','))))}")

    # --- Fuel mix for one region -----------------------------------------------------------
    with right:
        region = st.selectbox("Fuel mix for region", regions, key="mix_region")
        mix = query(
            """SELECT period, fueltype, greatest(mwh, 0) AS mwh FROM fuel_mix
               WHERE respondent = ? AND period >= ? ORDER BY period""",
            [region, latest_period - timedelta(hours=hours)],
        )
        area = (
            alt.Chart(mix)
            .mark_area()
            .encode(
                x=alt.X("period:T", title=None),
                y=alt.Y("mwh:Q", stack="zero", title="MWh"),
                color=alt.Color("fueltype:N", title="Fuel",
                                scale=alt.Scale(domain=list(FUEL_COLORS), range=list(FUEL_COLORS.values()))),
                tooltip=["fueltype", alt.Tooltip("mwh:Q", format=",.0f"),
                         alt.Tooltip("period:T", format="%b %d %H:00")],
            )
            .properties(height=280)
        )
        st.altair_chart(area, width="stretch")


live_view()
