"""Central configuration, read from environment variables (or a .env file)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # python-dotenv is optional
    pass

# Kafka topics. One topic per source dataset keeps schemas clean and lets
# consumers subscribe only to what they need.
TOPIC_FUEL_MIX = "eia.fuel_mix.raw"
TOPIC_DEMAND = "eia.demand.raw"

# Approximate direct-combustion CO2 emission factors, kg CO2 per MWh generated.
# These are reasonable starting values for a demo, NOT authoritative numbers.
# Next step for the project: replace with plant-weighted factors from EPA eGRID
# and document the source in the README.
EMISSION_FACTORS_KG_PER_MWH: dict[str, float] = {
    "COL": 1000.0,  # coal
    "NG": 410.0,    # natural gas (blend of combined-cycle and peakers)
    "OIL": 850.0,   # petroleum
    "NUC": 0.0,     # nuclear
    "WAT": 0.0,     # hydro
    "SUN": 0.0,     # solar
    "WND": 0.0,     # wind
    "OTH": 0.0,     # other (biomass, geothermal, etc.) - flagged as an assumption
}
FOSSIL_FUELS = {"COL", "NG", "OIL"}
CLEAN_FUELS = {"NUC", "WAT", "SUN", "WND"}


@dataclass(frozen=True)
class Settings:
    eia_api_key: str = field(default_factory=lambda: os.getenv("EIA_API_KEY", ""))
    kafka_bootstrap: str = field(default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP", "localhost:19092"))
    regions: tuple[str, ...] = field(
        default_factory=lambda: tuple(
            r.strip().upper()
            for r in os.getenv("REGIONS", "ERCO,PJM,MISO,CISO,NYIS").split(",")
            if r.strip()
        )
    )
    lookback_hours: int = field(default_factory=lambda: int(os.getenv("LOOKBACK_HOURS", "48")))
    poll_seconds: int = field(default_factory=lambda: int(os.getenv("POLL_SECONDS", "900")))
    duckdb_path: Path = field(default_factory=lambda: Path(os.getenv("DUCKDB_PATH", "data/grid.duckdb")))


def get_settings() -> Settings:
    return Settings()
