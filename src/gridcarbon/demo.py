"""Load 48h of mock data straight into DuckDB (no Kafka) to preview the dashboard.

  python -m gridcarbon.demo
"""
from datetime import datetime, timedelta, timezone

from . import mock, storage
from .config import get_settings

if __name__ == "__main__":
    s = get_settings()
    start = datetime.now(timezone.utc) - timedelta(hours=s.lookback_hours)
    fuel, demand = mock.generate(s.regions, start, s.lookback_hours, seed=42)
    print(storage.write_batch(s.duckdb_path, fuel + demand), "->", s.duckdb_path)
