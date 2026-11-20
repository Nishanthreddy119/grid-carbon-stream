"""Poll EIA (or the mock generator) and publish events to Redpanda.

Usage
  python -m gridcarbon.producer --mock           # synthetic data, no API key needed
  python -m gridcarbon.producer                  # real EIA data, polls every POLL_SECONDS
  python -m gridcarbon.producer --once           # one poll, then exit (good for cron / Dagster later)
"""
from __future__ import annotations

import argparse
import json
import logging
import signal
import time
from datetime import datetime, timedelta, timezone

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from . import eia, mock
from .config import TOPIC_DEMAND, TOPIC_FUEL_MIX, get_settings

log = logging.getLogger("producer")
_running = True


def _stop(*_):
    global _running
    _running = False


def event_key(e: dict) -> str:
    """Stable key: same logical row -> same key -> same partition (keeps per-region order)."""
    dim = e.get("fueltype") or e.get("type")
    return f"{e['respondent']}|{e['period']}|{dim}"


def ensure_topics(bootstrap: str, partitions: int = 3) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = set(admin.list_topics(timeout=10).topics)
    new = [NewTopic(t, num_partitions=partitions, replication_factor=1)
           for t in (TOPIC_FUEL_MIX, TOPIC_DEMAND) if t not in existing]
    if not new:
        return
    for topic, fut in admin.create_topics(new).items():
        try:
            fut.result()
            log.info("created topic %s", topic)
        except KafkaException as e:
            if "TOPIC_ALREADY_EXISTS" not in str(e):
                raise


def make_producer(bootstrap: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,  # no duplicates from producer retries
            "acks": "all",
            "linger.ms": 50,
            "compression.type": "zstd",
        }
    )


def publish(producer: Producer, topic: str, events: list[dict]) -> int:
    def on_delivery(err, msg):
        if err:
            log.error("delivery failed for %s: %s", msg.key(), err)

    for e in events:
        producer.produce(topic, key=event_key(e), value=json.dumps(e), on_delivery=on_delivery)
        producer.poll(0)
    producer.flush(30)
    return len(events)


def poll_eia(settings) -> tuple[list[dict], list[dict]]:
    start, end = eia.lookback_window(settings.lookback_hours)
    ingested_at = datetime.now(timezone.utc).isoformat()
    fuel = eia.fetch_fuel_mix(settings.eia_api_key, settings.regions, start, end, ingested_at)
    demand = eia.fetch_demand(settings.eia_api_key, settings.regions, start, end, ingested_at)
    return fuel, demand


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mock", action="store_true", help="publish synthetic data instead of calling EIA")
    parser.add_argument("--once", action="store_true", help="run a single poll and exit")
    parser.add_argument("--mock-interval", type=float, default=5.0,
                        help="seconds between simulated hours in mock mode (default 5)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    s = get_settings()
    ensure_topics(s.kafka_bootstrap)
    producer = make_producer(s.kafka_bootstrap)
    log.info("regions=%s mode=%s", ",".join(s.regions), "mock" if args.mock else "eia")

    if args.mock:
        # Backfill the lookback window, then emit one new simulated hour per interval.
        now = datetime.now(timezone.utc)
        start = now - timedelta(hours=s.lookback_hours)
        fuel, demand = mock.generate(s.regions, start, s.lookback_hours)
        log.info("mock backfill: %d fuel + %d demand events",
                 publish(producer, TOPIC_FUEL_MIX, fuel), publish(producer, TOPIC_DEMAND, demand))
        next_hour = start + timedelta(hours=s.lookback_hours)
        while _running and not args.once:
            time.sleep(args.mock_interval)
            fuel, demand = mock.generate(s.regions, next_hour, 1)
            publish(producer, TOPIC_FUEL_MIX, fuel)
            publish(producer, TOPIC_DEMAND, demand)
            log.info("mock hour %s published", eia.hour_str(next_hour))
            next_hour += timedelta(hours=1)
        return

    while _running:
        try:
            fuel, demand = poll_eia(s)
            log.info("published %d fuel + %d demand events",
                     publish(producer, TOPIC_FUEL_MIX, fuel), publish(producer, TOPIC_DEMAND, demand))
        except eia.EIAError as e:
            log.error("%s", e)
        if args.once:
            break
        # Sleep in small steps so Ctrl+C is responsive
        for _ in range(s.poll_seconds):
            if not _running:
                break
            time.sleep(1)


if __name__ == "__main__":
    main()
