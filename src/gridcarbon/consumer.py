"""Consume EIA events from Redpanda, upsert to DuckDB, compute carbon intensity.

Delivery semantics: at-least-once. Offsets are committed only AFTER the DuckDB
transaction commits. If the consumer crashes in between, the batch is re-read
and re-applied, and the idempotent upserts make the result identical.

Usage
  python -m gridcarbon.consumer
"""
from __future__ import annotations

import json
import logging
import signal

from confluent_kafka import Consumer, KafkaError

from . import storage
from .config import TOPIC_DEMAND, TOPIC_FUEL_MIX, get_settings
from .producer import ensure_topics


log = logging.getLogger("consumer")
_running = True


def _stop(*_):
    global _running
    _running = False


def decode(messages) -> tuple[list[dict], int]:
    """Parse message values; count (and skip) anything malformed instead of crashing."""
    events, bad = [], 0
    for m in messages:
        if m.error():
            if m.error().code() != KafkaError._PARTITION_EOF:
                log.error("kafka error: %s", m.error())
            continue
        try:
            events.append(json.loads(m.value()))
        except (TypeError, ValueError):
            bad += 1
    return events, bad


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    s = get_settings()
    ensure_topics(s.kafka_bootstrap)  # so start order doesn't matter

    consumer = Consumer(
        {
            "bootstrap.servers": s.kafka_bootstrap,
            "group.id": "carbon-intensity",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC_FUEL_MIX, TOPIC_DEMAND])
    log.info("consuming %s, %s -> %s", TOPIC_FUEL_MIX, TOPIC_DEMAND, s.duckdb_path)

    try:
        while _running:
            msgs = consumer.consume(num_messages=1000, timeout=1.0)
            if not msgs:
                continue
            events, bad = decode(msgs)
            if events:
                counts = storage.write_batch(s.duckdb_path, events)
                log.info("batch: %s%s", counts, f" (skipped {bad} malformed)" if bad else "")
                if events or bad:  # only commit when real messages were read
                     consumer.commit(asynchronous=False)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
