"""
consumer.py - Kafka consumer with manual offset commit and dead-letter queue
"""

import json
import logging
import signal
import sys
from confluent_kafka import Consumer, KafkaException, KafkaError, Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CONSUMER_CONFIG = {
      "bootstrap.servers": "localhost:9092",
      "group.id": "event-processor-group",
      "auto.offset.reset": "earliest",
      "enable.auto.commit": False,
      "max.poll.interval.ms": 300000,
}

DLQ_CONFIG = {"bootstrap.servers": "localhost:9092", "acks": "all"}

TOPIC     = "user-events"
DLQ_TOPIC = "user-events-dlq"

_running = True


def handle_shutdown(sig, frame):
      global _running
      logger.info("Shutdown signal received")
      _running = False


signal.signal(signal.SIGINT,  handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


def process_event(event: dict) -> dict:
      """Apply processing logic — enrich and validate."""
      if event.get("amount", 0) <= 0:
                raise ValueError(f"Invalid amount: {event.get('amount')}")
            event["processed"] = True
    event["amount_usd"] = round(event["amount"] * (1.08 if event.get("currency") == "eur" else 1.0), 2)
    return event


def send_to_dlq(producer: Producer, event_bytes: bytes, reason: str):
      payload = json.dumps({"raw": event_bytes.decode("utf-8"), "error": reason})
    producer.produce(DLQ_TOPIC, value=payload.encode("utf-8"))
    producer.poll(0)
    logger.warning(f"Sent to DLQ: {reason}")


def run():
      consumer = Consumer(CONSUMER_CONFIG)
    dlq_producer = Producer(DLQ_CONFIG)
    consumer.subscribe([TOPIC])
    logger.info(f"Subscribed to topic '{TOPIC}'")

    processed = failed = 0
    try:
              while _running:
                            msg = consumer.poll(timeout=1.0)
                            if msg is None:
                                              continue
                                          if msg.error():
                                                            if msg.error().code() == KafkaError._PARTITION_EOF:
                                                                                  logger.info(f"Reached end of partition {msg.partition()}")
                            else:
                                                  raise KafkaException(msg.error())
                                              continue
                            try:
                                              event = json.loads(msg.value().decode("utf-8"))
                                              process_event(event)
                                              consumer.commit(msg)
                                              processed += 1
                                              if processed % 50 == 0:
                                                                    logger.info(f"Processed={processed} Failed={failed}")
                            except Exception as e:
                                              send_to_dlq(dlq_producer, msg.value(), str(e))
                                              consumer.commit(msg)
                                              failed += 1
    finally:
        dlq_producer.flush()
              consumer.close()
        logger.info(f"Shutdown complete. Processed={processed} Failed={failed}")


if __name__ == "__main__":
      run()
