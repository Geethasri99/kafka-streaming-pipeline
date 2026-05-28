"""
producer.py - Kafka event producer with retry logic and schema validation
"""

import json
import logging
import time
import uuid
from datetime import datetime
from confluent_kafka import Producer, KafkaException
from schema import validate_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KAFKA_CONFIG = {
      "bootstrap.servers": "localhost:9092",
      "acks": "all",
      "retries": 3,
      "retry.backoff.ms": 500,
      "compression.type": "snappy",
}

TOPIC = "user-events"


def delivery_report(err, msg):
      if err:
                logger.error(f"Delivery failed for key={msg.key()}: {err}")
else:
        logger.info(f"Delivered to {msg.topic()}[{msg.partition()}] offset={msg.offset()}")


def build_event(user_id: str, event_type: str, amount: float, region: str) -> dict:
      return {
                "event_id": str(uuid.uuid4()),
                "user_id": user_id,
                "event_type": event_type,
                "amount": amount,
                "currency": "usd",
                "region": region,
                "timestamp": datetime.utcnow().isoformat(),
      }


def publish(producer: Producer, topic: str, event: dict, max_retries: int = 3):
      validate_event(event)
      key = event["user_id"].encode("utf-8")
      value = json.dumps(event).encode("utf-8")
      for attempt in range(1, max_retries + 1):
                try:
                              producer.produce(topic, key=key, value=value, callback=delivery_report)
                              producer.poll(0)
                              return
except KafkaException as e:
            logger.warning(f"Attempt {attempt} failed: {e}")
            time.sleep(0.5 * attempt)
    logger.error(f"All retries exhausted for event {event['event_id']}")


def run(num_events: int = 100):
      producer = Producer(KAFKA_CONFIG)
      logger.info(f"Publishing {num_events} events to topic '{TOPIC}'")
      for i in range(num_events):
                event = build_event(
                              user_id=f"user_{i % 20}",
                              event_type="purchase" if i % 3 == 0 else "click",
                              amount=round(100 + i * 1.5, 2),
                              region="us-east" if i % 2 == 0 else "eu-west",
                )
                publish(producer, TOPIC, event)
            producer.flush()
    logger.info("All events published")


if __name__ == "__main__":
      run()
