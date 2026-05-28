# kafka-streaming-pipeline

Real-time event streaming pipeline with **Apache Kafka** and Python — producer with retry logic, consumer with manual offset commits, and a dead-letter queue for failed messages.

## Architecture

```
Event Source -> [Producer] -> Kafka: user-events -> [Consumer] -> Processed
                                                          |
                                                     [DLQ Topic]
```

## Features

- Producer: UUID-keyed events, delivery callbacks, snappy compression, configurable retries
- - Consumer: Manual offset commits, graceful shutdown (SIGTERM/SIGINT), DLQ routing
  - - Event enrichment: currency normalization, amount validation
    - - Horizontally scalable via Kafka consumer groups
     
      - ## Project Structure
     
      - ```
        kafka-streaming-pipeline/
        ├── producer.py      # Kafka producer with retry and delivery report
        ├── consumer.py      # Consumer with DLQ and offset control
        └── requirements.txt
        ```

        ## Quick Start

        ```bash
        docker run -d -p 9092:9092 apache/kafka:latest
        pip install confluent-kafka
        python consumer.py   # Terminal 1
        python producer.py   # Terminal 2
        ```

        ## Tech Stack

        `confluent-kafka` · `Apache Kafka 3.x` · `Python 3.11`
