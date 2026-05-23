# Kafka Watchdog

Monitors Kafka topics and raises an alarm when no messages have been received within a configurable threshold.

## Configuration

Edit `config.yaml` to set your bootstrap servers and define which topics to watch:

```yaml
kafka:
  bootstrap_servers:
    - "kafka:9092"

watchdogs:
  - topic: "orders"
    threshold: 20       # seconds of silence before ERROR
```

The bootstrap servers can also be set via the `KAFKA_BOOTSTRAP_SERVERS` environment variable, which takes precedence over the config file.

## Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /state` | Per-topic status: `OK`, `ERROR`, or `UNKNOWN` |
| `GET /metrics` | Prometheus metrics |
| `GET /health` | Basic health check |
| `GET /livez` | Liveness probe |
| `GET /readyz` | Readiness probe — 503 until first message received |
| `GET /version` | Current version |

### Example `/state` response

```json
[
  {
    "topic": "orders",
    "status": "OK",
    "threshold_seconds": 20,
    "last_message": "2026-05-23T18:48:53+00:00"
  }
]
```

## Prometheus metrics

```
kafka_watchdog_topic_alarm{topic="orders"}              # 0=OK, 1=ERROR, -1=UNKNOWN
kafka_watchdog_threshold_seconds{topic="orders"}        # configured threshold
kafka_watchdog_last_message_timestamp{topic="orders"}   # unix timestamp of last message
```

## Docker

```bash
services:
  kafka-watchdog:
    container_name: kafka-watchdog
    ports:
      - "8000:8000"
    volumes:
      - ./config.yaml:/app/config.yaml:ro
```
`docker compose up -d`

## License

This project is licensed under the [GNU General Public License v3.0](LICENSE).