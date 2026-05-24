# Testing

Start Kafka:
```bash
docker compose up -d kafka
```

Build Watchdog:
```bash
docker compose up watchdog --build
```

Publish a message:
```bash
echo "hello" | docker exec -i kafka-watchdog-kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 --topic testing1

echo "hello" | docker exec -i kafka-watchdog-kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 --topic testing2
```