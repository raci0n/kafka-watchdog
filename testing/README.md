# Testing

Start everything:
```bash
docker compose up --build
```

Or start services individually:
```bash
docker compose up -d kafka
docker compose up --build watchdog
```

Publish a message:
```bash
echo "hello" | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 --topic testing1

echo "hello" | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 --topic testing2
```
