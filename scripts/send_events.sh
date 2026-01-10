#!/bin/bash

# Define Topic
TOPIC="events-input"

echo "Creating topic $TOPIC..."
docker exec kafka kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic $TOPIC
sleep 2

echo "Sending Test Events to Kafka (events-input)..."

# 1. Get Kafka Pod Name
KAFKA_POD=$(kubectl get pod -l app=kafka -o jsonpath="{.items[0].metadata.name}")

# 2. Define Events
EVENTS=$(cat <<EOF
{"id":"TST-1", "value":10,  "type":"ADD",    "timestamp":"2023-01-01T10:00:00Z", "metadata":["G1"]}
{"id":"TST-1", "value":200, "type":"RESET",  "timestamp":"2023-01-01T11:00:00Z", "metadata":["G1"]}
{"id":"TST-1", "value":10,  "type":"ADD",    "timestamp":"2023-01-01T12:00:00Z", "metadata":["G1"]}
EOF
)

# 3. Send via kubectl exec
echo "$EVENTS" | kubectl exec -i $KAFKA_POD -- kafka-console-producer --bootstrap-server localhost:9092 --topic events-input

echo "Events Sent!"
