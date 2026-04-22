#!/bin/bash
set -euo pipefail
echo "Starting Docker Environment..."
echo "Building JARs (required for service Dockerfiles)..."
mvn -q -DskipTests package
echo "Building and starting containers..."
docker compose up -d --build

echo "Waiting for services to stabilize..."
sleep 15

echo "Environment Ready!"
echo "- Kafka: localhost:9092"
echo "- Redis: localhost:6379"
echo "- API:   localhost:8080"
