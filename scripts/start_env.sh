#!/bin/bash
echo "Starting Docker Environment..."
echo "(This will build the JAR inside Docker using the multi-stage build)"
docker-compose up -d --build

echo "Waiting for services to stabilize..."
sleep 15

echo "Environment Ready!"
echo "- Kafka: localhost:9092"
echo "- Redis: localhost:6379"
echo "- API:   localhost:8080"
