#!/bin/bash
set -e

echo "Cleaning up..."
kubectl delete -f k8s/apps.yaml --ignore-not-found=true
kubectl delete -f k8s/infrastructure.yaml --ignore-not-found=true

echo "Deploying Infrastructure (Redis + Kafka)..."
kubectl apply -f k8s/infrastructure.yaml

echo "Waiting for Redis..."
kubectl wait --for=condition=available --timeout=60s deployment/redis

echo "Waiting for Kafka..."
# Kafka is a deployment, so we can wait for availability
kubectl wait --for=condition=available --timeout=120s deployment/kafka

echo "Infrastructure Deployed. Checking init-kafka job..."
kubectl get jobs
kubectl get po
