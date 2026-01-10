#!/bin/bash
set -e

echo "Stopping and Removing previous K8s resources..."
kubectl delete -f k8s/apps.yaml --ignore-not-found=true
kubectl delete -f k8s/infrastructure.yaml --ignore-not-found=true

# Wait for deletion
sleep 5

echo "Building Maven Project (via Docker)..."
docker run --rm -v "$(pwd)":/app -w /app maven:3.9-eclipse-temurin-17 mvn clean package -DskipTests

echo "Building Docker Images..."
docker build -t inventory-processor:latest processor/
docker build -t inventory-sink:latest sink/
docker build -t inventory-api:latest api/

echo "Deploying to Kubernetes..."
kubectl apply -f k8s/infrastructure.yaml
kubectl apply -f k8s/apps.yaml

echo "Waiting for pods to be ready..."
# Use a simple loop to wait for processor
echo "Waiting for processor to be ready..."
kubectl wait --for=condition=available --timeout=120s deployment/processor
kubectl wait --for=condition=available --timeout=120s deployment/sink
kubectl wait --for=condition=available --timeout=120s deployment/api

echo "Environment Clean and Deployed to K8s!"
