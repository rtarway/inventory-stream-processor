#!/bin/bash
echo "Testing Zero Grace Period (Immediate Processing)..."

# 1. Set Grace Period to 0
echo "Setting APP_PROCESSOR_GRACE_PERIOD_MS=0..."
kubectl set env deployment/processor APP_PROCESSOR_GRACE_PERIOD_MS=0
# Wait for rollout
kubectl rollout status deployment/processor

# 2. Get Pod
KAFKA_POD=$(kubectl get pod -l app=kafka -o jsonpath="{.items[0].metadata.name}")

# Function
send_event() {
  echo $1 | kubectl exec -i $KAFKA_POD -- kafka-console-producer --bootstrap-server localhost:9092 --topic events-input
}

# 3. Send Out-of-Order Events (Close togther)
# T2 (10:00:20) arrives FIRST
echo "Sending T2 (10:00:20) [Arrival 1]..."
send_event '{"id":"ZERO-TEST", "value":20, "type":"ADD", "timestamp":"2026-01-01T10:00:20Z", "metadata":["GZ"]}'

# T1 (10:00:10) arrives SECOND (Late by timestamp, but immediate arrival)
echo "Sending T1 (10:00:10) [Arrival 2]..."
send_event '{"id":"ZERO-TEST", "value":10, "type":"ADD", "timestamp":"2026-01-01T10:00:10Z", "metadata":["GZ"]}'

# 4. Wait & Validate
sleep 5
echo "Checking Audit Log Order..."
# We expect the Audit Log to show processing of T2 THEN T1 (Arrival Order).
# If it was buffered, it would reorder to T1 THEN T2 (Timestamp Order).
kubectl logs -l app=processor --tail=20
