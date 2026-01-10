#!/bin/bash
echo "Sending Late Reset Scenario..."

# 1. Clean topics handled by clean_restart.sh
KAFKA_POD=$(kubectl get pod -l app=kafka -o jsonpath="{.items[0].metadata.name}")

# Function to send event
send_event() {
  echo $1 | kubectl exec -i $KAFKA_POD -- kafka-console-producer --bootstrap-server localhost:9092 --topic events-input
}

# T0: 10:00:00 -> Add 10
echo "Sending T0: ADD 10..."
send_event '{"id":"LATE-TEST", "value":10, "type":"ADD", "timestamp":"2026-01-01T10:00:00Z", "metadata":["G1"]}'

# Wait for process
sleep 2

# T2: 10:02:00 -> Add 5 (Current Total 15)
echo "Sending T2: ADD 5..."
send_event '{"id":"LATE-TEST", "value":5, "type":"ADD", "timestamp":"2026-01-01T10:02:00Z", "metadata":["G1"]}'

sleep 2

# T1: 10:01:00 -> RESET 100 (Late!)
# Expected: Snapshot @ 10:01 (uses 10:00 value) = 10.
# Current = 15.
# Diff = 15 - 10 = 5.
# New = 100 + 5 = 105.
echo "Sending T1: LATE RESET 100..."
send_event '{"id":"LATE-TEST", "value":100, "type":"RESET", "timestamp":"2026-01-01T10:01:00Z", "metadata":["G1"]}'

sleep 5

echo "Validating Result..."
kubectl run validate-late --rm -i --image=curlimages/curl --restart=Never -- curl -s http://api:8080/total/ID:LATE-TEST
echo ""
