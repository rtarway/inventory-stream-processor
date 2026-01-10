#!/bin/bash
echo "Validating Results for TST-1..."

# 1. Curl from a temporary debug pod (API service is named 'api')
echo "Expected Final Total: 210 (Reset=200 + 10)"
kubectl run validate-curl --rm -i --image=curlimages/curl --restart=Never -- curl -s http://api:8080/total/ID:TST-1
echo ""
echo "--------------------------------"

echo "Validating Results for TST-1 Group G1..."
kubectl run validate-curl-2 --rm -i --image=curlimages/curl --restart=Never -- curl -s http://api:8080/total/ID:TST-1#G1
echo ""
