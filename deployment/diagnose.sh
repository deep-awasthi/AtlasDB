#!/bin/sh
PORT=${1:-7601}

echo "=== AtlasDB Node Diagnostics (Port $PORT) ==="

echo "Checking Node Liveness..."
LIVENESS=$(curl -s -w "\nHTTP_STATUS:%{http_code}" http://localhost:$PORT/health/liveness)
LIVENESS_BODY=$(echo "$LIVENESS" | sed '/HTTP_STATUS/d')
LIVENESS_STATUS=$(echo "$LIVENESS" | tr -d '\r' | awk -F: '/HTTP_STATUS/{print $2}')

if [ "$LIVENESS_STATUS" = "200" ]; then
  echo "  [OK] Node is alive: $LIVENESS_BODY"
else
  echo "  [FAIL] Node is dead or unreachable (Status: $LIVENESS_STATUS)"
fi

echo "Checking Node Readiness..."
READINESS=$(curl -s -w "\nHTTP_STATUS:%{http_code}" http://localhost:$PORT/health/readiness)
READINESS_BODY=$(echo "$READINESS" | sed '/HTTP_STATUS/d')
READINESS_STATUS=$(echo "$READINESS" | tr -d '\r' | awk -F: '/HTTP_STATUS/{print $2}')

if [ "$READINESS_STATUS" = "200" ]; then
  echo "  [OK] Node is ready: $READINESS_BODY"
else
  echo "  [WARN] Node is not ready (Status: $READINESS_STATUS, Output: $READINESS_BODY)"
fi

echo "Scraping Performance Metrics..."
METRICS=$(curl -s http://localhost:$PORT/metrics)
if [ $? -eq 0 ]; then
  DB_SIZE=$(echo "$METRICS" | grep "^atlasdb_database_size" | awk '{print $2}')
  ACTIVE_TX=$(echo "$METRICS" | grep "^atlasdb_transactions_active" | awk '{print $2}')
  COMMITTED_TX=$(echo "$METRICS" | grep "^atlasdb_transactions_committed_total" | awk '{print $2}')
  
  echo "  Database Size (Records): ${DB_SIZE:-0}"
  echo "  Active Transactions:     ${ACTIVE_TX:-0}"
  echo "  Committed Transactions:  ${COMMITTED_TX:-0}"
else
  echo "  [FAIL] Failed to scrape Prometheus metrics endpoint."
fi
echo "============================================="
