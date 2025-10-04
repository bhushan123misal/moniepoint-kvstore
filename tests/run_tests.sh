#!/bin/bash
set -euo pipefail

TEST_DIR="./test_data"
PORT=7071
HOST="127.0.0.1"
SERVER_CMD="python -u main.py --data-dir ${TEST_DIR} --port ${PORT}"
SERVER_LOG="test_server.log"
TEST_FILE="tests/tests.txt"

cleanup() {
  echo "[cleanup] stopping server & removing ${TEST_DIR}..." | tee -a "$SERVER_LOG"
  if [[ -n "${SERVER_PID:-}" ]]; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$TEST_DIR"
}
trap cleanup EXIT

echo "[1/3] Starting test server on port ${PORT}..." | tee "$SERVER_LOG"
rm -rf "$TEST_DIR"
mkdir -p "$TEST_DIR"

$SERVER_CMD >>"$SERVER_LOG" 2>&1 &
SERVER_PID=$!
echo "Server PID: $SERVER_PID" | tee -a "$SERVER_LOG"
echo "Waiting for server to start..." | tee -a "$SERVER_LOG"
ready=0
for i in {1..30}; do
  if nc -z "$HOST" "$PORT" 2>/dev/null; then
    echo "Server is up on http://${HOST}:${PORT}" | tee -a "$SERVER_LOG"
    ready=1; break
  fi
  sleep 0.5
done
if [[ $ready -ne 1 ]]; then
  echo "Server did not become ready in time" | tee -a "$SERVER_LOG"
  exit 1
fi


echo "[2/3] Running curl tests from $TEST_FILE..." | tee -a "$SERVER_LOG"
FAIL_COUNT=0
TOTAL=0

while IFS= read -r raw || [[ -n "$raw" ]]; do
  [[ -z "$raw" || "${raw:0:1}" == "#" ]] && continue
  ((TOTAL++))
  expect=""
  if [[ "$raw" =~ \#\ *expects?=([0-9]{3}) ]]; then
    expect="${BASH_REMATCH[1]}"
  fi

  cmd="${raw%%\#*}"
  cmd="${cmd//\{\{HOST\}\}/$HOST}"
  cmd="${cmd//\{\{PORT\}\}/$PORT}"
  cmd="${cmd%$'\r'}"
  echo "[TEST] → $cmd" | tee -a "$SERVER_LOG"

  status_code="$(bash -lc "$cmd -s -o /dev/null -w '%{http_code}' --max-time 8" 2>>"$SERVER_LOG" || echo "000")"

  if [[ -n "$expect" ]]; then
    if [[ "$status_code" == "$expect" ]]; then
      echo "[TEST] PASS (expected=$expect, got=$status_code)" | tee -a "$SERVER_LOG"
    else
      echo "[TEST] FAIL (expected=$expect, got=$status_code)" | tee -a "$SERVER_LOG"
      ((FAIL_COUNT++))
    fi
  else
    if [[ "$status_code" =~ ^2 ]]; then
      echo "[TEST] PASS (status_code=$status_code)" | tee -a "$SERVER_LOG"
    else
      echo "[TEST] FAIL (status_code=$status_code)" | tee -a "$SERVER_LOG"
      ((FAIL_COUNT++))
    fi
  fi
done < "$TEST_FILE"

echo "--------------------------------------------" | tee -a "$SERVER_LOG"
if [[ $FAIL_COUNT -gt 0 ]]; then
  echo "$FAIL_COUNT test(s) failed out of $TOTAL" | tee -a "$SERVER_LOG"
  exit 1
else
  echo "All $TOTAL tests passed successfully" | tee -a "$SERVER_LOG"
fi

echo "[3/3] Cleaning up..." | tee -a "$SERVER_LOG"
