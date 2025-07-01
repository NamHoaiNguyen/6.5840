#!/bin/bash

NUM_RUNS=2500
TEST_NAME="TestBasicAgree3B"

pass_count=0
fail_count=0

echo "Running $TEST_NAME $NUM_RUNS times..."
echo "========================================="

for i in $(seq 1 $NUM_RUNS); do
  echo "Run #$i"
  output=$(go test -v -run "^$TEST_NAME$" 2>&1)

  echo "$output" | grep -E "^--- (PASS|FAIL): $TEST_NAME"

  if echo "$output" | grep -q "^--- PASS: $TEST_NAME"; then
    ((pass_count++))
  else
    ((fail_count++))
  fi

  echo "-----------------------------------------"
done

echo ""
echo "============= Summary ============="
echo "$TEST_NAME -> PASS: $pass_count, FAIL: $fail_count"