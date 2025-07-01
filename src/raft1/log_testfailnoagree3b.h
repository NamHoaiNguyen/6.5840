#!/bin/bash

NUM_RUNS=900
TEST_NAME="TestFailNoAgree3B"

pass_count=0
fail_count=0

echo "Running $TEST_NAME $NUM_RUNS times..."
echo "========================================="

for i in $(seq 1 $NUM_RUNS); do
  echo "Run #$i"
  go test -v -run "^$TEST_NAME$"

  echo "-----------------------------------------"
done

echo ""
echo "============= Summary ============="
echo "$TEST_NAME -> PASS: $pass_count, FAIL: $fail_count"