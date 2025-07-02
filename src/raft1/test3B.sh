#!/bin/bash

# List of tests to run
tests=("TestBasicAgree3B" "TestRPCBytes3B" "TestFollowerFailure3B" "TestLeaderFailure3B" "TestFailAgree3B"
       "TestFailNoAgree3B" "TestConcurrentStarts3B" "TestRejoin3B" "TestBackup3B" "TestCount3B")
NUM_RUNS=120

# Initialize counters
declare -A pass_count
declare -A fail_count

echo "Running tests: ${tests[*]} for $NUM_RUNS iterations"
echo "========================================="

for i in $(seq 1 $NUM_RUNS); do
  echo "Run #$i"
  for test in "${tests[@]}"; do
    echo "- Running $test..."
    output=$(go test -v -run "^$test$" 2>&1)

    # Extract result and duration
    if echo "$output" | grep -q "^--- PASS: $test"; then
      ((pass_count[$test]++))
      duration=$(echo "$output" | grep "^--- PASS: $test" | awk '{print $4}')
      echo "  ✅ PASS: $test ($duration)"
    else
      ((fail_count[$test]++))
      echo "  ❌ FAIL: $test"
    fi
  done
  echo "-----------------------------------------"
done

echo ""
echo "============= Summary ============="
for test in "${tests[@]}"; do
  p=${pass_count[$test]:-0}
  f=${fail_count[$test]:-0}
  echo "$test -> PASS: $p, FAIL: $f"
done

