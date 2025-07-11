#!/bin/bash

# === CONFIGURATION ===
TEST_FILE="raft_test.go"
RUNS=15  # Number of full test passes

# Extract all test names from raft_test.go
ALL_TESTS=$(grep '^func Test' "$TEST_FILE" | awk '{print $2}' | cut -d'(' -f1)

# Initialize result tracking
declare -A PASS_COUNTS
declare -A FAIL_COUNTS
declare -A TOTAL_TIMES_NS

echo "🧪 Running Raft Lab 3 Test Suite ($RUNS passes)"
echo "=============================================="

for ((run = 1; run <= RUNS; run++)); do
    echo "🔁 Round #$run: running all tests..."
    LOOP_START=$(date +%s%N)

    for testname in $ALL_TESTS; do
        echo -n "  Running $testname ... "
        START=$(date +%s%N)

        output=$(go test -run "^$testname$" 2>&1)
        result=$?

        END=$(date +%s%N)
        duration_ns=$((END - START))
        duration_sec=$(awk "BEGIN { printf \"%.2f\", $duration_ns / 1000000000 }")
        TOTAL_TIMES_NS[$testname]=$((TOTAL_TIMES_NS[$testname] + duration_ns))

        if [[ $result -ne 0 || "$output" == *"FAIL"* ]]; then
            echo "❌ FAIL (${duration_sec}s)"
            FAIL_COUNTS[$testname]=$((FAIL_COUNTS[$testname] + 1))
        elif [[ "$output" == *"ok "* ]]; then
            echo "✅ PASS (${duration_sec}s)"
            PASS_COUNTS[$testname]=$((PASS_COUNTS[$testname] + 1))
        else
            echo "⚠️ UNKNOWN (${duration_sec}s)"
        fi
    done

    LOOP_END=$(date +%s%N)
    loop_duration_ns=$((LOOP_END - LOOP_START))
    loop_duration_sec=$(awk "BEGIN { printf \"%.2f\", $loop_duration_ns / 1000000000 }")
    echo "🕒 Round #$run completed in ${loop_duration_sec}s"
    echo
done

# Final summary
echo "=============================================="
echo "🧾 Final Summary (after $RUNS full rounds):"
for testname in $ALL_TESTS; do
    pass=${PASS_COUNTS[$testname]:-0}
    fail=${FAIL_COUNTS[$testname]:-0}
    total_runs=$((pass + fail))
    avg_time_sec=0
    if (( total_runs > 0 )); then
        avg_time_ns=${TOTAL_TIMES_NS[$testname]}
        avg_time_sec=$(awk "BEGIN { printf \"%.2f\", $avg_time_ns / $total_runs / 1000000000 }")
    fi
    printf "%-30s ✅ %2d / %2d   Avg Time: %5ss\n" "$testname" "$pass" "$RUNS" "$avg_time_sec"
done
echo "=============================================="
