#!/bin/bash

NUM_RUNS=4000
TEST_NAME="TestConcurrentStarts3B"

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

# !/bin/bash

# NUM_RUNS=2
# declare -A pass_count
# declare -A fail_count
# declare -a ordered_subtests

# echo "Running 'go test -v -run 3B' for $NUM_RUNS iterations"
# echo "=============================================="

# for i in $(seq 1 $NUM_RUNS); do
#   echo "========== Run #$i =========="

#   # output=$(go test -v -run 3B 2>&1)
#   output=$(go test -v -run 'Test.*3B' 2>&1)

#   while IFS= read -r line; do
#     if [[ "$line" =~ ---[[:space:]](PASS|FAIL):[[:space:]](.+)[[:space:]]\(([0-9.]+)s\) ]]; then
#       result="${BASH_REMATCH[1]}"
#       subtest="${BASH_REMATCH[2]}"
#       duration="${BASH_REMATCH[3]}"

#       echo "[$i] $result: $subtest (${duration}s)"

#       if [[ -z "${pass_count[$subtest]}" && -z "${fail_count[$subtest]}" ]]; then
#         ordered_subtests+=("$subtest")
#       fi

#       if [[ "$result" == "PASS" ]]; then
#         ((pass_count["$subtest"]++))
#       else
#         ((fail_count["$subtest"]++))
#       fi
#     fi
#   done <<< "$output"

#   echo "------------------------------"
# done

# echo ""
# echo "============ Summary ============"
# for subtest in "${ordered_subtests[@]}"; do
#   p=${pass_count[$subtest]:-0}
#   f=${fail_count[$subtest]:-0}
#   echo "$subtest -> PASS: $p, FAIL: $f"
# done
