#!/bin/bash

SUBTEST_FILTER="^TestManyElections3A$"

declare -A pass_count
declare -A fail_count
declare -A all_subtests

for i in {1..20}; do
  echo "================= Run #$i ================="

  # Run test and print all logs immediately
  output=$(go test -v -run "$SUBTEST_FILTER" 2>&1 | tee /tmp/go_test_run_${i}.log)

  # Extract subtest results from captured output
  while IFS= read -r line; do
    if [[ "$line" =~ ---\ (PASS|FAIL):\ ([^[:space:]]+)\ \(([0-9.]+)s\) ]]; then
      result="${BASH_REMATCH[1]}"
      subtest="${BASH_REMATCH[2]}"
      duration="${BASH_REMATCH[3]}"
      echo "[$i] $result: $subtest (${duration}s)"
      all_subtests["$subtest"]=1
      if [ "$result" == "PASS" ]; then
        ((pass_count["$subtest"]++))
      else
        ((fail_count["$subtest"]++))
      fi
    fi
  done <<< "$output"

  echo "------------------------------------------"
done

echo ""
echo "============= Summary ============="
for subtest in "${!all_subtests[@]}"; do
  p=${pass_count[$subtest]:-0}
  f=${fail_count[$subtest]:-0}
  echo "$subtest -> PASS: $p, FAIL: $f"
done

