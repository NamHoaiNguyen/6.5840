#!/bin/bash

declare -A pass_count
declare -A fail_count
declare -a ordered_subtests  # indexed array for order tracking

for i in {1..350}; do
  echo "================= Run #$i ================="

  output=$(go test -v -run 3A 2>&1)

  while IFS= read -r line; do
    if [[ "$line" =~ ---\ (PASS|FAIL):\ ([^[:space:]]+)\ \(([0-9.]+)s\) ]]; then
      result="${BASH_REMATCH[1]}"
      subtest="${BASH_REMATCH[2]}"
      duration="${BASH_REMATCH[3]}"

      echo "[$i] $result: $subtest (${duration}s)"

      # Add subtest to order list when first encountered
      if [[ -z "${pass_count[$subtest]}" && -z "${fail_count[$subtest]}" ]]; then
        ordered_subtests+=("$subtest")
      fi

      # Count pass/fail
      if [[ "$result" == "PASS" ]]; then
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

# Print summary in the original encounter order
for subtest in "${ordered_subtests[@]}"; do
  p=${pass_count[$subtest]:-0}
  f=${fail_count[$subtest]:-0}
  echo "$subtest -> PASS: $p, FAIL: $f"
done

