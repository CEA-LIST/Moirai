#!/usr/bin/env bash
set -euo pipefail

base_url="${1:-http://127.0.0.1:8081}"
if (($# >= 2)); then
  operation="$2"
else
  operation='{"Add":"apple"}'
fi

printf 'Health: '
curl --fail --silent --show-error "${base_url}/api/health"
printf '\nSubmitting: %s\n' "$operation"
curl --fail --silent --show-error \
  -X POST "${base_url}/api/op" \
  -H 'Content-Type: application/json' \
  --data "$operation"
printf '\nState: '
curl --fail --silent --show-error "${base_url}/api/state"
printf '\n'
