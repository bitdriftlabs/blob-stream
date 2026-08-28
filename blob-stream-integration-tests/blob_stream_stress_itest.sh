#!/usr/bin/env bash

set -euo pipefail

stress_binary=$1
pool_service=$2
wait_for_service=$3
shift 3

endpoints=$("$pool_service" ensure dynamodb,localstack "$wait_for_service")
while IFS='=' read -r name value; do
  export "$name=$value"
done <<<"$endpoints"

exec "$stress_binary" "$@"
