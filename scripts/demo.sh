#!/bin/sh
set -eu

base_url="${DEMO_URL:-http://localhost:8080}"

curl --fail-with-body --request POST "${base_url}/jobs" \
  --header 'Content-Type: application/json' \
  --data '{"job_id":"asset-1042","kind":"player_asset","asset":{"player_id":"player-7","asset_id":"skin-31","format":"png"}}'
printf '\n'
curl --fail-with-body --request POST "${base_url}/work"
printf '\n'
