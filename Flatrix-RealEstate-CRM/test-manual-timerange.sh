#!/bin/bash

# Test with manual time range to bypass sync log

NOW=$(date +%s)
DAY_AGO=$((NOW - 86400))

echo "Testing with manual time range..."
echo "Start: $DAY_AGO"
echo "End: $NOW"

curl -X POST 'https://flatrix-d32vke0f5-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/housing/fetch-leads' \
  -H 'Content-Type: application/json' \
  -H 'x-api-key: a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0u1v2w3x4y5z6' \
  -d "{\"start_date\": \"$DAY_AGO\", \"end_date\": \"$NOW\", \"force_timerange\": true}" \
  -s | jq .
