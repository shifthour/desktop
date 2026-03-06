#!/bin/bash

# Test script for Housing.com external cron job
# This shows you the exact format your external cron needs to use

# Replace with your actual API key from .env
HOUSING_FETCH_API_KEY="your-api-key-here"

# Latest production URL
URL="https://flatrix-p3bsgvy9m-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/housing/fetch-leads"

echo "Testing Housing.com fetch endpoint..."
echo "URL: $URL"
echo ""

# Make POST request with proper headers and empty JSON body
curl -X POST "$URL" \
  -H "Content-Type: application/json" \
  -H "x-api-key: $HOUSING_FETCH_API_KEY" \
  -d '{}' \
  -w "\n\nHTTP Status: %{http_code}\n" \
  -v

echo ""
echo "If you see HTTP Status: 200, the request was successful!"
echo "If you see 401, your API key is incorrect"
echo "If you see 400, check the response body for error details"
