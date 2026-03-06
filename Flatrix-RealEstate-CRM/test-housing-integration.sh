#!/bin/bash

# Housing.com Integration Test Script
# This script tests the Housing.com lead fetching integration

API_URL="https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/housing/fetch-leads"
API_KEY="a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0u1v2w3x4y5z6"

echo "==============================================="
echo "Housing.com Integration Test Suite"
echo "==============================================="
echo ""
echo "API URL: $API_URL"
echo "Testing with API Key: ${API_KEY:0:20}..."
echo ""

# Test 1: Check endpoint status
echo "Test 1: Checking endpoint status..."
echo "----------------------------------------"
response=$(curl -s -X GET "$API_URL" \
  -H "x-api-key: $API_KEY")
echo "$response" | python3 -m json.tool 2>/dev/null || echo "$response"
echo ""
echo "✓ Test 1 Complete"
echo ""

# Test 2: Fetch leads from last 24 hours (default)
echo "Test 2: Fetching leads from last 24 hours..."
echo "----------------------------------------"
response=$(curl -s -X POST "$API_URL" \
  -H "Content-Type: application/json" \
  -H "x-api-key: $API_KEY" \
  -d '{}')
echo "$response" | python3 -m json.tool 2>/dev/null || echo "$response"
echo ""
echo "✓ Test 2 Complete"
echo ""

# Test 3: Fetch leads with specific time range (last 7 days)
echo "Test 3: Fetching leads from last 7 days..."
echo "----------------------------------------"
end_time=$(date +%s)
start_time=$((end_time - 604800)) # 7 days ago

response=$(curl -s -X POST "$API_URL" \
  -H "Content-Type: application/json" \
  -H "x-api-key: $API_KEY" \
  -d "{
    \"start_date\": \"$start_time\",
    \"end_date\": \"$end_time\",
    \"per_page\": 100
  }")
echo "$response" | python3 -m json.tool 2>/dev/null || echo "$response"
echo ""
echo "✓ Test 3 Complete"
echo ""

# Test 4: Test with invalid API key (should fail)
echo "Test 4: Testing with invalid API key (should fail)..."
echo "----------------------------------------"
response=$(curl -s -X POST "$API_URL" \
  -H "Content-Type: application/json" \
  -H "x-api-key: INVALID_KEY" \
  -d '{}')
echo "$response" | python3 -m json.tool 2>/dev/null || echo "$response"
echo ""
echo "✓ Test 4 Complete (Expected to fail)"
echo ""

echo "==============================================="
echo "All Tests Complete!"
echo "==============================================="
echo ""
echo "Next Steps:"
echo "1. Login to CRM: https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app"
echo "2. Email: admin@flatrix.com"
echo "3. Password: admin123"
echo "4. Go to Leads section and filter by source 'Housing.com'"
echo "5. You should see the leads fetched above"
echo ""
echo "To set up automated fetching:"
echo "- Create a cron job to run this script every 1-2 hours"
echo "- Example: 0 */2 * * * /path/to/test-housing-integration.sh"
echo ""
