#!/bin/bash

# MagicBricks Integration Test Script
# This script tests the MagicBricks integration endpoint

API_URL="https://flatrix-816xq9s95-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/magicbricks"
API_KEY="9bb6bc225cec3f4bd79f9aae6b6657a16047886b79ceb7772ba81086081bf2ab"

echo "==============================================="
echo "MagicBricks Integration Test Suite"
echo "==============================================="
echo ""
echo "API URL: $API_URL"
echo "Testing with API Key: ${API_KEY:0:20}..."
echo ""

# Test 1: Check if endpoint is active
echo "Test 1: Checking if endpoint is active..."
echo "----------------------------------------"
response=$(curl -s -X GET "$API_URL" \
  -H "x-api-key: $API_KEY")
echo "$response" | python3 -m json.tool 2>/dev/null || echo "$response"
echo ""
echo "✓ Test 1 Complete"
echo ""

# Test 2: Create a new lead
echo "Test 2: Creating a new lead..."
echo "----------------------------------------"
response=$(curl -s -X POST "$API_URL" \
  -H "Content-Type: application/json" \
  -H "x-api-key: $API_KEY" \
  -d '{
    "name": "Test Customer MagicBricks",
    "phone": "9876549999",
    "email": "testmb@example.com",
    "message": "Interested in 3BHK apartment with good amenities",
    "property_type": "Apartment",
    "budget": 7500000,
    "location": "Andheri, Mumbai",
    "lead_id": "MB_TEST_001"
  }')
echo "$response" | python3 -m json.tool 2>/dev/null || echo "$response"
echo ""
echo "✓ Test 2 Complete"
echo ""

# Test 3: Update existing lead (same phone)
echo "Test 3: Updating existing lead..."
echo "----------------------------------------"
response=$(curl -s -X POST "$API_URL" \
  -H "Content-Type: application/json" \
  -H "x-api-key: $API_KEY" \
  -d '{
    "name": "Test Customer Updated",
    "phone": "9876549999",
    "email": "testmb.updated@example.com",
    "message": "Now interested in 4BHK villa",
    "property_type": "Villa",
    "budget": 15000000,
    "location": "Bandra, Mumbai"
  }')
echo "$response" | python3 -m json.tool 2>/dev/null || echo "$response"
echo ""
echo "✓ Test 3 Complete"
echo ""

# Test 4: Test with minimum required fields
echo "Test 4: Creating lead with minimal data..."
echo "----------------------------------------"
response=$(curl -s -X POST "$API_URL" \
  -H "Content-Type: application/json" \
  -H "x-api-key: $API_KEY" \
  -d '{
    "phone": "8888777766"
  }')
echo "$response" | python3 -m json.tool 2>/dev/null || echo "$response"
echo ""
echo "✓ Test 4 Complete"
echo ""

# Test 5: Test with invalid API key (should fail)
echo "Test 5: Testing with invalid API key (should fail)..."
echo "----------------------------------------"
response=$(curl -s -X POST "$API_URL" \
  -H "Content-Type: application/json" \
  -H "x-api-key: INVALID_KEY" \
  -d '{
    "name": "Test",
    "phone": "1234567890"
  }')
echo "$response" | python3 -m json.tool 2>/dev/null || echo "$response"
echo ""
echo "✓ Test 5 Complete (Expected to fail)"
echo ""

# Test 6: Test without phone number (should fail)
echo "Test 6: Testing without phone number (should fail)..."
echo "----------------------------------------"
response=$(curl -s -X POST "$API_URL" \
  -H "Content-Type: application/json" \
  -H "x-api-key: $API_KEY" \
  -d '{
    "name": "Test Customer",
    "email": "test@example.com"
  }')
echo "$response" | python3 -m json.tool 2>/dev/null || echo "$response"
echo ""
echo "✓ Test 6 Complete (Expected to fail)"
echo ""

echo "==============================================="
echo "All Tests Complete!"
echo "==============================================="
echo ""
echo "Next Steps:"
echo "1. Login to CRM: https://flatrix-816xq9s95-shifthourjobs-gmailcoms-projects.vercel.app"
echo "2. Email: admin@flatrix.com"
echo "3. Password: admin123"
echo "4. Go to Leads section and filter by source 'MagicBricks'"
echo "5. You should see the test leads created above"
echo ""
