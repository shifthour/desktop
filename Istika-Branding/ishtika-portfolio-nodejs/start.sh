#!/bin/bash

echo "Starting Ishtika Homes Portfolio Server..."
echo "=========================================="

# Kill any existing process on port 3001
lsof -ti:3001 | xargs kill -9 2>/dev/null

# Start the server
cd "$(dirname "$0")"
PORT=3001 node server.js

echo ""
echo "Server stopped."
