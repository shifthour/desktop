#!/bin/bash

echo "🚀 Starting Serene Living PG Management Portal..."
echo ""

# Check if node_modules exist
if [ ! -d "backend/node_modules" ]; then
    echo "📦 Installing backend dependencies..."
    cd backend && npm install && cd ..
fi

if [ ! -d "frontend/node_modules" ]; then
    echo "📦 Installing frontend dependencies..."
    cd frontend && npm install && cd ..
fi

# Check if database exists
if [ ! -f "backend/database.db" ]; then
    echo "📊 Database not found. Would you like to import data from Excel? (y/n)"
    read -r response
    if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]; then
        echo "📥 Importing data from Excel..."
        cd backend && node importData.js && cd ..
    fi
fi

echo ""
echo "✅ Starting servers..."
echo ""
echo "📡 Backend will run on: http://localhost:5000"
echo "🌐 Frontend will run on: http://localhost:3002"
echo ""
echo "Press Ctrl+C to stop both servers"
echo ""

# Start backend in background
cd backend && npm start &
BACKEND_PID=$!

# Wait a moment for backend to start
sleep 2

# Start frontend
cd ../frontend && npm run dev &
FRONTEND_PID=$!

# Wait for both processes
wait $BACKEND_PID $FRONTEND_PID
