"""
Vercel Serverless Function Entry Point
Exposes the FastAPI app for Vercel deployment
"""
import os
import sys

# Add the current directory (api/) to Python path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

# Set environment variable for RapidAPI
os.environ.setdefault('RAPIDAPI_KEY', '922556e08bmsh465b2b5025c11a5p176967jsn3ca78cdb094c')

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from mangum import Mangum

# Import routes (now local to api folder)
from routes.api import router as api_router

# Create FastAPI app
app = FastAPI(
    title="YSRCP Political Dashboard API",
    description="Real-time political analytics API for YSRCP vs TDP comparison.",
    version="1.0.0"
)

# Configure CORS for Vercel deployment
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for Vercel
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(api_router)


@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "name": "YSRCP Political Dashboard API",
        "version": "1.0.0",
        "status": "running",
        "docs": "/docs",
        "endpoints": {
            "dashboard": "/api/dashboard",
            "overall_stats": "/api/stats/overall",
            "platform_stats": "/api/stats/platforms",
            "google_trends": "/api/trends/google",
            "hashtags": "/api/hashtags",
            "news": "/api/news",
            "sentiment": "/api/sentiment",
            "health": "/api/health"
        }
    }


# Vercel handler
handler = Mangum(app, lifespan="off")
