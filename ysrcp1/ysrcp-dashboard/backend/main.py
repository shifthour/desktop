"""
YSRCP Political Dashboard - Backend API
FastAPI server providing real-time political analytics

Run with: uvicorn main:app --reload --port 8000
"""

from dotenv import load_dotenv
load_dotenv()  # Load environment variables from .env file

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from contextlib import asynccontextmanager
import uvicorn
import os
from pathlib import Path

# Import Mangum for Vercel serverless deployment
try:
    from mangum import Mangum
    SERVERLESS = True
except ImportError:
    SERVERLESS = False

from routes.api import router as api_router

# Check if we have a frontend build to serve
STATIC_DIR = Path(__file__).parent.parent / "dist"
SERVE_FRONTEND = STATIC_DIR.exists() and (STATIC_DIR / "index.html").exists()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events"""
    # Startup
    print("🚀 Starting YSRCP Dashboard API...")
    print("📊 Services: Google Trends, News, Sentiment, Social Media")
    print("🌐 API Documentation: http://localhost:8000/docs")
    yield
    # Shutdown
    print("👋 Shutting down API server...")


# Create FastAPI app
app = FastAPI(
    title="YSRCP Political Dashboard API",
    description="""
    Real-time political analytics API for YSRCP vs TDP comparison.

    ## Features
    - **Google Trends**: Search interest data from Google
    - **News Monitoring**: Real-time news from multiple sources
    - **Sentiment Analysis**: AI-powered sentiment classification
    - **Social Media Stats**: Aggregated metrics from all platforms

    ## Data Sources
    - Google Trends (via pytrends)
    - Google News RSS
    - NewsAPI.org (with API key)
    - Estimated social media metrics

    ## Note
    For production use with real-time social media data,
    consider integrating Brand24 or similar paid services.
    """,
    version="1.0.0",
    lifespan=lifespan
)

# Configure CORS - allow all origins for cross-platform deployment
# Backend on Render, Frontend on Vercel
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,  # Must be False when allow_origins is "*"
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


# Serve frontend static files in production (after API routes)
if SERVE_FRONTEND:
    # Mount static assets (JS, CSS, images)
    app.mount("/assets", StaticFiles(directory=str(STATIC_DIR / "assets")), name="assets")

    # Catch-all route for SPA - serve index.html for all non-API routes
    @app.get("/{full_path:path}")
    async def serve_spa(request: Request, full_path: str):
        """Serve the React SPA for all non-API routes"""
        # Check if it's a static file that exists
        file_path = STATIC_DIR / full_path
        if file_path.exists() and file_path.is_file():
            return FileResponse(str(file_path))
        # Otherwise serve index.html for client-side routing
        return FileResponse(str(STATIC_DIR / "index.html"))


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )

# Vercel serverless handler
if SERVERLESS:
    handler = Mangum(app, lifespan="off")
