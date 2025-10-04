from fastapi import APIRouter
from datetime import datetime

from app.api.v1 import convert, jobs, download, websocket

api_router = APIRouter()

# Health check endpoint for Railway
@api_router.get("/health")
async def health_check():
    """Health check endpoint for monitoring and Railway."""
    return {
        "status": "healthy",
        "service": "olmocr-backend",
        "timestamp": datetime.utcnow().isoformat()
    }

# Include all API route modules
api_router.include_router(convert.router)
api_router.include_router(jobs.router)
api_router.include_router(download.router)
api_router.include_router(websocket.router)