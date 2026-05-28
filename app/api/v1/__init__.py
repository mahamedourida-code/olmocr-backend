from fastapi import APIRouter
from datetime import datetime

from app.api.v1 import jobs, download, websocket, sessions, config, billing, vendor_rules, accounts_payable, integrations, email_intake, client_intake, workspaces

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
# Note: /convert/batch endpoint removed - use /jobs/batch instead
api_router.include_router(jobs.router)
api_router.include_router(download.router)
api_router.include_router(websocket.router)
api_router.include_router(sessions.router)
api_router.include_router(config.router)
api_router.include_router(billing.router)
api_router.include_router(vendor_rules.router)
api_router.include_router(accounts_payable.router)
api_router.include_router(integrations.router)
api_router.include_router(email_intake.router)
api_router.include_router(client_intake.router)
api_router.include_router(workspaces.router)
