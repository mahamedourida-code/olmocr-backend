import logging
from typing import Any, Dict

from fastapi import APIRouter

from app.models.requests import DemoLeadRequest
from app.services.supabase_service import get_supabase_service

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/demo", tags=["Demo"])


@router.post("/lead", response_model=Dict[str, Any])
async def submit_demo_lead(request: DemoLeadRequest):
    """Public: store a Request-a-demo lead. Never block the marketing flow on
    a storage hiccup — the prospect still proceeds to scheduling."""
    try:
        await get_supabase_service().create_demo_lead(request.model_dump())
    except Exception:
        logger.exception("Failed to store demo lead for %s", request.work_email)
    return {"ok": True}
