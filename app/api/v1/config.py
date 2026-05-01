from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends

from app.core.dependencies import get_optional_user
from app.core.limits import get_plan_limits, get_user_plan_type
from app.services.redis_service import RedisService, get_redis_service
from app.services.supabase_service import SupabaseService, get_supabase_service

router = APIRouter(prefix="/config", tags=["Config"])


@router.get("/limits", response_model=Dict[str, Any])
async def get_limits(
    user: Optional[dict] = Depends(get_optional_user),
    redis_service: RedisService = Depends(get_redis_service),
    supabase: SupabaseService = Depends(get_supabase_service),
) -> Dict[str, Any]:
    """
    Return the effective product limits for the current visitor/user.

    The frontend should use this response instead of hardcoded batch limits.
    """
    plan_type = get_user_plan_type(user, supabase)
    limits = get_plan_limits(plan_type)

    queue_status = {
        "queued_jobs": None,
        "active_jobs": None,
        "available": False,
    }
    try:
        queue_status = {
            "queued_jobs": await redis_service.get_celery_queue_depth("batch_processing"),
            "active_jobs": await redis_service.count_jobs_by_status(["queued", "processing"]),
            "available": True,
        }
    except Exception:
        pass

    credits = None
    if user and user.get("user_id"):
        credits = supabase.get_user_credits(user["user_id"])

    return {
        **limits,
        "queue": {
            **limits["queue"],
            **queue_status,
        },
        "credits": credits,
    }
