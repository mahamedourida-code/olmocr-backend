from typing import Any, Dict, Optional

from app.core.config import Settings, get_settings


def normalize_plan_type(plan_type: Optional[str]) -> str:
    plan = (plan_type or "free").strip().lower()
    if plan in {"pro", "paid"}:
        return "pro"
    if plan in {"enterprise", "business"}:
        return "enterprise"
    if plan == "anonymous":
        return "anonymous"
    return "free"


def get_plan_limits(plan_type: Optional[str], settings: Settings = get_settings()) -> Dict[str, Any]:
    plan = normalize_plan_type(plan_type)

    batch_limits = {
        "anonymous": settings.anonymous_max_files_per_batch,
        "free": settings.free_max_files_per_batch,
        "pro": settings.pro_max_files_per_batch,
        "enterprise": settings.enterprise_max_files_per_batch,
    }
    daily_limits = {
        "anonymous": settings.rate_limit_anonymous_images_per_day,
        "free": settings.rate_limit_authenticated_images_per_day,
        "pro": settings.rate_limit_pro_images_per_day,
        "enterprise": settings.rate_limit_enterprise_images_per_day,
    }

    return {
        "plan": plan,
        "max_files_per_batch": min(batch_limits[plan], settings.max_batch_size),
        "absolute_max_files_per_batch": settings.max_batch_size,
        "max_file_size_mb": settings.max_file_size_mb,
        "max_file_size_bytes": settings.max_file_size_bytes,
        "daily_image_limit": daily_limits[plan],
        "queue": {
            "max_queued_jobs": settings.queue_admission_max_queued_jobs,
            "max_active_jobs": settings.queue_admission_max_active_jobs,
        },
        "accepted_file_types": ["image/png", "image/jpeg", "image/webp", "image/heic", "image/heif"],
    }


def get_user_plan_type(user: Optional[dict], supabase_service: Any) -> str:
    if not user or not user.get("user_id"):
        return "anonymous"
    try:
        return normalize_plan_type(supabase_service.get_user_plan_type(user["user_id"]))
    except Exception:
        return "free"
