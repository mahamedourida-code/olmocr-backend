from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, status

from app.core.dependencies import get_current_user
from app.services.supabase_service import get_supabase_service


router = APIRouter(prefix="/account", tags=["Account"])


@router.delete("", response_model=Dict[str, Any])
async def delete_account(user: dict = Depends(get_current_user)):
    """Permanently delete the signed-in user's account and all data they own."""
    try:
        return await get_supabase_service().delete_account(user["user_id"], user.get("email"))
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
