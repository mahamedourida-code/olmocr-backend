from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.core.dependencies import get_current_user
from app.models.requests import VendorRuleUpdateRequest
from app.services.supabase_service import get_supabase_service


router = APIRouter(prefix="/vendor-rules", tags=["Vendor Memory"])


@router.get("", response_model=Dict[str, Any])
async def list_vendor_rules(
    workspace_id: Optional[str] = Query(None),
    user: dict = Depends(get_current_user),
):
    service = get_supabase_service()
    try:
        rules = await service.list_vendor_rules(user["user_id"], workspace_id)
        return {"rules": rules, "total": len(rules)}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))


@router.patch("/{rule_id}", response_model=Dict[str, Any])
async def update_vendor_rule(
    rule_id: str,
    request: VendorRuleUpdateRequest,
    user: dict = Depends(get_current_user),
):
    service = get_supabase_service()
    try:
        payload = request.model_dump(exclude_none=True)
        if request.suggested_fields is not None:
            payload["suggested_fields"] = request.suggested_fields.model_dump(exclude_none=True)
        rule = await service.update_vendor_rule(rule_id, user["user_id"], payload)
        return {"rule": rule}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))


@router.delete("/{rule_id}", response_model=Dict[str, Any])
async def delete_vendor_rule(
    rule_id: str,
    user: dict = Depends(get_current_user),
):
    service = get_supabase_service()
    try:
        await service.delete_vendor_rule(rule_id, user["user_id"])
        return {"success": True, "rule_id": rule_id}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
