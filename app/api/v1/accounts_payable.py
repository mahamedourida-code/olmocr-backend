from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.core.dependencies import get_current_user
from app.models.requests import (
    AccountsPayableBulkStatusRequest,
    AccountsPayableFromDocumentRequest,
    AccountsPayableUpdateRequest,
)
from app.services.supabase_service import get_supabase_service


router = APIRouter(prefix="/accounts-payable", tags=["Accounts Payable"])


@router.get("", response_model=Dict[str, Any])
async def list_accounts_payable_items(
    workspace_id: Optional[str] = Query(None),
    item_status: Optional[str] = Query(None, alias="status"),
    user: dict = Depends(get_current_user),
):
    service = get_supabase_service()
    try:
        items = await service.list_accounts_payable_items(
            user_id=user["user_id"],
            workspace_id=workspace_id,
            ap_status=item_status,
        )
        return {"items": items, "total": len(items)}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))


@router.post("/from-document", response_model=Dict[str, Any])
async def create_accounts_payable_item(
    request: AccountsPayableFromDocumentRequest,
    user: dict = Depends(get_current_user),
):
    service = get_supabase_service()
    try:
        item = await service.create_accounts_payable_item_from_document(
            job_id=request.job_id,
            document_id=request.document_id,
            user_id=user["user_id"],
        )
        return {"item": item}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


@router.get("/{item_id}", response_model=Dict[str, Any])
async def get_accounts_payable_item(
    item_id: str,
    user: dict = Depends(get_current_user),
):
    service = get_supabase_service()
    try:
        item = await service.get_accounts_payable_item(item_id, user["user_id"])
        return {"item": item}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))


@router.patch("/{item_id}", response_model=Dict[str, Any])
async def update_accounts_payable_item(
    item_id: str,
    request: AccountsPayableUpdateRequest,
    user: dict = Depends(get_current_user),
):
    service = get_supabase_service()
    try:
        payload = request.model_dump(exclude_none=True)
        if request.draft_data is not None:
            payload["draft_data"] = request.draft_data.model_dump(exclude_none=True)
        item = await service.update_accounts_payable_item(item_id, user["user_id"], payload)
        return {"item": item}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


@router.post("/bulk-status", response_model=Dict[str, Any])
async def bulk_update_accounts_payable_items(
    request: AccountsPayableBulkStatusRequest,
    user: dict = Depends(get_current_user),
):
    service = get_supabase_service()
    try:
        items = await service.bulk_publish_accounts_payable_items(
            item_ids=request.item_ids,
            user_id=user["user_id"],
            reason=request.reason,
        )
        return {"items": items, "total": len(items)}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
