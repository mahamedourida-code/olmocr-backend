from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.core.dependencies import get_current_user
from app.models.requests import (
    AccountsPayableBulkPublishRequest,
    AccountsPayableBulkStatusRequest,
    AccountsPayableDiscardRequest,
    AccountsPayableDuplicateDismissRequest,
    AccountsPayableFromDocumentRequest,
    AccountsPayableUpdateRequest,
)
from app.services.quickbooks_service import get_quickbooks_service
from app.services.supabase_service import get_supabase_service
from app.services.xero_service import get_xero_service


router = APIRouter(prefix="/accounts-payable", tags=["Accounts Payable"])


async def _publisher_for(item_id: str, user_id: str):
    """Pick the accounting connector that matches the item's workspace destination."""
    service = get_supabase_service()
    item = await service.get_accounts_payable_item(item_id, user_id)
    destination = await service.get_accounting_destination(user_id, item.get("workspace_id"))
    return get_xero_service() if destination == "xero" else get_quickbooks_service()


@router.get("", response_model=Dict[str, Any])
async def list_accounts_payable_items(
    workspace_id: Optional[str] = Query(None),
    item_status: Optional[str] = Query(None, alias="status"),
    duplicates_only: bool = Query(False, description="Return only items with an active duplicate warning"),
    user: dict = Depends(get_current_user),
):
    service = get_supabase_service()
    try:
        items = await service.list_accounts_payable_items(
            user_id=user["user_id"],
            workspace_id=workspace_id,
            ap_status=item_status,
            duplicates_only=duplicates_only,
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


@router.post("/{item_id}/duplicate/dismiss", response_model=Dict[str, Any])
async def dismiss_accounts_payable_duplicate_warning(
    item_id: str,
    request: AccountsPayableDuplicateDismissRequest,
    user: dict = Depends(get_current_user),
):
    service = get_supabase_service()
    try:
        item = await service.dismiss_ap_duplicate_warning(
            item_id=item_id,
            user_id=user["user_id"],
            warning_id=request.warning_id,
            reason=request.reason,
        )
        return {"item": item}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


@router.post("/{item_id}/discard", response_model=Dict[str, Any])
async def discard_accounts_payable_item(
    item_id: str,
    request: AccountsPayableDiscardRequest,
    user: dict = Depends(get_current_user),
):
    service = get_supabase_service()
    try:
        item = await service.discard_accounts_payable_item(
            item_id=item_id,
            user_id=user["user_id"],
            reason=request.reason,
        )
        return {"item": item}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


@router.post("/{item_id}/publish/quickbooks", response_model=Dict[str, Any])
async def publish_accounts_payable_item_to_quickbooks(
    item_id: str,
    user: dict = Depends(get_current_user),
):
    """Publish one item to its workspace's accounting destination (QuickBooks or Xero)."""
    try:
        publisher = await _publisher_for(item_id, user["user_id"])
        item = await publisher.publish_accounts_payable_bill(item_id, user["user_id"])
        return {"item": item}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))


@router.post("/publish/quickbooks", response_model=Dict[str, Any])
async def publish_accounts_payable_batch_to_quickbooks(
    request: AccountsPayableBulkPublishRequest,
    user: dict = Depends(get_current_user),
):
    """Bulk publish to the destination of the batch's workspace (read from the first item)."""
    if not request.item_ids:
        return {"items": [], "failures": [], "total": 0}
    publisher = await _publisher_for(request.item_ids[0], user["user_id"])
    return await publisher.publish_accounts_payable_bills(request.item_ids, user["user_id"])
