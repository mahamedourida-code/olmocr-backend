from typing import Any, Dict, Optional
from urllib.parse import quote

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import RedirectResponse

from app.core.config import settings
from app.core.dependencies import get_current_user
from app.models.requests import QuickBooksWorkspaceRequest
from app.services.quickbooks_service import get_quickbooks_service


router = APIRouter(prefix="/integrations", tags=["Integrations"])


@router.post("/quickbooks/connect", response_model=Dict[str, Any])
async def connect_quickbooks(
    request: QuickBooksWorkspaceRequest,
    user: dict = Depends(get_current_user),
):
    try:
        authorization_url = await get_quickbooks_service().begin_connection(
            user_id=user["user_id"],
            workspace_id=request.workspace_id,
        )
        return {"authorization_url": authorization_url}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


@router.get("/quickbooks/callback")
async def quickbooks_callback(
    code: Optional[str] = Query(None),
    state_value: Optional[str] = Query(None, alias="state"),
    realm_id: Optional[str] = Query(None, alias="realmId"),
    error: Optional[str] = Query(None),
):
    frontend = settings.frontend_url.rstrip("/")
    if error:
        return RedirectResponse(
            f"{frontend}/dashboard/integrations?quickbooks=cancelled",
            status_code=status.HTTP_302_FOUND,
        )
    if not code or not realm_id or not state_value:
        return RedirectResponse(
            f"{frontend}/dashboard/integrations?quickbooks=error",
            status_code=status.HTTP_302_FOUND,
        )
    try:
        await get_quickbooks_service().complete_connection(code, realm_id, state_value)
        return RedirectResponse(
            f"{frontend}/dashboard/integrations?quickbooks=connected",
            status_code=status.HTTP_302_FOUND,
        )
    except ValueError as exc:
        message = quote(str(exc)[:160])
        return RedirectResponse(
            f"{frontend}/dashboard/integrations?quickbooks=error&message={message}",
            status_code=status.HTTP_302_FOUND,
        )


@router.get("/quickbooks/status", response_model=Dict[str, Any])
async def quickbooks_status(
    workspace_id: Optional[str] = Query(None),
    user: dict = Depends(get_current_user),
):
    try:
        return await get_quickbooks_service().status(user["user_id"], workspace_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))


@router.post("/quickbooks/sync", response_model=Dict[str, Any])
async def sync_quickbooks_references(
    request: QuickBooksWorkspaceRequest,
    user: dict = Depends(get_current_user),
):
    try:
        return await get_quickbooks_service().sync_reference_data(user["user_id"], request.workspace_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


@router.get("/quickbooks/reference-data", response_model=Dict[str, Any])
async def quickbooks_reference_data(
    workspace_id: Optional[str] = Query(None),
    resource_type: Optional[str] = Query(None, pattern="^(vendor|account|tax_code)$"),
    user: dict = Depends(get_current_user),
):
    try:
        references = await get_quickbooks_service().list_reference_data(
            user["user_id"],
            workspace_id,
            resource_type,
        )
        return {"items": references, "total": len(references)}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


@router.delete("/quickbooks", response_model=Dict[str, Any])
async def disconnect_quickbooks(
    workspace_id: Optional[str] = Query(None),
    user: dict = Depends(get_current_user),
):
    try:
        return await get_quickbooks_service().disconnect(user["user_id"], workspace_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
