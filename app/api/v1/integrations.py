from typing import Any, Dict, Optional
from urllib.parse import quote

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import RedirectResponse

from app.core.config import settings
from app.core.dependencies import get_current_user
from app.models.requests import AccountingDestinationRequest, QuickBooksWorkspaceRequest
from app.services.quickbooks_service import get_quickbooks_service
from app.services.supabase_service import get_supabase_service
from app.services.xero_service import get_xero_service


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


# ── P8 — Xero connector (mirrors QuickBooks routes) ──────────────────────────

@router.post("/xero/connect", response_model=Dict[str, Any])
async def connect_xero(
    request: QuickBooksWorkspaceRequest,
    user: dict = Depends(get_current_user),
):
    try:
        authorization_url = await get_xero_service().begin_connection(
            user_id=user["user_id"],
            workspace_id=request.workspace_id,
        )
        return {"authorization_url": authorization_url}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


@router.get("/xero/callback")
async def xero_callback(
    code: Optional[str] = Query(None),
    state_value: Optional[str] = Query(None, alias="state"),
    error: Optional[str] = Query(None),
):
    frontend = settings.frontend_url.rstrip("/")
    if error:
        return RedirectResponse(f"{frontend}/dashboard/integrations?xero=cancelled", status_code=status.HTTP_302_FOUND)
    if not code or not state_value:
        return RedirectResponse(f"{frontend}/dashboard/integrations?xero=error", status_code=status.HTTP_302_FOUND)
    try:
        await get_xero_service().complete_connection(code, state_value)
        return RedirectResponse(f"{frontend}/dashboard/integrations?xero=connected", status_code=status.HTTP_302_FOUND)
    except ValueError as exc:
        message = quote(str(exc)[:160])
        return RedirectResponse(
            f"{frontend}/dashboard/integrations?xero=error&message={message}",
            status_code=status.HTTP_302_FOUND,
        )


@router.get("/xero/status", response_model=Dict[str, Any])
async def xero_status(
    workspace_id: Optional[str] = Query(None),
    user: dict = Depends(get_current_user),
):
    try:
        return await get_xero_service().status(user["user_id"], workspace_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))


@router.post("/xero/sync", response_model=Dict[str, Any])
async def sync_xero_references(
    request: QuickBooksWorkspaceRequest,
    user: dict = Depends(get_current_user),
):
    try:
        return await get_xero_service().sync_reference_data(user["user_id"], request.workspace_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


@router.get("/xero/reference-data", response_model=Dict[str, Any])
async def xero_reference_data(
    workspace_id: Optional[str] = Query(None),
    resource_type: Optional[str] = Query(None, pattern="^(vendor|account|tax_code)$"),
    user: dict = Depends(get_current_user),
):
    try:
        references = await get_xero_service().list_reference_data(user["user_id"], workspace_id, resource_type)
        return {"items": references, "total": len(references)}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


@router.delete("/xero", response_model=Dict[str, Any])
async def disconnect_xero(
    workspace_id: Optional[str] = Query(None),
    user: dict = Depends(get_current_user),
):
    try:
        return await get_xero_service().disconnect(user["user_id"], workspace_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


# ── P8 — Per-workspace accounting destination selector ───────────────────────

@router.get("/destination", response_model=Dict[str, Any])
async def get_accounting_destination(
    workspace_id: Optional[str] = Query(None),
    user: dict = Depends(get_current_user),
):
    destination = await get_supabase_service().get_accounting_destination(user["user_id"], workspace_id)
    return {"destination": destination}


@router.put("/destination", response_model=Dict[str, Any])
async def set_accounting_destination(
    request: AccountingDestinationRequest,
    user: dict = Depends(get_current_user),
):
    try:
        destination = await get_supabase_service().set_accounting_destination(
            user["user_id"], request.workspace_id, request.destination,
        )
        return {"destination": destination}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
