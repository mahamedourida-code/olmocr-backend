from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, status

from app.core.dependencies import get_current_user
from app.models.requests import WorkspaceCreateRequest, WorkspaceReviewerRequest
from app.services.supabase_service import get_supabase_service


router = APIRouter(prefix="/workspaces", tags=["Workspaces"])


@router.get("", response_model=Dict[str, Any])
async def list_workspaces(user: dict = Depends(get_current_user)):
    return await get_supabase_service().list_accessible_workspaces(user["user_id"], user.get("email"))


@router.post("", response_model=Dict[str, Any])
async def create_workspace(request: WorkspaceCreateRequest, user: dict = Depends(get_current_user)):
    try:
        workspace = await get_supabase_service().create_owned_workspace(
            user["user_id"],
            user.get("email"),
            request.name,
        )
        return {"workspace": workspace}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


@router.put("/{workspace_id}/active", response_model=Dict[str, Any])
async def set_active_workspace(workspace_id: str, user: dict = Depends(get_current_user)):
    try:
        workspace = await get_supabase_service().select_accessible_workspace(
            user["user_id"],
            user.get("email"),
            workspace_id,
        )
        return {"workspace": workspace}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))


@router.get("/{workspace_id}/members", response_model=Dict[str, Any])
async def list_members(workspace_id: str, user: dict = Depends(get_current_user)):
    try:
        members = await get_supabase_service().list_workspace_members(
            user["user_id"],
            user.get("email"),
            workspace_id,
        )
        return {"members": members, "total": len(members)}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))


@router.post("/{workspace_id}/members", response_model=Dict[str, Any])
async def invite_reviewer(
    workspace_id: str,
    request: WorkspaceReviewerRequest,
    user: dict = Depends(get_current_user),
):
    try:
        member = await get_supabase_service().invite_workspace_reviewer(
            user["user_id"],
            user.get("email"),
            workspace_id,
            request.email,
        )
        return {"member": member}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


@router.delete("/{workspace_id}/members/{membership_id}", response_model=Dict[str, Any])
async def revoke_reviewer(
    workspace_id: str,
    membership_id: str,
    user: dict = Depends(get_current_user),
):
    try:
        await get_supabase_service().revoke_workspace_member(
            user["user_id"],
            user.get("email"),
            workspace_id,
            membership_id,
        )
        return {"success": True, "membership_id": membership_id}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
