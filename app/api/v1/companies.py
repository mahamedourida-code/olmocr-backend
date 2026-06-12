from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.core.dependencies import get_current_user
from app.models.requests import CompanyCreateRequest, CompanyUpdateRequest
from app.services.supabase_service import get_supabase_service


router = APIRouter(prefix="/companies", tags=["Companies"])


@router.get("", response_model=Dict[str, Any])
async def list_companies(
    workspace_id: str = Query(...),
    user: dict = Depends(get_current_user),
):
    try:
        companies = await get_supabase_service().list_companies(user["user_id"], workspace_id)
        return {"companies": companies, "total": len(companies)}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))


@router.post("", response_model=Dict[str, Any])
async def create_company(
    request: CompanyCreateRequest,
    user: dict = Depends(get_current_user),
):
    try:
        company = await get_supabase_service().create_company(
            user["user_id"],
            request.workspace_id,
            request.name,
        )
        return {"company": company}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


@router.get("/{company_id}", response_model=Dict[str, Any])
async def get_company(
    company_id: str,
    user: dict = Depends(get_current_user),
):
    try:
        company = await get_supabase_service().get_company(company_id, user["user_id"])
        return {"company": company}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))


@router.patch("/{company_id}", response_model=Dict[str, Any])
async def update_company(
    company_id: str,
    request: CompanyUpdateRequest,
    user: dict = Depends(get_current_user),
):
    try:
        company = await get_supabase_service().update_company(
            company_id,
            user["user_id"],
            request.model_dump(exclude_none=True),
        )
        return {"company": company}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


@router.delete("/{company_id}", response_model=Dict[str, Any])
async def delete_company(
    company_id: str,
    user: dict = Depends(get_current_user),
):
    try:
        return await get_supabase_service().delete_company(company_id, user["user_id"])
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
