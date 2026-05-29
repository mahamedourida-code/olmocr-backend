from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, File, HTTPException, Query, Request, UploadFile, status

from app.api.v1.jobs import create_batch_job_multipart
from app.core.config import settings
from app.core.dependencies import get_current_user
from app.models.jobs import SessionMetadata
from app.models.requests import ClientUploadLinkRequest
from app.services.redis_service import RedisService, get_redis_service
from app.services.supabase_service import get_supabase_service


router = APIRouter(prefix="/client-intake", tags=["Client Intake"])


@router.get("/links", response_model=Dict[str, Any])
async def list_upload_links(
    workspace_id: str = Query(...),
    user: dict = Depends(get_current_user),
):
    try:
        links = await get_supabase_service().list_client_upload_links(
            user["user_id"], user.get("email"), workspace_id
        )
        return {"links": links, "total": len(links)}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))


@router.post("/links", response_model=Dict[str, Any])
async def create_upload_link(request: ClientUploadLinkRequest, user: dict = Depends(get_current_user)):
    try:
        link = await get_supabase_service().create_client_upload_link(
            user["user_id"],
            user.get("email"),
            request.workspace_id,
            request.label,
            request.expires_in_hours,
            request.max_submissions,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))
    raw_token = link.pop("token")
    return {
        "link": link,
        "upload_url": f"{settings.frontend_url.rstrip('/')}/upload/{raw_token}",
    }


@router.delete("/links/{link_id}", response_model=Dict[str, Any])
async def revoke_upload_link(
    link_id: str,
    workspace_id: str = Query(...),
    user: dict = Depends(get_current_user),
):
    try:
        await get_supabase_service().revoke_client_upload_link(
            user["user_id"], user.get("email"), workspace_id, link_id
        )
        return {"success": True, "link_id": link_id}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))


@router.get("/submissions", response_model=Dict[str, Any])
async def list_client_submissions(
    workspace_id: str = Query(...),
    limit: int = Query(50, ge=1, le=100),
    user: dict = Depends(get_current_user),
):
    try:
        submissions = await get_supabase_service().list_client_upload_submissions(
            user["user_id"], user.get("email"), workspace_id, limit
        )
        return {"submissions": submissions, "total": len(submissions)}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))


@router.get("/public/{token}", response_model=Dict[str, Any])
async def public_upload_context(token: str):
    link = await get_supabase_service().get_public_client_upload_link(token)
    if not link:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="This upload link is unavailable or expired")
    return {
        "label": link["label"],
        "workspace_name": link["workspace_name"],
        "expires_at": link["expires_at"],
    }


@router.get("/public/{token}/status", response_model=Dict[str, Any])
async def public_upload_status(token: str):
    status_view = await get_supabase_service().get_public_client_status(token)
    if not status_view:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="This status link is unavailable")
    return status_view


@router.post("/public/{token}/upload", response_model=Dict[str, Any])
async def submit_client_files(
    token: str,
    request: Request,
    files: List[UploadFile] = File(...),
    redis_service: RedisService = Depends(get_redis_service),
):
    service = get_supabase_service()
    link = await service.claim_client_upload_link(token)
    if not link:
        raise HTTPException(status_code=status.HTTP_410_GONE, detail="This upload link is unavailable or expired")
    submission = await service.create_client_upload_submission(link, len(files))
    client_session = SessionMetadata(
        session_id=f"client-link:{submission['id']}",
        expires_at=datetime.utcnow() + timedelta(hours=settings.file_retention_hours),
    )
    try:
        job = await create_batch_job_multipart(
            files=files,
            output_format="xlsx",
            consolidation_strategy="separate",
            document_mode="auto",
            workspace_id=str(link["workspace_id"]),
            session=client_session,
            redis_service=redis_service,
            user={"user_id": str(link["owner_user_id"])},
            http_request=request,
        )
        await service.update_client_upload_submission(submission["id"], {
            "status": "queued",
            "job_id": job.job_id,
        })
        await service.record_workspace_audit(
            str(link["workspace_id"]),
            None,
            "client_link",
            "client_batch_submitted",
            "client_submission",
            submission["id"],
            {"file_count": len(files)},
        )
        return {"accepted": True, "submission_id": submission["id"], "status": "queued"}
    except HTTPException:
        await service.update_client_upload_submission(submission["id"], {"status": "rejected"})
        raise
    except Exception:
        await service.update_client_upload_submission(submission["id"], {"status": "failed"})
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Files could not be submitted")
