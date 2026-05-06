from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status, Request
from fastapi.responses import FileResponse, StreamingResponse

from app.core.dependencies import (
    get_storage_service,
    get_current_user,
    get_optional_user,
    verify_file_metadata_access,
    verify_job_data_access,
)
from app.services.storage import FileStorageManager
from app.services.redis_service import get_redis_service, RedisService
from app.services.supabase_service import get_supabase_service
from app.models.jobs import SessionMetadata
import io
import json

router = APIRouter(prefix="/download", tags=["File Downloads"])


def _parse_metadata(metadata):
    if isinstance(metadata, dict):
        return metadata
    if isinstance(metadata, str) and metadata:
        try:
            parsed = json.loads(metadata)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


@router.get("/{file_or_job_id}")
async def download_file(
    file_or_job_id: str,
    request: Request,
    session_id: Optional[str] = None,  # Accept session_id as query param
    storage: FileStorageManager = Depends(get_storage_service),
    redis_service: RedisService = Depends(get_redis_service),
    user: Optional[dict] = Depends(get_optional_user),
):
    """
    Download a generated XLSX file.

    This endpoint accepts either:
    1. file_id - Direct file identifier (original behavior)
    2. job_id - Job identifier (will look up the associated file_id)

    Query Parameters:
    - session_id (optional): Session ID to look up files
    """
    import logging
    logger = logging.getLogger(__name__)

    try:
        target_session_id = (
            session_id
            or request.headers.get("x-session-id")
            or request.cookies.get("session_id")
        )
        logger.info(f"Download request for ID: {file_or_job_id}")
        logger.info(f"Target session ID: {target_session_id}")
        
        actual_file_id = file_or_job_id  # This might be either a file_id or job_id
        legacy_file_metadata = None
        supabase_service = get_supabase_service()
        job_data = await redis_service.get_job(file_or_job_id)

        if job_data:
            logger.info(f"Found Redis job data for {file_or_job_id}")
            verify_job_data_access(job_data, user, target_session_id)
            if job_data.get('file_id'):
                actual_file_id = job_data['file_id']
            elif job_data.get('generated_files'):
                generated_files = job_data.get('generated_files') or []
                if generated_files and generated_files[0].get('file_id'):
                    actual_file_id = generated_files[0]['file_id']
            elif job_data.get('download_url') and '/download/' in job_data['download_url']:
                actual_file_id = job_data['download_url'].split('/download/')[-1]

            if actual_file_id == file_or_job_id:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Job completed but no download file available"
                )
        else:
            durable_job_files = await supabase_service.get_job_files_for_job(file_or_job_id)
            if durable_job_files:
                first_file = durable_job_files[0]
                verify_job_data_access(
                    {
                        "job_id": file_or_job_id,
                        "user_id": first_file.get("user_id", ""),
                        "session_id": first_file.get("session_id", ""),
                    },
                    user,
                    target_session_id
                )
                actual_file_id = first_file["file_id"]
            else:
                supabase_job = await supabase_service.get_job(file_or_job_id)
                metadata = _parse_metadata(supabase_job.get("processing_metadata")) if supabase_job else {}
                generated_files = metadata.get("generated_files", []) if metadata else []
                if supabase_job and generated_files and generated_files[0].get("file_id"):
                    stored_user_id = str(supabase_job.get("user_id") or "")
                    owner_user_id = metadata.get("owner_user_id") or (stored_user_id if not stored_user_id.startswith("session:") else "")
                    owner_session_id = metadata.get("owner_session_id") or metadata.get("session_id", "")
                    verify_job_data_access(
                        {
                            "job_id": file_or_job_id,
                            "user_id": owner_user_id or "",
                            "session_id": owner_session_id or "",
                        },
                        user,
                        target_session_id
                    )
                    actual_file_id = generated_files[0]["file_id"]
                    legacy_file_metadata = generated_files[0]
                else:
                    logger.info(f"No job data found for {file_or_job_id}, checking as file_id")

        file_metadata = await redis_service.get_cache(f"file:{actual_file_id}")
        if not isinstance(file_metadata, dict):
            file_metadata = await supabase_service.get_job_file(actual_file_id)
        if not isinstance(file_metadata, dict) and legacy_file_metadata:
            file_metadata = legacy_file_metadata
        if not isinstance(file_metadata, dict) and job_data and job_data.get("generated_files"):
            for generated_file in job_data.get("generated_files") or []:
                if generated_file.get("file_id") == actual_file_id:
                    file_metadata = generated_file
                    break

        if isinstance(file_metadata, dict):
            verify_file_metadata_access(file_metadata, user, target_session_id)
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="File metadata not found"
            )
        
        # Now we have the actual_file_id, try local disk first, then durable storage.
        logger.info(f"Using actual_file_id: {actual_file_id}")

        try:
            file_path = storage.get_download_file_path(actual_file_id)

            if file_path.exists():
                logger.info(f"Serving file: {file_path}")
                return FileResponse(
                    path=str(file_path),
                    filename=f"{actual_file_id}.xlsx",
                    media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    headers={
                        "Content-Disposition": f"attachment; filename=\"{actual_file_id}.xlsx\"",
                        "Cache-Control": "no-cache, no-store, must-revalidate",
                        "Pragma": "no-cache",
                        "Expires": "0"
                    }
                )
        except Exception as local_error:
            logger.info(f"Local download miss for {actual_file_id}: {local_error}")

        storage_path = file_metadata.get("storage_path") if isinstance(file_metadata, dict) else None

        if storage_path:
            file_data = await supabase_service.download_file_from_storage(storage_path)
            filename = file_metadata.get("filename", f"{actual_file_id}.xlsx")
            content_type = file_metadata.get(
                "content_type",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            return StreamingResponse(
                io.BytesIO(file_data),
                media_type=content_type,
                headers={
                    "Content-Disposition": f"attachment; filename=\"{filename}\"",
                    "Cache-Control": "no-cache, no-store, must-revalidate",
                    "Content-Length": str(len(file_data))
                }
            )

        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found on disk or durable storage"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to serve file: {str(e)}"
        )


@router.get("/storage/{storage_path:path}")
async def download_from_storage(
    storage_path: str,
    user: dict = Depends(get_current_user),
):
    """
    Download a file from Supabase Storage.
    
    This endpoint downloads files that have been saved to permanent storage
    in Supabase. The storage_path should be the full path within the bucket
    (e.g., "user_id/job_id/filename.xlsx").
    
    Args:
        storage_path: Path to the file in Supabase Storage
        user: Optional authenticated user (for access control)
    
    Returns:
        The file as a streaming response
    """
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        logger.info(f"Storage download request for path: {storage_path}")
        
        supabase_service = get_supabase_service()
        file_metadata = await supabase_service.get_job_file_by_storage_path(storage_path)

        if not file_metadata:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="File metadata not found"
            )

        verify_file_metadata_access(file_metadata, user, None)

        if file_metadata.get("storage_path") != storage_path:
            logger.warning(f"Storage metadata mismatch for path: {storage_path}")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        # Download file from Supabase Storage
        try:
            file_data = await supabase_service.download_file_from_storage(storage_path)
            
            # Extract filename from path
            filename = storage_path.split('/')[-1] if '/' in storage_path else storage_path
            
            # Return file as streaming response
            return StreamingResponse(
                io.BytesIO(file_data),
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={
                    "Content-Disposition": f"attachment; filename=\"{filename}\"",
                    "Cache-Control": "public, max-age=3600",  # Cache for 1 hour
                    "Content-Length": str(len(file_data))
                }
            )
            
        except Exception as storage_error:
            logger.error(f"Failed to download from storage: {storage_error}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="File not found in storage"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in storage download: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to download file from storage: {str(e)}"
        )
