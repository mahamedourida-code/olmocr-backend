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

router = APIRouter(prefix="/download", tags=["File Downloads"])


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
        session = None

        # Load the session if we have a session_id
        if target_session_id:
            session = await storage.get_session_metadata(target_session_id)
            if session:
                logger.info(f"Loaded session {target_session_id} with {len(session.result_files)} files")
            else:
                logger.warning(f"Session {target_session_id} not found in storage")

        # Enhanced debugging
        logger.info(f"Download request for ID: {file_or_job_id}")
        logger.info(f"Target session ID: {target_session_id}")
        logger.info(f"Session result_files: {session.result_files if session else 'NO SESSION'}")
        
        actual_file_id = file_or_job_id  # This might be either a file_id or job_id

        authorized_by_session = False
        authorized_by_job = False

        # First, try to use the ID as a file_id for the current session.
        if session and file_or_job_id in session.result_files:
            logger.info(f"Found {file_or_job_id} in session result_files - treating as file_id")
            actual_file_id = file_or_job_id
            authorized_by_session = True
        else:
            # ID not found in session result_files (or no session)
            # Check if this might be a job_id instead
            logger.info(f"ID {file_or_job_id} not in session result_files, checking if it's a job_id")

            # Try to look up as job_id in Redis
            job_data = await redis_service.get_job(file_or_job_id)

            if job_data:
                logger.info(f"Found job data for job_id {file_or_job_id}")
                verify_job_data_access(job_data, user, target_session_id)
                authorized_by_job = True
                
                # Extract file_id from job data
                if job_data.get('file_id'):
                    actual_file_id = job_data['file_id']
                    logger.info(f"Using file_id {actual_file_id} from job data")
                elif job_data.get('download_url'):
                    # Extract file_id from download_url (e.g., "/api/v1/download/abc123" -> "abc123")
                    download_url = job_data['download_url']
                    if '/download/' in download_url:
                        actual_file_id = download_url.split('/download/')[-1]
                        logger.info(f"Extracted file_id {actual_file_id} from download_url")
                    else:
                        logger.error(f"Invalid download_url format in job data: {download_url}")
                        raise HTTPException(
                            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="Invalid download URL in job data"
                        )
                else:
                    logger.error(f"No file_id or download_url found in job data for job {file_or_job_id}")
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="Job completed but no download file available"
                    )
            else:
                # Not a valid job_id. Treat it as a file_id and let the unified
                # local-disk/Supabase lookup below decide whether it exists.
                logger.info(f"No job data found for {file_or_job_id}, checking as file_id")
                actual_file_id = file_or_job_id

        file_metadata = await redis_service.get_cache(f"file:{actual_file_id}")
        if isinstance(file_metadata, dict) and not authorized_by_session and not authorized_by_job:
            verify_file_metadata_access(file_metadata, user, target_session_id)
        elif not authorized_by_session and not authorized_by_job:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied to this file"
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
            supabase_service = get_supabase_service()
            file_data = await supabase_service.download_file_from_storage(storage_path)
            filename = file_metadata.get("filename", f"{actual_file_id}.xlsx")
            return StreamingResponse(
                io.BytesIO(file_data),
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
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
        
        if not (
            storage_path.startswith(f"{user['user_id']}/")
            or storage_path.startswith(f"users/{user['user_id']}/")
        ):
            logger.warning(f"User {user['user_id']} attempted to access file outside their directory: {storage_path}")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied - you can only download your own files"
            )
        
        # Download file from Supabase Storage
        supabase_service = get_supabase_service()
        
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
