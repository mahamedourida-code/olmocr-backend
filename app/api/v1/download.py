from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import FileResponse

from app.core.dependencies import get_storage_service
from app.services.storage import FileStorageManager
from app.services.redis_service import get_redis_service, RedisService
from app.models.jobs import SessionMetadata

router = APIRouter(prefix="/download", tags=["File Downloads"])


@router.get("/{file_or_job_id}")
async def download_file(
    file_or_job_id: str,
    session_id: Optional[str] = None,  # Accept session_id as query param
    storage: FileStorageManager = Depends(get_storage_service),
    redis_service: RedisService = Depends(get_redis_service)
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
        # CRITICAL FIX: Use session_id from query param OR try to find it from the job
        target_session_id = session_id
        session = None

        # If no session_id provided, try to get it from job data
        if not target_session_id:
            logger.info(f"No session_id provided, looking up job {file_or_job_id}")
            job_data = await redis_service.get_job(file_or_job_id)
            if job_data and job_data.get('session_id'):
                target_session_id = job_data['session_id']
                logger.info(f"Found session_id from job: {target_session_id}")

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

        # First, try to use the ID as a file_id (original behavior)
        if session and file_or_job_id in session.result_files:
            logger.info(f"Found {file_or_job_id} in session result_files - treating as file_id")
            actual_file_id = file_or_job_id
        else:
            # ID not found in session result_files (or no session)
            # Check if this might be a job_id instead
            logger.info(f"ID {file_or_job_id} not in session result_files, checking if it's a job_id")

            # Try to look up as job_id in Redis
            job_data = await redis_service.get_job(file_or_job_id)

            if job_data:
                # This is a valid job_id for this session
                logger.info(f"Found job data for job_id {file_or_job_id}")
                
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
                # Not a valid job_id, try fallback: check if file exists on disk
                logger.info(f"No job data found for {file_or_job_id}, trying file system fallback")

                # FALLBACK: Check if file exists on disk
                try:
                    file_path = storage.get_download_file_path(file_or_job_id)

                    if file_path.exists():
                        # File exists on disk - allow download even without session verification
                        # This handles cases where session management fails but files are valid
                        from datetime import datetime, timedelta
                        import os

                        file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                        if datetime.utcnow() - file_mtime < timedelta(hours=24):  # Extended to 24h
                            logger.info(f"Found file {file_or_job_id} on disk (modified {file_mtime})")
                            actual_file_id = file_or_job_id

                            # Update session if we have one
                            if session and file_or_job_id not in session.result_files:
                                session.result_files.append(file_or_job_id)
                                await storage.update_session_metadata(session)
                                logger.info(f"Added {file_or_job_id} to session {session.session_id}")
                        else:
                            logger.error(f"File {file_or_job_id} exists but is too old ({file_mtime})")
                            raise HTTPException(
                                status_code=status.HTTP_410_GONE,
                                detail="File has expired"
                            )
                    else:
                        logger.error(f"File {file_or_job_id} not found anywhere (disk, job, session)")
                        raise HTTPException(
                            status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"File {file_or_job_id} not found or expired"
                        )
                except HTTPException:
                    raise
                except Exception as e:
                    logger.error(f"Error in fallback file check: {e}")
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"File {file_or_job_id} not found or expired"
                    )
        
        # Now we have the actual_file_id, get the file path
        logger.info(f"Using actual_file_id: {actual_file_id}")
        file_path = storage.get_download_file_path(actual_file_id)
        
        if not file_path.exists():
            logger.error(f"File not found on disk: {file_path}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="File not found on disk"
            )
        
        # Return file as download
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
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to serve file: {str(e)}"
        )