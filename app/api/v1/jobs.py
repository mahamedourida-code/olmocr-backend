from fastapi import APIRouter, Depends, HTTPException, status, Request, File, UploadFile, Form
from fastapi.responses import JSONResponse
from typing import Dict, Any, List
import os
import uuid
import logging
import traceback
import threading
import base64
from datetime import datetime, timedelta

from app.models.requests import BatchConvertRequest
from app.models.responses import BatchConvertResponse, JobStatusResponse, ErrorResponse
from app.models.jobs import JobStatus, BatchProcessingContext, ImageProcessingResult
from app.core.dependencies import (
    get_or_create_session, get_storage_service,
    verify_job_ownership, get_optional_user, get_current_user
)
from app.core.config import settings
from app.services.storage import FileStorageManager
from app.services.redis_service import get_redis_service, RedisService
from app.services.supabase_service import get_supabase_service
from app.tasks.batch_tasks import process_batch_images
from app.utils.exceptions import ProcessingError, ValidationError
from app.models.jobs import SessionMetadata
from typing import Optional

logger = logging.getLogger(__name__)


def simple_rate_limit_check(request: Request) -> None:
    """Simple rate limit check without dependencies."""
    # For now, just skip rate limiting to simplify debugging
    # In production, implement proper rate limiting
    pass


def simple_batch_validation(image_count: int) -> None:
    """Simple batch validation without dependencies."""
    MAX_BATCH_SIZE = settings.max_batch_size

    if image_count == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No images provided for batch processing"
        )

    if image_count > MAX_BATCH_SIZE:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Batch size ({image_count}) exceeds maximum allowed ({MAX_BATCH_SIZE})"
        )

router = APIRouter(prefix="/jobs", tags=["Batch Jobs"])


@router.post("/batch", response_model=BatchConvertResponse)
async def create_batch_job(
    request: BatchConvertRequest,
    session: SessionMetadata = Depends(get_or_create_session),
    storage: FileStorageManager = Depends(get_storage_service),
    redis_service: RedisService = Depends(get_redis_service),
    user: Optional[dict] = Depends(get_optional_user),
    http_request: Request = None
):
    """
    Create a new batch processing job for multiple images.
    
    This endpoint accepts multiple base64-encoded images and starts a distributed
    Celery job to process them concurrently and generate consolidated XLSX output.
    """
    # Rate limiting
    simple_rate_limit_check(http_request)
    
    # Validate batch request
    simple_batch_validation(len(request.images))
    
    # Generate job ID
    job_id = str(uuid.uuid4())
    
    try:
        # Create job metadata for Redis (convert None values to empty strings for Redis compatibility)
        job_data = {
            'job_id': job_id,
            'session_id': session.session_id,
            'user_id': user['user_id'] if user else '',  # Convert None to empty string for Redis
            'status': 'queued',
            'total_images': len(request.images),
            'processed_images': 0,
            'progress': 0,
            'output_format': request.output_format,
            'consolidation_strategy': request.consolidation_strategy,
            'images': [{'id': f"img_{i}", 'data': img.image, 'filename': img.filename or f"image_{i}.png"} for i, img in enumerate(request.images)],
            'results': [],
            'errors': [],
            'created_at': datetime.utcnow().isoformat(),
            'updated_at': datetime.utcnow().isoformat()
        }

        # Create job in Supabase if user is authenticated
        if user:
            try:
                supabase_service = get_supabase_service()
                await supabase_service.create_job(
                    job_id=job_id,
                    user_id=user['user_id'],
                    filename=f"batch_{len(request.images)}_images",
                    metadata={
                        'total_images': len(request.images),
                        'consolidation_strategy': request.consolidation_strategy,
                        'output_format': request.output_format,
                        'session_id': session.session_id
                    }
                )
                logger.info(f"Created Supabase job {job_id} for user {user['user_id']}")
            except Exception as e:
                logger.error(f"Failed to create Supabase job: {e}")
                # Continue anyway - don't fail the request
        
        # Store job in Redis
        create_success = await redis_service.create_job(job_id, job_data)
        
        if not create_success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to store job in Redis"
            )
        
        # Start simple async batch processing
        from app.tasks.simple_batch import process_batch_simple
        import asyncio

        # Launch background task to process batch
        asyncio.create_task(process_batch_simple(
            job_id=job_id,
            session_id=session.session_id,
            images=job_data['images'],
            user_id=user['user_id'] if user else None
        ))

        logger.info(f"Started background processing for job {job_id} with {len(job_data['images'])} images")
        
        # Update session
        session.jobs_count += 1
        session.active_jobs.append(job_id)
        await storage.update_session_metadata(session)
        
        # Calculate estimated completion (now much faster with concurrency)
        estimated_completion = datetime.utcnow() + timedelta(
            seconds=max(30, len(request.images) * 2)  # Much faster with parallel processing
        )
        
        return BatchConvertResponse(
            success=True,
            job_id=job_id,
            estimated_completion=estimated_completion,
            status_url=f"/api/v1/jobs/{job_id}/status",
            session_id=session.session_id
        )
        
    except Exception as e:
        # Clean up job if creation failed
        try:
            await redis_service.delete_job(job_id)
        except:
            pass
        
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create batch job: {str(e)}"
        )


@router.post("/batch-upload", response_model=BatchConvertResponse)
async def create_batch_job_multipart(
    files: List[UploadFile] = File(..., description="Image files to process (PNG, JPEG, WebP)"),
    output_format: str = Form("xlsx"),
    consolidation_strategy: str = Form("consolidated"),
    session: SessionMetadata = Depends(get_or_create_session),
    storage: FileStorageManager = Depends(get_storage_service),
    redis_service: RedisService = Depends(get_redis_service),
    user: Optional[dict] = Depends(get_optional_user),
    http_request: Request = None
):
    """
    Create a new batch processing job using multipart/form-data file uploads.

    This endpoint accepts multiple binary image files directly (no base64 encoding needed),
    making uploads faster and more memory-efficient than the base64 endpoint.

    This is the recommended endpoint for file uploads.
    """
    # Detailed logging for debugging
    logger.info(f"[BATCH-UPLOAD] Received request with {len(files)} files")
    logger.info(f"[BATCH-UPLOAD] Parameters: output_format={output_format}, consolidation_strategy={consolidation_strategy}")
    logger.info(f"[BATCH-UPLOAD] Session ID: {session.session_id if session else 'None'}")
    logger.info(f"[BATCH-UPLOAD] User ID: {user['user_id'] if user else 'None'}")

    # Log file details for debugging
    for i, file in enumerate(files):
        logger.info(f"[BATCH-UPLOAD] File {i+1}: name={file.filename}, content_type={file.content_type}, size=pending")

    # Rate limiting
    simple_rate_limit_check(http_request)

    # Validate batch request
    simple_batch_validation(len(files))

    # Validate each file
    SUPPORTED_TYPES = {"image/png", "image/jpeg", "image/jpg", "image/webp"}
    MAX_FILE_SIZE = settings.max_file_size_bytes

    for i, file in enumerate(files):
        logger.info(f"[BATCH-UPLOAD] Validating file {i+1}: {file.filename}")

        # Check content type
        if file.content_type not in SUPPORTED_TYPES:
            error_msg = f"File {i+1} '{file.filename}': Unsupported file type '{file.content_type}'. Supported types: PNG, JPEG, WebP"
            logger.error(f"[BATCH-UPLOAD] Validation failed - {error_msg}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=error_msg
            )

        # Check file size (read first to get size, then seek back)
        file_content = await file.read()
        file_size = len(file_content)
        logger.info(f"[BATCH-UPLOAD] File {i+1} size: {file_size} bytes ({file_size / 1024 / 1024:.2f}MB)")

        if file_size > MAX_FILE_SIZE:
            error_msg = f"File {i+1} '{file.filename}': File size ({file_size / 1024 / 1024:.2f}MB) exceeds maximum allowed ({MAX_FILE_SIZE / 1024 / 1024:.0f}MB)"
            logger.error(f"[BATCH-UPLOAD] Validation failed - {error_msg}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=error_msg
            )

        if file_size < 100:  # Minimum reasonable file size
            error_msg = f"File {i+1} '{file.filename}': File appears to be empty or corrupted (size: {file_size} bytes)"
            logger.error(f"[BATCH-UPLOAD] Validation failed - {error_msg}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=error_msg
            )

        # Reset file pointer for later processing
        await file.seek(0)
        logger.info(f"[BATCH-UPLOAD] File {i+1} validation passed")

    # Generate job ID
    job_id = str(uuid.uuid4())
    logger.info(f"[BATCH-UPLOAD] Generated job ID: {job_id}")

    try:
        # Convert uploaded files to base64 format expected by existing processing pipeline
        # In future, we can refactor the pipeline to work directly with binary data
        images_data = []
        for i, file in enumerate(files):
            # Read binary data (we already validated it above)
            file_content = await file.read()

            # Convert to base64 for compatibility with existing processing pipeline
            base64_data = base64.b64encode(file_content).decode('utf-8')

            images_data.append({
                'id': f"img_{i}",
                'data': base64_data,
                'filename': file.filename or f"image_{i}.png"
            })

            # Reset file pointer (in case we need to read again)
            await file.seek(0)

        # Create job metadata for Redis (convert None values to empty strings for Redis compatibility)
        job_data = {
            'job_id': job_id,
            'session_id': session.session_id,
            'user_id': user['user_id'] if user else '',  # Convert None to empty string for Redis
            'status': 'queued',
            'total_images': len(files),
            'processed_images': 0,
            'progress': 0,
            'output_format': output_format,
            'consolidation_strategy': consolidation_strategy,
            'images': images_data,
            'results': [],
            'errors': [],
            'created_at': datetime.utcnow().isoformat(),
            'updated_at': datetime.utcnow().isoformat()
        }

        # Create job in Supabase if user is authenticated
        if user:
            try:
                supabase_service = get_supabase_service()
                await supabase_service.create_job(
                    job_id=job_id,
                    user_id=user['user_id'],
                    filename=f"batch_{len(files)}_images",
                    metadata={
                        'total_images': len(files),
                        'consolidation_strategy': consolidation_strategy,
                        'output_format': output_format,
                        'session_id': session.session_id
                    }
                )
                logger.info(f"Created Supabase job {job_id} for user {user['user_id']}")
            except Exception as e:
                logger.error(f"Failed to create Supabase job: {e}")
                # Continue anyway - don't fail the request

        # Store job in Redis
        create_success = await redis_service.create_job(job_id, job_data)

        if not create_success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to store job in Redis"
            )

        # Start simple async batch processing
        from app.tasks.simple_batch import process_batch_simple
        import asyncio

        # Launch background task to process batch
        asyncio.create_task(process_batch_simple(
            job_id=job_id,
            session_id=session.session_id,
            images=job_data['images'],
            user_id=user['user_id'] if user else None
        ))

        logger.info(f"Started background processing for job {job_id} with {len(job_data['images'])} images")

        # Update session
        session.jobs_count += 1
        session.active_jobs.append(job_id)
        await storage.update_session_metadata(session)

        # Calculate estimated completion (now much faster with concurrency)
        estimated_completion = datetime.utcnow() + timedelta(
            seconds=max(30, len(files) * 2)  # Much faster with parallel processing
        )

        return BatchConvertResponse(
            success=True,
            job_id=job_id,
            estimated_completion=estimated_completion,
            status_url=f"/api/v1/jobs/{job_id}/status",
            session_id=session.session_id
        )

    except Exception as e:
        # Enhanced error logging for debugging
        logger.error(f"[BATCH-UPLOAD] Failed to create batch job {job_id}: {str(e)}")
        logger.error(f"[BATCH-UPLOAD] Error type: {type(e).__name__}")
        import traceback
        logger.error(f"[BATCH-UPLOAD] Traceback: {traceback.format_exc()}")

        # Clean up job if creation failed
        try:
            await redis_service.delete_job(job_id)
            logger.info(f"[BATCH-UPLOAD] Cleaned up failed job {job_id}")
        except Exception as cleanup_error:
            logger.warning(f"[BATCH-UPLOAD] Failed to cleanup job {job_id}: {cleanup_error}")

        # Return detailed error information
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create batch job: {str(e)}"
        )


@router.get("/{job_id}/status", response_model=JobStatusResponse)
async def get_job_status(
    job_id: str,
    session: SessionMetadata = Depends(get_or_create_session),
    storage: FileStorageManager = Depends(get_storage_service),
    redis_service: RedisService = Depends(get_redis_service)
):
    """
    Get the current status of a batch processing job.
    
    Returns detailed information about job progress, including:
    - Current processing status
    - Progress percentage
    - Results (when completed)
    - Error messages (if any)
    
    Also supports single image conversion job_ids for consistency.
    """
    # Basic validation of job_id format
    if not job_id or len(job_id.strip()) == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Job ID cannot be empty"
        )
    
    # Get job data from Redis first
    job_data = await redis_service.get_job(job_id)
    
    if not job_data:
        # Check if this is a job_id from single image conversion
        # For single image conversions, we create a synthetic job status response
        try:
            # Check if the ID corresponds to a file in the user's session
            if job_id in session.result_files:
                # Create synthetic completed job data for single image conversion
                job_data = {
                    'status': 'completed',
                    'total_images': 1,
                    'processed_images': 1,
                    'progress': 100,
                    'download_url': f"/api/v1/download/{job_id}",
                    'results': [{'job_id': job_id}],  # Updated to use job_id for consistency
                    'errors': [],
                    'session_id': session.session_id,
                    'created_at': datetime.utcnow().isoformat(),
                    'updated_at': datetime.utcnow().isoformat()
                }
            else:
                # Check if file exists in storage (fallback check for old jobs)
                try:
                    file_path = storage.get_download_file_path(job_id)
                    if file_path.exists():
                        job_data = {
                            'status': 'completed',
                            'total_images': 1,
                            'processed_images': 1,
                            'progress': 100,
                            'download_url': f"/api/v1/download/{job_id}",
                            'results': [{'job_id': job_id}],
                            'errors': [],
                            'session_id': session.session_id,
                            'created_at': datetime.utcnow().isoformat(),
                            'updated_at': datetime.utcnow().isoformat()
                        }
                    else:
                        raise HTTPException(
                            status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Job '{job_id}' not found. Make sure you're using the same session (cookies/headers) as when you created the job."
                        )
                except Exception:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"Job '{job_id}' not found. Make sure you're using the same session (cookies/headers) as when you created the job."
                    )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error checking job status: {e}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Job not found"
            )
    
    # Build progress info
    progress = None
    total_images = int(job_data.get('total_images', 0))
    processed_images = int(job_data.get('processed_images', 0))
    
    if total_images > 0:
        from app.models.responses import JobProgress
        progress = JobProgress(
            total_images=total_images,
            processed_images=processed_images,
            current_image=job_data.get('current_image'),
            percentage=int(job_data.get('progress', 0))
        )
    
    # Build results info
    results = None
    if job_data.get('status') in ['completed', 'partially_completed']:
        from app.models.responses import BatchJobResults, ProcessedFile
        
        try:
            # Calculate expiration time safely
            expires_at = None
            if job_data.get('completed_at'):
                try:
                    completed_time = datetime.fromisoformat(job_data['completed_at'])
                    expires_at = completed_time + timedelta(hours=24)
                except ValueError:
                    # If datetime parsing fails, set expires_at to 24 hours from now
                    expires_at = datetime.utcnow() + timedelta(hours=24)
            else:
                expires_at = datetime.utcnow() + timedelta(hours=24)
            
            # Handle multiple files (new format)
            files = []
            if job_data.get('generated_files'):
                for file_info in job_data['generated_files']:
                    files.append(ProcessedFile(
                        file_id=file_info['file_id'],
                        download_url=f"/api/v1/download/{file_info['file_id']}",
                        filename=file_info['filename'],
                        original_image=file_info.get('original_filename', file_info.get('image_id', 'unknown')),
                        size_bytes=file_info.get('size_bytes'),
                        created_at=datetime.fromisoformat(job_data.get('completed_at', datetime.utcnow().isoformat()))
                    ))
            
            # Get processing statistics
            total_images = int(job_data.get('total_images', 0))
            
            # Calculate success/failure based on generated files vs total images
            successful_count = len(files)  # Number of files generated = successful images
            failed_count = total_images - successful_count  # Remaining images failed
            
            # Get processing time
            processing_time = float(job_data.get('processing_time', 0))
            
            # Get completion time
            completed_at = datetime.utcnow()
            if job_data.get('completed_at'):
                try:
                    completed_at = datetime.fromisoformat(job_data['completed_at'])
                except ValueError:
                    pass
            
            results = BatchJobResults(
                total_images=total_images,
                successful_images=successful_count,
                failed_images=failed_count,
                files=files,
                total_files=len(files),
                primary_download=f"/api/v1/download/{files[0].file_id}" if files else None,
                expires_at=expires_at,
                processing_time_seconds=processing_time,
                completed_at=completed_at
            )
        except Exception as e:
            logger.error(f"Error creating BatchJobResults: {e}")
            # Create a minimal BatchJobResults if there's an error
            results = BatchJobResults(
                total_images=int(job_data.get('total_images', 0)),
                successful_images=0,
                failed_images=0,
                files=[],
                total_files=0,
                primary_download=None,
                expires_at=datetime.utcnow() + timedelta(hours=24),
                processing_time_seconds=0.0,
                completed_at=datetime.utcnow()
            )
    
    # Build error info
    errors = []
    if job_data.get('errors'):
        # Convert error objects to strings if needed
        raw_errors = job_data['errors']
        if isinstance(raw_errors, list):
            for error in raw_errors:
                if isinstance(error, dict):
                    errors.append(error.get('error', str(error)))
                else:
                    errors.append(str(error))
        else:
            errors.append(str(raw_errors))
    elif job_data.get('error'):
        errors.append(str(job_data['error']))
    
    try:
        # Build the response with proper error handling
        response = JobStatusResponse(
            job_id=job_id,
            status=job_data.get('status', 'unknown'),  # Use .get() with default
            progress=progress,
            results=results,
            errors=errors,
            created_at=datetime.fromisoformat(job_data['created_at']) if job_data.get('created_at') else datetime.utcnow(),
            updated_at=datetime.fromisoformat(job_data['updated_at']) if job_data.get('updated_at') else datetime.utcnow()
        )
        return response
        
    except Exception as e:
        # Log the error and provide detailed debugging info
        logger.error(f"Failed to create JobStatusResponse for job {job_id}: {e}")
        logger.error(f"Job data keys: {list(job_data.keys()) if job_data else 'None'}")
        logger.error(f"Job data: {job_data}")
        
        # Instead of failing, create a minimal response
        try:
            return JobStatusResponse(
                job_id=job_id,
                status=job_data.get('status', 'unknown') if job_data else 'unknown',
                progress=None,
                results=None,
                errors=['Failed to parse job status'],
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
        except Exception as fallback_error:
            logger.error(f"Even fallback response failed: {fallback_error}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create job status response: {str(e)}"
            )


@router.delete("/{job_id}")
async def cancel_job(
    job_id: str,
    session: SessionMetadata = Depends(get_or_create_session),
    storage: FileStorageManager = Depends(get_storage_service),
    redis_service: RedisService = Depends(get_redis_service)
):
    """
    Cancel a running batch job.
    
    Cancels the Celery task and marks the job as cancelled.
    """
    # Verify job ownership
    await verify_job_ownership(job_id, session, storage)
    
    job_data = await redis_service.get_job(job_id)
    
    if not job_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found"
        )
    
    if job_data.get('status') in ["completed", "failed"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot cancel completed or failed job"
        )
    
    # Cancel Celery task if we have the task ID
    if job_data.get('celery_task_id'):
        from app.tasks.celery_app import celery_app
        celery_app.control.revoke(job_data['celery_task_id'], terminate=True)
    
    # Mark job as cancelled
    await redis_service.update_job(job_id, {
        'status': 'failed',
        'error': 'Job cancelled by user',
        'updated_at': datetime.utcnow().isoformat()
    })
    
    # Update session
    if job_id in session.active_jobs:
        session.active_jobs.remove(job_id)
        await storage.update_session_metadata(session)
    
    return {"message": "Job cancelled successfully"}


@router.get("/", response_model=Dict[str, Any])
async def list_jobs(
    session: SessionMetadata = Depends(get_or_create_session),
    storage: FileStorageManager = Depends(get_storage_service),
    limit: int = 50,
    offset: int = 0
):
    """
    List all batch jobs for the current session.

    Returns a paginated list of jobs with their current status.
    """
    # Get all jobs for the session
    jobs = []

    for job_id in session.active_jobs + [job_id for job_id in session.active_jobs]:
        job_status = await storage.get_job_status(job_id)
        if job_status:
            jobs.append({
                "job_id": job_status.job_id,
                "status": job_status.status,
                "created_at": job_status.created_at,
                "total_images": job_status.total_images,
                "processed_images": job_status.processed_images,
                "percentage": job_status.percentage
            })

    # Sort by creation time (newest first)
    jobs.sort(key=lambda x: x["created_at"], reverse=True)

    # Apply pagination
    total = len(jobs)
    jobs = jobs[offset:offset + limit]

    return {
        "jobs": jobs,
        "total": total,
        "limit": limit,
        "offset": offset,
        "has_more": offset + limit < total
    }


@router.get("/history", response_model=Dict[str, Any])
async def get_job_history(
    user: Optional[dict] = Depends(get_optional_user),
    limit: int = 50,
    offset: int = 0
):
    """
    Get job history for authenticated user.

    Returns a paginated list of completed jobs with download URLs.
    Requires authentication.
    """
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required to view job history"
        )

    try:
        supabase_service = get_supabase_service()

        # Get user's jobs from Supabase (already sorted by created_at desc)
        all_jobs = await supabase_service.get_user_jobs(user['user_id'], limit=limit + offset)

        # Apply offset
        jobs = all_jobs[offset:offset + limit]

        # Format response with download URLs
        formatted_jobs = []
        for job in jobs:
            formatted_job = {
                "job_id": job.get("id"),
                "filename": job.get("filename", "Unknown"),
                "status": job.get("status", "unknown"),
                "result_url": job.get("result_url"),
                "created_at": job.get("created_at"),
                "updated_at": job.get("updated_at"),
                "metadata": job.get("processing_metadata", {})
            }
            formatted_jobs.append(formatted_job)

        return {
            "jobs": formatted_jobs,
            "total": len(all_jobs),
            "limit": limit,
            "offset": offset,
            "has_more": offset + limit < len(all_jobs)
        }

    except Exception as e:
        logger.error(f"Failed to get job history: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve job history: {str(e)}"
        )


@router.post("/{job_id}/save")
async def save_job_to_history(
    job_id: str,
    user: dict = Depends(get_current_user),
    redis_service: RedisService = Depends(get_redis_service),
    storage: FileStorageManager = Depends(get_storage_service),
):
    """
    Save a completed job to the user's permanent history.
    
    This endpoint allows users to explicitly save their job results to their history.
    Only completed jobs can be saved, and only by the job owner.
    Files are uploaded to Supabase Storage for permanent storage.
    """
    try:
        # Get job from Redis
        job_data = await redis_service.get_job(job_id)
        if not job_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Job not found"
            )

        # Verify job ownership (either by user_id or session if not authenticated)
        if job_data.get('user_id') and job_data.get('user_id') != user['user_id']:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied - job belongs to another user"
            )

        # Check if job is completed
        if job_data.get('status') != 'completed':
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Only completed jobs can be saved to history"
            )

        # Check if job has results
        results = job_data.get('generated_files', [])
        if not results:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Job has no results to save"
            )

        # Save to Supabase if not already saved
        supabase_service = get_supabase_service()
        
        # Check if already exists in user's history
        existing_jobs = await supabase_service.get_user_jobs(user['user_id'], limit=1000)
        job_exists = any(job.get('id') == job_id for job in existing_jobs)
        
        if job_exists:
            return {
                "success": True,
                "message": "Job already saved to history",
                "job_id": job_id
            }

        # Upload each generated Excel file to Supabase Storage
        storage_urls = []
        logger.info(f"Starting file upload process for job {job_id}. Found {len(results)} files to upload")
        
        for file_info in results:
            file_id = file_info.get('file_id')
            if not file_id:
                logger.warning(f"File info missing file_id: {file_info}")
                continue
            
            # Get the file from local storage
            file_path = storage.get_download_file_path(file_id)
            if not file_path.exists():
                logger.warning(f"File {file_id} not found on disk at {file_path}, skipping upload")
                continue
            
            # Log file size and location
            file_size = file_path.stat().st_size
            logger.info(f"Reading file {file_id} from {file_path} (size: {file_size} bytes)")
            
            # Read file content
            with open(file_path, 'rb') as f:
                file_data = f.read()
            
            # Upload to Supabase Storage with enforced path structure
            filename = file_info.get('filename', f'{file_id}.xlsx')
            logger.info(f"Attempting to upload file {file_id} as {filename} to Supabase Storage")
            
            try:
                upload_result = await supabase_service.upload_file_to_storage(
                    file_data=file_data,
                    file_path="",  # Deprecated parameter, kept for compatibility
                    user_id=user['user_id'],
                    job_id=job_id,
                    filename=filename,
                    content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    max_file_size_mb=10  # Set reasonable limit
                )
                # Log successful upload response
                logger.info(f"Upload succeeded! Response: {upload_result}")
                
                # Handle both public and signed URLs
                url_key = 'public_url' if 'public_url' in upload_result else 'signed_url'
                storage_urls.append({
                    'file_id': file_id,
                    'filename': filename,
                    'storage_path': upload_result['storage_path'],
                    'url': upload_result[url_key],  # Use generic 'url' key
                    'url_type': upload_result.get('url_type', 'public'),
                    'size_mb': upload_result['size_mb']
                })
                logger.info(f"Successfully uploaded file {file_id} to Supabase Storage at path: {upload_result['storage_path']} ({upload_result['size_mb']:.2f}MB)")
            except Exception as upload_error:
                logger.error(f"Failed to upload file {file_id} to storage: {upload_error}")
                # Continue with other files even if one fails

        # Create job in Supabase with completed status and storage URLs
        primary_url = storage_urls[0]['url'] if storage_urls else None
        await supabase_service.create_job(
            job_id=job_id,
            user_id=user['user_id'],
            filename=f"batch_{job_data.get('total_images', 0)}_images",
            status="completed",  # Set status to completed since this is saving a completed job
            result_url=primary_url,  # Set the primary result URL
            metadata={
                'total_images': job_data.get('total_images', 0),
                'processed_images': job_data.get('processed_images', 0),
                'consolidation_strategy': job_data.get('consolidation_strategy', 'separate'),
                'output_format': job_data.get('output_format', 'xlsx'),
                'session_id': job_data.get('session_id'),
                'results': results,
                'storage_files': storage_urls,  # Add storage URLs to metadata
                'completed_at': job_data.get('updated_at'),
                'processing_time_seconds': job_data.get('processing_time_seconds')
            }
        )
        
        # Note: No need to update job status separately since we already created it with 
        # status='completed' and result_url set. The update_job_status call is removed
        # to avoid redundant database operations.

        logger.info(f"Job {job_id} saved to history with {len(storage_urls)} files uploaded to storage")

        return {
            "success": True,
            "message": "Job saved to history successfully",
            "job_id": job_id,
            "files_uploaded": len(storage_urls)
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to save job {job_id} to history: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to save job to history: {str(e)}"
        )