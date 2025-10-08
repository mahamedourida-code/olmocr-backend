from fastapi import APIRouter, Depends, HTTPException, status, Request
from fastapi.responses import JSONResponse
from typing import Dict, Any
import os
import uuid
import logging
import traceback
import threading
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
    MAX_BATCH_SIZE = 20  # Default from config
    
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
        
        # Check if Celery workers are available (only use Celery if explicitly enabled)
        use_celery = os.getenv('CELERY_ENABLED', 'false').lower() == 'true'

        if use_celery:
            # Submit Celery task for batch processing
            try:
                task_result = process_batch_images.apply_async(
                    args=[job_id, job_data['images']],
                    queue='batch_processing',
                    priority=5
                )

                # Store Celery task ID in job data
                update_success = await redis_service.update_job(job_id, {
                    'celery_task_id': task_result.id,
                    'status': 'processing'
                })

                logger.info(f"Submitted Celery task {task_result.id} for job {job_id}")

            except Exception as celery_error:
                logger.warning(f"Celery task submission failed: {celery_error}, using direct processing")
                use_celery = False  # Fall back to direct processing

        if not use_celery:
            logger.info(f"Celery workers not available, using direct processing for job {job_id}")

            # Fallback: Process directly without Celery
            from app.tasks.batch_tasks import process_batch_images_direct

            # Update job status to processing
            update_success = await redis_service.update_job(job_id, {
                'status': 'processing'
            })

            # Create wrapper function to catch and log thread errors
            def thread_wrapper():
                try:
                    logger.info(f"[Thread] Background thread started for job {job_id}")
                    process_batch_images_direct(job_id, job_data['images'])
                    logger.info(f"[Thread] Background thread completed for job {job_id}")
                except Exception as thread_error:
                    logger.error(f"[Thread] Background thread crashed for job {job_id}: {thread_error}")
                    logger.error(f"[Thread] Traceback: {traceback.format_exc()}")

                    # Try to update job status to failed
                    try:
                        import asyncio
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        loop.run_until_complete(redis_service.update_job(job_id, {
                            'status': 'failed',
                            'error': f"Background processing crashed: {str(thread_error)}",
                            'updated_at': datetime.utcnow().isoformat()
                        }))
                        loop.close()
                    except Exception as update_error:
                        logger.error(f"[Thread] Failed to update job status after crash: {update_error}")

            # Process in background thread to avoid blocking the API
            processing_thread = threading.Thread(target=thread_wrapper, name=f"batch-{job_id[:8]}")
            processing_thread.daemon = False  # Don't suppress errors
            processing_thread.start()
            logger.info(f"Started background thread {processing_thread.name} for job {job_id}")

            update_success = True
        
        if not update_success:
            logger.warning(f"Failed to update job {job_id} with Celery task ID")
        
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
):
    """
    Save a completed job to the user's permanent history.
    
    This endpoint allows users to explicitly save their job results to their history.
    Only completed jobs can be saved, and only by the job owner.
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
        results = job_data.get('results', [])
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

        # Create/update job in Supabase
        await supabase_service.create_job(
            job_id=job_id,
            user_id=user['user_id'],
            filename=f"batch_{job_data.get('total_images', 0)}_images",
            metadata={
                'total_images': job_data.get('total_images', 0),
                'processed_images': job_data.get('processed_images', 0),
                'consolidation_strategy': job_data.get('consolidation_strategy', 'separate'),
                'output_format': job_data.get('output_format', 'xlsx'),
                'session_id': job_data.get('session_id'),
                'results': results,
                'completed_at': job_data.get('updated_at'),
                'processing_time_seconds': job_data.get('processing_time_seconds')
            }
        )

        logger.info(f"Job {job_id} saved to history for user {user['user_id']}")

        return {
            "success": True,
            "message": "Job saved to history successfully",
            "job_id": job_id
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to save job {job_id} to history: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to save job to history: {str(e)}"
        )