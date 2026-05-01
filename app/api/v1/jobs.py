from fastapi import APIRouter, Depends, HTTPException, status, Request, File, UploadFile, Form
from fastapi.responses import JSONResponse
from typing import Dict, Any, List
import os
import uuid
import logging
import traceback
import threading
import base64
import json
from datetime import datetime, timedelta

from app.models.requests import BatchConvertRequest
from app.models.responses import BatchConvertResponse, JobStatusResponse, ErrorResponse
from app.models.jobs import JobStatus, BatchProcessingContext, ImageProcessingResult
from app.core.dependencies import (
    get_or_create_session,
    get_optional_user, get_current_user,
    enforce_upload_rate_limits, verify_job_data_access
)
from app.core.config import settings
from app.core.limits import get_plan_limits, get_user_plan_type
from app.services.redis_service import get_redis_service, RedisService
from app.services.supabase_service import get_supabase_service
from app.tasks.batch_tasks import process_batch_from_storage
from app.utils.exceptions import ProcessingError, ValidationError
from app.models.jobs import SessionMetadata
from typing import Optional

logger = logging.getLogger(__name__)


def simple_batch_validation(image_count: int) -> None:
    """Simple batch validation without dependencies."""
    if image_count == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "code": "NO_IMAGES",
                "message": "No images provided for batch processing"
            }
        )

    if image_count > settings.max_batch_size:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "code": "ABSOLUTE_BATCH_LIMIT_EXCEEDED",
                "message": f"Batch size ({image_count}) exceeds maximum allowed ({settings.max_batch_size})",
                "max_files_per_batch": settings.max_batch_size,
                "received_files": image_count,
            }
        )

router = APIRouter(prefix="/jobs", tags=["Batch Jobs"])


def resolve_upload_limits(user: Optional[dict], supabase_service) -> Dict[str, Any]:
    plan_type = get_user_plan_type(user, supabase_service)
    return get_plan_limits(plan_type)


def enforce_batch_limit(image_count: int, limits: Dict[str, Any]) -> None:
    max_files = int(limits["max_files_per_batch"])
    if image_count > max_files:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "code": "PLAN_BATCH_LIMIT_EXCEEDED",
                "message": f"Your current plan allows up to {max_files} images per batch.",
                "plan": limits["plan"],
                "max_files_per_batch": max_files,
                "received_files": image_count,
            }
        )


def _parse_metadata(metadata: Any) -> Dict[str, Any]:
    if isinstance(metadata, dict):
        return metadata
    if isinstance(metadata, str) and metadata:
        try:
            parsed = json.loads(metadata)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


ACTIVE_JOB_STATUSES = {"queued", "processing"}


def _job_record_summary(record: Dict[str, Any]) -> Dict[str, Any]:
    metadata = _parse_metadata(record.get("processing_metadata"))
    status_value = record.get("status", "unknown")
    total_images = int(metadata.get("total_images") or 0)
    processed_images = int(
        metadata.get("processed_images")
        or metadata.get("successful_images")
        or 0
    )
    progress = metadata.get("progress")
    if progress is None:
        progress = 100 if status_value in {"completed", "partially_completed", "failed"} else 0

    return {
        "job_id": record.get("id"),
        "status": status_value,
        "session_id": metadata.get("owner_session_id") or metadata.get("session_id") or "",
        "created_at": record.get("created_at"),
        "updated_at": record.get("updated_at"),
        "total_images": total_images,
        "processed_images": processed_images,
        "percentage": progress,
        "active": status_value in ACTIVE_JOB_STATUSES,
    }


@router.get("/credits")
async def get_user_credits(
    user: dict = Depends(get_current_user)
):
    """Get user's credit information."""
    try:
        supabase_service = get_supabase_service()
        credits = supabase_service.get_user_credits(user['user_id'])
        return credits
    except Exception as e:
        logger.error(f"Failed to get user credits: {e}")
        return {
            'total_credits': 80,
            'used_credits': 0,
            'available_credits': 80
        }


@router.get("/recover/latest", response_model=Dict[str, Any])
async def get_latest_recoverable_job(
    session: SessionMetadata = Depends(get_or_create_session),
    user: Optional[dict] = Depends(get_optional_user),
    limit: int = 10
):
    """
    Return the latest active/recent job for reload recovery.

    Authenticated users are recovered by user id. Anonymous users are recovered
    by the same session id used for uploads and downloads.
    """
    supabase_service = get_supabase_service()
    owner_id = user["user_id"] if user else f"session:{session.session_id}"
    records = await supabase_service.get_user_jobs(owner_id, limit=max(1, min(limit, 25)))
    summaries = [_job_record_summary(record) for record in records]
    summaries = [job for job in summaries if job.get("job_id")]

    active_job = next((job for job in summaries if job["status"] in ACTIVE_JOB_STATUSES), None)
    latest_job = active_job or (summaries[0] if summaries else None)

    return {
        "job": latest_job,
        "active": bool(latest_job and latest_job["status"] in ACTIVE_JOB_STATUSES),
        "jobs": summaries,
    }


@router.post("/batch", response_model=BatchConvertResponse)
async def create_batch_job(
    request: BatchConvertRequest,
    session: SessionMetadata = Depends(get_or_create_session),
    redis_service: RedisService = Depends(get_redis_service),
    user: Optional[dict] = Depends(get_optional_user),
    http_request: Request = None
):
    """
    Create a new batch processing job for multiple images.
    
    This endpoint accepts multiple base64-encoded images and starts a distributed
    Celery job to process them concurrently and generate consolidated XLSX output.
    """
    # Validate batch request
    simple_batch_validation(len(request.images))
    supabase_service = get_supabase_service()
    limits = resolve_upload_limits(user, supabase_service)
    enforce_batch_limit(len(request.images), limits)

    await enforce_upload_rate_limits(
        request=http_request,
        redis_service=redis_service,
        user=user,
        session_id=session.session_id,
        image_count=len(request.images),
        daily_image_limit_override=limits["daily_image_limit"]
    )
    
    credits_reserved = False

    # Check credits if user is authenticated
    if user:
        logger.info(f"[Credits] Checking credits for user {user['user_id']}, need {len(request.images)} credits")
        
        try:
            # Check if user has enough credits
            credits_result = supabase_service.check_and_use_credits(
                user['user_id'], 
                len(request.images)
            )
            logger.info(f"[Credits] Credit check result: {credits_result}")
            
            if not credits_result:
                logger.warning(f"[Credits] Insufficient credits for user {user['user_id']}")
                raise HTTPException(
                    status_code=status.HTTP_402_PAYMENT_REQUIRED,
                    detail={
                        "code": "INSUFFICIENT_CREDITS",
                        "message": f"Insufficient credits. You need {len(request.images)} credits but don't have enough remaining.",
                        "credits_required": len(request.images)
                    }
                )
            else:
                credits_reserved = True
                logger.info(f"[Credits] Successfully deducted {len(request.images)} credits for user {user['user_id']}")
                
        except HTTPException:
            # Re-raise HTTP exceptions
            raise
        except Exception as e:
            logger.error(f"[Credits] Critical error checking credits: {e}", exc_info=True)
            # DO NOT allow processing on credit check failure - this is money!
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Credit system error. Please try again or contact support."
            )
    
    # Generate job ID
    job_id = str(uuid.uuid4())
    
    try:
        storage_owner_id = user['user_id'] if user else session.session_id
        stored_images = []

        for i, img in enumerate(request.images):
            image_data = img.image.split(',', 1)[1] if img.image.startswith('data:') else img.image
            image_bytes = base64.b64decode(image_data)
            filename = img.filename or f"image_{i}.png"
            source_file = await supabase_service.upload_source_file(
                file_data=image_bytes,
                owner_id=storage_owner_id,
                job_id=job_id,
                filename=f"{i}_{filename}",
                content_type="image/png"
            )
            stored_images.append({
                'id': f"img_{i}",
                'storage_path': source_file['storage_path'],
                'filename': filename,
                'content_type': source_file['content_type'],
                'size_bytes': source_file['size_bytes']
            })

        # Create job metadata for Redis with storage paths, not raw image payloads.
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
            'images': stored_images,
            'results': [],
            'errors': [],
            'plan': limits["plan"],
            'max_files_per_batch': limits["max_files_per_batch"],
            'credits_reserved': len(request.images) if user else 0,
            'credits_settled': False if user else True,
            'created_at': datetime.utcnow().isoformat(),
            'updated_at': datetime.utcnow().isoformat()
        }

        durable_owner_id = user['user_id'] if user else f"session:{session.session_id}"
        await supabase_service.create_job(
            job_id=job_id,
            user_id=durable_owner_id,
            filename=f"batch_{len(request.images)}_images",
            metadata={
                'total_images': len(request.images),
                'consolidation_strategy': request.consolidation_strategy,
                'output_format': request.output_format,
                'session_id': session.session_id,
                'owner_user_id': user['user_id'] if user else None,
                'owner_session_id': None if user else session.session_id,
                'plan': limits["plan"],
                'max_files_per_batch': limits["max_files_per_batch"],
                'credits_reserved': len(request.images) if user else 0,
                'credits_settled': False if user else True
            },
            status='queued'
        )
        logger.info(f"Created durable Supabase job {job_id} for owner {durable_owner_id}")
        
        # Store job in Redis
        create_success = await redis_service.create_job(job_id, job_data)
        
        if not create_success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to store job in Redis"
            )
        
        async_result = process_batch_from_storage.apply_async(
            args=[job_id, session.session_id, stored_images, user['user_id'] if user else None],
            queue='batch_processing',
            priority=6
        )
        await redis_service.update_job(job_id, {'celery_task_id': async_result.id})
        try:
            await supabase_service.update_job_status(
                job_id=job_id,
                status='queued',
                metadata={
                    'total_images': len(request.images),
                    'consolidation_strategy': request.consolidation_strategy,
                    'output_format': request.output_format,
                    'session_id': session.session_id,
                    'owner_user_id': user['user_id'] if user else None,
                    'owner_session_id': None if user else session.session_id,
                    'celery_task_id': async_result.id,
                    'plan': limits["plan"],
                    'max_files_per_batch': limits["max_files_per_batch"],
                    'credits_reserved': len(request.images) if user else 0,
                    'credits_settled': False if user else True
                }
            )
        except Exception as e:
            logger.debug(f"Failed to store Celery task id for job {job_id}: {e}")

        logger.info(f"Queued Celery processing for job {job_id} with {len(stored_images)} images")
        
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

        if user and credits_reserved:
            supabase_service.refund_credits(user['user_id'], len(request.images))
        
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

    # Validate batch request
    simple_batch_validation(len(files))
    supabase_service = get_supabase_service()
    limits = resolve_upload_limits(user, supabase_service)
    enforce_batch_limit(len(files), limits)

    await enforce_upload_rate_limits(
        request=http_request,
        redis_service=redis_service,
        user=user,
        session_id=session.session_id,
        image_count=len(files),
        daily_image_limit_override=limits["daily_image_limit"]
    )

    credits_reserved = False
    if user:
        logger.info(f"[Credits] Checking credits for user {user['user_id']}, need {len(files)} credits")
        try:
            credits_result = supabase_service.check_and_use_credits(
                user['user_id'],
                len(files)
            )

            if not credits_result:
                logger.warning(f"[Credits] Insufficient credits for user {user['user_id']}")
                raise HTTPException(
                    status_code=status.HTTP_402_PAYMENT_REQUIRED,
                    detail={
                        "code": "INSUFFICIENT_CREDITS",
                        "message": f"Insufficient credits. You need {len(files)} credits but don't have enough remaining.",
                        "credits_required": len(files)
                    }
                )

            credits_reserved = True
            logger.info(f"[Credits] Reserved {len(files)} credits for user {user['user_id']}")
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"[Credits] Critical error checking credits: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Credit system error. Please try again or contact support."
            )

    # Validate each file
    SUPPORTED_TYPES = {"image/png", "image/jpeg", "image/jpg", "image/webp", "image/heic", "image/heif"}
    MAX_FILE_SIZE = settings.max_file_size_bytes

    for i, file in enumerate(files):
        logger.info(f"[BATCH-UPLOAD] Validating file {i+1}: {file.filename}")

        # Check content type or file extension
        # HEIC files might not have proper MIME type, so check extension too
        file_ext = file.filename.lower().split('.')[-1] if file.filename else ""
        is_heic_by_extension = file_ext in ['heic', 'heif']
        
        # Accept if content type is valid OR if it's a HEIC/HEIF by extension
        if file.content_type not in SUPPORTED_TYPES and not is_heic_by_extension:
            error_msg = f"File {i+1} '{file.filename}': Unsupported file type '{file.content_type}'. Supported types: PNG, JPEG, WebP, HEIC, HEIF"
            logger.error(f"[BATCH-UPLOAD] Validation failed - {error_msg}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=error_msg
            )
        
        # Log if we're accepting based on extension
        if is_heic_by_extension and file.content_type not in SUPPORTED_TYPES:
            logger.info(f"[BATCH-UPLOAD] File {i+1} accepted as HEIC/HEIF based on extension (content_type={file.content_type})")

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
        storage_owner_id = user['user_id'] if user else session.session_id
        stored_images = []

        for i, file in enumerate(files):
            # Read binary data (we already validated it above)
            file_content = await file.read()

            source_file = await supabase_service.upload_source_file(
                file_data=file_content,
                owner_id=storage_owner_id,
                job_id=job_id,
                filename=f"{i}_{file.filename or f'image_{i}.png'}",
                content_type=file.content_type or "application/octet-stream"
            )

            stored_images.append({
                'id': f"img_{i}",
                'storage_path': source_file['storage_path'],
                'filename': file.filename or f"image_{i}.png",
                'content_type': source_file['content_type'],
                'size_bytes': source_file['size_bytes']
            })

            # Reset file pointer (in case we need to read again)
            await file.seek(0)

        # Create job metadata for Redis with storage paths, not raw image payloads.
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
            'images': stored_images,
            'results': [],
            'errors': [],
            'plan': limits["plan"],
            'max_files_per_batch': limits["max_files_per_batch"],
            'credits_reserved': len(files) if user else 0,
            'credits_settled': False if user else True,
            'created_at': datetime.utcnow().isoformat(),
            'updated_at': datetime.utcnow().isoformat()
        }

        durable_owner_id = user['user_id'] if user else f"session:{session.session_id}"
        await supabase_service.create_job(
            job_id=job_id,
            user_id=durable_owner_id,
            filename=f"batch_{len(files)}_images",
            metadata={
                'total_images': len(files),
                'consolidation_strategy': consolidation_strategy,
                'output_format': output_format,
                'session_id': session.session_id,
                'owner_user_id': user['user_id'] if user else None,
                'owner_session_id': None if user else session.session_id,
                'plan': limits["plan"],
                'max_files_per_batch': limits["max_files_per_batch"],
                'credits_reserved': len(files) if user else 0,
                'credits_settled': False if user else True
            },
            status='queued'
        )
        logger.info(f"Created durable Supabase job {job_id} for owner {durable_owner_id}")

        # Store job in Redis
        create_success = await redis_service.create_job(job_id, job_data)

        if not create_success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to store job in Redis"
            )

        async_result = process_batch_from_storage.apply_async(
            args=[job_id, session.session_id, stored_images, user['user_id'] if user else None],
            queue='batch_processing',
            priority=6
        )
        await redis_service.update_job(job_id, {'celery_task_id': async_result.id})
        try:
            await supabase_service.update_job_status(
                job_id=job_id,
                status='queued',
                metadata={
                    'total_images': len(files),
                    'consolidation_strategy': consolidation_strategy,
                    'output_format': output_format,
                    'session_id': session.session_id,
                    'owner_user_id': user['user_id'] if user else None,
                    'owner_session_id': None if user else session.session_id,
                    'celery_task_id': async_result.id,
                    'plan': limits["plan"],
                    'max_files_per_batch': limits["max_files_per_batch"],
                    'credits_reserved': len(files) if user else 0,
                    'credits_settled': False if user else True
                }
            )
        except Exception as e:
            logger.debug(f"Failed to store Celery task id for job {job_id}: {e}")

        logger.info(f"Queued Celery processing for job {job_id} with {len(stored_images)} images")

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

        if user and credits_reserved:
            supabase_service.refund_credits(user['user_id'], len(files))

        # Return detailed error information
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create batch job: {str(e)}"
        )


@router.get("/{job_id}/status", response_model=JobStatusResponse)
async def get_job_status(
    job_id: str,
    session: SessionMetadata = Depends(get_or_create_session),
    redis_service: RedisService = Depends(get_redis_service),
    user: Optional[dict] = Depends(get_optional_user)
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
    
    # Get job data from Redis first. Redis is the fast cache, not the
    # authorization source; if Redis misses, rebuild from Supabase metadata.
    job_data = await redis_service.get_job(job_id)
    
    if not job_data:
        supabase_service = get_supabase_service()
        durable_files = await supabase_service.get_job_files_for_job(job_id)
        supabase_job = await supabase_service.get_job(job_id)

        if supabase_job:
            metadata = _parse_metadata(supabase_job.get("processing_metadata"))
            stored_user_id = str(supabase_job.get("user_id") or "")
            owner_user_id = metadata.get("owner_user_id") or (stored_user_id if not stored_user_id.startswith("session:") else "")
            owner_session_id = metadata.get("owner_session_id") or metadata.get("session_id", "")
            generated_files = durable_files or metadata.get("generated_files", [])
            total_images = metadata.get("total_images") or max(len(generated_files), 1)
            successful_images = metadata.get("successful_images") or len(generated_files)
            failed_images = metadata.get("failed_images") or 0
            processed_images = metadata.get("processed_images") or successful_images + failed_images
            progress = metadata.get("progress")
            if progress is None:
                progress = 100 if supabase_job.get("status") in ("completed", "failed") else 0

            job_data = {
                "job_id": job_id,
                "user_id": owner_user_id or "",
                "session_id": owner_session_id or "",
                "status": supabase_job.get("status", "unknown"),
                "total_images": total_images,
                "processed_images": processed_images,
                "failed_images": failed_images,
                "progress": progress,
                "generated_files": generated_files,
                "download_urls": [f"/api/v1/download/{file_info['file_id']}" for file_info in generated_files if file_info.get("file_id")],
                "image_results": metadata.get("image_results", {}),
                "processing_time": metadata.get("processing_time") or metadata.get("processing_time_seconds", 0),
                "created_at": supabase_job.get("created_at") or datetime.utcnow().isoformat(),
                "updated_at": supabase_job.get("updated_at") or datetime.utcnow().isoformat(),
                "completed_at": metadata.get("completed_at")
            }
            verify_job_data_access(job_data, user, session.session_id)
        elif durable_files:
            first_file = durable_files[0]
            job_data = {
                "job_id": job_id,
                "user_id": first_file.get("user_id", ""),
                "session_id": first_file.get("session_id", ""),
                "status": "completed",
                "total_images": len(durable_files),
                "processed_images": len(durable_files),
                "progress": 100,
                "generated_files": durable_files,
                "download_urls": [f"/api/v1/download/{file_info['file_id']}" for file_info in durable_files if file_info.get("file_id")],
                "errors": [],
                "created_at": first_file.get("created_at") or datetime.utcnow().isoformat(),
                "updated_at": first_file.get("updated_at") or datetime.utcnow().isoformat(),
                "completed_at": first_file.get("completed_at") or first_file.get("created_at")
            }
            verify_job_data_access(job_data, user, session.session_id)
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Job not found"
            )
    else:
        verify_job_data_access(job_data, user, session.session_id)
    
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
                    completed_time = datetime.fromisoformat(str(job_data['completed_at']).replace('Z', '+00:00'))
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
                    created_at_value = (
                        file_info.get('completed_at')
                        or file_info.get('created_at')
                        or job_data.get('completed_at')
                        or datetime.utcnow().isoformat()
                    )
                    try:
                        file_created_at = datetime.fromisoformat(str(created_at_value).replace('Z', '+00:00'))
                    except ValueError:
                        file_created_at = datetime.utcnow()

                    files.append(ProcessedFile(
                        file_id=file_info['file_id'],
                        download_url=f"/api/v1/download/{file_info['file_id']}",
                        filename=file_info['filename'],
                        original_image=file_info.get('original_filename', file_info.get('image_id', 'unknown')),
                        size_bytes=file_info.get('size_bytes'),
                        created_at=file_created_at
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
                    completed_at = datetime.fromisoformat(str(job_data['completed_at']).replace('Z', '+00:00'))
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
    redis_service: RedisService = Depends(get_redis_service),
    user: Optional[dict] = Depends(get_optional_user)
):
    """
    Cancel a running batch job.
    
    Cancels the Celery task and marks the job as cancelled.
    """
    job_data = await redis_service.get_job(job_id)
    
    if not job_data:
        supabase_service = get_supabase_service()
        supabase_job = await supabase_service.get_job(job_id)
        durable_files = await supabase_service.get_job_files_for_job(job_id)

        if supabase_job:
            metadata = _parse_metadata(supabase_job.get("processing_metadata"))
            stored_user_id = str(supabase_job.get("user_id") or "")
            owner_user_id = metadata.get("owner_user_id") or (stored_user_id if not stored_user_id.startswith("session:") else "")
            owner_session_id = metadata.get("owner_session_id") or metadata.get("session_id", "")
            job_data = {
                "job_id": job_id,
                "user_id": owner_user_id or "",
                "session_id": owner_session_id or "",
                "status": supabase_job.get("status", "unknown"),
                "celery_task_id": metadata.get("celery_task_id"),
                "created_at": supabase_job.get("created_at") or datetime.utcnow().isoformat(),
                "updated_at": supabase_job.get("updated_at") or datetime.utcnow().isoformat(),
            }
        elif durable_files:
            first_file = durable_files[0]
            job_data = {
                "job_id": job_id,
                "user_id": first_file.get("user_id", ""),
                "session_id": first_file.get("session_id", ""),
                "status": "completed",
                "created_at": first_file.get("created_at") or datetime.utcnow().isoformat(),
                "updated_at": first_file.get("updated_at") or datetime.utcnow().isoformat(),
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Job not found"
            )

    verify_job_data_access(job_data, user, session.session_id)
    
    if job_data.get('status') in ["completed", "partially_completed", "failed"]:
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

    if user:
        try:
            supabase_service = get_supabase_service()
            supabase_job = await supabase_service.get_job(job_id) or {}
            metadata = _parse_metadata(supabase_job.get("processing_metadata"))
            credit_metadata = {}
            if not metadata.get("credits_settled"):
                reserved_credits = int(metadata.get("credits_reserved") or job_data.get("total_images") or 0)
                completed_files = await supabase_service.get_job_files_for_job(job_id)
                charged_credits = min(len(completed_files), reserved_credits)
                refund_credits = max(0, reserved_credits - charged_credits)
                if refund_credits:
                    supabase_service.refund_credits(user["user_id"], refund_credits)
                credit_metadata = {
                    "credits_reserved": reserved_credits,
                    "credits_charged": charged_credits,
                    "credits_refunded": refund_credits,
                    "credits_settled": True
                }
            metadata["cancelled_at"] = datetime.utcnow().isoformat()
            metadata["cancelled_by"] = user["user_id"]
            await supabase_service.update_job_status(
                job_id=job_id,
                status="failed",
                error_message="Job cancelled by user",
                metadata={**metadata, **credit_metadata}
            )
        except Exception as e:
            logger.debug(f"Failed to update Supabase cancellation for job {job_id}: {e}")
    
    return {"message": "Job cancelled successfully"}


@router.get("/", response_model=Dict[str, Any])
async def list_jobs(
    session: SessionMetadata = Depends(get_or_create_session),
    user: Optional[dict] = Depends(get_optional_user),
    limit: int = 50,
    offset: int = 0
):
    """
    List all batch jobs for the current session.

    Returns a paginated list of jobs with their current status.
    """
    supabase_service = get_supabase_service()

    if user:
        records = await supabase_service.get_user_jobs(user["user_id"], limit=limit + offset)
        jobs = []
        for record in records:
            jobs.append(_job_record_summary(record))
    else:
        records = await supabase_service.get_user_jobs(f"session:{session.session_id}", limit=limit + offset)
        jobs = [_job_record_summary(record) for record in records]

        files = await supabase_service.get_job_files_for_session(session.session_id, limit=limit + offset)
        grouped: Dict[str, Dict[str, Any]] = {}
        for file_info in files:
            job_id = file_info.get("job_id")
            if not job_id:
                continue
            if any(job.get("job_id") == job_id for job in jobs):
                continue
            current = grouped.setdefault(job_id, {
                "job_id": job_id,
                "status": "completed",
                "created_at": file_info.get("created_at"),
                "total_images": 0,
                "processed_images": 0,
                "percentage": 100
            })
            current["total_images"] += 1
            current["processed_images"] += 1
            if file_info.get("created_at") and (not current.get("created_at") or file_info["created_at"] < current["created_at"]):
                current["created_at"] = file_info["created_at"]
        jobs = jobs + list(grouped.values())

    jobs.sort(key=lambda x: x.get("created_at") or "", reverse=True)

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


@router.get("/saved-history")
async def get_saved_history(
    user: dict = Depends(get_current_user),
    limit: int = 50,
    offset: int = 0
):
    """
    Get saved job history for the authenticated user.
    
    Returns a paginated list of explicitly saved completed jobs from job_history table.
    Only shows jobs that were saved by the user, not all processing jobs.
    """
    try:
        supabase_service = get_supabase_service()

        # Get user's saved jobs from job_history table
        result = await supabase_service.get_user_saved_history(user['user_id'], limit=limit, offset=offset)

        # Format response with download URLs
        formatted_jobs = []
        for job in result.get("jobs", []):
            formatted_job = {
                "job_id": job.get("original_job_id", job.get("id")),
                "filename": job.get("filename", "Unknown"),
                "status": job.get("status", "completed"),
                "result_url": job.get("result_url"),
                "created_at": job.get("created_at"),
                "updated_at": job.get("updated_at"),
                "saved_at": job.get("saved_at"),
                "metadata": job.get("processing_metadata", {})
            }
            formatted_jobs.append(formatted_job)

        return {
            "jobs": formatted_jobs,
            "total": result.get("total", 0),
            "limit": limit,
            "offset": offset,
            "has_more": result.get("has_more", False)
        }

    except Exception as e:
        logger.error(f"Failed to get saved job history: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve saved job history: {str(e)}"
        )


@router.post("/{job_id}/save")
async def save_job_to_history(
    job_id: str,
    user: dict = Depends(get_current_user),
    session: SessionMetadata = Depends(get_or_create_session),
    redis_service: RedisService = Depends(get_redis_service),
):
    """
    Save a completed job to the user's permanent history.
    
    This endpoint allows users to explicitly save their job results to their history.
    Only completed jobs can be saved, and only by the job owner.
    Files are saved from existing durable Supabase Storage metadata.
    """
    try:
        supabase_service = get_supabase_service()

        # Get job from Redis first, then rebuild from Supabase if Redis expired.
        job_data = await redis_service.get_job(job_id)
        if not job_data:
            supabase_job = await supabase_service.get_job(job_id)
            if not supabase_job:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Job not found"
                )
            metadata = _parse_metadata(supabase_job.get("processing_metadata"))
            stored_user_id = str(supabase_job.get("user_id") or "")
            owner_user_id = metadata.get("owner_user_id") or (stored_user_id if not stored_user_id.startswith("session:") else "")
            owner_session_id = metadata.get("owner_session_id") or metadata.get("session_id", "")
            job_data = {
                "job_id": job_id,
                "user_id": owner_user_id or "",
                "session_id": owner_session_id or "",
                "status": supabase_job.get("status", "unknown"),
                "generated_files": metadata.get("generated_files", []),
                "consolidation_strategy": metadata.get("consolidation_strategy", "separate"),
                "output_format": metadata.get("output_format", "xlsx"),
                "updated_at": supabase_job.get("updated_at"),
                "processing_time_seconds": metadata.get("processing_time") or metadata.get("processing_time_seconds"),
            }

        verify_job_data_access(job_data, user, session.session_id)

        # Check if job is completed
        if job_data.get('status') not in ['completed', 'partially_completed']:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Only completed jobs can be saved to history"
            )

        results = await supabase_service.get_job_files_for_job(job_id)
        if not results:
            results = job_data.get('generated_files', [])
        if not results:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Job has no results to save"
            )

        # Check if already exists in user's saved history (in job_history table)
        # Check for the parent job ID in metadata to avoid duplicates
        existing_history = await supabase_service.get_user_saved_history(user['user_id'], limit=1000)
        job_exists = any(
            job.get('original_job_id', '').startswith(f"{job_id}_") or 
            job.get('processing_metadata', {}).get('parent_job_id') == job_id 
            for job in existing_history.get("jobs", [])
        )
        
        if job_exists:
            return {
                "success": True,
                "message": "Job already saved to history",
                "job_id": job_id
            }

        # Use existing durable Supabase Storage files; do not depend on Fly local disk.
        storage_urls = []
        logger.info(f"Preparing durable history records for job {job_id}. Found {len(results)} files")
        
        for file_info in results:
            file_id = file_info.get('file_id')
            if not file_id:
                logger.warning(f"File info missing file_id: {file_info}")
                continue

            storage_path = file_info.get('storage_path')
            if not storage_path:
                logger.warning(f"File {file_id} missing storage_path, skipping history save")
                continue

            filename = file_info.get('filename', f'{file_id}.xlsx')

            try:
                signed_url = await supabase_service.create_signed_url(
                    storage_path,
                    expires_in=7 * 24 * 3600
                )
                storage_urls.append({
                    'file_id': file_id,
                    'filename': filename,
                    'storage_path': storage_path,
                    'url': signed_url,
                    'url_type': 'signed_url',
                    'size_mb': (file_info.get('size_bytes') or 0) / (1024 * 1024),
                    'size_bytes': file_info.get('size_bytes'),
                    'download_url': f"/api/v1/download/{file_id}"
                })
            except Exception as url_error:
                logger.error(f"Failed to create signed URL for file {file_id}: {url_error}")
                # Continue with other files even if one fails

        if not storage_urls:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No durable result files were available to save"
            )

        # Save EACH FILE as a separate entry in job_history table
        saved_count = 0
        
        # Create a separate history entry for each file
        for idx, storage_file in enumerate(storage_urls):
            try:
                # Get the original filename for this specific file
                original_filename = storage_file.get('filename', f'file_{idx + 1}.xlsx')
                
                # Create individual entry in job_history table for each file
                await supabase_service.save_to_job_history(
                    user_id=user['user_id'],
                    original_job_id=f"{job_id}_{idx}",  # Make unique ID for each file
                    filename=original_filename,  # Use individual filename
                    status="completed",
                    result_url=storage_file['url'],  # Use this file's specific URL
                    metadata={
                        'parent_job_id': job_id,  # Keep reference to parent batch job
                        'file_index': idx,
                        'total_images': 1,  # Each file represents 1 processed image
                        'total_files': len(storage_urls),
                        # Include storage_files array that frontend expects
                        'storage_files': [storage_file],  # Single file in array format
                        'storage_path': storage_file['storage_path'],
                        'file_id': storage_file['file_id'],
                        'size_mb': storage_file['size_mb'],
                        'consolidation_strategy': job_data.get('consolidation_strategy', 'separate'),
                        'output_format': job_data.get('output_format', 'xlsx'),
                        'session_id': job_data.get('session_id'),
                        'completed_at': job_data.get('updated_at'),
                        'processing_time_seconds': job_data.get('processing_time_seconds')
                    }
                )
                saved_count += 1
                logger.info(f"Saved file {original_filename} to job_history as separate entry")
            except Exception as e:
                logger.error(f"Failed to save file {idx} to history: {e}")
                # Continue saving other files even if one fails

        logger.info(f"Job {job_id} saved to job_history table with {saved_count} separate file entries")

        return {
            "success": True,
            "message": f"Saved {saved_count} files to history successfully",
            "job_id": job_id,
            "files_uploaded": saved_count
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to save job {job_id} to history: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to save job to history: {str(e)}"
        )


@router.delete("/saved-history/all")
async def delete_all_from_history(
    user: dict = Depends(get_current_user)
):
    """
    Delete all jobs from user's saved history.
    
    This is a destructive operation that removes all saved job history for the user.
    """
    try:
        supabase_service = get_supabase_service()
        
        # Delete all from job_history table for this user
        deleted_count = await supabase_service.delete_all_from_job_history(user['user_id'])
        
        return {
            "success": True, 
            "message": f"Deleted {deleted_count} jobs from history",
            "deleted_count": deleted_count
        }
    except Exception as e:
        logger.error(f"Failed to delete all jobs from history for user {user['user_id']}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete jobs from history: {str(e)}"
        )


@router.delete("/saved-history/{job_id}")
async def delete_from_history(
    job_id: str,
    user: dict = Depends(get_current_user)
):
    """
    Delete a specific job from saved history.
    
    Only the owner of the job can delete it.
    """
    try:
        supabase_service = get_supabase_service()
        
        # Delete from job_history table
        success = await supabase_service.delete_from_job_history(
            user_id=user['user_id'],
            original_job_id=job_id
        )
        
        if success:
            return {"success": True, "message": "Job deleted from history"}
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Job not found in history"
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete job {job_id} from history: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete job from history: {str(e)}"
        )
