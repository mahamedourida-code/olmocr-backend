from fastapi import APIRouter, Depends, HTTPException, status, Request, File, UploadFile, Form
from fastapi.responses import JSONResponse, StreamingResponse
from celery import chord
from typing import Dict, Any, List, Tuple
import os
import uuid
import logging
import traceback
import threading
import base64
import hashlib
import json
import io
import zipfile
from datetime import datetime, timedelta
from urllib.parse import quote

from app.models.requests import (
    BatchConvertRequest,
    DocumentModeOverrideRequest,
    DocumentDuplicateOverrideRequest,
    DocumentReviewChangeRequest,
    DocumentReviewStatusRequest,
    ReceiptQuickBooksPublishRequest,
    VendorRuleFromDocumentRequest,
)
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
from app.services.quickbooks_service import get_quickbooks_service
from app.services.excel import ExcelService
from app.tasks.batch_tasks import (
    finalize_document_override,
    process_batch_from_storage,
    process_single_stored_image,
)
from app.utils.exceptions import ProcessingError, ValidationError
from app.utils.pdf_pages import is_pdf_bytes, render_pdf_pages_to_png
from app.models.jobs import SessionMetadata
from typing import Optional

logger = logging.getLogger(__name__)


def estimate_parallel_completion(image_count: int) -> datetime:
    effective_parallelism = max(
        1,
        min(
            settings.max_concurrent_ocr_calls,
            settings.worker_concurrency,
            max(1, image_count)
        )
    )
    waves = (max(1, image_count) + effective_parallelism - 1) // effective_parallelism
    return datetime.utcnow() + timedelta(seconds=max(15, waves * 12))


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
SUPPORTED_DOCUMENT_MODES = {
    "auto",
    "table",
    "invoice",
    "receipt",
    "bank_statement",
    "notes",
    "invoice_receipt",
}


def normalize_document_mode(document_mode: str) -> str:
    """Normalize public mode names while preserving the legacy combined mode."""
    requested_mode = str(document_mode or "table").strip().lower().replace("-", "_")
    aliases = {
        "statement": "bank_statement",
        "bankstatement": "bank_statement",
        "note": "notes",
    }
    normalized_mode = aliases.get(requested_mode, requested_mode)
    return normalized_mode if normalized_mode in SUPPORTED_DOCUMENT_MODES else "table"


async def require_owned_durable_job(
    supabase_service,
    job_id: str,
    session: SessionMetadata,
    user: Optional[dict],
) -> Dict[str, Any]:
    """Load a durable job and enforce the same owner/session rule for review routes."""
    supabase_job = await supabase_service.get_job(job_id)
    if not supabase_job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    metadata = _parse_metadata(supabase_job.get("processing_metadata"))
    stored_user_id = str(supabase_job.get("user_id") or "")
    owner_user_id = metadata.get("owner_user_id") or (
        stored_user_id if not stored_user_id.startswith("session:") else ""
    )
    owner_session_id = metadata.get("owner_session_id") or metadata.get("session_id", "")
    verify_job_data_access(
        {"job_id": job_id, "user_id": owner_user_id or "", "session_id": owner_session_id or ""},
        user,
        session.session_id,
    )
    return supabase_job


def verify_durable_document_access(
    document: Dict[str, Any],
    user: Optional[dict],
    session_id: str,
) -> None:
    """Validate document ownership independently from its containing job."""
    verify_job_data_access(
        {
            "job_id": str(document.get("job_id") or ""),
            "user_id": str(document.get("owner_user_id") or ""),
            "session_id": str(document.get("owner_session_id") or ""),
        },
        user,
        session_id,
    )


async def require_owned_durable_document(
    supabase_service,
    job_id: str,
    document_id: str,
    session: SessionMetadata,
    user: Optional[dict],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Load an owned document only after validating job and document ownership."""
    job = await require_owned_durable_job(supabase_service, job_id, session, user)
    document = await supabase_service.get_job_document(job_id, document_id)
    if not document:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Document not found")
    verify_durable_document_access(document, user, session.session_id)
    return job, document


def require_stored_content_deletable(job: Dict[str, Any]) -> None:
    """Avoid deleting document records while a worker can still write results."""
    if str(job.get("status") or "").lower() in ACTIVE_JOB_STATUSES:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Cancel the active conversion before permanently deleting its stored files.",
        )


async def persist_queued_documents(
    supabase_service,
    documents: List[Dict[str, Any]],
    processing_units: List[Dict[str, Any]],
) -> None:
    """Create durable source and extraction rows before worker dispatch."""
    for document in documents:
        await supabase_service.create_job_document(document)

    for unit in processing_units:
        await supabase_service.upsert_document_extraction({
            "document_id": unit["document_id"],
            "job_id": unit["job_id"],
            "processing_unit_id": unit["id"],
            "status": "queued",
            "structured_data": {},
            "review_status": "pending",
            "validation_flags": [],
            "edited": False,
            "metadata": {
                "source_storage_path": unit.get("storage_path"),
                "source_content_type": unit.get("content_type"),
                "source_page": unit.get("source_page"),
                "source_page_count": unit.get("source_page_count"),
                "source_filename": unit.get("original_filename") or unit.get("source_filename"),
                "source_sha256": unit.get("source_sha256"),
            },
        })


async def refresh_uploaded_duplicate_warnings(supabase_service, documents: List[Dict[str, Any]]) -> None:
    """Populate source-hash duplicate warnings without failing job creation."""
    for document in documents:
        try:
            await supabase_service.refresh_document_duplicate_warnings(document["job_id"], document["id"])
        except Exception as exc:
            logger.warning(f"Unable to evaluate duplicate source warning for document {document['id']}: {exc}")


def _parse_job_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed.replace(tzinfo=None)
        except ValueError:
            return datetime.utcnow()
    return datetime.utcnow()


def _job_image_count(record: Dict[str, Any]) -> int:
    metadata = _parse_metadata(record.get("processing_metadata"))
    files = metadata.get("generated_files")
    file_count = len(files) if isinstance(files, list) else 0
    return int(
        metadata.get("successful_images")
        or metadata.get("processed_images")
        or metadata.get("total_images")
        or file_count
        or 1
    )


def _job_processing_time(record: Dict[str, Any]) -> float:
    metadata = _parse_metadata(record.get("processing_metadata"))
    value = metadata.get("processing_time") or metadata.get("processing_time_seconds") or record.get("processing_time")
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _dashboard_range_start(range_key: str, now: datetime) -> datetime:
    if range_key == "1d":
        return now - timedelta(hours=24)
    if range_key == "30d":
        return now - timedelta(days=30)
    if range_key == "3m":
        return now - timedelta(days=90)
    return now - timedelta(days=7)


def _dashboard_chart_points(jobs: List[Dict[str, Any]], range_key: str, now: datetime) -> List[Dict[str, Any]]:
    if range_key == "1d":
        start = now - timedelta(hours=23)
        points = []
        for index in range(24):
            bucket_start = start.replace(minute=0, second=0, microsecond=0) + timedelta(hours=index)
            bucket_end = bucket_start + timedelta(hours=1)
            count = sum(
                _job_image_count(job)
                for job in jobs
                if bucket_start <= _parse_job_datetime(job.get("created_at")) < bucket_end
            )
            points.append({
                "timestamp": bucket_start.isoformat(),
                "count": count,
                "formattedTime": bucket_start.strftime("%I%p").lstrip("0").lower(),
            })
        return points

    days = 90 if range_key == "3m" else 30 if range_key == "30d" else 7
    start_day = (now - timedelta(days=days - 1)).replace(hour=0, minute=0, second=0, microsecond=0)
    points = []
    for index in range(days):
        bucket_start = start_day + timedelta(days=index)
        bucket_end = bucket_start + timedelta(days=1)
        count = sum(
            _job_image_count(job)
            for job in jobs
            if bucket_start <= _parse_job_datetime(job.get("created_at")) < bucket_end
        )
        points.append({
            "timestamp": bucket_start.isoformat(),
            "count": count,
            "formattedDate": bucket_start.strftime("%b %d").replace(" 0", " "),
        })
    return points


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
            'total_credits': settings.rate_limit_authenticated_images_per_day,
            'used_credits': 0,
            'available_credits': settings.rate_limit_authenticated_images_per_day
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
    normalized_document_mode = normalize_document_mode(request.document_mode)
    normalized_output_format = (
        "xlsx"
        if normalized_document_mode == "table" and request.output_format == "txt"
        else request.output_format
    )
    try:
        workspace_id = await supabase_service.resolve_owned_workspace_id(
            user["user_id"] if user else None,
            request.workspace_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))

    await enforce_upload_rate_limits(
        request=http_request,
        redis_service=redis_service,
        user=user,
        session_id=session.session_id,
        image_count=len(request.images),
        daily_image_limit_override=limits["daily_image_limit"],
        daily_run_limit_override=limits.get("daily_run_limit")
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
        document_records = []

        for i, img in enumerate(request.images):
            document_id = str(uuid.uuid4())
            image_data = img.image.split(',', 1)[1] if img.image.startswith('data:') else img.image
            image_bytes = base64.b64decode(image_data)
            source_sha256 = hashlib.sha256(image_bytes).hexdigest()
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
                'document_id': document_id,
                'storage_path': source_file['storage_path'],
                'filename': filename,
                'content_type': source_file['content_type'],
                'size_bytes': source_file['size_bytes'],
                'output_format': normalized_output_format,
                'document_mode': normalized_document_mode,
                'original_filename': filename,
                'source_sha256': source_sha256,
            })
            document_records.append({
                "id": document_id,
                "job_id": job_id,
                "owner_user_id": user['user_id'] if user else None,
                "owner_session_id": None if user else session.session_id,
                "workspace_id": workspace_id,
                "original_filename": filename,
                "source_storage_path": source_file["storage_path"],
                "source_content_type": source_file["content_type"],
                "selected_mode": normalized_document_mode,
                "resolved_mode": (
                    normalized_document_mode
                    if normalized_document_mode in {"table", "invoice", "receipt", "bank_statement", "notes"}
                    else None
                ),
                "status": "queued",
                "metadata": {"source_sha256": source_sha256},
                "expires_at": (datetime.utcnow() + timedelta(hours=settings.file_retention_hours)).isoformat(),
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
            'output_format': normalized_output_format,
            'document_mode': normalized_document_mode,
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
                'output_format': normalized_output_format,
                'document_mode': normalized_document_mode,
                'session_id': session.session_id,
                'owner_user_id': user['user_id'] if user else None,
                'owner_session_id': None if user else session.session_id,
                'workspace_id': workspace_id,
                'plan': limits["plan"],
                'max_files_per_batch': limits["max_files_per_batch"],
                'credits_reserved': len(request.images) if user else 0,
                'credits_settled': False if user else True
            },
            status='queued'
        )
        await persist_queued_documents(
            supabase_service,
            document_records,
            [
                {
                    **stored_image,
                    "job_id": job_id,
                    "source_page": None,
                    "source_page_count": None,
                }
                for stored_image in stored_images
            ],
        )
        await refresh_uploaded_duplicate_warnings(supabase_service, document_records)
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
                    'output_format': normalized_output_format,
                    'document_mode': normalized_document_mode,
                    'session_id': session.session_id,
                    'owner_user_id': user['user_id'] if user else None,
                    'owner_session_id': None if user else session.session_id,
                    'workspace_id': workspace_id,
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
        
        estimated_completion = estimate_parallel_completion(len(request.images))
        
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
        try:
            await supabase_service.update_job_documents_status(job_id, "failed")
        except Exception:
            pass

        if user and credits_reserved:
            supabase_service.refund_credits(user['user_id'], len(request.images))
        
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create batch job: {str(e)}"
        )


@router.post("/batch-upload", response_model=BatchConvertResponse)
async def create_batch_job_multipart(
    files: List[UploadFile] = File(..., description="Image or PDF files to process (PNG, JPEG, WebP, HEIC, PDF)"),
    output_format: str = Form("xlsx"),
    consolidation_strategy: str = Form("consolidated"),
    document_mode: str = Form("table"),
    workspace_id: Optional[str] = Form(None),
    session: SessionMetadata = Depends(get_or_create_session),
    redis_service: RedisService = Depends(get_redis_service),
    user: Optional[dict] = Depends(get_optional_user),
    http_request: Request = None
):
    """
    Create a new batch processing job using multipart/form-data file uploads.

    This endpoint accepts multiple binary image or PDF files directly (no base64 encoding needed),
    making uploads faster and more memory-efficient than the base64 endpoint.

    This is the recommended endpoint for file uploads.
    """
    # Detailed logging for debugging
    logger.info(f"[BATCH-UPLOAD] Received request with {len(files)} files")
    logger.info(f"[BATCH-UPLOAD] Parameters: output_format={output_format}, consolidation_strategy={consolidation_strategy}, document_mode={document_mode}")
    logger.info(f"[BATCH-UPLOAD] Session ID: {session.session_id if session else 'None'}")
    logger.info(f"[BATCH-UPLOAD] User ID: {user['user_id'] if user else 'None'}")

    # Log file details for debugging
    for i, file in enumerate(files):
        logger.info(f"[BATCH-UPLOAD] File {i+1}: name={file.filename}, content_type={file.content_type}, size=pending")

    # Validate batch request
    simple_batch_validation(len(files))
    supabase_service = get_supabase_service()
    limits = resolve_upload_limits(user, supabase_service)
    normalized_document_mode = normalize_document_mode(document_mode)
    requested_output_format = str(output_format).lower()
    normalized_output_format = (
        "txt"
        if requested_output_format in {"txt", "text", "plain_text"}
        else "csv"
        if requested_output_format == "csv" and normalized_document_mode in {"table", "invoice", "receipt", "bank_statement", "notes"}
        else "xlsx"
    )
    if normalized_document_mode in {"table", "bank_statement", "invoice", "receipt"} and normalized_output_format == "txt":
        normalized_output_format = "xlsx"
    try:
        workspace_id = await supabase_service.resolve_owned_workspace_id(
            user["user_id"] if user else None,
            workspace_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))

    # Validate each file
    SUPPORTED_TYPES = {"image/png", "image/jpeg", "image/jpg", "image/webp", "image/heic", "image/heif", "application/pdf"}
    MAX_FILE_SIZE = settings.max_file_size_bytes
    validated_files = []

    for i, file in enumerate(files):
        logger.info(f"[BATCH-UPLOAD] Validating file {i+1}: {file.filename}")

        # Check content type or file extension
        # HEIC and PDF files might not have proper MIME type, so check extension too
        file_ext = file.filename.lower().split('.')[-1] if file.filename else ""
        is_supported_by_extension = file_ext in ['heic', 'heif', 'pdf']
        
        # Accept if content type is valid OR if it's supported by extension
        if file.content_type not in SUPPORTED_TYPES and not is_supported_by_extension:
            error_msg = f"File {i+1} '{file.filename}': Unsupported file type '{file.content_type}'. Supported types: PNG, JPEG, WebP, HEIC, HEIF, PDF"
            logger.error(f"[BATCH-UPLOAD] Validation failed - {error_msg}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=error_msg
            )
        
        # Log if we're accepting based on extension
        if is_supported_by_extension and file.content_type not in SUPPORTED_TYPES:
            logger.info(f"[BATCH-UPLOAD] File {i+1} accepted based on extension .{file_ext} (content_type={file.content_type})")

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

        if file_ext == "pdf" or file.content_type == "application/pdf" or is_pdf_bytes(file_content):
            normalized_content_type = "application/pdf"
        elif file_ext in {"heic", "heif"}:
            normalized_content_type = f"image/{file_ext}"
        elif file.content_type == "image/jpg":
            normalized_content_type = "image/jpeg"
        else:
            normalized_content_type = file.content_type or "application/octet-stream"
        validated_files.append({
            "index": i,
            "document_id": str(uuid.uuid4()),
            "filename": file.filename or f"image_{i}.png",
            "content_type": normalized_content_type,
            "content": file_content,
            "source_sha256": hashlib.sha256(file_content).hexdigest(),
        })
        logger.info(f"[BATCH-UPLOAD] File {i+1} validation passed")

    processing_units = []
    for file_info in validated_files:
        document_id = file_info["document_id"]
        filename = file_info["filename"]
        content = file_info["content"]

        if file_info["content_type"] == "application/pdf" or is_pdf_bytes(content):
            try:
                page_images = render_pdf_pages_to_png(content)
            except ProcessingError as exc:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"File '{filename}': {str(exc)}"
                )

            base_name = os.path.splitext(os.path.basename(filename))[0] or "document"
            for page_index, page_bytes in enumerate(page_images, start=1):
                processing_units.append({
                    "id": f"img_{file_info['index']}_page_{page_index}",
                    "document_id": document_id,
                    "content": page_bytes,
                    "filename": f"{file_info['index']}_{base_name}_page_{page_index}.png",
                    "content_type": "image/png",
                    "output_format": normalized_output_format,
                    "document_mode": normalized_document_mode,
                    "source_filename": filename,
                    "source_content_type": "application/pdf",
                    "source_page": page_index,
                    "source_page_count": len(page_images),
                    "source_sha256": hashlib.sha256(page_bytes).hexdigest(),
                })
            continue

        processing_units.append({
            "id": f"img_{file_info['index']}",
            "document_id": document_id,
            "content": content,
            "filename": f"{file_info['index']}_{filename}",
            "content_type": file_info["content_type"],
            "output_format": normalized_output_format,
            "document_mode": normalized_document_mode,
            "source_filename": filename,
            "source_content_type": file_info["content_type"],
            "source_page": None,
            "source_page_count": None,
            "source_sha256": file_info["source_sha256"],
        })

    processing_count = len(processing_units)
    simple_batch_validation(processing_count)
    enforce_batch_limit(processing_count, limits)

    await enforce_upload_rate_limits(
        request=http_request,
        redis_service=redis_service,
        user=user,
        session_id=session.session_id,
        image_count=processing_count,
        daily_image_limit_override=limits["daily_image_limit"],
        daily_run_limit_override=limits.get("daily_run_limit")
    )

    credits_reserved = False
    credits_reserved_count = 0
    if user:
        logger.info(f"[Credits] Checking credits for user {user['user_id']}, need {processing_count} credits")
        try:
            credits_result = supabase_service.check_and_use_credits(
                user['user_id'],
                processing_count
            )

            if not credits_result:
                logger.warning(f"[Credits] Insufficient credits for user {user['user_id']}")
                raise HTTPException(
                    status_code=status.HTTP_402_PAYMENT_REQUIRED,
                    detail={
                        "code": "INSUFFICIENT_CREDITS",
                        "message": f"Insufficient credits. You need {processing_count} credits but don't have enough remaining.",
                        "credits_required": processing_count
                    }
                )

            credits_reserved = True
            credits_reserved_count = processing_count
            logger.info(f"[Credits] Reserved {processing_count} credits for user {user['user_id']}")
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"[Credits] Critical error checking credits: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Credit system error. Please try again or contact support."
            )

    # Generate job ID
    job_id = str(uuid.uuid4())
    logger.info(f"[BATCH-UPLOAD] Generated job ID: {job_id}")

    try:
        storage_owner_id = user['user_id'] if user else session.session_id
        stored_images = []

        for unit in processing_units:
            source_file = await supabase_service.upload_source_file(
                file_data=unit["content"],
                owner_id=storage_owner_id,
                job_id=job_id,
                filename=unit["filename"],
                content_type=unit["content_type"]
            )

            stored_images.append({
                'id': unit["id"],
                'document_id': unit["document_id"],
                'storage_path': source_file['storage_path'],
                'filename': unit["filename"],
                'content_type': source_file['content_type'],
                'output_format': unit["output_format"],
                'document_mode': unit["document_mode"],
                'size_bytes': source_file['size_bytes'],
                'original_filename': unit["source_filename"],
                'source_content_type': unit["source_content_type"],
                'source_page': unit["source_page"],
                'source_page_count': unit["source_page_count"],
                'source_sha256': unit["source_sha256"],
            })

        document_records = []
        for source_file_info in validated_files:
            document_id = source_file_info["document_id"]
            first_unit = next(
                stored_image for stored_image in stored_images
                if stored_image["document_id"] == document_id
            )
            source_storage_path = first_unit["storage_path"]

            if source_file_info["content_type"] == "application/pdf":
                durable_source = await supabase_service.upload_source_file(
                    file_data=source_file_info["content"],
                    owner_id=storage_owner_id,
                    job_id=job_id,
                    filename=f"source_{source_file_info['index']}_{source_file_info['filename']}",
                    content_type=source_file_info["content_type"],
                )
                source_storage_path = durable_source["storage_path"]

            document_records.append({
                "id": document_id,
                "job_id": job_id,
                "owner_user_id": user['user_id'] if user else None,
                "owner_session_id": None if user else session.session_id,
                "workspace_id": workspace_id,
                "original_filename": source_file_info["filename"],
                "source_storage_path": source_storage_path,
                "source_content_type": source_file_info["content_type"],
                "selected_mode": normalized_document_mode,
                "resolved_mode": (
                    normalized_document_mode
                    if normalized_document_mode in {"table", "invoice", "receipt", "bank_statement", "notes"}
                    else None
                ),
                "status": "queued",
                "metadata": {
                    "source_page_count": first_unit.get("source_page_count"),
                    "source_sha256": source_file_info["source_sha256"],
                },
                "expires_at": (datetime.utcnow() + timedelta(hours=settings.file_retention_hours)).isoformat(),
            })

        # Create job metadata for Redis with storage paths, not raw image payloads.
        job_data = {
            'job_id': job_id,
            'session_id': session.session_id,
            'user_id': user['user_id'] if user else '',  # Convert None to empty string for Redis
            'status': 'queued',
            'total_images': processing_count,
            'processed_images': 0,
            'progress': 0,
            'output_format': normalized_output_format,
            'document_mode': normalized_document_mode,
            'consolidation_strategy': consolidation_strategy,
            'images': stored_images,
            'results': [],
            'errors': [],
            'plan': limits["plan"],
            'max_files_per_batch': limits["max_files_per_batch"],
            'credits_reserved': credits_reserved_count,
            'credits_settled': False if user else True,
            'created_at': datetime.utcnow().isoformat(),
            'updated_at': datetime.utcnow().isoformat()
        }

        durable_owner_id = user['user_id'] if user else f"session:{session.session_id}"
        await supabase_service.create_job(
            job_id=job_id,
            user_id=durable_owner_id,
            filename=f"batch_{processing_count}_pages",
            metadata={
                'total_images': processing_count,
                'uploaded_files': len(files),
                'consolidation_strategy': consolidation_strategy,
                'output_format': normalized_output_format,
                'document_mode': normalized_document_mode,
                'session_id': session.session_id,
                'owner_user_id': user['user_id'] if user else None,
                'owner_session_id': None if user else session.session_id,
                'workspace_id': workspace_id,
                'plan': limits["plan"],
                'max_files_per_batch': limits["max_files_per_batch"],
                'credits_reserved': credits_reserved_count,
                'credits_settled': False if user else True
            },
            status='queued'
        )
        await persist_queued_documents(
            supabase_service,
            document_records,
            [{**stored_image, "job_id": job_id} for stored_image in stored_images],
        )
        await refresh_uploaded_duplicate_warnings(supabase_service, document_records)
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
                    'total_images': processing_count,
                    'uploaded_files': len(files),
                    'consolidation_strategy': consolidation_strategy,
                    'output_format': normalized_output_format,
                    'document_mode': normalized_document_mode,
                    'session_id': session.session_id,
                    'owner_user_id': user['user_id'] if user else None,
                    'owner_session_id': None if user else session.session_id,
                    'workspace_id': workspace_id,
                    'celery_task_id': async_result.id,
                    'plan': limits["plan"],
                    'max_files_per_batch': limits["max_files_per_batch"],
                    'credits_reserved': credits_reserved_count,
                    'credits_settled': False if user else True
                }
            )
        except Exception as e:
            logger.debug(f"Failed to store Celery task id for job {job_id}: {e}")

        logger.info(f"Queued Celery processing for job {job_id} with {len(stored_images)} page image(s)")

        estimated_completion = estimate_parallel_completion(processing_count)

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
        try:
            await supabase_service.update_job_documents_status(job_id, "failed")
        except Exception:
            pass

        if user and credits_reserved:
            supabase_service.refund_credits(user['user_id'], credits_reserved_count)

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
                    expires_at = completed_time + timedelta(hours=settings.file_retention_hours)
                except ValueError:
                    expires_at = datetime.utcnow() + timedelta(hours=settings.file_retention_hours)
            else:
                expires_at = datetime.utcnow() + timedelta(hours=settings.file_retention_hours)
            
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
                        document_id=file_info.get('document_id'),
                        source_page=file_info.get('source_page'),
                        source_page_count=file_info.get('source_page_count'),
                        size_bytes=file_info.get('size_bytes'),
                        status=file_info.get('status'),
                        document_mode=file_info.get('document_mode'),
                        requires_review=file_info.get('requires_review'),
                        confidence_score=file_info.get('confidence_score'),
                        review_flags=file_info.get('review_flags') or [],
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
                expires_at=datetime.utcnow() + timedelta(hours=settings.file_retention_hours),
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


@router.get("/{job_id}/documents", response_model=Dict[str, Any])
async def get_job_documents(
    job_id: str,
    session: SessionMetadata = Depends(get_or_create_session),
    user: Optional[dict] = Depends(get_optional_user),
):
    """Return durable logical documents and extraction states for an owned batch."""
    supabase_service = get_supabase_service()
    supabase_job = await supabase_service.get_job(job_id)
    if not supabase_job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found",
        )

    metadata = _parse_metadata(supabase_job.get("processing_metadata"))
    stored_user_id = str(supabase_job.get("user_id") or "")
    owner_user_id = metadata.get("owner_user_id") or (
        stored_user_id if not stored_user_id.startswith("session:") else ""
    )
    owner_session_id = metadata.get("owner_session_id") or metadata.get("session_id", "")
    verify_job_data_access(
        {
            "job_id": job_id,
            "user_id": owner_user_id or "",
            "session_id": owner_session_id or "",
        },
        user,
        session.session_id,
    )

    documents = await supabase_service.get_job_documents(job_id)
    preview_expires_in = 60 * 60
    for document in documents:
        verify_durable_document_access(document, user, session.session_id)
        if user:
            document["vendor_suggestion"] = await supabase_service.get_vendor_suggestion(document, user["user_id"])
            document["quickbooks_receipt_publication"] = await supabase_service.get_quickbooks_receipt_publication(
                document["id"], user["user_id"]
            )
        document["source_access_url"] = await supabase_service.create_signed_url(
            document["source_storage_path"],
            expires_in=preview_expires_in,
        )
        document["preview_expires_in"] = preview_expires_in

        result_files_by_id = {
            result_file["file_id"]: result_file
            for result_file in document.get("result_files", [])
            if result_file.get("file_id")
        }
        for extraction in document.get("extractions", []):
            extraction_metadata = _parse_metadata(extraction.get("metadata"))
            source_preview_path = (
                extraction_metadata.get("source_storage_path")
                or document["source_storage_path"]
            )
            extraction["source_preview_url"] = await supabase_service.create_signed_url(
                source_preview_path,
                expires_in=preview_expires_in,
            )
            extraction["source_page"] = extraction_metadata.get("source_page")
            extraction["source_page_count"] = extraction_metadata.get("source_page_count")
            extraction["source_filename"] = (
                extraction_metadata.get("source_filename")
                or document["original_filename"]
            )
            result_file = result_files_by_id.get(extraction.get("result_file_id"))
            if result_file:
                result_file["input_preview_url"] = extraction["source_preview_url"]
                result_file["source_page"] = extraction["source_page"]
                result_file["source_page_count"] = extraction["source_page_count"]
                result_file["source_filename"] = extraction["source_filename"]

    return {
        "job_id": job_id,
        "status": supabase_job.get("status", "unknown"),
        "documents": documents,
        "total": len(documents),
    }


@router.get("/{job_id}/documents/{document_id}/review", response_model=Dict[str, Any])
async def get_document_review(
    job_id: str,
    document_id: str,
    session: SessionMetadata = Depends(get_or_create_session),
    user: Optional[dict] = Depends(get_optional_user),
):
    """Return raw extraction data, reviewed values, and audit history for an owned document."""
    supabase_service = get_supabase_service()
    await require_owned_durable_document(supabase_service, job_id, document_id, session, user)
    document = await supabase_service.get_document_review(job_id, document_id)
    if not document:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Document not found")
    if user:
        document["vendor_suggestion"] = await supabase_service.get_vendor_suggestion(document, user["user_id"])
        document["quickbooks_receipt_publication"] = await supabase_service.get_quickbooks_receipt_publication(
            document_id, user["user_id"]
        )
    return {"job_id": job_id, "document": document}


@router.post("/{job_id}/documents/{document_id}/review/changes", response_model=Dict[str, Any])
async def update_document_review_value(
    job_id: str,
    document_id: str,
    request: DocumentReviewChangeRequest,
    session: SessionMetadata = Depends(get_or_create_session),
    user: Optional[dict] = Depends(get_optional_user),
):
    """Persist one corrected structured value or grid cell for an owned document."""
    supabase_service = get_supabase_service()
    await require_owned_durable_document(supabase_service, job_id, document_id, session, user)
    actor = {"user_id": user["user_id"]} if user else {"session_id": session.session_id}
    try:
        result = await supabase_service.apply_document_review_change(
            job_id=job_id,
            document_id=document_id,
            processing_unit_id=request.processing_unit_id,
            field_path=request.field_path,
            value=request.value,
            actor=actor,
            base_review_grid=request.base_review_grid,
        )
        return {"job_id": job_id, "document_id": document_id, **result}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


@router.post("/{job_id}/documents/{document_id}/review/status", response_model=Dict[str, Any])
async def update_document_review_status(
    job_id: str,
    document_id: str,
    request: DocumentReviewStatusRequest,
    session: SessionMetadata = Depends(get_or_create_session),
    user: Optional[dict] = Depends(get_optional_user),
):
    """Confirm Ready or persist another supported document review state."""
    supabase_service = get_supabase_service()
    await require_owned_durable_document(supabase_service, job_id, document_id, session, user)
    actor = {"user_id": user["user_id"]} if user else {"session_id": session.session_id}
    try:
        document = await supabase_service.set_document_review_status(
            job_id=job_id,
            document_id=document_id,
            review_status=request.review_status,
            actor=actor,
            reason=request.reason,
        )
        return {"job_id": job_id, "document": document}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


@router.post("/{job_id}/documents/{document_id}/vendor-rule", response_model=Dict[str, Any])
async def save_document_vendor_rule(
    job_id: str,
    document_id: str,
    request: VendorRuleFromDocumentRequest,
    session: SessionMetadata = Depends(get_or_create_session),
    user: dict = Depends(get_current_user),
):
    """Store explicit vendor memory only from a confirmed owned invoice or receipt."""
    supabase_service = get_supabase_service()
    await require_owned_durable_document(supabase_service, job_id, document_id, session, user)
    try:
        rule = await supabase_service.save_vendor_rule_from_document(
            job_id=job_id,
            document_id=document_id,
            user_id=user["user_id"],
            suggested_fields=request.suggested_fields.model_dump(exclude_none=True),
        )
        document = await supabase_service.get_document_review(job_id, document_id)
        if document:
            document["vendor_suggestion"] = rule
        return {"job_id": job_id, "rule": rule, "document": document}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


@router.post("/{job_id}/documents/{document_id}/quickbooks/receipt/publish", response_model=Dict[str, Any])
async def publish_receipt_to_quickbooks(
    job_id: str,
    document_id: str,
    request: ReceiptQuickBooksPublishRequest,
    session: SessionMetadata = Depends(get_or_create_session),
    user: dict = Depends(get_current_user),
):
    """Publish one explicitly reviewed receipt as a paid Purchase or unpaid Bill."""
    supabase_service = get_supabase_service()
    await require_owned_durable_document(supabase_service, job_id, document_id, session, user)
    try:
        document = await get_quickbooks_service().publish_reviewed_receipt(
            job_id=job_id,
            document_id=document_id,
            user_id=user["user_id"],
            request=request.model_dump(exclude_none=True),
        )
        return {"job_id": job_id, "document": document}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))


@router.post("/{job_id}/documents/{document_id}/duplicates/override", response_model=Dict[str, Any])
async def override_document_duplicate_warning(
    job_id: str,
    document_id: str,
    request: DocumentDuplicateOverrideRequest,
    session: SessionMetadata = Depends(get_or_create_session),
    user: Optional[dict] = Depends(get_optional_user),
):
    """Acknowledge a duplicate warning while retaining an auditable document."""
    supabase_service = get_supabase_service()
    await require_owned_durable_document(supabase_service, job_id, document_id, session, user)
    actor = {"user_id": user["user_id"]} if user else {"session_id": session.session_id}
    try:
        document = await supabase_service.override_document_duplicate_warning(
            job_id=job_id,
            document_id=document_id,
            warning_id=request.warning_id,
            actor=actor,
            reason=request.reason,
        )
        return {"job_id": job_id, "document": document}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


@router.get("/{job_id}/documents/{document_id}/export")
async def export_reviewed_document(
    job_id: str,
    document_id: str,
    format: str = "xlsx",
    session: SessionMetadata = Depends(get_or_create_session),
    user: Optional[dict] = Depends(get_optional_user),
):
    """Download one document rendered from durable human-reviewed values."""
    export_format = str(format or "xlsx").lower()
    if export_format not in {"xlsx", "csv", "txt"}:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unsupported export format")

    supabase_service = get_supabase_service()
    await require_owned_durable_document(supabase_service, job_id, document_id, session, user)
    document = await supabase_service.refresh_document_duplicate_warnings(job_id, document_id)
    if supabase_service.active_duplicate_warnings(document):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": "DUPLICATE_REVIEW_REQUIRED",
                "message": "Review or keep this possible duplicate before exporting.",
                "document_id": document_id,
            },
        )
    document = await supabase_service.get_document_review(job_id, document_id)
    if not document:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Document not found")
    if not document.get("extractions"):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Document has no extracted data to export")

    rendered = ExcelService().export_reviewed_document(document, export_format)
    return StreamingResponse(
        io.BytesIO(rendered["content"]),
        media_type=rendered["content_type"],
        headers={
            "Content-Disposition": f"attachment; filename*=UTF-8''{quote(rendered['filename'], safe='')}",
            "Content-Length": str(len(rendered["content"])),
            "Cache-Control": "no-cache, no-store, must-revalidate",
        },
    )


@router.get("/{job_id}/exports/reviewed")
async def export_reviewed_batch(
    job_id: str,
    format: str = "xlsx",
    session: SessionMetadata = Depends(get_or_create_session),
    user: Optional[dict] = Depends(get_optional_user),
):
    """Download one reviewed output per durable source document in a ZIP archive."""
    export_format = str(format or "xlsx").lower()
    if export_format not in {"xlsx", "csv", "txt"}:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unsupported export format")

    supabase_service = get_supabase_service()
    await require_owned_durable_job(supabase_service, job_id, session, user)
    documents = await supabase_service.get_job_documents(job_id)
    for document in documents:
        verify_durable_document_access(document, user, session.session_id)
    documents = [
        await supabase_service.refresh_document_duplicate_warnings(job_id, document["id"])
        for document in documents
    ]
    duplicate_documents = [
        document["id"]
        for document in documents
        if supabase_service.active_duplicate_warnings(document)
    ]
    if duplicate_documents:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": "DUPLICATE_REVIEW_REQUIRED",
                "message": "Resolve possible duplicate files before downloading the reviewed batch.",
                "document_ids": duplicate_documents,
            },
        )
    documents = [
        document
        for document in documents
        if document.get("review_status") != "deleted" and document.get("extractions")
    ]
    if not documents:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No reviewed documents available")

    exporter = ExcelService()
    buffer = io.BytesIO()
    used_names: Dict[str, int] = {}
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for document in documents:
            rendered = exporter.export_reviewed_document(document, export_format)
            filename = rendered["filename"]
            count = used_names.get(filename, 0)
            used_names[filename] = count + 1
            if count:
                stem, extension = os.path.splitext(filename)
                filename = f"{stem}_{count + 1}{extension}"
            archive.writestr(filename, rendered["content"])

    buffer.seek(0)
    archive_name = f"reviewed_batch_{job_id[:8]}.zip"
    return StreamingResponse(
        buffer,
        media_type="application/zip",
        headers={
            "Content-Disposition": f"attachment; filename*=UTF-8''{quote(archive_name, safe='')}",
            "Content-Length": str(len(buffer.getvalue())),
            "Cache-Control": "no-cache, no-store, must-revalidate",
        },
    )


@router.delete("/{job_id}/documents/{document_id}", response_model=Dict[str, Any])
async def delete_stored_document(
    job_id: str,
    document_id: str,
    session: SessionMetadata = Depends(get_or_create_session),
    user: Optional[dict] = Depends(get_optional_user),
    redis_service: RedisService = Depends(get_redis_service),
):
    """Permanently erase one owned financial document and its generated content."""
    supabase_service = get_supabase_service()
    job, _ = await require_owned_durable_document(supabase_service, job_id, document_id, session, user)
    require_stored_content_deletable(job)
    actor = {"user_id": user["user_id"]} if user else {"session_id": session.session_id}
    try:
        result = await supabase_service.delete_document_content(job_id, document_id, actor)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    await redis_service.delete_job(job_id)
    for file_id in result.get("deleted_file_ids", []):
        await redis_service.delete_cache(f"file:{file_id}")
    return result


@router.delete("/{job_id}/documents", response_model=Dict[str, Any])
async def delete_stored_batch_documents(
    job_id: str,
    session: SessionMetadata = Depends(get_or_create_session),
    user: Optional[dict] = Depends(get_optional_user),
    redis_service: RedisService = Depends(get_redis_service),
):
    """Permanently erase every stored financial document in an owned batch."""
    supabase_service = get_supabase_service()
    job = await require_owned_durable_job(supabase_service, job_id, session, user)
    require_stored_content_deletable(job)
    documents = await supabase_service.get_job_documents(job_id)
    for document in documents:
        verify_durable_document_access(document, user, session.session_id)
    actor = {"user_id": user["user_id"]} if user else {"session_id": session.session_id}
    try:
        result = await supabase_service.delete_batch_content(job_id, actor)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    await redis_service.delete_job(job_id)
    for file_id in result.get("deleted_file_ids", []):
        await redis_service.delete_cache(f"file:{file_id}")
    return result


@router.post("/{job_id}/documents/{document_id}/override-mode", response_model=Dict[str, Any])
async def override_job_document_mode(
    job_id: str,
    document_id: str,
    request: DocumentModeOverrideRequest,
    session: SessionMetadata = Depends(get_or_create_session),
    user: Optional[dict] = Depends(get_optional_user),
):
    """Audit a manual auto-detection override and rerun the selected extractor."""
    supabase_service = get_supabase_service()
    supabase_job = await supabase_service.get_job(job_id)
    if not supabase_job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")

    metadata = _parse_metadata(supabase_job.get("processing_metadata"))
    stored_user_id = str(supabase_job.get("user_id") or "")
    owner_user_id = metadata.get("owner_user_id") or (
        stored_user_id if not stored_user_id.startswith("session:") else ""
    )
    owner_session_id = metadata.get("owner_session_id") or metadata.get("session_id", "")
    verify_job_data_access(
        {"job_id": job_id, "user_id": owner_user_id or "", "session_id": owner_session_id or ""},
        user,
        session.session_id,
    )

    document = await supabase_service.get_job_document(job_id, document_id)
    if not document:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Document not found")
    verify_durable_document_access(document, user, session.session_id)

    units = document.get("extractions") or []
    if not units:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Document has no source units to reprocess")

    selected_mode = request.document_mode
    output_format = request.output_format
    if selected_mode != "notes" and output_format == "txt":
        output_format = "xlsx"

    credits_reserved = 0
    if user:
        if not supabase_service.check_and_use_credits(user["user_id"], len(units)):
            raise HTTPException(
                status_code=status.HTTP_402_PAYMENT_REQUIRED,
                detail={
                    "code": "INSUFFICIENT_CREDITS",
                    "message": "More credits are required to reprocess this document.",
                    "credits_required": len(units),
                },
            )
        credits_reserved = len(units)

    actor = {"user_id": user["user_id"]} if user else {"session_id": session.session_id}
    try:
        await supabase_service.override_job_document_mode(
            job_id=job_id,
            document_id=document_id,
            document_mode=selected_mode,
            actor=actor,
            reason=request.reason,
        )
        rerun_units = []
        for extraction in units:
            source_metadata = _parse_metadata(extraction.get("metadata"))
            processing_unit_id = extraction["processing_unit_id"]
            source_path = source_metadata.get("source_storage_path") or document["source_storage_path"]
            image_info = {
                "id": processing_unit_id,
                "document_id": document_id,
                "storage_path": source_path,
                "filename": source_metadata.get("source_filename") or document["original_filename"],
                "original_filename": source_metadata.get("source_filename") or document["original_filename"],
                "content_type": source_metadata.get("source_content_type") or document.get("source_content_type"),
                "output_format": output_format,
                "document_mode": selected_mode,
                "source_page": source_metadata.get("source_page"),
                "source_page_count": source_metadata.get("source_page_count"),
                "source_sha256": source_metadata.get("source_sha256"),
                "force_reprocess": True,
            }
            rerun_units.append(image_info)
            await supabase_service.upsert_document_extraction({
                "document_id": document_id,
                "job_id": job_id,
                "processing_unit_id": processing_unit_id,
                "status": "queued",
                "structured_data": {},
                "review_status": "pending",
                "validation_flags": [],
                "edited": False,
                "metadata": {
                    **source_metadata,
                    "manual_override_mode": selected_mode,
                    "manual_override_requested_at": datetime.utcnow().isoformat(),
                },
            })

        header = [
            process_single_stored_image.s(
                job_id,
                session.session_id,
                image_info,
                index,
                len(rerun_units),
                user["user_id"] if user else None,
            ).set(queue="image_processing", priority=8)
            for index, image_info in enumerate(rerun_units)
        ]
        callback = finalize_document_override.s(
            job_id,
            document_id,
            user["user_id"] if user else None,
            credits_reserved,
        ).set(queue="batch_processing", priority=7)
        chord_result = chord(header)(callback)
        return {
            "job_id": job_id,
            "document_id": document_id,
            "status": "processing",
            "resolved_mode": selected_mode,
            "task_id": str(chord_result.id),
        }
    except HTTPException:
        if user and credits_reserved:
            supabase_service.refund_credits(user["user_id"], credits_reserved)
        raise
    except Exception as exc:
        if user and credits_reserved:
            supabase_service.refund_credits(user["user_id"], credits_reserved)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unable to reprocess this document: {str(exc)}",
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
    try:
        await get_supabase_service().update_job_documents_status(job_id, "cancelled")
    except Exception as e:
        logger.debug(f"Failed to update durable document cancellation for job {job_id}: {e}")

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


@router.get("/dashboard", response_model=Dict[str, Any])
async def get_dashboard_metrics(
    user: dict = Depends(get_current_user),
    range: str = "7d"
):
    """
    Return dashboard-ready metrics from durable Supabase job data.

    The frontend should use this endpoint for charts instead of reconstructing
    stats from a small client-side history page.
    """
    range_key = range if range in {"1d", "7d", "30d", "3m"} else "7d"

    try:
        supabase_service = get_supabase_service()
        now = datetime.utcnow()
        range_start = _dashboard_range_start(range_key, now)
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        week_start = now - timedelta(days=7)

        jobs = await supabase_service.get_user_jobs(user["user_id"], limit=5000)

        range_jobs = [
            job for job in jobs
            if _parse_job_datetime(job.get("created_at")) >= range_start
        ]
        today_jobs = [
            job for job in jobs
            if _parse_job_datetime(job.get("created_at")) >= today_start
        ]
        month_jobs = [
            job for job in jobs
            if _parse_job_datetime(job.get("created_at")) >= month_start
        ]
        week_jobs = [
            job for job in jobs
            if _parse_job_datetime(job.get("created_at")) >= week_start
        ]

        terminal_jobs = [
            job for job in range_jobs
            if job.get("status") not in ACTIVE_JOB_STATUSES
        ]
        successful_jobs = [
            job for job in range_jobs
            if job.get("status") in {"completed", "partially_completed"}
        ]
        failed_jobs = [
            job for job in range_jobs
            if job.get("status") == "failed"
        ]
        active_jobs = [
            job for job in jobs
            if job.get("status") in ACTIVE_JOB_STATUSES
        ]
        processing_times = [
            _job_processing_time(job)
            for job in successful_jobs
            if _job_processing_time(job) > 0
        ]

        credits = supabase_service.get_user_credits(user["user_id"])
        all_time_images = sum(_job_image_count(job) for job in jobs)
        total_processed = max(int(credits.get("used_credits") or 0), all_time_images)
        selected_period_processed = sum(_job_image_count(job) for job in range_jobs)

        return {
            "range": range_key,
            "chart": _dashboard_chart_points(range_jobs, range_key, now),
            "stats": {
                "totalProcessed": total_processed,
                "todayProcessed": sum(_job_image_count(job) for job in today_jobs),
                "thisMonthProcessed": sum(_job_image_count(job) for job in month_jobs),
                "monthProcessed": sum(_job_image_count(job) for job in month_jobs),
                "lastWeekProcessed": sum(_job_image_count(job) for job in week_jobs),
                "selectedPeriodProcessed": selected_period_processed,
                "averageTime": (
                    sum(processing_times) / len(processing_times)
                    if processing_times else 0
                ),
                "successRate": (
                    (len(successful_jobs) / len(terminal_jobs)) * 100
                    if terminal_jobs else 0
                ),
                "totalJobs": len(jobs),
                "activeJobs": len(active_jobs),
                "failedJobs": len(failed_jobs),
                "successfulJobs": len(successful_jobs),
            },
        }

    except Exception as e:
        logger.error(f"Failed to get dashboard metrics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve dashboard metrics: {str(e)}"
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
