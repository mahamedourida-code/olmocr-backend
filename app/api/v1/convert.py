from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks, Request, Response
import uuid
from datetime import datetime, timedelta
import logging

from app.models.requests import BatchConvertRequest
from app.models.responses import ConvertResponse, ErrorResponse
from app.core.dependencies import (
    get_or_create_session, get_storage_service, get_optional_user
)
from app.core.config import settings
from app.services.storage import FileStorageManager
from app.services.olmocr import get_olmocr_service
from app.services.excel import ExcelService
from app.services.supabase_service import get_supabase_service
from app.utils.exceptions import ProcessingError, ValidationError
from app.models.jobs import SessionMetadata
import base64

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/convert", tags=["Image Conversion"])


def validate_image_upload(file_size: int, content_type: str) -> None:
    """Validate uploaded image parameters."""
    # Check file size
    if file_size > settings.max_file_size:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"File size ({file_size} bytes) exceeds maximum allowed size ({settings.max_file_size} bytes)"
        )
    
    # Check content type
    allowed_types = [
        "image/jpeg", "image/jpg", "image/png", "image/gif", 
        "image/bmp", "image/tiff", "image/webp"
    ]
    
    if content_type not in allowed_types:
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail=f"Unsupported file type: {content_type}. Allowed types: {', '.join(allowed_types)}"
        )


def simple_rate_limit_check(request: Request) -> None:
    """Simple rate limit check without dependencies."""
    # For now, just skip rate limiting to simplify debugging
    # In production, implement proper rate limiting
    pass



async def schedule_file_cleanup(
    file_id: str,
    storage: FileStorageManager,
    retention_hours: int
):
    """
    Schedule cleanup of temporary files.
    
    This background task ensures that uploaded and generated files
    are automatically removed after the retention period.
    """
    import asyncio
    
    # Wait for retention period
    await asyncio.sleep(retention_hours * 3600)
    
    try:
        # Clean up the file
        await storage.cleanup_file(file_id)
    except Exception as e:
        # Log error but don't raise (this is background cleanup)
        print(f"Failed to cleanup file {file_id}: {e}")


@router.post("/batch", response_model=ConvertResponse)
async def convert_batch_images(
    request: BatchConvertRequest,
    response: Response,
    background_tasks: BackgroundTasks,
    session: SessionMetadata = Depends(get_or_create_session),
    storage: FileStorageManager = Depends(get_storage_service),
    user: Optional[dict] = Depends(get_optional_user),
    http_request: Request = None
):
    """
    Convert multiple images containing tables to XLSX format.
    
    This endpoint processes multiple base64-encoded images and returns
    a job_id for downloading the consolidated XLSX file containing all table data.
    
    The processing includes:
    1. Validation of all images
    2. Background processing with Celery
    3. OCR extraction using OlmOCR API for each image
    4. XLSX generation with multiple sheets or consolidated data
    5. Secure file storage with expiration
    """
    # Rate limiting
    simple_rate_limit_check(http_request)
    
    if not request.images:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No images provided for batch processing"
        )
    
    if len(request.images) > 20:  # Max batch size
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"Batch size ({len(request.images)}) exceeds maximum allowed (20)"
        )
    
    try:
        # Generate unique job ID for the batch
        job_id = str(uuid.uuid4())

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

        # Validate all images first
        validated_images = []
        for i, img_data in enumerate(request.images):
            try:
                # Handle data URL format
                image_data = img_data.image
                if image_data.startswith('data:'):
                    image_data = image_data.split(',', 1)[1]
                
                image_bytes = base64.b64decode(image_data)
                
                # Basic validation
                if len(image_bytes) > settings.max_file_size:
                    raise HTTPException(
                        status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                        detail=f"Image {i+1} size exceeds maximum allowed size"
                    )
                
                validated_images.append({
                    'image': image_data,
                    'filename': img_data.filename or f"image_{i+1}.png",
                    'index': i
                })
                
            except Exception as e:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid image data at index {i}: {str(e)}"
                )
        
        # Create batch processing context
        batch_context = {
            'job_id': job_id,
            'session_id': session.session_id,
            'user_id': user['user_id'] if user else None,  # Store user_id for Supabase operations
            'images': validated_images,
            'consolidation_strategy': request.consolidation_strategy,
            'output_format': request.output_format,
            'total_images': len(validated_images),
            'created_at': datetime.utcnow().isoformat()
        }
        
        # Schedule background processing
        background_tasks.add_task(
            process_batch_images_background_sync,
            batch_context,
            storage
        )
        
        # Update session immediately
        session.jobs_count += 1
        session.active_jobs.append(job_id)
        await storage.update_session_metadata(session)
        
        # Calculate estimated completion
        estimated_completion = datetime.utcnow() + timedelta(
            seconds=len(request.images) * 5  # 5 seconds per image estimate
        )
        
        # Calculate expiration time
        expires_at = datetime.utcnow() + timedelta(hours=settings.file_retention_hours)
        
        # Schedule cleanup
        background_tasks.add_task(
            schedule_file_cleanup,
            job_id,
            storage,
            settings.file_retention_hours
        )
        
        # Return immediate response with job_id
        response_data = ConvertResponse(
            success=True,
            job_id=job_id,  # This will be the download ID once processing completes
            download_url=f"/api/v1/download/{job_id}",
            expires_at=expires_at,
            processing_time=0.0,  # Processing happens in background
            session_id=session.session_id
        )
        
        # Set session cookie
        response.set_cookie(
            key="session_id",
            value=session.session_id,
            max_age=settings.file_retention_hours * 3600,
            httponly=True,
            secure=False,
            samesite="lax"
        )
        
        return response_data
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal batch processing error: {str(e)}"
        )


async def process_batch_images_background(
    batch_context: dict,
    storage: FileStorageManager
):
    """
    Background task to process batch images like single conversion.
    
    This mimics the single conversion logic but for multiple images,
    creating a consolidated Excel file with multiple sheets.
    """
    from app.services.olmocr import OlmOCRService
    from app.services.excel import ExcelService
    
    olmocr_service = OlmOCRService()
    excel_service = ExcelService()
    
    job_id = batch_context['job_id']
    session_id = batch_context['session_id']
    images = batch_context['images']
    consolidation_strategy = batch_context['consolidation_strategy']
    
    try:
        # Process all images
        all_results = []
        
        for img in images:
            try:
                # Decode image
                image_bytes = base64.b64decode(img['image'])
                
                # Extract table data using OlmOCR
                csv_data = await olmocr_service.extract_table_from_image(image_bytes)
                
                # Store result with sheet name
                sheet_name = img['filename']
                if sheet_name.endswith(('.png', '.jpg', '.jpeg')):
                    sheet_name = sheet_name.rsplit('.', 1)[0]
                
                all_results.append({
                    'sheet_name': sheet_name,
                    'csv_data': csv_data,
                    'filename': img['filename']
                })
                
            except Exception as e:
                # Add error result but continue processing
                all_results.append({
                    'sheet_name': img['filename'],
                    'csv_data': None,
                    'error': str(e),
                    'filename': img['filename']
                })
        
        # Generate consolidated XLSX
        if consolidation_strategy == "separate":
            # Multiple sheets in one file
            xlsx_file = excel_service.create_multi_sheet_xlsx(all_results)
        elif consolidation_strategy == "concatenated":
            # Single sheet with all data concatenated
            xlsx_file = excel_service.create_concatenated_xlsx(all_results)
        else:  # consolidated (default)
            # Multiple sheets in one file (same as separate)
            xlsx_file = excel_service.create_multi_sheet_xlsx(all_results)
        
        # Store result file with job_id as file_id
        filename = f"batch_results_{job_id}.xlsx"
        await storage.save_result_file(
            session_id,
            filename,
            xlsx_file,
            file_id=job_id  # Use job_id as file_id for download
        )
        
        logger.info(f"Batch processing completed for job {job_id}")
        
    except Exception as e:
        logger.error(f"Batch processing failed for job {job_id}: {str(e)}")
        
        # Create error XLSX file
        try:
            error_xlsx = excel_service._create_empty_xlsx_with_message(
                f"Batch processing failed: {str(e)}", 
                "Error"
            )
            filename = f"batch_error_{job_id}.xlsx"
            await storage.save_result_file(
                session_id,
                filename,
                error_xlsx,
                file_id=job_id
            )
        except Exception as save_error:
            logger.error(f"Failed to save error file for job {job_id}: {str(save_error)}")


def process_batch_images_background_sync(
    batch_context: dict,
    storage: FileStorageManager
):
    """
    Synchronous wrapper for batch processing - works with FastAPI BackgroundTasks.
    
    FastAPI BackgroundTasks runs in a thread pool, so we need to create 
    a new event loop for async operations.
    """
    import asyncio
    
    try:
        # Create a new event loop for this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Run the async processing
        loop.run_until_complete(
            process_batch_images_background_async(batch_context, storage)
        )
    except Exception as e:
        logger.error(f"Background task wrapper failed: {str(e)}")
    finally:
        # Clean up the event loop
        loop.close()


async def process_batch_images_background_async(
    batch_context: dict,
    storage: FileStorageManager
):
    """
    Async implementation of batch processing with WebSocket updates.
    """
    from app.services.olmocr import OlmOCRService
    from app.services.excel import ExcelService
    from app.services.redis_service import RedisService
    from app.models.websocket import JobProgressUpdate, JobCompletedMessage, JobErrorMessage, WebSocketTopics

    olmocr_service = OlmOCRService()
    excel_service = ExcelService()
    redis_service = None

    job_id = batch_context['job_id']
    session_id = batch_context['session_id']
    images = batch_context['images']
    consolidation_strategy = batch_context['consolidation_strategy']

    logger.info(f"Starting batch processing for job {job_id} with {len(images)} images")
    start_time = datetime.utcnow()

    try:
        # Initialize Redis for WebSocket pub/sub
        try:
            redis_service = RedisService()
            await redis_service.initialize()
            logger.info(f"Redis service initialized for WebSocket updates")
        except Exception as e:
            logger.warning(f"Redis not available, WebSocket updates will be skipped: {e}")

        # Process all images
        all_results = []
        successful_count = 0

        for i, img in enumerate(images):
            logger.info(f"Processing image {i+1}/{len(images)} for job {job_id}")

            # Send progress update via WebSocket
            if redis_service:
                try:
                    progress = int((i / len(images)) * 100)
                    progress_msg = JobProgressUpdate(
                        job_id=job_id,
                        session_id=session_id,
                        status="processing",
                        progress=progress,
                        total_images=len(images),
                        processed_images=i,
                        current_image=img['filename']
                    )

                    # Publish to both job-specific and session-specific topics
                    await redis_service.publish_message(WebSocketTopics.job_topic(job_id), progress_msg)
                    await redis_service.publish_message(WebSocketTopics.session_topic(session_id), progress_msg)
                    await redis_service.publish_message(WebSocketTopics.JOB_PROGRESS, progress_msg)
                except Exception as ws_error:
                    logger.warning(f"Failed to send progress update: {ws_error}")

            try:
                # Decode image
                image_bytes = base64.b64decode(img['image'])
                logger.info(f"Decoded image {i+1}: {len(image_bytes)} bytes")

                # Extract table data using OlmOCR
                csv_data = await olmocr_service.extract_table_from_image(image_bytes)
                logger.info(f"OCR completed for image {i+1}: got {len(csv_data)} characters of CSV data")

                # Store result with sheet name
                sheet_name = img['filename']
                if sheet_name.endswith(('.png', '.jpg', '.jpeg')):
                    sheet_name = sheet_name.rsplit('.', 1)[0]

                all_results.append({
                    'sheet_name': sheet_name,
                    'csv_data': csv_data,
                    'filename': img['filename'],
                    'success': True
                })
                successful_count += 1

            except Exception as e:
                logger.error(f"Failed to process image {i+1} for job {job_id}: {str(e)}")
                # Add error result but continue processing
                all_results.append({
                    'sheet_name': img['filename'],
                    'csv_data': None,
                    'error': str(e),
                    'filename': img['filename'],
                    'success': False
                })

        logger.info(f"Finished processing all images for job {job_id}, generating XLSX")

        # Generate consolidated XLSX
        if consolidation_strategy == "separate":
            # Multiple sheets in one file
            xlsx_file = excel_service.create_multi_sheet_xlsx(all_results)
        elif consolidation_strategy == "concatenated":
            # Single sheet with all data concatenated
            xlsx_file = excel_service.create_concatenated_xlsx(all_results)
        else:  # consolidated (default)
            # Multiple sheets in one file (same as separate)
            xlsx_file = excel_service.create_multi_sheet_xlsx(all_results)

        logger.info(f"Generated XLSX file for job {job_id}: {len(xlsx_file)} bytes")

        # Store result file with job_id as file_id
        filename = f"batch_results_{job_id}.xlsx"
        await storage.save_result_file(
            session_id,
            filename,
            xlsx_file,
            file_id=job_id  # Use job_id as file_id for download
        )

        processing_time = (datetime.utcnow() - start_time).total_seconds()
        expires_at = datetime.utcnow() + timedelta(hours=settings.file_retention_hours)
        download_url = f"/api/v1/download/{job_id}"

        logger.info(f"Batch processing completed successfully for job {job_id}")

        # Send completion message via WebSocket
        if redis_service:
            try:
                failed_count = len(images) - successful_count
                status = "completed" if failed_count == 0 else ("partially_completed" if successful_count > 0 else "failed")

                completion_msg = JobCompletedMessage(
                    job_id=job_id,
                    session_id=session_id,
                    status=status,
                    successful_images=successful_count,
                    failed_images=failed_count,
                    download_urls=[download_url],
                    primary_download_url=download_url,
                    processing_time=processing_time,
                    expires_at=expires_at,
                    errors=[r.get('error', '') for r in all_results if not r.get('success', False)]
                )

                # Publish to multiple topics for frontend flexibility
                await redis_service.publish_message(WebSocketTopics.job_topic(job_id), completion_msg)
                await redis_service.publish_message(WebSocketTopics.session_topic(session_id), completion_msg)
                await redis_service.publish_message(WebSocketTopics.JOB_COMPLETED, completion_msg)
                logger.info(f"Sent completion WebSocket message for job {job_id}")
            except Exception as ws_error:
                logger.warning(f"Failed to send completion update: {ws_error}")

    except Exception as e:
        logger.error(f"Batch processing failed for job {job_id}: {str(e)}")

        # Send error message via WebSocket
        if redis_service:
            try:
                error_msg = JobErrorMessage(
                    job_id=job_id,
                    session_id=session_id,
                    error=str(e)
                )
                await redis_service.publish_message(WebSocketTopics.job_topic(job_id), error_msg)
                await redis_service.publish_message(WebSocketTopics.session_topic(session_id), error_msg)
                await redis_service.publish_message(WebSocketTopics.JOB_ERROR, error_msg)
            except Exception as ws_error:
                logger.warning(f"Failed to send error update: {ws_error}")

        # Create error XLSX file
        try:
            error_xlsx = excel_service._create_empty_xlsx_with_message(
                f"Batch processing failed: {str(e)}",
                "Error"
            )
            filename = f"batch_error_{job_id}.xlsx"
            await storage.save_result_file(
                session_id,
                filename,
                error_xlsx,
                file_id=job_id
            )
            logger.info(f"Created error file for failed job {job_id}")
        except Exception as save_error:
            logger.error(f"Failed to save error file for job {job_id}: {str(save_error)}")

    finally:
        # Clean up Redis connection
        if redis_service:
            try:
                await redis_service.close()
            except Exception as e:
                logger.warning(f"Failed to close Redis service: {e}")