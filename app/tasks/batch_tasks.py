"""
Celery tasks for batch image processing.

This module contains all Celery tasks for concurrent image processing,
including single image processing, batch coordination, and cleanup tasks.
"""

import asyncio
import base64
import logging
import traceback
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

from celery import Task
from celery.exceptions import Retry, WorkerLostError
from celery.signals import task_prerun, task_postrun, task_failure

from app.tasks.celery_app import celery_app
from app.services.olmocr import get_olmocr_service
from app.services.excel import ExcelService
from app.services.storage import FileStorageManager
from app.services.redis_service import RedisService
from app.services.supabase_service import get_supabase_service
from app.models.jobs import ImageProcessingResult
from app.models.websocket import (
    JobProgressUpdate,
    JobCompletedMessage,
    SingleFileCompletedMessage,
    ProcessedFileInfo,
    WebSocketTopics
)
from app.core.config import settings

logger = logging.getLogger(__name__)


class BaseTask(Task):
    """Base task class with common functionality."""
    
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Handle task failure."""
        logger.error(f"Task {task_id} failed: {exc}")
        logger.error(f"Traceback: {einfo}")
    
    def on_retry(self, exc, task_id, args, kwargs, einfo):
        """Handle task retry."""
        logger.warning(f"Task {task_id} retrying: {exc}")
    
    def on_success(self, retval, task_id, args, kwargs):
        """Handle task success."""
        logger.info(f"Task {task_id} completed successfully")


@celery_app.task(
    bind=True,
    base=BaseTask,
    name="process_single_image",
    max_retries=3,
    default_retry_delay=60,
    autoretry_for=(Exception,),
    retry_kwargs={'max_retries': 3, 'countdown': 60},
    queue='image_processing',
    priority=8
)
def process_single_image(self, image_data: str, image_id: str, job_id: str) -> Dict[str, Any]:
    """
    Process a single image through OCR and return extracted data.
    
    Args:
        image_data: Base64 encoded image data
        image_id: Unique identifier for the image
        job_id: Parent job identifier
    
    Returns:
        Dictionary containing processing results
    """
    redis_service = None
    main_loop = None
    
    try:
        logger.info(f"Processing image {image_id} for job {job_id}")
        
        # Create and set a single event loop for this task
        main_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(main_loop)
        
        # Initialize services
        redis_service = RedisService()
        main_loop.run_until_complete(redis_service.initialize())
        
        olmocr_service = get_olmocr_service()
        
        # Update job status
        main_loop.run_until_complete(redis_service.update_job(job_id, {
            'status': 'processing',
            'current_image': image_id,
            'updated_at': datetime.utcnow().isoformat()
        }))
        
        # Process image through OlmOCR
        start_time = datetime.utcnow()
        
        # Decode base64 image data to bytes
        try:
            # Handle data URL format (data:image/png;base64,{base64_data})
            if image_data.startswith('data:'):
                # Extract base64 part after comma
                image_data = image_data.split(',', 1)[1]
            
            image_bytes = base64.b64decode(image_data)
        except Exception as e:
            raise Exception(f"Invalid base64 image data: {str(e)}")
        
        # Run async OCR processing with rate limiting using main loop
        try:
            ocr_result = main_loop.run_until_complete(
                olmocr_service.extract_table_from_image(image_bytes)
            )
        except Exception as ocr_error:
            logger.error(f"OlmOCR processing failed for image {image_id}: {ocr_error}")
            raise ocr_error
        
        processing_time = (datetime.utcnow() - start_time).total_seconds()
        
        # Create processing result
        result = {
            'image_id': image_id,
            'status': 'success',
            'extracted_data': ocr_result,
            'processing_time': processing_time,
            'processed_at': datetime.utcnow().isoformat(),
            'worker_id': self.request.hostname
        }
        
        logger.info(f"Image {image_id} processed successfully in {processing_time:.2f}s")
        return result
        
    except Exception as exc:
        error_msg = f"Failed to process image {image_id}: {str(exc)}"
        logger.error(error_msg)
        logger.error(f"Traceback: {traceback.format_exc()}")
        
        # Update job with error
        if redis_service and main_loop:
            try:
                main_loop.run_until_complete(redis_service.update_job(job_id, {
                    'status': 'failed',
                    'error': error_msg,
                    'updated_at': datetime.utcnow().isoformat()
                }))
            except Exception as e:
                logger.error(f"Failed to update job status: {e}")
        
        # Return error result
        return {
            'image_id': image_id,
            'status': 'error',
            'error': error_msg,
            'processed_at': datetime.utcnow().isoformat(),
            'worker_id': self.request.hostname
        }
    
    finally:
        # Cleanup services
        if redis_service and main_loop:
            try:
                main_loop.run_until_complete(redis_service.close())
            except Exception as e:
                logger.error(f"Error closing Redis service: {e}")
        
        # Close event loop
        if main_loop:
            try:
                main_loop.close()
            except Exception as e:
                logger.error(f"Error closing event loop: {e}")


@celery_app.task(
    bind=True,
    base=BaseTask,
    name="process_batch_images",
    max_retries=2,
    default_retry_delay=120,
    queue='batch_processing',
    priority=6
)
def process_batch_images(self, job_id: str, images: List[Dict[str, str]]) -> Dict[str, Any]:
    """
    Coordinate batch processing of multiple images with PARALLEL PROCESSING.

    This Celery task now uses the same parallel processing logic as the direct processing
    path to ensure consistent performance improvements across all deployment modes.

    Args:
        job_id: Unique job identifier
        images: List of image data dictionaries

    Returns:
        Dictionary containing batch processing results
    """
    main_loop = None

    try:
        logger.info(f"[Celery] Starting PARALLEL batch processing for job {job_id} with {len(images)} images")

        # Create and set a single event loop for this task
        main_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(main_loop)

        # Use the same parallel processing logic as direct processing
        # This ensures both Celery and direct paths benefit from concurrent processing
        result = main_loop.run_until_complete(_process_batch_images_direct_async(job_id, images))

        logger.info(f"[Celery] Parallel batch processing completed for job {job_id}")
        return result

    except Exception as exc:
        error_msg = f"[Celery] Batch processing failed for job {job_id}: {str(exc)}"
        logger.error(error_msg)
        logger.error(f"[Celery] Traceback: {traceback.format_exc()}")
        raise self.retry(exc=exc, countdown=120, max_retries=2)

    finally:
        # Close event loop
        if main_loop:
            try:
                main_loop.close()
            except Exception as e:
                logger.error(f"[Celery] Error closing event loop: {e}")


@celery_app.task(
    bind=True,
    base=BaseTask,
    name="cleanup_job_files",
    max_retries=1,
    queue='cleanup',
    priority=1
)
def cleanup_job_files(self, job_id: str) -> Dict[str, Any]:
    """
    Clean up temporary files for a completed job.
    
    Args:
        job_id: Job identifier
    
    Returns:
        Cleanup result dictionary
    """
    main_loop = None
    redis_service = None
    
    try:
        logger.info(f"Starting cleanup for job {job_id}")
        
        # Create and set a single event loop for this task
        main_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(main_loop)
        
        # Initialize services
        storage_service = FileStorageManager()
        redis_service = RedisService()
        main_loop.run_until_complete(redis_service.initialize())
        
        # Get job data
        job_data = main_loop.run_until_complete(redis_service.get_job(job_id))
        
        if not job_data:
            logger.warning(f"Job {job_id} not found for cleanup")
            return {'status': 'skipped', 'reason': 'job_not_found'}
        
        # Clean up files
        cleaned_files = main_loop.run_until_complete(
            storage_service.cleanup_job_files(job_id)
        )
        
        # Remove job from Redis (optional - keep for audit trail)
        # await redis_service.delete_job(job_id)
        
        logger.info(f"Cleanup completed for job {job_id}: {cleaned_files} files removed")
        
        return {
            'status': 'completed',
            'job_id': job_id,
            'files_cleaned': cleaned_files,
            'cleaned_at': datetime.utcnow().isoformat()
        }
        
    except Exception as exc:
        logger.error(f"Cleanup failed for job {job_id}: {exc}")
        return {
            'status': 'failed',
            'job_id': job_id,
            'error': str(exc),
            'failed_at': datetime.utcnow().isoformat()
        }
    
    finally:
        # Cleanup Redis service
        if redis_service and main_loop:
            try:
                main_loop.run_until_complete(redis_service.close())
            except Exception as e:
                logger.error(f"Error closing Redis service: {e}")
        
        # Close event loop
        if main_loop:
            try:
                main_loop.close()
            except Exception as e:
                logger.error(f"Error closing event loop: {e}")


@celery_app.task(
    bind=True,
    base=BaseTask,
    name="cleanup_expired_jobs",
    queue='cleanup',
    priority=1
)
def cleanup_expired_jobs(self) -> Dict[str, Any]:
    """
    Periodic task to clean up expired jobs and files.
    
    Returns:
        Cleanup statistics
    """
    main_loop = None
    redis_service = None
    
    try:
        logger.info("Starting periodic cleanup of expired jobs")
        
        # Create and set a single event loop for this task
        main_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(main_loop)
        
        # Initialize Redis service
        redis_service = RedisService()
        main_loop.run_until_complete(redis_service.initialize())
        
        # Clean up expired jobs from Redis
        expired_jobs = main_loop.run_until_complete(redis_service.cleanup_expired_jobs())
        
        # Clean up orphaned files
        storage_service = FileStorageManager()
        orphaned_files = main_loop.run_until_complete(
            storage_service.cleanup_orphaned_files()
        )
        
        logger.info(f"Periodic cleanup completed: {expired_jobs} jobs, {orphaned_files} files")
        
        return {
            'status': 'completed',
            'expired_jobs': expired_jobs,
            'orphaned_files': orphaned_files,
            'cleaned_at': datetime.utcnow().isoformat()
        }
        
    except Exception as exc:
        logger.error(f"Periodic cleanup failed: {exc}")
        return {
            'status': 'failed',
            'error': str(exc),
            'failed_at': datetime.utcnow().isoformat()
        }
    
    finally:
        # Cleanup Redis service
        if redis_service and main_loop:
            try:
                main_loop.run_until_complete(redis_service.close())
            except Exception as e:
                logger.error(f"Error closing Redis service: {e}")
        
        # Close event loop
        if main_loop:
            try:
                main_loop.close()
            except Exception as e:
                logger.error(f"Error closing event loop: {e}")



# Signal handlers for monitoring and logging

@task_prerun.connect
def task_prerun_handler(task_id, task, *args, **kwargs):
    """Log task start."""
    logger.info(f"Task {task.name}[{task_id}] started")


@task_postrun.connect
def task_postrun_handler(task_id, task, *args, **kwargs):
    """Log task completion."""
    logger.info(f"Task {task.name}[{task_id}] completed")


@task_failure.connect
def task_failure_handler(task_id, exception, einfo, *args, **kwargs):
    """Log task failure."""
    logger.error(f"Task {task_id} failed: {exception}")
    logger.error(f"Error info: {einfo}")


def process_batch_images_direct(job_id: str, images: List[Dict[str, str]]):
    """
    Direct processing function that works without Celery.
    This is a synchronous wrapper that handles batch processing in a background thread.
    """
    import asyncio

    loop = None
    try:
        logger.info(f"[Direct] Starting batch processing for job {job_id}")

        # Create new event loop for this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # Run the batch processing logic directly
        result = loop.run_until_complete(_process_batch_images_direct_async(job_id, images))
        logger.info(f"[Direct] Batch processing completed for job {job_id}")
        return result

    except Exception as e:
        logger.error(f"[Direct] Batch processing failed for job {job_id}: {str(e)}")
        logger.error(f"[Direct] Traceback: {traceback.format_exc()}")

        # Try to update job status to failed
        if loop:
            try:
                from app.services.redis_service import RedisService
                redis_service = RedisService()
                loop.run_until_complete(redis_service.initialize())
                loop.run_until_complete(redis_service.update_job(job_id, {
                    'status': 'failed',
                    'error': str(e),
                    'updated_at': datetime.utcnow().isoformat()
                }))
                logger.info(f"[Direct] Updated job {job_id} status to failed")
            except Exception as update_error:
                logger.error(f"[Direct] Failed to update failed job status: {update_error}")

    finally:
        # Clean up the event loop
        if loop:
            try:
                loop.close()
            except:
                pass


async def _process_single_image_concurrent(
    img: Dict[str, str],
    img_index: int,
    total_images: int,
    job_id: str,
    session_id: str,
    redis_service: 'RedisService',
    olmocr_service: 'OlmOCRService',
    excel_service: ExcelService,
    storage_service: FileStorageManager,
    job_data: Dict[str, Any],
    semaphore: asyncio.Semaphore,
    processed_count: List[int]  # Shared list to track processed count
) -> Dict[str, Any]:
    """
    Process a single image concurrently with semaphore limiting.

    Args:
        img: Image data dictionary
        img_index: Index of this image in the batch
        total_images: Total number of images in batch
        job_id: Job identifier
        session_id: Session identifier
        redis_service: Redis service instance
        olmocr_service: OlmOCR service instance
        excel_service: Excel service instance
        storage_service: Storage service instance
        job_data: Job data dictionary
        semaphore: Semaphore to limit concurrent processing
        processed_count: Shared list to track how many images have been processed

    Returns:
        Dictionary containing result (success/error) and file info if successful
    """
    async with semaphore:  # Limit concurrent API requests
        try:
            logger.info(f"[Concurrent] Processing image {img_index+1}/{total_images} for job {job_id}")

            # Decode image
            image_data = img['data']
            if image_data.startswith('data:'):
                image_data = image_data.split(',', 1)[1]
            image_bytes = base64.b64decode(image_data)

            # Extract table data using OlmOCR with rate limiting
            csv_data = await olmocr_service.extract_table_from_image(image_bytes)

            # Create Excel file
            excel_data = excel_service.csv_to_xlsx(csv_data, f"Table_{img['id']}")

            # Generate filename
            original_filename = img.get('filename', f"image_{img['id']}")
            base_name = original_filename.split('.')[0] if '.' in original_filename else original_filename
            excel_filename = f"{base_name}_processed.xlsx"

            # Save file
            file_id = storage_service.save_result_file_sync(session_id, excel_filename, excel_data)

            # Upload to Supabase Storage if user is authenticated
            supabase_url = None
            try:
                if job_data.get('user_id'):
                    supabase_service = get_supabase_service()
                    file_path = f"{job_data['user_id']}/{job_id}/{excel_filename}"
                    supabase_url = await supabase_service.upload_file_to_storage(
                        file_data=excel_data,
                        file_path=file_path
                    )
                    logger.info(f"[Concurrent] Uploaded to Supabase Storage: {file_path}")
            except Exception as upload_error:
                logger.warning(f"[Concurrent] Failed to upload to Supabase Storage: {upload_error}")
                # Continue even if upload fails

            # Update processed count
            processed_count[0] += 1
            current_progress = int((processed_count[0] / total_images) * 100)

            # Update Redis job progress
            current_image_id = img.get('id', img.get('filename', f'image_{img_index+1}'))
            await redis_service.update_job(job_id, {
                'processed_images': processed_count[0],
                'progress': current_progress,
                'current_image': current_image_id,
                'updated_at': datetime.utcnow().isoformat()
            })

            # Publish WebSocket progress update
            progress_message = JobProgressUpdate(
                job_id=job_id,
                status='processing',
                progress=current_progress,
                total_images=total_images,
                processed_images=processed_count[0],
                current_image=current_image_id,
                session_id=session_id
            )

            # Publish ONLY to session topic to prevent duplicate messages
            if session_id:
                await redis_service.publish_message(
                    WebSocketTopics.session_topic(session_id), progress_message
                )
                logger.debug(f"[Concurrent] Published progress update: {processed_count[0]}/{total_images}")

            # PROGRESSIVE RESULTS: Add file to session immediately for download access
            session_metadata = await storage_service.get_session_metadata(session_id)
            if session_metadata:
                if file_id not in session_metadata.result_files:
                    session_metadata.result_files.append(file_id)
                    session_metadata.update_activity()
                    await storage_service.update_session_metadata(session_metadata)
                    logger.info(f"[Concurrent] Added {file_id} to session {session_id} result_files")

            # PROGRESSIVE RESULTS: Send individual file completion message immediately
            from app.models.websocket import SingleFileCompletedMessage
            file_ready_message = SingleFileCompletedMessage(
                job_id=job_id,
                file_info=ProcessedFileInfo(
                    file_id=file_id,
                    download_url=f"/api/v1/download/{file_id}",
                    filename=excel_filename,
                    image_id=img['id'],
                    size_bytes=len(excel_data)
                ),
                image_number=img_index+1,
                total_images=total_images,
                session_id=session_id
            )

            # Publish immediately so user can download this file right away
            if session_id:
                await redis_service.publish_message(
                    WebSocketTopics.session_topic(session_id), file_ready_message
                )
                logger.info(f"[Concurrent] Published file_ready message for {excel_filename}")

            logger.info(f"[Concurrent] Successfully processed image {img_index+1}: {file_id}")

            # Return success result
            return {
                'status': 'success',
                'image_id': img['id'],
                'filename': original_filename,
                'extracted_data': csv_data,
                'file_id': file_id,
                'file_info': {
                    'file_id': file_id,
                    'image_id': img['id'],
                    'filename': excel_filename,
                    'original_filename': original_filename,
                    'size_bytes': len(excel_data),
                    'supabase_url': supabase_url
                },
                'download_url': f"/api/v1/download/{file_id}"
            }

        except Exception as e:
            logger.error(f"[Concurrent] Failed to process image {img_index+1} for job {job_id}: {str(e)}")

            # Update processed count even for failures
            processed_count[0] += 1
            current_progress = int((processed_count[0] / total_images) * 100)

            # Update progress even on failure
            await redis_service.update_job(job_id, {
                'processed_images': processed_count[0],
                'progress': current_progress,
                'updated_at': datetime.utcnow().isoformat()
            })

            return {
                'status': 'error',
                'error': str(e),
                'image_id': img['id'],
                'filename': img.get('filename', f"image_{img['id']}")
            }


async def _process_batch_images_direct_async(job_id: str, images: List[Dict[str, str]], max_concurrent: int = 5):
    """
    Direct async implementation of batch processing with PARALLEL PROCESSING.

    Uses asyncio.gather() to process multiple images concurrently, with a semaphore
    to limit the number of simultaneous API requests.

    Args:
        job_id: Job identifier
        images: List of image data dictionaries
        max_concurrent: Maximum number of concurrent image processing tasks (default: 5)
    """
    from app.services.redis_service import RedisService
    from app.services.olmocr import OlmOCRService
    from app.services.excel import ExcelService
    from app.services.storage import FileStorageManager

    logger.info(f"[Concurrent] Starting parallel batch processing for job {job_id} with {len(images)} images (max {max_concurrent} concurrent)")

    # Initialize services
    redis_service = RedisService()
    await redis_service.initialize()  # IMPORTANT: Initialize Redis connection

    olmocr_service = OlmOCRService()
    excel_service = ExcelService()
    storage_service = FileStorageManager()

    start_time = datetime.utcnow()

    # Get job data to find session ID
    job_data = await redis_service.get_job(job_id)
    if not job_data:
        raise Exception(f"Job {job_id} not found in Redis")

    session_id = job_data['session_id']

    # Update Supabase job status to processing if user is authenticated
    try:
        if job_data.get('user_id'):
            supabase_service = get_supabase_service()
            await supabase_service.update_job_status(
                job_id=job_id,
                status='processing',
                metadata={
                    'total_images': len(images),
                    'started_at': datetime.utcnow().isoformat()
                }
            )
            logger.info(f"[Concurrent] Updated Supabase job {job_id} to processing")
    except Exception as supabase_error:
        logger.warning(f"[Concurrent] Failed to update Supabase job status: {supabase_error}")

    try:
        # PARALLEL PROCESSING: Create semaphore to limit concurrent API requests
        semaphore = asyncio.Semaphore(max_concurrent)

        # Track processed count with a shared list (mutable)
        processed_count = [0]

        # Create concurrent tasks for all images
        logger.info(f"[Concurrent] Creating {len(images)} parallel processing tasks (max {max_concurrent} concurrent)")

        tasks = [
            _process_single_image_concurrent(
                img=img,
                img_index=i,
                total_images=len(images),
                job_id=job_id,
                session_id=session_id,
                redis_service=redis_service,
                olmocr_service=olmocr_service,
                excel_service=excel_service,
                storage_service=storage_service,
                job_data=job_data,
                semaphore=semaphore,
                processed_count=processed_count
            )
            for i, img in enumerate(images)
        ]

        # Execute all tasks concurrently with asyncio.gather()
        logger.info(f"[Concurrent] Executing {len(tasks)} tasks in parallel...")
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        successful_results = []
        failed_results = []
        generated_files = []
        download_urls = []

        for i, result in enumerate(results):
            # Handle exceptions returned by gather()
            if isinstance(result, Exception):
                logger.error(f"[Concurrent] Task {i+1} raised exception: {result}")
                failed_results.append({
                    'error': str(result),
                    'image_id': images[i].get('id', f'image_{i+1}'),
                    'filename': images[i].get('filename', f"image_{images[i]['id']}")
                })
            elif result['status'] == 'success':
                # Success result
                successful_results.append({
                    'image_id': result['image_id'],
                    'filename': result['filename'],
                    'extracted_data': result['extracted_data'],
                    'file_id': result['file_id']
                })

                generated_files.append(result['file_info'])
                download_urls.append(result['download_url'])
            else:
                # Error result
                failed_results.append({
                    'error': result['error'],
                    'image_id': result['image_id'],
                    'filename': result['filename']
                })

        logger.info(f"[Concurrent] Parallel processing completed: {len(successful_results)} successful, {len(failed_results)} failed")
        
        # Calculate final status and processing time
        processing_time = (datetime.utcnow() - start_time).total_seconds()
        
        if len(successful_results) == len(images):
            final_status = 'completed'
        elif len(successful_results) > 0:
            final_status = 'partially_completed'
        else:
            final_status = 'failed'
        
        # Update job with final results in the new format
        final_update = {
            'status': final_status,
            'total_images': len(images),
            'processed_images': len(successful_results),
            'progress': 100,
            'processing_time': processing_time,
            'completed_at': datetime.utcnow().isoformat(),
            'updated_at': datetime.utcnow().isoformat(),
            'results': successful_results,
            'errors': failed_results
        }
        
        # Store generated files information
        if generated_files:
            final_update['generated_files'] = generated_files
            final_update['download_urls'] = download_urls
            
            # For backward compatibility with download endpoint
            if generated_files:
                final_update['file_id'] = generated_files[0]['file_id']
                final_update['download_url'] = download_urls[0]
        
        await redis_service.update_job(job_id, final_update)

        # Build files list for WebSocket message
        from app.models.websocket import ProcessedFileInfo
        files_info = []
        for file_data in generated_files:
            files_info.append(ProcessedFileInfo(
                file_id=file_data['file_id'],
                download_url=f"/api/v1/download/{file_data['file_id']}",
                filename=file_data['filename'],
                image_id=file_data.get('image_id'),
                size_bytes=file_data.get('size_bytes')
            ))

        # Publish WebSocket completion message
        completion_message = JobCompletedMessage(
            job_id=job_id,
            status=final_status,
            successful_images=len(successful_results),
            failed_images=len(failed_results),
            files=files_info,
            download_urls=download_urls if download_urls else [],
            primary_download_url=download_urls[0] if download_urls else None,
            processing_time=processing_time,
            expires_at=datetime.utcnow() + timedelta(hours=settings.file_retention_hours),
            session_id=session_id
        )

        # Publish ONLY to session topic to prevent duplicate messages
        if session_id:
            await redis_service.publish_message(
                WebSocketTopics.session_topic(session_id), completion_message
            )
            logger.info(f"[Direct] Published completion message to session topic: {session_id}")

        # Update Supabase with final status if user is authenticated
        try:
            if job_data.get('user_id'):
                # Get the first Supabase URL if available
                result_url = None
                if generated_files and generated_files[0].get('supabase_url'):
                    result_url = generated_files[0]['supabase_url']

                supabase_service = get_supabase_service()
                await supabase_service.update_job_status(
                    job_id=job_id,
                    status=final_status,
                    result_url=result_url,
                    metadata={
                        'total_images': len(images),
                        'processed_images': len(successful_results),
                        'failed_images': len(failed_results),
                        'processing_time': processing_time,
                        'files_generated': len(generated_files),
                        'completed_at': datetime.utcnow().isoformat()
                    }
                )
                logger.info(f"[Direct] Updated Supabase job {job_id} with final status {final_status}")
        except Exception as supabase_error:
            logger.warning(f"[Direct] Failed to update Supabase final status: {supabase_error}")

        logger.info(f"Direct batch processing completed for job {job_id}: "
                   f"{len(successful_results)}/{len(images)} successful")

        return {
            'job_id': job_id,
            'status': final_status,
            'successful': len(successful_results),
            'failed': len(failed_results),
            'files_generated': len(generated_files)
        }

    except Exception as e:
        logger.error(f"[Direct Async] Error processing batch: {str(e)}")
        logger.error(f"[Direct Async] Traceback: {traceback.format_exc()}")

        # Update job status to failed
        try:
            await redis_service.update_job(job_id, {
                'status': 'failed',
                'error': str(e),
                'updated_at': datetime.utcnow().isoformat()
            })

            # Update Supabase with error status if user is authenticated
            try:
                if job_data and job_data.get('user_id'):
                    supabase_service = get_supabase_service()
                    await supabase_service.update_job_status(
                        job_id=job_id,
                        status='failed',
                        error_message=str(e)
                    )
                    logger.info(f"[Direct] Updated Supabase job {job_id} with error status")
            except Exception as supabase_error:
                logger.warning(f"[Direct] Failed to update Supabase error status: {supabase_error}")

        except Exception as update_error:
            logger.error(f"[Direct Async] Failed to update job status: {update_error}")

        raise e

    finally:
        # Cleanup Redis connection
        try:
            await redis_service.close()
        except Exception as cleanup_error:
            logger.error(f"[Direct Async] Error closing Redis: {cleanup_error}")


# Export tasks for import
__all__ = [
    'process_single_image',
    'process_batch_images', 
    'cleanup_job_files',
    'cleanup_expired_jobs',
    'process_batch_images_direct'
]