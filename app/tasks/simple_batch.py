"""
Simple async batch image processor.

One job_id → One background task → Process all images with asyncio.gather()
No queues, no workers, just clean async processing.
"""

import asyncio
import logging
from typing import Dict, Any, List
from datetime import datetime, timedelta

from app.services.redis_service import get_redis_service
from app.services.olmocr import get_olmocr_service
from app.services.excel import ExcelService
from app.services.storage import FileStorageManager
from app.services.supabase_service import get_supabase_service
from app.models.websocket import (
    JobProgressUpdate,
    JobCompletedMessage,
    SingleFileCompletedMessage,
    ProcessedFileInfo,
    WebSocketTopics
)
from app.core.config import settings

logger = logging.getLogger(__name__)


async def process_single_image_simple(
    img: Dict[str, str],
    img_index: int,
    total_images: int,
    job_id: str,
    session_id: str,
    user_id: str = None
) -> Dict[str, Any]:
    """
    Process a single image and return result.
    Simple, isolated function - no shared state.
    """
    redis = await get_redis_service()
    olmocr = get_olmocr_service()
    excel = ExcelService()
    storage = FileStorageManager()

    try:
        logger.info(f"[Job {job_id}] Processing image {img_index+1}/{total_images}")

        # Get base64 image data (already encoded from upload)
        image_data = img['data']
        # Remove data URL prefix if present
        if image_data.startswith('data:'):
            image_data = image_data.split(',', 1)[1]

        # Extract table with OlmOCR - pass base64 string directly (optimization!)
        # No need to decode→encode, olmocr service handles both formats
        csv_data = await olmocr.extract_table_from_image(image_data)

        # Create Excel
        excel_data = excel.csv_to_xlsx(csv_data, f"Table_{img['id']}")

        # Generate filename
        original_filename = img.get('filename', f"image_{img['id']}")
        base_name = original_filename.split('.')[0] if '.' in original_filename else original_filename
        excel_filename = f"{base_name}_processed.xlsx"

        # Save file
        file_id = storage.save_result_file_sync(session_id, excel_filename, excel_data)

        # Upload to Supabase if authenticated
        supabase_url = None
        if user_id:
            try:
                supabase = get_supabase_service()
                file_path = f"{user_id}/{job_id}/{excel_filename}"
                supabase_url = await supabase.upload_file_to_storage(
                    file_data=excel_data,
                    file_path=file_path
                )
                logger.info(f"[Job {job_id}] Uploaded to Supabase: {file_path}")
            except Exception as upload_error:
                logger.warning(f"[Job {job_id}] Supabase upload failed: {upload_error}")

        # Get current job to read processed count (with fallback if Redis unavailable)
        job_data = await redis.get_job(job_id)
        if not job_data:
            logger.warning(f"Job {job_id} not found in Redis (may be unavailable), using estimated progress")
            # Estimate progress without Redis
            processed_count = img_index + 1
        else:
            # Increment processed count
            processed_count = int(job_data.get('processed_images', 0)) + 1

        current_progress = int((processed_count / total_images) * 100)

        # Update job (will fail silently if Redis unavailable)
        await redis.update_job(job_id, {
            'processed_images': processed_count,
            'progress': current_progress,
            'current_image': img.get('id'),
            'updated_at': datetime.utcnow().isoformat()
        })

        # Publish progress (will fail silently if Redis unavailable)
        try:
            progress_message = JobProgressUpdate(
                job_id=job_id,
                status='processing',
                progress=current_progress,
                total_images=total_images,
                processed_images=processed_count,
                current_image=img.get('id'),
                session_id=session_id
            )
            await redis.publish_message(
                WebSocketTopics.session_topic(session_id),
                progress_message
            )
        except Exception as pub_error:
            logger.debug(f"Failed to publish progress (Redis may be unavailable): {pub_error}")

        # Publish file_ready for progressive download
        file_info = ProcessedFileInfo(
            file_id=file_id,
            download_url=f"/api/v1/download/{file_id}",
            filename=excel_filename,
            image_id=img.get('id'),
            size_bytes=len(excel_data)
        )

        # Publish file_ready (will fail silently if Redis unavailable)
        try:
            file_ready_message = SingleFileCompletedMessage(
                job_id=job_id,
                file_info=file_info,
                image_number=processed_count,
                total_images=total_images,
                session_id=session_id
            )
            await redis.publish_message(
                WebSocketTopics.session_topic(session_id),
                file_ready_message
            )
        except Exception as pub_error:
            logger.debug(f"Failed to publish file_ready (Redis may be unavailable): {pub_error}")

        logger.info(f"[Job {job_id}] Image {img_index+1}/{total_images} completed")

        return {
            'status': 'success',
            'image_id': img.get('id'),
            'filename': excel_filename,
            'file_id': file_id,
            'file_info': {
                'file_id': file_id,
                'filename': excel_filename,
                'image_id': img.get('id'),
                'size_bytes': len(excel_data),
                'supabase_url': supabase_url
            },
            'download_url': f"/api/v1/download/{file_id}"
        }

    except Exception as e:
        error_msg = f"Failed to process image {img_index+1}: {str(e)}"
        logger.error(f"[Job {job_id}] {error_msg}", exc_info=True)

        # Update job with error (gracefully handle Redis unavailability)
        try:
            job_data = await redis.get_job(job_id)
            if job_data:
                errors = job_data.get('errors', [])
                if isinstance(errors, str):
                    import json
                    errors = json.loads(errors) if errors else []
                errors.append(error_msg)

                failed_count = int(job_data.get('failed_images', 0)) + 1

                await redis.update_job(job_id, {
                    'failed_images': failed_count,
                    'errors': errors,
                    'updated_at': datetime.utcnow().isoformat()
                })
        except Exception as redis_error:
            logger.debug(f"Failed to update error in Redis: {redis_error}")

        return {
            'status': 'error',
            'image_id': img.get('id'),
            'filename': img.get('filename'),
            'error': error_msg
        }


async def process_batch_simple(
    job_id: str,
    session_id: str,
    images: List[Dict[str, str]],
    user_id: str = None
):
    """
    Process a batch of images with simple asyncio.gather().

    This is the main entry point. It:
    1. Processes all images concurrently (up to 5 at a time)
    2. Updates the SAME job_id in Redis
    3. Sends completion message when done

    Simple and straightforward.
    """
    redis = await get_redis_service()
    storage = FileStorageManager()
    start_time = datetime.utcnow()

    logger.info(f"[Job {job_id}] Starting batch processing: {len(images)} images")

    try:
        # Update job to processing (gracefully handle Redis unavailability)
        try:
            await redis.update_job(job_id, {
                'status': 'processing',
                'updated_at': datetime.utcnow().isoformat()
            })
        except Exception as redis_error:
            logger.warning(f"[Job {job_id}] Failed to update job status in Redis: {redis_error}")

        # Process images with semaphore to limit concurrency
        semaphore = asyncio.Semaphore(5)  # Max 5 concurrent

        async def process_with_semaphore(img, idx):
            async with semaphore:
                return await process_single_image_simple(
                    img=img,
                    img_index=idx,
                    total_images=len(images),
                    job_id=job_id,
                    session_id=session_id,
                    user_id=user_id
                )

        # Process all images concurrently
        tasks = [process_with_semaphore(img, i) for i, img in enumerate(images)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Count successes and failures
        successful_results = []
        failed_results = []
        generated_files = []
        download_urls = []

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"[Job {job_id}] Task {i+1} raised exception: {result}")
                failed_results.append({
                    'error': str(result),
                    'image_id': images[i].get('id', f'image_{i+1}'),
                    'filename': images[i].get('filename', f"image_{i}.png")
                })
            elif result['status'] == 'success':
                successful_results.append(result)
                generated_files.append(result['file_info'])
                download_urls.append(result['download_url'])
            else:
                failed_results.append(result)

        # Determine final status
        if len(successful_results) == len(images):
            final_status = 'completed'
        elif len(successful_results) > 0:
            final_status = 'partially_completed'
        else:
            final_status = 'failed'

        processing_time = (datetime.utcnow() - start_time).total_seconds()

        # Update job with final results (gracefully handle Redis unavailability)
        try:
            await redis.update_job(job_id, {
                'status': final_status,
                'progress': 100,
                'processing_time': processing_time,
                'completed_at': datetime.utcnow().isoformat(),
                'generated_files': generated_files,
                'download_urls': download_urls,
                'file_id': generated_files[0]['file_id'] if generated_files else None,
                'download_url': download_urls[0] if download_urls else None
            })
            logger.info(f"[Job {job_id}] Updated final status in Redis: {final_status}")
        except Exception as redis_error:
            logger.warning(f"[Job {job_id}] Failed to update final status in Redis: {redis_error}")

        # Build files list for completion message
        files_info = []
        for file_data in generated_files:
            files_info.append(ProcessedFileInfo(
                file_id=file_data['file_id'],
                download_url=f"/api/v1/download/{file_data['file_id']}",
                filename=file_data['filename'],
                image_id=file_data.get('image_id'),
                size_bytes=file_data.get('size_bytes')
            ))

        # Send completion message (gracefully handle Redis unavailability)
        try:
            completion_message = JobCompletedMessage(
                job_id=job_id,
                status=final_status,
                successful_images=len(successful_results),
                failed_images=len(failed_results),
                files=files_info,
                download_urls=download_urls,
                primary_download_url=download_urls[0] if download_urls else None,
                processing_time=processing_time,
                expires_at=datetime.utcnow() + timedelta(hours=settings.file_retention_hours),
                session_id=session_id
            )

            await redis.publish_message(
                WebSocketTopics.session_topic(session_id),
                completion_message
            )
            logger.info(f"[Job {job_id}] Published completion message")
        except Exception as pub_error:
            logger.warning(f"[Job {job_id}] Failed to publish completion message: {pub_error}")

        logger.info(f"[Job {job_id}] Completed: {len(successful_results)} successful, {len(failed_results)} failed")

    except Exception as e:
        logger.error(f"[Job {job_id}] Batch processing failed: {e}", exc_info=True)

        # Mark job as failed (gracefully handle Redis unavailability)
        try:
            await redis.update_job(job_id, {
                'status': 'failed',
                'error': str(e),
                'updated_at': datetime.utcnow().isoformat()
            })
        except Exception as redis_error:
            logger.warning(f"[Job {job_id}] Failed to update failed status in Redis: {redis_error}")
