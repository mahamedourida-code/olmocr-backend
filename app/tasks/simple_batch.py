"""
Simple async batch image processor.

One job_id → One background task → Process all images with asyncio.gather()
No queues, no workers, just clean async processing.
"""

import asyncio
import json
import logging
import uuid
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


def _deterministic_file_id(job_id: str, image_id: str) -> str:
    return uuid.uuid5(uuid.NAMESPACE_URL, f"axliner:{job_id}:{image_id}").hex


def _result_from_file_info(file_info: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'status': 'success',
        'image_id': file_info.get('image_id'),
        'filename': file_info.get('filename'),
        'file_id': file_info.get('file_id'),
        'file_info': file_info,
        'download_url': f"/api/v1/download/{file_info.get('file_id')}",
        'skipped': True
    }


def _safe_filename_part(value: str) -> str:
    return "".join(
        char if char.isalnum() or char in ("-", "_") else "_"
        for char in str(value)
    ).strip("_") or "image"


def _image_id_for(img: Dict[str, Any], img_index: int) -> str:
    return str(img.get('id') or f"img_{img_index}")


async def _restore_image_results_from_supabase(redis, job_id: str, user_id: str = None) -> None:
    if not user_id:
        return

    try:
        supabase = get_supabase_service()
        job_record = await supabase.get_job(job_id)
        metadata = job_record.get('processing_metadata') if job_record else {}
        if isinstance(metadata, str):
            metadata = json.loads(metadata) if metadata else {}

        image_results = metadata.get('image_results') if isinstance(metadata, dict) else {}
        if not isinstance(image_results, dict):
            return

        for image_id, result_data in image_results.items():
            if not isinstance(result_data, dict) or result_data.get('status') != 'success':
                continue

            file_info = result_data.get('file_info') or {}
            file_id = file_info.get('file_id')
            storage_path = file_info.get('storage_path')
            if not file_id or not storage_path:
                continue

            await redis.set_job_image_result(job_id, image_id, result_data, settings.job_expiry_seconds)
            await redis.set_cache(
                f"file:{file_id}",
                {
                    "storage_path": storage_path,
                    "filename": file_info.get('filename', f"{file_id}.xlsx"),
                    "content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    "job_id": job_id,
                    "session_id": file_info.get('session_id', ''),
                    "user_id": file_info.get('user_id', user_id),
                    "image_id": image_id,
                    "file_id": file_id,
                    "size_bytes": file_info.get('size_bytes')
                },
                settings.file_retention_seconds
            )
    except Exception as e:
        logger.debug(f"[Job {job_id}] Failed to restore image results from Supabase: {e}")


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
    image_id = _image_id_for(img, img_index)
    file_id = _deterministic_file_id(job_id, image_id)

    try:
        existing_result = await redis.get_job_image_result(job_id, image_id)
        if existing_result and existing_result.get('status') == 'success':
            file_info = existing_result.get('file_info') or existing_result
            if file_info.get('file_id') and file_info.get('storage_path'):
                logger.info(f"[Job {job_id}] Skipping image {image_id}; completed result already exists")
                return _result_from_file_info(file_info)

        olmocr = get_olmocr_service()
        excel = ExcelService()
        storage = FileStorageManager()

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
        excel_data = excel.csv_to_xlsx(csv_data, f"Table_{image_id}")

        # Generate filename
        original_filename = img.get('filename', f"image_{image_id}")
        base_name = original_filename.split('.')[0] if '.' in original_filename else original_filename
        excel_filename = f"{base_name}_{_safe_filename_part(image_id)}_processed.xlsx"

        # Save file with a deterministic file_id so retries overwrite, not duplicate.
        file_id = storage.save_result_file_sync(session_id, excel_filename, excel_data, file_id=file_id)

        # Upload result to durable storage so downloads work from separate workers.
        supabase_url = None
        supabase_storage_path = None
        try:
            supabase = get_supabase_service()
            storage_owner_id = user_id or session_id
            upload_result = await supabase.upload_file_to_storage(
                file_data=excel_data,
                file_path=f"users/{storage_owner_id}/jobs/{job_id}/{excel_filename}",
                user_id=storage_owner_id,
                job_id=job_id,
                filename=excel_filename
            )
            supabase_url = upload_result.get('signed_url')
            supabase_storage_path = upload_result.get('storage_path')
            if supabase_storage_path:
                file_metadata = {
                    "storage_path": supabase_storage_path,
                    "filename": excel_filename,
                    "content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    "job_id": job_id,
                    "session_id": session_id,
                    "user_id": user_id or "",
                    "image_id": image_id,
                    "file_id": file_id,
                    "size_bytes": len(excel_data),
                    "completed_at": datetime.utcnow().isoformat()
                }
                await redis.set_cache(f"file:{file_id}", file_metadata, settings.file_retention_seconds)
            logger.info(f"[Job {job_id}] Uploaded result to Supabase: {supabase_storage_path}")
        except Exception as upload_error:
            logger.warning(f"[Job {job_id}] Supabase result upload failed: {upload_error}")

        file_record = {
            'file_id': file_id,
            'filename': excel_filename,
            'image_id': image_id,
            'size_bytes': len(excel_data),
            'supabase_url': supabase_url,
            'storage_path': supabase_storage_path,
            'session_id': session_id,
            'user_id': user_id or "",
            'completed_at': datetime.utcnow().isoformat()
        }

        if supabase_storage_path:
            await redis.set_job_image_result(
                job_id,
                image_id,
                {
                    'status': 'success',
                    'image_id': image_id,
                    'file_info': file_record,
                    'download_url': f"/api/v1/download/{file_id}"
                },
                settings.job_expiry_seconds
            )

        # Get current job to read processed count (with fallback if Redis unavailable)
        completed_results = await redis.get_job_image_results(job_id)
        processed_count = len(completed_results) if completed_results else img_index + 1

        current_progress = int((processed_count / total_images) * 100)
        completed_file_records = [
            result.get('file_info')
            for result in completed_results.values()
            if isinstance(result, dict) and result.get('status') == 'success' and result.get('file_info')
        ]

        # Update job (will fail silently if Redis unavailable)
        await redis.update_job(job_id, {
            'processed_images': processed_count,
            'progress': current_progress,
            'current_image': image_id,
            'updated_at': datetime.utcnow().isoformat()
        })

        if user_id:
            try:
                supabase = get_supabase_service()
                await supabase.update_job_status(
                    job_id=job_id,
                    status='processing',
                    metadata={
                        'progress': current_progress,
                        'processed_images': processed_count,
                        'total_images': total_images,
                        'generated_files': completed_file_records,
                        'image_results': completed_results
                    }
                )
            except Exception as e:
                logger.debug(f"Failed to update Supabase progress: {e}")

        # Publish progress (will fail silently if Redis unavailable)
        try:
            progress_message = JobProgressUpdate(
                job_id=job_id,
                status='processing',
                progress=current_progress,
                total_images=total_images,
                processed_images=processed_count,
                current_image=image_id,
                session_id=session_id
            )
            await redis.publish_message(
                WebSocketTopics.session_topic(session_id),
                progress_message
            )
        except Exception as pub_error:
            logger.debug(f"Failed to publish progress (Redis may be unavailable): {pub_error}")

        # Publish file_ready for progressive download
        ready_file_info = ProcessedFileInfo(
            file_id=file_id,
            download_url=f"/api/v1/download/{file_id}",
            filename=excel_filename,
            image_id=image_id,
            size_bytes=len(excel_data)
        )

        # Publish file_ready (will fail silently if Redis unavailable)
        try:
            file_ready_message = SingleFileCompletedMessage(
                job_id=job_id,
                file_info=ready_file_info,
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
            'image_id': image_id,
            'filename': excel_filename,
            'file_id': file_id,
            'file_info': file_record,
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
            'image_id': image_id,
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
    await _restore_image_results_from_supabase(redis, job_id, user_id)
    
    # Update job status to "processing" in Supabase at start
    if user_id:
        try:
            supabase = get_supabase_service()
            existing_results = await redis.get_job_image_results(job_id)
            existing_files = [
                result.get('file_info')
                for result in existing_results.values()
                if isinstance(result, dict) and result.get('status') == 'success' and result.get('file_info')
            ]
            await supabase.update_job_status(
                job_id=job_id,
                status='processing',
                metadata={
                    'started_at': datetime.utcnow().isoformat(),
                    'total_images': len(images),
                    'generated_files': existing_files,
                    'image_results': existing_results
                }
            )
            logger.info(f"[Job {job_id}] Updated Supabase status to processing")
        except Exception as e:
            logger.warning(f"[Job {job_id}] Failed to update initial status in Supabase: {e}")

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

        # Count successes and failures. Redis image_results is the source of
        # truth for completed images so retries do not duplicate output.
        failed_results = []

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"[Job {job_id}] Task {i+1} raised exception: {result}")
                failed_results.append({
                    'error': str(result),
                    'image_id': _image_id_for(images[i], i),
                    'filename': images[i].get('filename', f"image_{i}.png")
                })
            elif result.get('status') != 'success':
                failed_results.append(result)

        completed_image_results = await redis.get_job_image_results(job_id)
        generated_files = []
        successful_results = []

        for idx, img in enumerate(images):
            image_id = _image_id_for(img, idx)
            result_data = completed_image_results.get(image_id)
            file_info = result_data.get('file_info') if isinstance(result_data, dict) else None
            if file_info and file_info.get('file_id') and file_info.get('storage_path'):
                generated_files.append(file_info)
                successful_results.append(result_data)
            elif not any(failure.get('image_id') == image_id for failure in failed_results if isinstance(failure, dict)):
                failed_results.append({
                    'status': 'error',
                    'image_id': image_id,
                    'filename': img.get('filename'),
                    'error': 'Image did not complete with a durable result'
                })

        download_urls = [f"/api/v1/download/{file_info['file_id']}" for file_info in generated_files]

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
                'image_results': completed_image_results,
                'file_id': generated_files[0]['file_id'] if generated_files else None,
                'download_url': download_urls[0] if download_urls else None
            })
            logger.info(f"[Job {job_id}] Updated final status in Redis: {final_status}")
        except Exception as redis_error:
            logger.warning(f"[Job {job_id}] Failed to update final status in Redis: {redis_error}")

        # Update job status in Supabase database (CRITICAL for dashboard)
        if user_id:
            try:
                supabase = get_supabase_service()
                await supabase.update_job_status(
                    job_id=job_id,
                    status='completed' if final_status in ['completed', 'partially_completed'] else 'failed',
                    result_url=download_urls[0] if download_urls else None,
                    metadata={
                        'processing_time': processing_time,
                        'successful_images': len(successful_results),
                        'failed_images': len(failed_results),
                        'total_images': len(images),
                        'completed_at': datetime.utcnow().isoformat(),
                        'generated_files': generated_files,
                        'download_urls': download_urls,
                        'image_results': completed_image_results
                    }
                )
                logger.info(f"[Job {job_id}] Updated status in Supabase: {final_status}")
            except Exception as supabase_error:
                logger.error(f"[Job {job_id}] Failed to update Supabase status: {supabase_error}")

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
        
        # Update failed status in Supabase
        if user_id:
            try:
                supabase = get_supabase_service()
                await supabase.update_job_status(
                    job_id=job_id,
                    status='failed',
                    error_message=str(e)
                )
                logger.info(f"[Job {job_id}] Updated failed status in Supabase")
            except Exception as supabase_error:
                logger.error(f"[Job {job_id}] Failed to update Supabase status: {supabase_error}")
