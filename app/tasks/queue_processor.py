"""
Redis Queue-based Image Processor

Simple async image processing using Redis queues.
No complex asyncio.gather(), just clean queue-based processing.
"""

import asyncio
import base64
import logging
import json
from typing import Dict, Any
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


class ImageQueueProcessor:
    """Processes images from Redis queue with controlled concurrency."""

    def __init__(self, max_concurrent: int = 5):
        self.max_concurrent = max_concurrent
        self.running = False
        self.workers: list[asyncio.Task] = []

    async def start(self):
        """Start queue processor workers."""
        self.running = True
        logger.info(f"Starting {self.max_concurrent} queue processor workers")

        # Start multiple worker tasks for concurrent processing
        for i in range(self.max_concurrent):
            worker = asyncio.create_task(self._worker(worker_id=i))
            self.workers.append(worker)

        logger.info(f"Queue processor started with {self.max_concurrent} workers")

    async def stop(self):
        """Stop queue processor workers."""
        self.running = False

        # Cancel all workers
        for worker in self.workers:
            worker.cancel()

        # Wait for workers to finish
        await asyncio.gather(*self.workers, return_exceptions=True)
        self.workers.clear()

        logger.info("Queue processor stopped")

    async def _worker(self, worker_id: int):
        """Worker that processes images from the queue."""
        redis = await get_redis_service()
        olmocr = get_olmocr_service()
        excel = ExcelService()
        storage = FileStorageManager()

        logger.info(f"Worker {worker_id} started")

        while self.running:
            try:
                # Pull task from queue (blocking with timeout)
                task_data = await redis.dequeue_task("image_processing", timeout=5)

                if not task_data:
                    # No task available, continue waiting
                    continue

                # Process the image task
                await self._process_image_task(
                    task_data=task_data,
                    worker_id=worker_id,
                    redis=redis,
                    olmocr=olmocr,
                    excel=excel,
                    storage=storage
                )

            except asyncio.CancelledError:
                logger.info(f"Worker {worker_id} cancelled")
                break
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}", exc_info=True)
                await asyncio.sleep(1)  # Brief pause on error

        logger.info(f"Worker {worker_id} stopped")

    async def _process_image_task(
        self,
        task_data: Dict[str, Any],
        worker_id: int,
        redis,
        olmocr,
        excel,
        storage
    ):
        """Process a single image task from the queue."""
        job_id = task_data['job_id']
        session_id = task_data['session_id']
        img = task_data['image']
        img_index = task_data['index']
        total_images = task_data['total']
        user_id = task_data.get('user_id')

        try:
            logger.info(f"[Worker {worker_id}] Processing image {img_index+1}/{total_images} for job {job_id}")

            # Decode image
            image_data = img['data']
            if image_data.startswith('data:'):
                image_data = image_data.split(',', 1)[1]
            image_bytes = base64.b64decode(image_data)

            # Extract table data using OlmOCR
            csv_data = await olmocr.extract_table_from_image(image_bytes)

            # Create Excel file
            excel_data = excel.csv_to_xlsx(csv_data, f"Table_{img['id']}")

            # Generate filename
            original_filename = img.get('filename', f"image_{img['id']}")
            base_name = original_filename.split('.')[0] if '.' in original_filename else original_filename
            excel_filename = f"{base_name}_processed.xlsx"

            # Save file to local storage
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
                    logger.info(f"[Worker {worker_id}] Uploaded to Supabase: {file_path}")
                except Exception as upload_error:
                    logger.warning(f"[Worker {worker_id}] Supabase upload failed: {upload_error}")

            # Get current job data to increment processed count
            job_data = await redis.get_job(job_id)
            if not job_data:
                raise Exception(f"Job {job_id} not found")

            processed_count = int(job_data.get('processed_images', 0)) + 1
            current_progress = int((processed_count / total_images) * 100)

            # Update job progress
            await redis.update_job(job_id, {
                'processed_images': processed_count,
                'progress': current_progress,
                'current_image': img.get('id', img.get('filename', f'image_{img_index+1}')),
                'updated_at': datetime.utcnow().isoformat()
            })

            # Publish progress update
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

            # Publish file_ready message for progressive download
            file_info = ProcessedFileInfo(
                file_id=file_id,
                download_url=f"/api/v1/download/{file_id}",
                filename=excel_filename,
                image_id=img.get('id'),
                size_bytes=len(excel_data)
            )

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

            logger.info(f"[Worker {worker_id}] Image {img_index+1}/{total_images} completed")

            # Check if this was the last image
            if processed_count == total_images:
                await self._complete_job(job_id, session_id, redis, storage)

        except Exception as e:
            error_msg = f"Failed to process image {img_index+1}: {str(e)}"
            logger.error(f"[Worker {worker_id}] {error_msg}", exc_info=True)

            # Update job with error
            job_data = await redis.get_job(job_id)
            if job_data:
                errors = job_data.get('errors', [])
                if isinstance(errors, str):
                    errors = json.loads(errors) if errors else []
                errors.append(error_msg)

                processed_count = int(job_data.get('processed_images', 0))
                failed_count = int(job_data.get('failed_images', 0)) + 1

                await redis.update_job(job_id, {
                    'failed_images': failed_count,
                    'errors': errors,
                    'updated_at': datetime.utcnow().isoformat()
                })

                # Check if all images are done (including failures)
                total_images_from_data = int(job_data.get('total_images', 0))
                if (processed_count + failed_count) == total_images_from_data:
                    await self._complete_job(job_id, session_id, redis, storage)

    async def _complete_job(self, job_id: str, session_id: str, redis, storage):
        """Mark job as completed and send completion message."""
        job_data = await redis.get_job(job_id)
        if not job_data:
            return

        total_images = int(job_data.get('total_images', 0))
        processed_images = int(job_data.get('processed_images', 0))
        failed_images = int(job_data.get('failed_images', 0))

        # Determine final status
        if processed_images == total_images:
            final_status = 'completed'
        elif processed_images > 0:
            final_status = 'partially_completed'
        else:
            final_status = 'failed'

        # Get session files
        session_metadata = await storage.get_session_metadata(session_id)
        files_info = []
        download_urls = []

        if session_metadata:
            for file_id in session_metadata.result_files:
                file_info = ProcessedFileInfo(
                    file_id=file_id,
                    download_url=f"/api/v1/download/{file_id}",
                    filename=f"{file_id}.xlsx"
                )
                files_info.append(file_info)
                download_urls.append(file_info.download_url)

        # Update job with final status
        processing_time = (datetime.utcnow() - datetime.fromisoformat(job_data['created_at'])).total_seconds()

        await redis.update_job(job_id, {
            'status': final_status,
            'progress': 100,
            'processing_time': processing_time,
            'completed_at': datetime.utcnow().isoformat()
        })

        # Send completion message
        completion_message = JobCompletedMessage(
            job_id=job_id,
            status=final_status,
            successful_images=processed_images,
            failed_images=failed_images,
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

        logger.info(f"Job {job_id} completed: {processed_images} successful, {failed_images} failed")


# Global processor instance
_processor: ImageQueueProcessor = None


async def get_queue_processor() -> ImageQueueProcessor:
    """Get or create the global queue processor instance."""
    global _processor
    if _processor is None:
        _processor = ImageQueueProcessor(max_concurrent=5)
        await _processor.start()
    return _processor


async def enqueue_batch_processing(
    job_id: str,
    session_id: str,
    images: list[Dict[str, str]],
    user_id: str = None
):
    """
    Enqueue all images in a batch for processing.

    This is the main entry point for batch processing.
    """
    redis = await get_redis_service()

    logger.info(f"Enqueuing {len(images)} images for job {job_id}")

    # Enqueue each image as a separate task
    for i, img in enumerate(images):
        task_data = {
            'job_id': job_id,
            'session_id': session_id,
            'image': img,
            'index': i,
            'total': len(images),
            'user_id': user_id
        }

        await redis.enqueue_task("image_processing", task_data)

    logger.info(f"Enqueued {len(images)} tasks for job {job_id}")

    # Ensure processor is running
    await get_queue_processor()
