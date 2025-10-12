"""
Supabase service for database and storage operations.
"""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
from uuid import UUID

from supabase import create_client, Client
from app.core.config import settings

logger = logging.getLogger(__name__)


class SupabaseService:
    """Service for interacting with Supabase database and storage."""

    def __init__(self):
        """Initialize Supabase client."""
        # Use anon key for backend operations (respects RLS with user context)
        self.client: Client = create_client(
            settings.supabase_url,
            settings.supabase_anon_key
        )
        self.storage_bucket = settings.supabase_storage_bucket
        logger.info("Supabase service initialized with service role key")

    async def create_job(
        self,
        job_id: str,
        user_id: str,
        image_url: Optional[str] = None,
        filename: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create a new processing job in Supabase.

        Args:
            job_id: Unique job identifier
            user_id: User ID from JWT
            image_url: Optional URL to the original image
            filename: Optional original filename
            metadata: Optional additional metadata

        Returns:
            Created job record
        """
        try:
            # Convert job_id string to UUID format if needed
            from uuid import UUID
            try:
                # Validate UUID format
                UUID(job_id)
                job_uuid = job_id
            except (ValueError, AttributeError):
                logger.error(f"Invalid UUID format for job_id: {job_id}")
                raise ValueError(f"job_id must be a valid UUID: {job_id}")

            job_data = {
                "id": job_uuid,  # Use validated UUID
                "user_id": user_id,
                "status": "pending",
                "image_url": image_url,
                "filename": filename,
                "processing_metadata": metadata or {},
                "created_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat()
            }

            response = self.client.table("processing_jobs").insert(job_data).execute()
            logger.info(f"Created job {job_id} in Supabase for user {user_id}")
            return response.data[0] if response.data else {}

        except Exception as e:
            logger.error(f"Failed to create job in Supabase: {e}")
            raise

    async def update_job_status(
        self,
        job_id: str,
        status: str,
        result_url: Optional[str] = None,
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Update job status in Supabase.

        Args:
            job_id: Job identifier
            status: New status (pending, processing, completed, failed)
            result_url: Optional URL to the result file in storage
            error_message: Optional error message if failed
            metadata: Optional additional metadata

        Returns:
            Updated job record
        """
        try:
            update_data = {
                "status": status,
                "updated_at": datetime.utcnow().isoformat()
            }

            if result_url:
                update_data["result_url"] = result_url

            if error_message:
                update_data["error_message"] = error_message

            if metadata:
                # Merge with existing metadata
                update_data["processing_metadata"] = metadata

            response = self.client.table("processing_jobs").update(update_data).eq("id", job_id).execute()
            logger.info(f"Updated job {job_id} status to {status}")
            return response.data[0] if response.data else {}

        except Exception as e:
            logger.error(f"Failed to update job status in Supabase: {e}")
            raise

    async def upload_file_to_storage(
        self,
        file_data: bytes,
        file_path: str,
        content_type: str = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    ) -> str:
        """
        Upload a file to Supabase Storage.

        Args:
            file_data: File content as bytes
            file_path: Path within the bucket (e.g., "user_id/job_id/file.xlsx")
            content_type: MIME type of the file

        Returns:
            Public URL of the uploaded file
        """
        try:
            # Upload file to storage with upsert to overwrite if exists
            response = self.client.storage.from_(self.storage_bucket).upload(
                file_path,
                file_data,
                {"content-type": content_type, "upsert": "true"}
            )

            # Get public URL
            public_url = self.client.storage.from_(self.storage_bucket).get_public_url(file_path)

            logger.info(f"Uploaded file to Supabase Storage: {file_path}")
            return public_url

        except Exception as e:
            logger.error(f"Failed to upload file to Supabase Storage: {e}")
            raise

    async def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get job details from Supabase.

        Args:
            job_id: Job identifier

        Returns:
            Job record or None if not found
        """
        try:
            response = self.client.table("processing_jobs").select("*").eq("id", job_id).execute()
            return response.data[0] if response.data else None
        except Exception as e:
            logger.error(f"Failed to get job from Supabase: {e}")
            return None

    async def get_user_jobs(self, user_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get all jobs for a user.

        Args:
            user_id: User identifier
            limit: Maximum number of jobs to return

        Returns:
            List of job records
        """
        try:
            response = self.client.table("processing_jobs")\
                .select("*")\
                .eq("user_id", user_id)\
                .order("created_at", desc=True)\
                .limit(limit)\
                .execute()
            return response.data
        except Exception as e:
            logger.error(f"Failed to get user jobs from Supabase: {e}")
            return []
    
    async def download_file_from_storage(
        self,
        file_path: str
    ) -> bytes:
        """
        Download a file from Supabase Storage.

        Args:
            file_path: Path within the bucket (e.g., "user_id/job_id/file.xlsx")

        Returns:
            File content as bytes
        """
        try:
            # Download file from storage
            response = self.client.storage.from_(self.storage_bucket).download(file_path)
            
            logger.info(f"Downloaded file from Supabase Storage: {file_path}")
            return response

        except Exception as e:
            logger.error(f"Failed to download file from Supabase Storage: {e}")
            raise
    
    async def get_storage_public_url(
        self,
        file_path: str
    ) -> str:
        """
        Get the public URL for a file in Supabase Storage.

        Args:
            file_path: Path within the bucket

        Returns:
            Public URL of the file
        """
        try:
            public_url = self.client.storage.from_(self.storage_bucket).get_public_url(file_path)
            return public_url
        except Exception as e:
            logger.error(f"Failed to get public URL: {e}")
            raise


# Global instance
_supabase_service: Optional[SupabaseService] = None


def get_supabase_service() -> SupabaseService:
    """Get or create the global Supabase service instance."""
    global _supabase_service
    if _supabase_service is None:
        _supabase_service = SupabaseService()
    return _supabase_service
