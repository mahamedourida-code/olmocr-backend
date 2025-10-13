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
        # Use service role key for backend operations (bypasses RLS)
        if not settings.supabase_service_role_key:
            logger.error("SUPABASE_SERVICE_ROLE_KEY is not configured! Storage operations will fail.")
            raise ValueError("SUPABASE_SERVICE_ROLE_KEY is required for storage operations")
        
        self.client: Client = create_client(
            settings.supabase_url,
            settings.supabase_service_role_key
        )
        logger.info(f"Supabase client initialized with service role key (has full access)")
        
        self.storage_bucket = settings.supabase_storage_bucket
        # Configure whether the storage bucket is public or private
        # For user job data, it should be private for security
        self.is_bucket_public = False  # Set to False for private bucket
        logger.info(f"Storage bucket configured: '{self.storage_bucket}' (public: {self.is_bucket_public})")

    async def create_job(
        self,
        job_id: str,
        user_id: str,
        image_url: Optional[str] = None,
        filename: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        status: str = "pending",
        result_url: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a new processing job in Supabase.

        Args:
            job_id: Unique job identifier
            user_id: User ID from JWT
            image_url: Optional URL to the original image
            filename: Optional original filename
            metadata: Optional additional metadata
            status: Job status (default: "pending")
            result_url: Optional result URL for completed jobs

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
                "status": status,  # Use the provided status instead of hardcoding "pending"
                "image_url": image_url,
                "filename": filename,
                "result_url": result_url,  # Include result_url if provided
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
        user_id: str,
        job_id: str,
        filename: str,
        content_type: str = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        max_file_size_mb: int = 10
    ) -> Dict[str, str]:
        """
        Upload a file to Supabase Storage with consistent path structure.

        Args:
            file_data: File content as bytes
            file_path: DEPRECATED - will be auto-generated. Keep for backward compatibility.
            user_id: User ID for path structure
            job_id: Job ID for path structure
            filename: Original filename
            content_type: MIME type of the file
            max_file_size_mb: Maximum file size in MB

        Returns:
            Dict with 'public_url' or 'signed_url' and 'storage_path'
        """
        try:
            # Validate file size
            file_size_mb = len(file_data) / (1024 * 1024)
            if file_size_mb > max_file_size_mb:
                raise ValueError(f"File size ({file_size_mb:.2f}MB) exceeds maximum allowed ({max_file_size_mb}MB)")
            
            # Enforce consistent path structure: {user_id}/{job_id}/{filename}
            storage_path = f"{user_id}/{job_id}/{filename}"
            
            # Upload file to storage with upsert to overwrite if exists
            # Python client expects different parameters than JS client
            response = self.client.storage.from_(self.storage_bucket).upload(
                path=storage_path,
                file=file_data,
                file_options={"content-type": content_type, "x-upsert": "true"}
            )
            
            # Check if upload was successful
            if hasattr(response, 'error') and response.error:
                raise Exception(f"Storage upload failed: {response.error}")
            
            logger.info(f"Successfully uploaded file to path: {storage_path}")

            # Generate URL based on bucket visibility
            if self.is_bucket_public:
                # For public buckets, use public URL
                url_data = self.client.storage.from_(self.storage_bucket).get_public_url(storage_path)
                url = url_data if isinstance(url_data, str) else url_data.get('publicUrl', url_data.get('publicURL'))
                url_type = "public_url"
            else:
                # For private buckets, create a signed URL with 7 days expiration
                signed_response = self.client.storage.from_(self.storage_bucket).create_signed_url(
                    path=storage_path,
                    expires_in=7 * 24 * 3600  # 7 days expiration for job history
                )
                # Handle different response formats from Python client
                if isinstance(signed_response, dict):
                    if 'signedURL' in signed_response:
                        url = signed_response['signedURL']
                    elif 'signedUrl' in signed_response:
                        url = signed_response['signedUrl']
                    elif 'data' in signed_response:
                        url = signed_response['data'].get('signedURL') or signed_response['data'].get('signedUrl')
                    else:
                        raise Exception(f"Unexpected signed URL response format: {signed_response}")
                else:
                    url = signed_response
                url_type = "signed_url"

            logger.info(f"Uploaded file to Supabase Storage: {storage_path} ({file_size_mb:.2f}MB) - {url_type}")
            
            return {
                "public_url" if self.is_bucket_public else "signed_url": url,
                "storage_path": storage_path,
                "size_mb": file_size_mb,
                "url_type": url_type
            }

        except Exception as e:
            logger.error(f"Failed to upload file to Supabase Storage: {e}")
            logger.error(f"Upload parameters - bucket: {self.storage_bucket}, path: {storage_path}, size: {file_size_mb:.2f}MB")
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
    
    async def create_signed_url(
        self,
        file_path: str,
        expires_in: int = 3600
    ) -> str:
        """
        Create a signed URL for temporary access to a file in private bucket.

        Args:
            file_path: Path within the bucket
            expires_in: URL expiration time in seconds (default: 1 hour)

        Returns:
            Signed URL for the file
        """
        try:
            # Use named parameters for Python client
            response = self.client.storage.from_(self.storage_bucket).create_signed_url(
                path=file_path,
                expires_in=expires_in
            )
            
            # Handle different response formats
            if isinstance(response, str):
                signed_url = response
            elif isinstance(response, dict):
                if response.get('error'):
                    raise Exception(f"Failed to create signed URL: {response['error']}")
                if 'signedURL' in response:
                    signed_url = response['signedURL']
                elif 'signedUrl' in response:
                    signed_url = response['signedUrl']
                elif 'data' in response:
                    signed_url = response['data'].get('signedUrl') or response['data'].get('signedURL')
                else:
                    raise Exception(f"Unexpected response format: {response}")
            else:
                raise Exception(f"Unexpected response type: {type(response)}")
            
            logger.info(f"Created signed URL for {file_path} (expires in {expires_in}s)")
            return signed_url
        except Exception as e:
            logger.error(f"Failed to create signed URL: {e}")
            raise


# Global instance
_supabase_service: Optional[SupabaseService] = None


def get_supabase_service() -> SupabaseService:
    """Get or create the global Supabase service instance."""
    global _supabase_service
    if _supabase_service is None:
        _supabase_service = SupabaseService()
    return _supabase_service
