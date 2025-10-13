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

    async def upload_job_file(
        self,
        file_data: bytes,
        user_id: str,
        job_id: str,
        filename: str
    ) -> Dict[str, Any]:
        """
        Simplified method to upload a job file to Supabase Storage.
        
        This follows the standard cloud storage pattern:
        - Files stored at: users/{user_id}/jobs/{job_id}/{filename}
        - Returns storage path and access URL
        
        Args:
            file_data: File content as bytes
            user_id: User ID (for path organization and access control)
            job_id: Job ID
            filename: File name
            
        Returns:
            Dict with storage_path and access_url
        """
        try:
            # Create storage path following standard pattern
            storage_path = f"users/{user_id}/jobs/{job_id}/{filename}"
            
            # Log upload attempt
            file_size_mb = len(file_data) / (1024 * 1024)
            logger.info(f"Uploading {filename} ({file_size_mb:.2f}MB) to {storage_path}")
            
            # First check if file already exists and remove it (to avoid conflicts)
            try:
                # Try to delete existing file if it exists
                self.client.storage.from_(self.storage_bucket).remove([storage_path])
                logger.info(f"Removed existing file at {storage_path}")
            except Exception:
                # File doesn't exist, which is fine
                pass
            
            # Upload using Python client syntax from documentation
            # Using upsert=true to overwrite if it exists (as backup)
            response = self.client.storage.from_(self.storage_bucket).upload(
                path=storage_path,
                file=file_data,
                file_options={
                    "content-type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    "upsert": "true"  # Use string "true" as per docs
                }
            )
            
            # Log response for debugging
            logger.info(f"Upload response type: {type(response)}")
            logger.info(f"Upload response: {response}")
            
            # Check for errors - Supabase Python client returns different types
            if hasattr(response, 'error') and response.error:
                raise Exception(f"Upload error: {response.error}")
            elif isinstance(response, dict) and response.get('error'):
                raise Exception(f"Upload error: {response['error']}")
            elif hasattr(response, 'path'):
                # Success - response is an UploadResponse object
                logger.info(f"File uploaded successfully to: {response.path}")
            else:
                # Assume success if no error
                logger.info(f"File upload completed (response: {type(response)})")
                
            # Generate a signed URL for private bucket (7 days expiry)
            signed_response = self.client.storage.from_(self.storage_bucket).create_signed_url(
                path=storage_path,
                expires_in=7 * 24 * 3600  # 7 days in seconds
            )
            
            # Extract URL from response (Python client returns dict with signedURL)
            access_url = None
            if isinstance(signed_response, dict):
                if 'error' in signed_response:
                    logger.error(f"Failed to create signed URL: {signed_response['error']}")
                    # Fall back to public URL if signing fails
                    access_url = f"{settings.supabase_url}/storage/v1/object/sign/{self.storage_bucket}/{storage_path}"
                else:
                    access_url = signed_response.get('signedURL') or signed_response.get('signedUrl') or signed_response.get('data', {}).get('signedUrl')
                    logger.info(f"Generated signed URL for {storage_path}")
            elif isinstance(signed_response, str):
                access_url = signed_response
                logger.info(f"Got signed URL string for {storage_path}")
            else:
                logger.warning(f"Unexpected signed URL response: {type(signed_response)}, {signed_response}")
                # Fall back to constructing the URL
                access_url = f"{settings.supabase_url}/storage/v1/object/sign/{self.storage_bucket}/{storage_path}"
                
            logger.info(f"File uploaded successfully to {storage_path} with URL: {access_url[:100]}...")
            
            return {
                "storage_path": storage_path,
                "access_url": access_url,
                "size_mb": file_size_mb,
                "filename": filename
            }
            
        except Exception as e:
            logger.error(f"Failed to upload file to Supabase Storage: {str(e)}")
            logger.error(f"Error type: {type(e).__name__}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
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
        Legacy upload method - redirects to simplified version.
        Kept for backward compatibility.
        """
        try:
            result = await self.upload_job_file(
                file_data=file_data,
                user_id=user_id,
                job_id=job_id,
                filename=filename
            )
            
            # Convert to legacy format
            return {
                "signed_url": result['access_url'],
                "storage_path": result['storage_path'],
                "size_mb": result['size_mb'],
                "url_type": "signed_url"
            }
        except Exception as e:
            logger.error(f"Legacy upload failed: {e}")
            raise
            
    # Old upload method removed - using simplified upload_job_file() instead

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
    
    async def save_to_job_history(
        self,
        user_id: str,
        original_job_id: str,
        filename: str,
        status: str = "completed",
        result_url: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Save a job to the job_history table.

        Args:
            user_id: User identifier
            original_job_id: Original job ID from processing_jobs
            filename: Job filename
            status: Job status (default: completed)
            result_url: URL to the result file
            metadata: Additional metadata

        Returns:
            Created history record
        """
        try:
            import uuid
            history_data = {
                "id": str(uuid.uuid4()),  # Generate new UUID for history entry
                "user_id": user_id,
                "original_job_id": original_job_id,
                "filename": filename,
                "status": status,
                "result_url": result_url,
                "processing_metadata": metadata or {},
                "saved_at": datetime.utcnow().isoformat(),
                "created_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat()
            }

            response = self.client.table("job_history").insert(history_data).execute()
            logger.info(f"Saved job {original_job_id} to history for user {user_id}")
            return response.data[0] if response.data else {}

        except Exception as e:
            logger.error(f"Failed to save job to history in Supabase: {e}")
            raise
    
    async def get_user_saved_history(self, user_id: str, limit: int = 50, offset: int = 0) -> Dict[str, Any]:
        """
        Get saved job history for a user from job_history table.

        Args:
            user_id: User identifier
            limit: Maximum number of jobs to return
            offset: Number of records to skip

        Returns:
            List of saved job records
        """
        try:
            # Get total count first
            count_response = self.client.table("job_history")\
                .select("*", count="exact")\
                .eq("user_id", user_id)\
                .execute()
            
            total_count = count_response.count if hasattr(count_response, 'count') else 0
            
            # Get paginated data
            response = self.client.table("job_history")\
                .select("*")\
                .eq("user_id", user_id)\
                .order("saved_at", desc=True)\
                .range(offset, offset + limit - 1)\
                .execute()
            
            return {
                "jobs": response.data,
                "total": total_count,
                "limit": limit,
                "offset": offset,
                "has_more": offset + limit < total_count
            }
        except Exception as e:
            logger.error(f"Failed to get user saved history from Supabase: {e}")
            return {"jobs": [], "total": 0, "limit": limit, "offset": offset, "has_more": False}
    
    async def delete_from_job_history(
        self, 
        user_id: str, 
        original_job_id: str
    ) -> bool:
        """
        Delete a specific job from job_history table.
        
        This handles both individual file deletion and parent job deletion.
        If original_job_id contains underscore, it's a specific file.
        Otherwise, we delete all files from that parent job.

        Args:
            user_id: User identifier
            original_job_id: Original job ID to delete (can be parent_id or parent_id_index)

        Returns:
            True if deleted successfully, False if not found
        """
        try:
            # Check if this is a specific file (has _index suffix) or parent job
            if '_' in original_job_id:
                # Delete specific file entry
                response = self.client.table("job_history")\
                    .delete()\
                    .eq("user_id", user_id)\
                    .eq("original_job_id", original_job_id)\
                    .execute()
            else:
                # Delete all entries with this parent job ID
                # First try to delete by parent_job_id in metadata
                response = self.client.table("job_history")\
                    .delete()\
                    .eq("user_id", user_id)\
                    .like("original_job_id", f"{original_job_id}_%")\
                    .execute()
            
            # Check if any rows were deleted
            return len(response.data) > 0 if response.data else False
        except Exception as e:
            logger.error(f"Failed to delete job from history: {e}")
            raise
    
    async def delete_all_from_job_history(self, user_id: str) -> int:
        """
        Delete all jobs from job_history table for a user.

        Args:
            user_id: User identifier

        Returns:
            Number of deleted records
        """
        try:
            # Get all records for this user to count them
            get_response = self.client.table("job_history")\
                .select("id")\
                .eq("user_id", user_id)\
                .execute()
            
            # Count the records we're about to delete
            count = len(get_response.data) if get_response.data else 0
            
            # Delete all records for this user if any exist
            if count > 0:
                delete_response = self.client.table("job_history")\
                    .delete()\
                    .eq("user_id", user_id)\
                    .execute()
                
                logger.info(f"Deleted {count} job history records for user {user_id}")
                return count
            
            logger.info(f"No job history records found to delete for user {user_id}")
            return 0
        except Exception as e:
            logger.error(f"Failed to delete all jobs from history: {e}")
            raise
    
    async def download_file_from_storage(
        self,
        file_path: str
    ) -> bytes:
        """
        Download a file from Supabase Storage.

        Args:
            file_path: Path within the bucket 
                      New format: "users/user_id/jobs/job_id/file.xlsx"
                      Old format: "user_id/job_id/file.xlsx" (auto-converted)

        Returns:
            File content as bytes
        """
        try:
            # Ensure the path uses the new format if needed
            if not file_path.startswith("users/"):
                # Convert old format to new format
                parts = file_path.split("/")
                if len(parts) >= 3:  # old format: user_id/job_id/filename
                    file_path = f"users/{parts[0]}/jobs/{parts[1]}/{'/'.join(parts[2:])}"
                    logger.info(f"Converted path to new format: {file_path}")
            
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
