"""
Supabase service for database and storage operations.
"""

import logging
import json
from typing import Optional, Dict, Any, List
from datetime import date, datetime
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

    def check_and_use_credits(
        self,
        user_id: str,
        credits_needed: int
    ) -> bool:
        """
        Check if user has enough credits and deduct them.

        Args:
            user_id: User ID to check credits for
            credits_needed: Number of credits needed

        Returns:
            True if credits were successfully deducted, False if insufficient
        """
        try:
            logger.info(f"[Credits] Calling use_credits RPC with user_id={user_id}, credits={credits_needed}")
            
            # Call the database function to check and use credits
            response = self.client.rpc(
                'use_credits', 
                {'p_user_id': user_id, 'p_credits': credits_needed}
            ).execute()
            
            logger.info(f"[Credits] RPC response: {response.data}")
            
            # The function returns a boolean
            result = response.data if response.data is not None else False
            
            if result:
                logger.info(f"[Credits] Successfully used {credits_needed} credits for user {user_id}")
            else:
                logger.warning(f"[Credits] Failed to use credits - insufficient balance for user {user_id}")
                
            return result

        except Exception as e:
            logger.error(f"[Credits] Exception in check_and_use_credits for user {user_id}: {e}", exc_info=True)
            logger.error(f"[Credits] RPC params were: user_id={user_id}, credits_needed={credits_needed}")
            # Raise the exception to be handled by the caller
            raise

    def get_user_credits(
        self,
        user_id: str
    ) -> Dict[str, int]:
        """
        Get user's credit information.

        Args:
            user_id: User ID to get credits for

        Returns:
            Dictionary with total_credits, used_credits, and available_credits
        """
        try:
            logger.info(f"[Credits] Getting credits for user {user_id}")
            
            response = self.client.rpc(
                'get_user_credits',
                {'p_user_id': user_id}
            ).execute()
            
            logger.info(f"[Credits] Get credits RPC response: {response.data}")

            if response.data and len(response.data) > 0:
                # The RPC returns a single record, extract it properly
                result = response.data[0]

                # Check if result is a dict (already parsed) or needs parsing
                if isinstance(result, dict):
                    # Already a dictionary, just ensure all required keys exist
                    return {
                        'total_credits': result.get('total_credits', 80),
                        'used_credits': result.get('used_credits', 0),
                        'available_credits': result.get('available_credits', 80)
                    }
                else:
                    # Result might be a tuple or string, try to extract values
                    logger.warning(f"[Credits] Unexpected response format: {type(result)}, value: {result}")

                    # Try to parse if it's a string representation of a tuple like "(80,3,77)"
                    if isinstance(result, str) and result.startswith('(') and result.endswith(')'):
                        # Parse the tuple string
                        values = result.strip('()').split(',')
                        if len(values) >= 3:
                            total_credits = int(values[0])
                            used_credits = int(values[1])
                            available_credits = int(values[2])
                            logger.info(f"[Credits] Parsed tuple string: total={total_credits}, used={used_credits}, available={available_credits}")
                            return {
                                'total_credits': total_credits,
                                'used_credits': used_credits,
                                'available_credits': available_credits
                            }

                    # Fall through to defaults if we can't parse
                    logger.error(f"[Credits] Could not parse RPC response: {result}")
            else:
                # No record found, create one for the user
                logger.warning(f"[Credits] No credit record found for user {user_id}, creating default record")
                
                # Insert default credits for the user
                try:
                    insert_response = self.client.table('user_credits').insert({
                        'user_id': user_id,
                        'total_credits': 80,
                        'used_credits': 0,
                        'reset_date': 'CURRENT_DATE'
                    }).execute()
                    
                    logger.info(f"[Credits] Created credit record for user {user_id}")
                except Exception as insert_error:
                    logger.error(f"[Credits] Failed to create credit record: {insert_error}")
                
                # Return default values
                return {
                    'total_credits': 80,
                    'used_credits': 0,
                    'available_credits': 80
                }

        except Exception as e:
            logger.error(f"[Credits] Failed to get credits for user {user_id}: {e}", exc_info=True)
            return {
                'total_credits': 80,
                'used_credits': 0,
                'available_credits': 80
            }

    def get_user_plan_type(self, user_id: str) -> str:
        """Return the user's plan from profiles, defaulting to free."""
        try:
            response = self.client.table("profiles")\
                .select("plan_type")\
                .eq("id", user_id)\
                .limit(1)\
                .execute()

            if response.data:
                return response.data[0].get("plan_type") or "free"
            return "free"
        except Exception as e:
            logger.error(f"Failed to get plan type for user {user_id}: {e}")
            return "free"

    def refund_credits(self, user_id: str, credits_to_refund: int) -> bool:
        """
        Refund reserved credits by reducing used_credits.

        This is used after batch completion/cancellation so users only pay for
        successfully generated spreadsheets.
        """
        try:
            credits_to_refund = int(credits_to_refund or 0)
            if credits_to_refund <= 0:
                return True

            response = self.client.table("user_credits")\
                .select("used_credits")\
                .eq("user_id", user_id)\
                .limit(1)\
                .execute()

            if not response.data:
                logger.warning(f"[Credits] No credit record found for refund user {user_id}")
                return False

            used_credits = int(response.data[0].get("used_credits") or 0)
            new_used_credits = max(0, used_credits - credits_to_refund)

            self.client.table("user_credits")\
                .update({
                    "used_credits": new_used_credits,
                    "last_updated": datetime.utcnow().isoformat()
                })\
                .eq("user_id", user_id)\
                .execute()

            logger.info(f"[Credits] Refunded {credits_to_refund} credits for user {user_id}")
            return True
        except Exception as e:
            logger.error(f"[Credits] Failed to refund {credits_to_refund} credits for user {user_id}: {e}", exc_info=True)
            return False

    def update_user_plan_type(self, user_id: str, plan_type: str) -> bool:
        """Update the user's application plan in profiles."""
        try:
            self.client.table("profiles")\
                .update({
                    "plan_type": plan_type,
                    "updated_at": datetime.utcnow().isoformat()
                })\
                .eq("id", user_id)\
                .execute()
            return True
        except Exception as e:
            logger.error(f"Failed to update plan for user {user_id}: {e}", exc_info=True)
            return False

    def upsert_billing_customer(
        self,
        user_id: str,
        provider_customer_id: str,
        email: Optional[str] = None,
        name: Optional[str] = None,
        portal_url: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        provider: str = "lemonsqueezy"
    ) -> Dict[str, Any]:
        """Create or update a provider customer mapping."""
        data = {
            "user_id": user_id,
            "provider": provider,
            "provider_customer_id": str(provider_customer_id),
            "email": email,
            "name": name,
            "portal_url": portal_url,
            "metadata": metadata or {},
            "updated_at": datetime.utcnow().isoformat(),
        }

        existing = self.client.table("billing_customers")\
            .select("*")\
            .eq("provider", provider)\
            .eq("provider_customer_id", str(provider_customer_id))\
            .limit(1)\
            .execute()
        if not existing.data:
            existing = self.client.table("billing_customers")\
                .select("*")\
                .eq("provider", provider)\
                .eq("user_id", user_id)\
                .limit(1)\
                .execute()

        if existing.data:
            response = self.client.table("billing_customers")\
                .update(data)\
                .eq("id", existing.data[0]["id"])\
                .execute()
        else:
            response = self.client.table("billing_customers").insert(data).execute()

        return response.data[0] if response.data else {}

    def upsert_subscription(
        self,
        user_id: str,
        provider_subscription_id: str,
        provider_customer_id: Optional[str],
        provider_variant_id: Optional[str],
        plan: str,
        status: str,
        renews_at: Optional[str] = None,
        ends_at: Optional[str] = None,
        cancelled: bool = False,
        customer_portal_url: Optional[str] = None,
        update_payment_method_url: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        provider: str = "lemonsqueezy"
    ) -> Dict[str, Any]:
        """Create or update subscription state from a provider webhook."""
        data = {
            "user_id": user_id,
            "provider": provider,
            "provider_subscription_id": str(provider_subscription_id),
            "provider_customer_id": str(provider_customer_id) if provider_customer_id else None,
            "provider_variant_id": str(provider_variant_id) if provider_variant_id else None,
            "plan": plan,
            "status": status,
            "renews_at": renews_at,
            "ends_at": ends_at,
            "cancelled": cancelled,
            "customer_portal_url": customer_portal_url,
            "update_payment_method_url": update_payment_method_url,
            "metadata": metadata or {},
            "updated_at": datetime.utcnow().isoformat(),
        }

        existing = self.client.table("subscriptions")\
            .select("*")\
            .eq("provider", provider)\
            .eq("provider_subscription_id", str(provider_subscription_id))\
            .limit(1)\
            .execute()

        if existing.data:
            response = self.client.table("subscriptions")\
                .update(data)\
                .eq("id", existing.data[0]["id"])\
                .execute()
        else:
            response = self.client.table("subscriptions").insert(data).execute()

        return response.data[0] if response.data else {}

    def find_user_for_provider_customer(self, provider_customer_id: str, provider: str = "lemonsqueezy") -> Optional[str]:
        response = self.client.table("billing_customers")\
            .select("user_id")\
            .eq("provider", provider)\
            .eq("provider_customer_id", str(provider_customer_id))\
            .limit(1)\
            .execute()
        return response.data[0]["user_id"] if response.data else None

    def find_user_for_provider_subscription(self, provider_subscription_id: str, provider: str = "lemonsqueezy") -> Optional[str]:
        response = self.client.table("subscriptions")\
            .select("user_id")\
            .eq("provider", provider)\
            .eq("provider_subscription_id", str(provider_subscription_id))\
            .limit(1)\
            .execute()
        return response.data[0]["user_id"] if response.data else None

    def get_latest_subscription_for_user(self, user_id: str, provider: str = "lemonsqueezy") -> Optional[Dict[str, Any]]:
        response = self.client.table("subscriptions")\
            .select("*")\
            .eq("provider", provider)\
            .eq("user_id", user_id)\
            .order("updated_at.desc")\
            .limit(1)\
            .execute()
        return response.data[0] if response.data else None

    def get_subscription_by_provider_id(self, provider_subscription_id: str, provider: str = "lemonsqueezy") -> Optional[Dict[str, Any]]:
        response = self.client.table("subscriptions")\
            .select("*")\
            .eq("provider", provider)\
            .eq("provider_subscription_id", str(provider_subscription_id))\
            .limit(1)\
            .execute()
        return response.data[0] if response.data else None

    def get_billing_customer_for_user(self, user_id: str, provider: str = "lemonsqueezy") -> Optional[Dict[str, Any]]:
        response = self.client.table("billing_customers")\
            .select("*")\
            .eq("provider", provider)\
            .eq("user_id", user_id)\
            .limit(1)\
            .execute()
        return response.data[0] if response.data else None

    def record_webhook_event(
        self,
        event_hash: str,
        event_name: str,
        payload: Dict[str, Any],
        resource_type: Optional[str] = None,
        resource_id: Optional[str] = None,
        provider: str = "lemonsqueezy"
    ) -> Optional[Dict[str, Any]]:
        """Insert a webhook event. Returns None if it was already seen."""
        existing = self.client.table("webhook_events")\
            .select("*")\
            .eq("provider", provider)\
            .eq("event_hash", event_hash)\
            .limit(1)\
            .execute()
        if existing.data:
            return None if existing.data[0].get("processed") else existing.data[0]

        response = self.client.table("webhook_events").insert({
            "provider": provider,
            "event_hash": event_hash,
            "event_name": event_name,
            "resource_type": resource_type,
            "resource_id": str(resource_id) if resource_id else None,
            "payload": payload,
        }).execute()
        return response.data[0] if response.data else {}

    def mark_webhook_event_processed(
        self,
        event_hash: str,
        processed: bool,
        processing_error: Optional[str] = None,
        provider: str = "lemonsqueezy"
    ) -> None:
        self.client.table("webhook_events")\
            .update({
                "processed": processed,
                "processing_error": processing_error,
                "processed_at": datetime.utcnow().isoformat() if processed else None,
            })\
            .eq("provider", provider)\
            .eq("event_hash", event_hash)\
            .execute()

    def grant_plan_credits(
        self,
        user_id: str,
        credits: int,
        movement_type: str,
        provider_subscription_id: Optional[str] = None,
        provider_order_id: Optional[str] = None,
        provider_event_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        provider: str = "lemonsqueezy"
    ) -> Dict[str, int]:
        """Reset the user's monthly credit allocation and record a ledger entry."""
        credits = max(0, int(credits or 0))

        existing = self.client.table("user_credits")\
            .select("*")\
            .eq("user_id", user_id)\
            .limit(1)\
            .execute()

        credit_data = {
            "user_id": user_id,
            "total_credits": credits,
            "used_credits": 0,
            "reset_date": date.today().isoformat(),
            "last_updated": datetime.utcnow().isoformat(),
        }
        if existing.data:
            self.client.table("user_credits").update(credit_data).eq("user_id", user_id).execute()
        else:
            self.client.table("user_credits").insert(credit_data).execute()

        self.client.table("credit_ledger").insert({
            "user_id": user_id,
            "provider": provider,
            "provider_event_id": provider_event_id,
            "provider_subscription_id": provider_subscription_id,
            "provider_order_id": provider_order_id,
            "movement_type": movement_type,
            "credits_delta": credits,
            "balance_after": credits,
            "metadata": metadata or {},
        }).execute()

        return {
            "total_credits": credits,
            "used_credits": 0,
            "available_credits": credits,
        }

    def get_billing_status(self, user_id: str) -> Dict[str, Any]:
        """Return current app billing state for dashboard/API consumers."""
        return {
            "plan": self.get_user_plan_type(user_id),
            "credits": self.get_user_credits(user_id),
            "subscription": self.get_latest_subscription_for_user(user_id),
            "customer": self.get_billing_customer_for_user(user_id),
        }

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

            if metadata is not None:
                existing_metadata = {}
                try:
                    existing_job = await self.get_job(job_id)
                    raw_metadata = existing_job.get("processing_metadata") if existing_job else {}
                    if isinstance(raw_metadata, dict):
                        existing_metadata = raw_metadata
                    elif isinstance(raw_metadata, str) and raw_metadata:
                        parsed_metadata = json.loads(raw_metadata)
                        existing_metadata = parsed_metadata if isinstance(parsed_metadata, dict) else {}
                except Exception as metadata_error:
                    logger.debug(f"Could not load existing metadata for {job_id}: {metadata_error}")

                update_data["processing_metadata"] = {
                    **existing_metadata,
                    **metadata
                }

            response = self.client.table("processing_jobs").update(update_data).eq("id", job_id).execute()
            logger.info(f"Updated job {job_id} status to {status}")
            return response.data[0] if response.data else {}

        except Exception as e:
            logger.error(f"Failed to update job status in Supabase: {e}")
            raise

    def _job_file_record_to_metadata(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a job_files row to the metadata shape used by API responses."""
        owner_user_id = record.get("owner_user_id")
        owner_session_id = record.get("owner_session_id")
        metadata = record.get("metadata") or {}

        return {
            "file_id": record.get("file_id"),
            "job_id": record.get("job_id"),
            "image_id": record.get("image_id"),
            "storage_path": record.get("storage_path"),
            "filename": record.get("filename"),
            "original_filename": record.get("original_filename"),
            "content_type": record.get("content_type") or "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "size_bytes": record.get("size_bytes"),
            "status": record.get("status", "completed"),
            "user_id": str(owner_user_id) if owner_user_id else "",
            "session_id": owner_session_id or "",
            "owner_user_id": str(owner_user_id) if owner_user_id else None,
            "owner_session_id": owner_session_id,
            "created_at": record.get("created_at"),
            "updated_at": record.get("updated_at"),
            "completed_at": metadata.get("completed_at") or record.get("created_at"),
            "expires_at": record.get("expires_at"),
            "metadata": metadata,
        }

    async def upsert_job_file(self, file_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Store durable generated-file ownership metadata.

        Redis may cache this data, but this table is the authorization source
        for downloads, share sessions, and job recovery after worker/web restarts.
        """
        try:
            file_id = file_metadata.get("file_id")
            job_id = file_metadata.get("job_id")
            storage_path = file_metadata.get("storage_path")
            filename = file_metadata.get("filename") or f"{file_id}.xlsx"

            if not file_id or not job_id or not storage_path:
                raise ValueError("file_id, job_id, and storage_path are required")

            owner_user_id = (
                file_metadata.get("owner_user_id")
                or file_metadata.get("user_id")
                or None
            )
            owner_session_id = (
                file_metadata.get("owner_session_id")
                or file_metadata.get("session_id")
                or None
            )

            if owner_user_id in ("", "None", "null"):
                owner_user_id = None
            if owner_session_id in ("", "None", "null"):
                owner_session_id = None

            if not owner_user_id and not owner_session_id:
                raise ValueError("generated file metadata must include a user or session owner")

            data = {
                "file_id": file_id,
                "job_id": job_id,
                "image_id": file_metadata.get("image_id"),
                "owner_user_id": owner_user_id,
                "owner_session_id": owner_session_id,
                "storage_path": storage_path,
                "filename": filename,
                "original_filename": file_metadata.get("original_filename"),
                "content_type": file_metadata.get("content_type") or "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "size_bytes": file_metadata.get("size_bytes"),
                "status": file_metadata.get("status") or "completed",
                "metadata": file_metadata.get("metadata") or {
                    "completed_at": file_metadata.get("completed_at"),
                    "supabase_url": file_metadata.get("supabase_url"),
                },
                "expires_at": file_metadata.get("expires_at"),
                "updated_at": datetime.utcnow().isoformat(),
            }

            existing = await self.get_job_file(file_id)
            if existing:
                response = self.client.table("job_files").update(data).eq("file_id", file_id).execute()
            else:
                response = self.client.table("job_files").insert(data).execute()

            if not response.data:
                raise Exception("Supabase returned no job_files row")

            logger.info(f"Stored durable file metadata for {file_id}")
            return self._job_file_record_to_metadata(response.data[0])
        except Exception as e:
            logger.error(f"Failed to upsert job file metadata: {e}")
            raise

    async def get_job_file(self, file_id: str) -> Optional[Dict[str, Any]]:
        """Get durable generated-file metadata by file_id."""
        try:
            response = self.client.table("job_files").select("*").eq("file_id", file_id).execute()
            if not response.data:
                return None
            return self._job_file_record_to_metadata(response.data[0])
        except Exception as e:
            logger.error(f"Failed to get job file {file_id}: {e}")
            return None

    async def get_job_file_by_storage_path(self, storage_path: str) -> Optional[Dict[str, Any]]:
        """Get durable generated-file metadata by Supabase Storage path."""
        try:
            response = self.client.table("job_files").select("*").eq("storage_path", storage_path).execute()
            if not response.data:
                return None
            return self._job_file_record_to_metadata(response.data[0])
        except Exception as e:
            logger.error(f"Failed to get job file by storage path {storage_path}: {e}")
            return None

    async def get_job_files_for_job(self, job_id: str) -> List[Dict[str, Any]]:
        """Get all durable generated files for a job."""
        try:
            response = self.client.table("job_files")\
                .select("*")\
                .eq("job_id", job_id)\
                .order("created_at")\
                .execute()
            return [self._job_file_record_to_metadata(record) for record in (response.data or [])]
        except Exception as e:
            logger.error(f"Failed to get files for job {job_id}: {e}")
            return []

    async def get_job_files_for_session(self, session_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Get durable generated files owned by an anonymous session."""
        try:
            response = self.client.table("job_files")\
                .select("*")\
                .eq("owner_session_id", session_id)\
                .order("created_at.desc")\
                .limit(limit)\
                .execute()
            return [self._job_file_record_to_metadata(record) for record in (response.data or [])]
        except Exception as e:
            logger.error(f"Failed to get files for session {session_id}: {e}")
            return []

    async def get_job_files_by_ids(self, file_ids: List[str]) -> List[Dict[str, Any]]:
        """Get durable generated files for a small list of file IDs."""
        files = []
        for file_id in file_ids:
            file_metadata = await self.get_job_file(file_id)
            if file_metadata:
                files.append(file_metadata)
        return files

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

    async def upload_source_file(
        self,
        file_data: bytes,
        owner_id: str,
        job_id: str,
        filename: str,
        content_type: str = "application/octet-stream"
    ) -> Dict[str, Any]:
        """
        Upload an original input image to durable storage before queueing work.

        Workers should receive this storage path instead of the raw image bytes.
        """
        try:
            safe_filename = "".join(
                char if char.isalnum() or char in ("-", "_", ".") else "_"
                for char in filename
            ).strip("._") or "image"
            storage_path = f"users/{owner_id}/jobs/{job_id}/inputs/{safe_filename}"

            response = self.client.storage.from_(self.storage_bucket).upload(
                path=storage_path,
                file=file_data,
                file_options={
                    "content-type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    "upsert": "true"
                }
            )

            if hasattr(response, "error") and response.error:
                raise Exception(f"Upload error: {response.error}")
            if isinstance(response, dict) and response.get("error"):
                raise Exception(f"Upload error: {response['error']}")

            logger.info(f"Uploaded source file to Supabase Storage: {storage_path}")
            return {
                "storage_path": storage_path,
                "filename": safe_filename,
                "content_type": content_type,
                "size_bytes": len(file_data)
            }
        except Exception as e:
            logger.error(f"Failed to upload source file to Supabase Storage: {e}")
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
                .order("created_at.desc")\
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
                .order("saved_at.desc")\
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

    async def create_share_session(
        self,
        session_id: str,
        user_id: Optional[str],
        file_ids: List[str],
        title: str,
        description: Optional[str] = None,
        expires_at: Optional[datetime] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Create a new share session."""
        try:
            data = {
                'session_id': session_id,
                'file_ids': file_ids,
                'title': title,
                'description': description,
                'expires_at': expires_at.isoformat() if expires_at else None,
                'metadata': metadata or {},
                'is_active': True
            }
            
            # Only include user_id if it's not None
            if user_id is not None:
                data['user_id'] = user_id
            
            result = self.client.table('share_sessions').insert(data).execute()
            
            if result.data:
                logger.info(f"Created share session {session_id} with {len(file_ids)} files")
                return result.data[0]
            else:
                raise Exception("Failed to create share session")
                
        except Exception as e:
            logger.error(f"Failed to create share session: {e}")
            raise

    async def get_share_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get a share session by ID."""
        try:
            result = self.client.table('share_sessions').select('*').eq(
                'session_id', session_id
            ).execute()
            
            if result.data and len(result.data) > 0:
                return result.data[0]
            return None
            
        except Exception as e:
            logger.error(f"Failed to get share session: {e}")
            return None

    async def increment_session_access(self, session_id: str) -> None:
        """Increment the access count for a share session."""
        try:
            # Call the stored function
            self.client.rpc('increment_session_access', {
                'p_session_id': session_id
            }).execute()
            
            logger.debug(f"Incremented access count for session {session_id}")
            
        except Exception as e:
            logger.error(f"Failed to increment session access: {e}")
            # Don't raise, this is not critical

    async def deactivate_share_session(self, session_id: str) -> None:
        """Deactivate a share session."""
        try:
            result = self.client.table('share_sessions').update({
                'is_active': False
            }).eq('session_id', session_id).execute()
            
            if result.data:
                logger.info(f"Deactivated share session {session_id}")
            else:
                raise Exception("Failed to deactivate share session")
                
        except Exception as e:
            logger.error(f"Failed to deactivate share session: {e}")
            raise

    async def get_user_share_sessions(self, user_id: str) -> List[Dict[str, Any]]:
        """Get all share sessions for a user."""
        try:
            result = self.client.table('share_sessions').select('*').eq(
                'user_id', user_id
            ).order('created_at.desc').execute()
            
            return result.data or []
            
        except Exception as e:
            logger.error(f"Failed to get user share sessions: {e}")
            return []


# Global instance
_supabase_service: Optional[SupabaseService] = None


def get_supabase_service() -> SupabaseService:
    """Get or create the global Supabase service instance."""
    global _supabase_service
    if _supabase_service is None:
        _supabase_service = SupabaseService()
    return _supabase_service
