"""
Supabase service for database and storage operations.
"""

import logging
import json
import copy
import csv
import io
import re
from typing import Optional, Dict, Any, List
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
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
                        'total_credits': result.get('total_credits', settings.rate_limit_authenticated_images_per_day),
                        'used_credits': result.get('used_credits', 0),
                        'available_credits': result.get('available_credits', settings.rate_limit_authenticated_images_per_day)
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
                        'total_credits': settings.rate_limit_authenticated_images_per_day,
                        'used_credits': 0,
                        'reset_date': date.today().replace(day=1).isoformat()
                    }).execute()
                    
                    logger.info(f"[Credits] Created credit record for user {user_id}")
                except Exception as insert_error:
                    logger.error(f"[Credits] Failed to create credit record: {insert_error}")
                
                # Return default values
                return {
                    'total_credits': settings.rate_limit_authenticated_images_per_day,
                    'used_credits': 0,
                    'available_credits': settings.rate_limit_authenticated_images_per_day
                }

        except Exception as e:
            logger.error(f"[Credits] Failed to get credits for user {user_id}: {e}", exc_info=True)
            return {
                'total_credits': settings.rate_limit_authenticated_images_per_day,
                'used_credits': 0,
                'available_credits': settings.rate_limit_authenticated_images_per_day
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
            "document_id": record.get("document_id"),
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
            "document_mode": metadata.get("document_mode"),
            "requires_review": metadata.get("requires_review"),
            "confidence_score": metadata.get("confidence_score"),
            "review_flags": metadata.get("review_flags") or [],
            "source_page": metadata.get("source_page"),
            "source_page_count": metadata.get("source_page_count"),
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

            metadata = {
                **(file_metadata.get("metadata") or {}),
                "completed_at": file_metadata.get("completed_at"),
                "supabase_url": file_metadata.get("supabase_url"),
                "document_mode": file_metadata.get("document_mode"),
                "selected_mode": file_metadata.get("selected_mode"),
                "detected_mode": file_metadata.get("detected_mode"),
                "detection_confidence": file_metadata.get("detection_confidence"),
                "detection_review_reason": file_metadata.get("detection_review_reason"),
                "requires_review": file_metadata.get("requires_review"),
                "confidence_score": file_metadata.get("confidence_score"),
                "review_flags": file_metadata.get("review_flags") or [],
                "source_page": file_metadata.get("source_page"),
                "source_page_count": file_metadata.get("source_page_count"),
            }
            data = {
                "file_id": file_id,
                "job_id": job_id,
                "document_id": file_metadata.get("document_id"),
                "image_id": file_metadata.get("image_id"),
                "owner_user_id": owner_user_id,
                "owner_session_id": owner_session_id,
                "storage_path": storage_path,
                "filename": filename,
                "original_filename": file_metadata.get("original_filename"),
                "content_type": file_metadata.get("content_type") or "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "size_bytes": file_metadata.get("size_bytes"),
                "status": file_metadata.get("status") or "completed",
                "metadata": metadata,
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

    async def create_job_document(self, document: Dict[str, Any]) -> Dict[str, Any]:
        """Store one durable logical source document for a processing job."""
        owner_user_id = document.get("owner_user_id") or None
        owner_session_id = document.get("owner_session_id") or None
        if bool(owner_user_id) == bool(owner_session_id):
            raise ValueError("document metadata must include exactly one user or session owner")

        data = {
            "id": document["id"],
            "job_id": document["job_id"],
            "workspace_id": document.get("workspace_id"),
            "owner_user_id": owner_user_id,
            "owner_session_id": owner_session_id,
            "original_filename": document["original_filename"],
            "source_storage_path": document["source_storage_path"],
            "source_content_type": document.get("source_content_type"),
            "selected_mode": document.get("selected_mode") or "table",
            "detected_mode": document.get("detected_mode"),
            "resolved_mode": document.get("resolved_mode") or (
                document.get("selected_mode")
                if document.get("selected_mode") in {"table", "invoice", "receipt", "bank_statement", "notes"}
                else None
            ),
            "detection_confidence": document.get("detection_confidence"),
            "detection_review_reason": document.get("detection_review_reason"),
            "mode_override_history": document.get("mode_override_history") or [],
            "status": document.get("status") or "queued",
            "review_status": document.get("review_status") or "needs_review",
            "metadata": document.get("metadata") or {},
            "expires_at": document.get("expires_at"),
            "updated_at": datetime.utcnow().isoformat(),
        }
        response = self.client.table("job_documents").insert(data).execute()
        if not response.data:
            raise Exception("Supabase returned no job_documents row")
        return response.data[0]

    async def update_job_documents_status(self, job_id: str, status: str) -> None:
        """Set the lifecycle status for every logical document in a job."""
        self.client.table("job_documents")\
            .update({"status": status, "updated_at": datetime.utcnow().isoformat()})\
            .eq("job_id", job_id)\
            .execute()

    async def finalize_job_document_statuses(self, job_id: str) -> None:
        """Set each document result status from its own page/unit extraction outcomes."""
        documents = await self.get_job_documents(job_id)
        for document in documents:
            extraction_statuses = {
                extraction.get("status")
                for extraction in document.get("extractions", [])
            }
            if extraction_statuses and extraction_statuses == {"completed"}:
                status = "completed"
            elif extraction_statuses and extraction_statuses <= {"completed", "needs_review"}:
                status = "needs_review"
            elif "completed" in extraction_statuses:
                status = "partially_completed"
            elif "needs_review" in extraction_statuses:
                status = "needs_review"
            elif "failed" in extraction_statuses:
                status = "failed"
            else:
                status = document.get("status") or "processing"

            current_review_status = document.get("review_status")
            extraction_review_statuses = {
                extraction.get("review_status")
                for extraction in document.get("extractions", [])
            }
            if current_review_status in {"published", "deleted"}:
                review_status = current_review_status
            elif "edited" in extraction_review_statuses:
                review_status = "edited"
            elif "needs_review" in extraction_review_statuses or status in {"needs_review", "failed"}:
                review_status = "needs_review" if status != "failed" else "failed"
            elif extraction_review_statuses and extraction_review_statuses <= {"ready"}:
                review_status = "ready"
            else:
                review_status = current_review_status or "needs_review"

            self.client.table("job_documents")\
                .update({
                    "status": status,
                    "review_status": review_status,
                    "updated_at": datetime.utcnow().isoformat(),
                })\
                .eq("id", document["id"])\
                .execute()

    async def upsert_document_extraction(self, extraction: Dict[str, Any]) -> Dict[str, Any]:
        """Persist page/unit extraction state independently from Redis progress."""
        data = {
            "document_id": extraction["document_id"],
            "job_id": extraction["job_id"],
            "processing_unit_id": extraction["processing_unit_id"],
            "result_file_id": extraction.get("result_file_id"),
            "status": extraction.get("status") or "queued",
            "structured_data": extraction.get("structured_data") or {},
            "review_status": extraction.get("review_status") or "pending",
            "validation_flags": extraction.get("validation_flags") or [],
            "edited": bool(extraction.get("edited", False)),
            "metadata": extraction.get("metadata") or {},
            "updated_at": datetime.utcnow().isoformat(),
        }
        if "raw_structured_data" in extraction:
            data["raw_structured_data"] = extraction.get("raw_structured_data") or {}
        if "reviewed_data" in extraction:
            data["reviewed_data"] = extraction.get("reviewed_data") or {}
        elif extraction.get("status") == "completed" and "structured_data" in extraction:
            data["raw_structured_data"] = extraction.get("structured_data") or {}
            data["reviewed_data"] = extraction.get("structured_data") or {}
        response = self.client.table("document_extractions")\
            .upsert(data, on_conflict="document_id,processing_unit_id")\
            .execute()
        if not response.data:
            raise Exception("Supabase returned no document_extractions row")
        return response.data[0]

    @staticmethod
    def _initial_review_data(extraction: Dict[str, Any]) -> Dict[str, Any]:
        data = extraction.get("reviewed_data") or extraction.get("structured_data") or {}
        reviewed_data = copy.deepcopy(data) if isinstance(data, dict) else {"value": data}
        csv_content = reviewed_data.get("csv")
        if isinstance(csv_content, str) and "review_grid" not in reviewed_data:
            reviewed_data["review_grid"] = list(csv.reader(io.StringIO(csv_content)))
        return reviewed_data

    @staticmethod
    def _document_metadata(document: Dict[str, Any]) -> Dict[str, Any]:
        metadata = document.get("metadata") or {}
        if isinstance(metadata, dict):
            return metadata
        if isinstance(metadata, str):
            try:
                parsed = json.loads(metadata)
                return parsed if isinstance(parsed, dict) else {}
            except json.JSONDecodeError:
                return {}
        return {}

    @classmethod
    def active_duplicate_warnings(cls, document: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Return unresolved duplicate warnings stored against a durable document."""
        warnings = cls._document_metadata(document).get("duplicate_warnings") or []
        return [
            warning for warning in warnings
            if isinstance(warning, dict) and not warning.get("overridden")
        ]

    @staticmethod
    def _normalize_duplicate_text(value: Any) -> str:
        return " ".join(str(value or "").casefold().split())

    @staticmethod
    def _normalize_duplicate_amount(value: Any) -> str:
        amount = re.sub(r"[^0-9,.\-]", "", str(value or "")).replace(",", "")
        if not amount:
            return ""
        try:
            return str(Decimal(amount).quantize(Decimal("0.01")))
        except InvalidOperation:
            return SupabaseService._normalize_duplicate_text(value)

    def _duplicate_review_payload(self, document: Dict[str, Any]) -> Dict[str, Any]:
        """Merge scalar reviewed fields needed for accounting duplicate signatures."""
        payload: Dict[str, Any] = {}
        ordered_extractions = sorted(
            document.get("extractions") or [],
            key=lambda item: (
                self._document_metadata(item).get("source_page") is None,
                self._document_metadata(item).get("source_page") or 0,
            ),
        )
        for extraction in ordered_extractions:
            data = self._initial_review_data(extraction)
            for key, value in data.items():
                if key in {"line_items", "transactions", "tables", "review_flags", "review_grid", "csv"}:
                    continue
                if key not in payload and value not in (None, "", [], {}):
                    payload[key] = value
        return payload

    def _duplicate_document_mode(self, document: Dict[str, Any], payload: Dict[str, Any]) -> str:
        mode = document.get("resolved_mode") or document.get("detected_mode") or document.get("selected_mode") or ""
        if mode == "invoice_receipt":
            return "invoice" if payload.get("invoice_number") else "receipt"
        return str(mode)

    async def resolve_owned_workspace_id(self, user_id: Optional[str], workspace_id: Optional[str] = None) -> Optional[str]:
        """Resolve an authenticated user's requested or active workspace."""
        if not user_id:
            return None
        if workspace_id:
            response = self.client.table("workspaces")\
                .select("id")\
                .eq("id", workspace_id)\
                .eq("owner_user_id", user_id)\
                .limit(1)\
                .execute()
            if not response.data:
                raise ValueError("Workspace not found or access denied")
            return str(response.data[0]["id"])

        preference = self.client.table("workspace_preferences")\
            .select("active_workspace_id")\
            .eq("user_id", user_id)\
            .limit(1)\
            .execute()
        active_workspace_id = (
            preference.data[0].get("active_workspace_id")
            if preference.data
            else None
        )
        if active_workspace_id:
            return await self.resolve_owned_workspace_id(user_id, str(active_workspace_id))

        fallback = self.client.table("workspaces")\
            .select("id")\
            .eq("owner_user_id", user_id)\
            .order("created_at")\
            .limit(1)\
            .execute()
        return str(fallback.data[0]["id"]) if fallback.data else None

    @staticmethod
    def _clean_vendor_rule_fields(fields: Dict[str, Any]) -> Dict[str, str]:
        allowed = {
            "category_account",
            "tax_code",
            "currency",
            "payment_terms",
            "destination_treatment",
        }
        return {
            key: str(value).strip()
            for key, value in fields.items()
            if key in allowed and value not in (None, "") and str(value).strip()
        }

    def _vendor_identity(self, document: Dict[str, Any]) -> Optional[Dict[str, str]]:
        payload = self._duplicate_review_payload(document)
        mode = self._duplicate_document_mode(document, payload)
        if mode == "invoice":
            name = str(payload.get("vendor_name") or "").strip()
        elif mode == "receipt":
            name = str(payload.get("merchant") or "").strip()
        else:
            return None
        vendor_key = self._normalize_duplicate_text(name)
        return {"vendor_key": vendor_key, "display_name": name, "applies_to": mode} if vendor_key else None

    async def get_vendor_suggestion(self, document: Dict[str, Any], user_id: str) -> Optional[Dict[str, Any]]:
        """Return a saved rule as an optional suggestion, never applied extraction data."""
        identity = self._vendor_identity(document)
        if not identity:
            return None
        workspace_id = document.get("workspace_id") or await self.resolve_owned_workspace_id(user_id)
        if not workspace_id:
            return None
        response = self.client.table("vendor_rules")\
            .select("*")\
            .eq("owner_user_id", user_id)\
            .eq("workspace_id", workspace_id)\
            .eq("vendor_key", identity["vendor_key"])\
            .eq("enabled", True)\
            .execute()
        rules = response.data or []
        return next(
            (rule for rule in rules if rule.get("applies_to") == identity["applies_to"]),
            next((rule for rule in rules if rule.get("applies_to") == "both"), None),
        )

    async def save_vendor_rule_from_document(
        self,
        job_id: str,
        document_id: str,
        user_id: str,
        suggested_fields: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Create or update a remembered vendor only from a confirmed document."""
        document = await self.get_job_document(job_id, document_id)
        if not document or document.get("owner_user_id") != user_id:
            raise ValueError("Document not found")
        if document.get("review_status") not in {"ready", "published"}:
            raise ValueError("Confirm this document as Ready before remembering its vendor")
        identity = self._vendor_identity(document)
        if not identity:
            raise ValueError("Vendor memory is available only for reviewed invoices and receipts with a vendor name")
        clean_fields = self._clean_vendor_rule_fields(suggested_fields)
        if not clean_fields:
            raise ValueError("Enter at least one vendor suggestion before saving")
        workspace_id = document.get("workspace_id") or await self.resolve_owned_workspace_id(user_id)
        if not workspace_id:
            raise ValueError("Select a workspace before saving vendor memory")
        changed_at = datetime.utcnow().isoformat()
        data = {
            "owner_user_id": user_id,
            "workspace_id": workspace_id,
            "vendor_key": identity["vendor_key"],
            "display_name": identity["display_name"],
            "applies_to": identity["applies_to"],
            "suggested_fields": clean_fields,
            "enabled": True,
            "source_document_id": document_id,
            "approved_at": changed_at,
            "updated_at": changed_at,
        }
        response = self.client.table("vendor_rules")\
            .upsert(data, on_conflict="workspace_id,vendor_key,applies_to")\
            .execute()
        if not response.data:
            raise Exception("Supabase returned no vendor rule row")
        return response.data[0]

    async def list_vendor_rules(self, user_id: str, workspace_id: Optional[str] = None) -> List[Dict[str, Any]]:
        resolved_workspace_id = await self.resolve_owned_workspace_id(user_id, workspace_id)
        if not resolved_workspace_id:
            return []
        response = self.client.table("vendor_rules")\
            .select("*")\
            .eq("owner_user_id", user_id)\
            .eq("workspace_id", resolved_workspace_id)\
            .order("display_name")\
            .execute()
        return response.data or []

    async def update_vendor_rule(self, rule_id: str, user_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        response = self.client.table("vendor_rules")\
            .select("*")\
            .eq("id", rule_id)\
            .eq("owner_user_id", user_id)\
            .limit(1)\
            .execute()
        if not response.data:
            raise ValueError("Vendor rule not found")
        update: Dict[str, Any] = {"updated_at": datetime.utcnow().isoformat()}
        if updates.get("display_name") is not None:
            update["display_name"] = str(updates["display_name"]).strip()
        if updates.get("suggested_fields") is not None:
            update["suggested_fields"] = self._clean_vendor_rule_fields(updates["suggested_fields"])
        if updates.get("enabled") is not None:
            update["enabled"] = bool(updates["enabled"])
        updated = self.client.table("vendor_rules")\
            .update(update)\
            .eq("id", rule_id)\
            .eq("owner_user_id", user_id)\
            .execute()
        if not updated.data:
            raise Exception("Supabase returned no updated vendor rule")
        return updated.data[0]

    async def delete_vendor_rule(self, rule_id: str, user_id: str) -> None:
        existing = self.client.table("vendor_rules")\
            .select("id")\
            .eq("id", rule_id)\
            .eq("owner_user_id", user_id)\
            .limit(1)\
            .execute()
        if not existing.data:
            raise ValueError("Vendor rule not found")
        self.client.table("vendor_rules")\
            .delete()\
            .eq("id", rule_id)\
            .eq("owner_user_id", user_id)\
            .execute()

    @staticmethod
    def _accounts_payable_editable_fields(fields: Dict[str, Any]) -> Dict[str, Any]:
        allowed_text = {"vendor", "due_date", "account_category", "tax_code", "reference"}
        cleaned: Dict[str, Any] = {
            key: str(value).strip()
            for key, value in fields.items()
            if key in allowed_text and value is not None
        }
        if isinstance(fields.get("line_items"), list):
            cleaned["line_items"] = [
                item for item in fields["line_items"] if isinstance(item, dict)
            ]
        return cleaned

    def _accounts_payable_draft_from_document(self, document: Dict[str, Any]) -> Dict[str, Any]:
        payload = self._duplicate_review_payload(document)
        line_items: List[Dict[str, Any]] = []
        ordered_extractions = sorted(
            document.get("extractions") or [],
            key=lambda item: (
                self._document_metadata(item).get("source_page") is None,
                self._document_metadata(item).get("source_page") or 0,
            ),
        )
        for extraction in ordered_extractions:
            extracted_lines = self._initial_review_data(extraction).get("line_items")
            if isinstance(extracted_lines, list):
                line_items.extend(
                    copy.deepcopy(item) for item in extracted_lines if isinstance(item, dict)
                )
        return {
            "vendor": str(payload.get("vendor_name") or "").strip(),
            "invoice_number": str(payload.get("invoice_number") or "").strip(),
            "invoice_date": str(payload.get("invoice_date") or "").strip(),
            "due_date": str(payload.get("due_date") or "").strip(),
            "reference": str(payload.get("po_reference") or payload.get("reference") or "").strip(),
            "account_category": "",
            "tax_code": "",
            "currency": str(payload.get("currency") or "").strip(),
            "subtotal": payload.get("subtotal"),
            "tax_amount": payload.get("tax_amount") or payload.get("tax_vat_amount"),
            "total": payload.get("total"),
            "line_items": line_items,
        }

    async def _enrich_accounts_payable_item(self, item: Dict[str, Any], user_id: str) -> Dict[str, Any]:
        document = await self.get_job_document(str(item["job_id"]), str(item["document_id"]))
        enriched = {**item}
        if document:
            enriched["document_review_status"] = document.get("review_status")
            enriched["vendor_suggestion"] = await self.get_vendor_suggestion(document, user_id)
        if item.get("attachment_visible") and item.get("source_storage_path"):
            enriched["source_access_url"] = await self.create_signed_url(
                str(item["source_storage_path"]),
                expires_in=60 * 60,
            )
        return enriched

    async def create_accounts_payable_item_from_document(
        self,
        job_id: str,
        document_id: str,
        user_id: str,
    ) -> Dict[str, Any]:
        """Create a draft-bill queue record from a confirmed invoice."""
        document = await self.get_job_document(job_id, document_id)
        if not document or document.get("owner_user_id") != user_id:
            raise ValueError("Document not found")
        payload = self._duplicate_review_payload(document)
        if self._duplicate_document_mode(document, payload) != "invoice":
            raise ValueError("Only invoices can enter Accounts Payable")
        if document.get("review_status") not in {"ready", "published"}:
            raise ValueError("Confirm this invoice as Ready before adding it to Accounts Payable")
        workspace_id = document.get("workspace_id") or await self.resolve_owned_workspace_id(user_id)
        if not workspace_id:
            raise ValueError("Select a workspace before adding an Accounts Payable item")
        existing = self.client.table("accounts_payable_items")\
            .select("*")\
            .eq("owner_user_id", user_id)\
            .eq("workspace_id", workspace_id)\
            .eq("document_id", document_id)\
            .limit(1)\
            .execute()
        if existing.data:
            return await self._enrich_accounts_payable_item(existing.data[0], user_id)
        changed_at = datetime.utcnow().isoformat()
        item = {
            "owner_user_id": user_id,
            "workspace_id": workspace_id,
            "document_id": document_id,
            "job_id": job_id,
            "status": "needs_coding",
            "draft_data": self._accounts_payable_draft_from_document(document),
            "attachment_visible": True,
            "source_filename": document["original_filename"],
            "source_storage_path": document["source_storage_path"],
            "metadata": {
                "entered_from_review_status": document.get("review_status"),
                "status_history": [{
                    "from_status": None,
                    "to_status": "needs_coding",
                    "actor": {"user_id": user_id},
                    "changed_at": changed_at,
                }],
            },
            "published_at": None,
            "updated_at": changed_at,
        }
        response = self.client.table("accounts_payable_items").insert(item).execute()
        if not response.data:
            raise Exception("Supabase returned no Accounts Payable item")
        return await self._enrich_accounts_payable_item(response.data[0], user_id)

    async def list_accounts_payable_items(
        self,
        user_id: str,
        workspace_id: Optional[str] = None,
        ap_status: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        resolved_workspace_id = await self.resolve_owned_workspace_id(user_id, workspace_id)
        if not resolved_workspace_id:
            return []
        query = self.client.table("accounts_payable_items")\
            .select("*")\
            .eq("owner_user_id", user_id)\
            .eq("workspace_id", resolved_workspace_id)
        if ap_status:
            query = query.eq("status", ap_status)
        response = query.order("updated_at", desc=True).execute()
        return [
            await self._enrich_accounts_payable_item(item, user_id)
            for item in (response.data or [])
        ]

    async def get_accounts_payable_item(self, item_id: str, user_id: str) -> Dict[str, Any]:
        response = self.client.table("accounts_payable_items")\
            .select("*")\
            .eq("id", item_id)\
            .eq("owner_user_id", user_id)\
            .limit(1)\
            .execute()
        if not response.data:
            raise ValueError("Accounts Payable item not found")
        return await self._enrich_accounts_payable_item(response.data[0], user_id)

    async def update_accounts_payable_item(
        self,
        item_id: str,
        user_id: str,
        updates: Dict[str, Any],
    ) -> Dict[str, Any]:
        current = await self.get_accounts_payable_item(item_id, user_id)
        if current.get("status") == "published" and (
            updates.get("draft_data") is not None or updates.get("attachment_visible") is not None
        ):
            raise ValueError("Published items cannot be edited")
        changed_at = datetime.utcnow().isoformat()
        update: Dict[str, Any] = {"updated_at": changed_at}
        draft_data = dict(current.get("draft_data") or {})
        if updates.get("draft_data") is not None:
            draft_data.update(self._accounts_payable_editable_fields(updates["draft_data"]))
            update["draft_data"] = draft_data
        if updates.get("attachment_visible") is not None:
            update["attachment_visible"] = bool(updates["attachment_visible"])
        next_status = updates.get("status")
        if next_status:
            if current.get("status") == "published" and next_status != "published":
                raise ValueError("Published items cannot be returned to preparation")
            if next_status == "ready_to_publish":
                missing = [
                    label
                    for label, key in (
                        ("vendor", "vendor"),
                        ("due date", "due_date"),
                        ("account/category", "account_category"),
                    )
                    if not str(draft_data.get(key) or "").strip()
                ]
                if missing:
                    raise ValueError(f"Complete {', '.join(missing)} before marking Ready to publish")
                if not draft_data.get("total") and not draft_data.get("line_items"):
                    raise ValueError("A bill candidate needs a total or line items before publishing")
            if next_status == "published" and current.get("status") != "ready_to_publish":
                raise ValueError("Only Ready to publish items can be marked Published")
            update["status"] = next_status
            if next_status == "published":
                update["published_at"] = changed_at
            metadata = dict(current.get("metadata") or {})
            history = list(metadata.get("status_history") or [])
            history.append({
                "from_status": current.get("status"),
                "to_status": next_status,
                "reason": updates.get("reason") or "",
                "actor": {"user_id": user_id},
                "changed_at": changed_at,
            })
            update["metadata"] = {**metadata, "status_history": history}
        response = self.client.table("accounts_payable_items")\
            .update(update)\
            .eq("id", item_id)\
            .eq("owner_user_id", user_id)\
            .execute()
        if not response.data:
            raise ValueError("Accounts Payable item not found")
        return await self._enrich_accounts_payable_item(response.data[0], user_id)

    async def bulk_publish_accounts_payable_items(
        self,
        item_ids: List[str],
        user_id: str,
        reason: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        published: List[Dict[str, Any]] = []
        for item_id in item_ids:
            published.append(await self.update_accounts_payable_item(
                item_id=item_id,
                user_id=user_id,
                updates={"status": "published", "reason": reason or "Marked published outside AxLiner"},
            ))
        return published

    def _source_page_hashes(self, document: Dict[str, Any]) -> set:
        return {
            self._document_metadata(extraction).get("source_sha256")
            for extraction in document.get("extractions") or []
            if self._document_metadata(extraction).get("source_sha256")
        }

    def _accounting_duplicate_signature(self, document: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        payload = self._duplicate_review_payload(document)
        mode = self._duplicate_document_mode(document, payload)
        if mode == "invoice":
            vendor = self._normalize_duplicate_text(payload.get("vendor_name"))
            invoice_number = self._normalize_duplicate_text(payload.get("invoice_number"))
            if not vendor or not invoice_number:
                return None
            return {
                "mode": mode,
                "primary_key": f"{vendor}|{invoice_number}",
                "support": {
                    "invoice_date": self._normalize_duplicate_text(payload.get("invoice_date")),
                    "total": self._normalize_duplicate_amount(payload.get("total")),
                },
            }
        if mode == "receipt":
            merchant = self._normalize_duplicate_text(payload.get("merchant"))
            receipt_date = self._normalize_duplicate_text(payload.get("date"))
            total = self._normalize_duplicate_amount(payload.get("total"))
            if not merchant or not receipt_date or not total:
                return None
            return {
                "mode": mode,
                "primary_key": f"{merchant}|{receipt_date}|{total}",
                "support": {},
            }
        return None

    async def _duplicate_peer_documents(self, document: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Load only documents belonging to the same user or anonymous session."""
        query = self.client.table("job_documents").select("*")
        if document.get("owner_user_id"):
            query = query.eq("owner_user_id", document["owner_user_id"])
        elif document.get("owner_session_id"):
            query = query.eq("owner_session_id", document["owner_session_id"])
        else:
            return []
        response = query.order("created_at").limit(500).execute()
        peers = [
            row for row in (response.data or [])
            if row.get("id") != document.get("id") and row.get("review_status") != "deleted"
        ]
        if not peers:
            return []
        extraction_response = self.client.table("document_extractions")\
            .select("*")\
            .in_("document_id", [peer["id"] for peer in peers])\
            .execute()
        extractions_by_document: Dict[str, List[Dict[str, Any]]] = {}
        for extraction in extraction_response.data or []:
            extractions_by_document.setdefault(extraction["document_id"], []).append(extraction)
        return [
            {**peer, "extractions": extractions_by_document.get(peer["id"], [])}
            for peer in peers
        ]

    async def refresh_document_duplicate_warnings(self, job_id: str, document_id: str) -> Dict[str, Any]:
        """Recompute durable exact-source and accounting-key duplicate warnings."""
        document = await self.get_job_document(job_id, document_id)
        if not document:
            raise ValueError("Document not found")
        metadata = self._document_metadata(document)
        peers = await self._duplicate_peer_documents(document)
        override_history = [
            entry for entry in (metadata.get("duplicate_override_history") or [])
            if isinstance(entry, dict)
        ]
        overridden_warning_ids = {entry.get("warning_id") for entry in override_history}
        warnings: List[Dict[str, Any]] = []
        current_hash = metadata.get("source_sha256")
        current_payload = self._duplicate_review_payload(document)
        current_mode = self._duplicate_document_mode(document, current_payload)
        current_page_hashes = self._source_page_hashes(document)
        exact_peer_ids = set()

        for peer in peers:
            peer_metadata = self._document_metadata(peer)
            same_document_source = bool(current_hash and peer_metadata.get("source_sha256") == current_hash)
            matched_statement_pages = (
                current_page_hashes.intersection(self._source_page_hashes(peer))
                if current_mode == "bank_statement"
                else set()
            )
            if same_document_source or matched_statement_pages:
                exact_peer_ids.add(peer["id"])
                warning_type = "statement_fingerprint" if current_mode == "bank_statement" else "exact_source"
                warning_id = f"{warning_type}:{peer['id']}"
                warnings.append({
                    "id": warning_id,
                    "type": warning_type,
                    "code": "repeated_statement_source" if warning_type == "statement_fingerprint" else "same_source_file",
                    "message": (
                        "This statement source matches a previously uploaded statement."
                        if warning_type == "statement_fingerprint"
                        else "This source file matches an earlier upload."
                    ),
                    "matched_document_id": peer["id"],
                    "matched_job_id": peer.get("job_id"),
                    "matched_filename": peer.get("original_filename"),
                    "matched_created_at": peer.get("created_at"),
                    "fields": {"matched_pages": len(matched_statement_pages)} if matched_statement_pages else {},
                    "overridden": warning_id in overridden_warning_ids,
                    "detected_at": datetime.utcnow().isoformat(),
                })

        signature = self._accounting_duplicate_signature(document)
        if signature:
            for peer in peers:
                if peer["id"] in exact_peer_ids:
                    continue
                peer_signature = self._accounting_duplicate_signature(peer)
                if not peer_signature or peer_signature["mode"] != signature["mode"]:
                    continue
                if peer_signature["primary_key"] != signature["primary_key"]:
                    continue
                warning_id = f"accounting_key:{peer['id']}"
                warnings.append({
                    "id": warning_id,
                    "type": "accounting_key",
                    "code": f"duplicate_{signature['mode']}_key",
                    "message": (
                        "Vendor and invoice number match an earlier invoice."
                        if signature["mode"] == "invoice"
                        else "Merchant, date, and total match an earlier receipt."
                    ),
                    "matched_document_id": peer["id"],
                    "matched_job_id": peer.get("job_id"),
                    "matched_filename": peer.get("original_filename"),
                    "matched_created_at": peer.get("created_at"),
                    "fields": signature["support"],
                    "overridden": warning_id in overridden_warning_ids,
                    "detected_at": datetime.utcnow().isoformat(),
                })

        updated_metadata = {
            **metadata,
            "duplicate_warnings": warnings,
            "duplicate_checked_at": datetime.utcnow().isoformat(),
        }
        update_response = self.client.table("job_documents").update({
            "metadata": updated_metadata,
            "updated_at": datetime.utcnow().isoformat(),
        }).eq("id", document_id).eq("job_id", job_id).execute()
        if update_response.data:
            document = {**document, **update_response.data[0], "metadata": updated_metadata}
        else:
            document = {**document, "metadata": updated_metadata}
        document["duplicate_warnings"] = warnings
        return document

    async def override_document_duplicate_warning(
        self,
        job_id: str,
        document_id: str,
        warning_id: str,
        actor: Dict[str, Any],
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Persist an explicit user acknowledgement for a duplicate warning."""
        document = await self.refresh_document_duplicate_warnings(job_id, document_id)
        metadata = self._document_metadata(document)
        warnings = list(metadata.get("duplicate_warnings") or [])
        warning = next((item for item in warnings if item.get("id") == warning_id), None)
        if not warning:
            raise ValueError("Duplicate warning not found")
        changed_at = datetime.utcnow().isoformat()
        history = list(metadata.get("duplicate_override_history") or [])
        history.append({
            "warning_id": warning_id,
            "warning_type": warning.get("type"),
            "matched_document_id": warning.get("matched_document_id"),
            "reason": reason or "Confirmed as a separate document",
            "actor": actor,
            "changed_at": changed_at,
        })
        for item in warnings:
            if item.get("id") == warning_id:
                item.update({
                    "overridden": True,
                    "overridden_at": changed_at,
                    "overridden_by": actor,
                    "override_reason": reason or "Confirmed as a separate document",
                })
        updated_metadata = {
            **metadata,
            "duplicate_warnings": warnings,
            "duplicate_override_history": history,
        }
        self.client.table("job_documents").update({
            "metadata": updated_metadata,
            "updated_at": changed_at,
        }).eq("id", document_id).eq("job_id", job_id).execute()
        refreshed = await self.get_job_document(job_id, document_id)
        if not refreshed:
            raise ValueError("Document not found")
        refreshed["metadata"] = updated_metadata
        refreshed["duplicate_warnings"] = warnings
        return refreshed

    @staticmethod
    def _value_at_path(data: Any, path: List[Any]) -> Any:
        current = data
        for part in path:
            if isinstance(current, dict) and isinstance(part, str):
                current = current.get(part)
            elif isinstance(current, list) and isinstance(part, int) and 0 <= part < len(current):
                current = current[part]
            else:
                return None
        return current

    @staticmethod
    def _set_value_at_path(data: Dict[str, Any], path: List[Any], value: Any) -> Dict[str, Any]:
        updated = copy.deepcopy(data)
        current: Any = updated
        for index, part in enumerate(path[:-1]):
            following = path[index + 1]
            if isinstance(part, str):
                if not isinstance(current, dict):
                    raise ValueError("String field path requires an object container")
                if part not in current or current[part] is None:
                    current[part] = [] if isinstance(following, int) else {}
                current = current[part]
            elif isinstance(part, int):
                if not isinstance(current, list) or part < 0:
                    raise ValueError("Numeric field path requires an array container")
                while len(current) <= part:
                    current.append([] if isinstance(following, int) else {})
                current = current[part]
            else:
                raise ValueError("Field path may contain only string keys or non-negative indexes")
        last = path[-1]
        if isinstance(last, str) and isinstance(current, dict):
            current[last] = value
        elif isinstance(last, int) and isinstance(current, list) and last >= 0:
            while len(current) <= last:
                current.append(None)
            current[last] = value
        else:
            raise ValueError("Field path does not match the reviewed data shape")
        return updated

    async def get_document_review(self, job_id: str, document_id: str) -> Optional[Dict[str, Any]]:
        """Return durable raw/reviewed extraction data and correction audit rows."""
        document = await self.get_job_document(job_id, document_id)
        if not document:
            return None
        changes = self.client.table("document_review_changes")\
            .select("*")\
            .eq("document_id", document_id)\
            .order("created_at")\
            .execute()
        document["changes"] = changes.data or []
        for extraction in document.get("extractions", []):
            extraction["reviewed_data"] = self._initial_review_data(extraction)
        return document

    async def apply_document_review_change(
        self,
        job_id: str,
        document_id: str,
        processing_unit_id: str,
        field_path: List[Any],
        value: Any,
        actor: Dict[str, Any],
        base_review_grid: Optional[List[List[Any]]] = None,
    ) -> Dict[str, Any]:
        """Persist a reviewed value and append its immutable change record."""
        document = await self.get_job_document(job_id, document_id)
        if not document:
            raise ValueError("Document not found")
        extraction = next(
            (
                item for item in document.get("extractions", [])
                if item.get("processing_unit_id") == processing_unit_id
            ),
            None,
        )
        if not extraction:
            raise ValueError("Extraction unit not found")
        reviewed_data = self._initial_review_data(extraction)
        if base_review_grid and field_path and field_path[0] == "review_grid" and "review_grid" not in reviewed_data:
            reviewed_data["review_grid"] = copy.deepcopy(base_review_grid)
        previous_value = self._value_at_path(reviewed_data, field_path)
        updated_reviewed_data = self._set_value_at_path(reviewed_data, field_path, value)
        if field_path[0] == "review_grid" and isinstance(updated_reviewed_data.get("review_grid"), list):
            reviewed_csv = io.StringIO()
            csv.writer(reviewed_csv).writerows(updated_reviewed_data["review_grid"])
            updated_reviewed_data["csv"] = reviewed_csv.getvalue()
        changed_at = datetime.utcnow().isoformat()
        actor_user_id = actor.get("user_id")
        actor_session_id = actor.get("session_id")
        if bool(actor_user_id) == bool(actor_session_id):
            raise ValueError("Review actor must be exactly one user or session")
        change = {
            "document_id": document_id,
            "extraction_id": extraction["id"],
            "job_id": job_id,
            "workspace_id": document.get("workspace_id"),
            "field_path": field_path,
            "previous_value": previous_value,
            "changed_value": value,
            "actor_user_id": actor_user_id,
            "actor_session_id": actor_session_id,
            "created_at": changed_at,
        }
        change_response = self.client.table("document_review_changes").insert(change).execute()
        update_response = self.client.table("document_extractions").update({
            "reviewed_data": updated_reviewed_data,
            "edited": True,
            "review_status": "edited",
            "reviewed_at": changed_at,
            "updated_at": changed_at,
        }).eq("id", extraction["id"]).execute()
        self.client.table("job_documents").update({
            "review_status": "edited",
            "reviewed_at": changed_at,
            "reviewed_by_user_id": actor_user_id,
            "reviewed_by_session_id": actor_session_id,
            "updated_at": changed_at,
        }).eq("id", document_id).eq("job_id", job_id).execute()
        try:
            await self.refresh_document_duplicate_warnings(job_id, document_id)
        except Exception as exc:
            logger.warning(f"Unable to refresh duplicate warnings after correction for {document_id}: {exc}")
        return {
            "change": (change_response.data or [change])[0],
            "extraction": (update_response.data or [{**extraction, "reviewed_data": updated_reviewed_data}])[0],
            "review_status": "edited",
        }

    async def set_document_review_status(
        self,
        job_id: str,
        document_id: str,
        review_status: str,
        actor: Dict[str, Any],
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Persist a human-review lifecycle decision for one owned document."""
        document = await self.get_job_document(job_id, document_id)
        if not document:
            raise ValueError("Document not found")
        if review_status == "ready" and any(
            extraction.get("status") == "failed"
            for extraction in document.get("extractions", [])
        ):
            raise ValueError("A failed extraction cannot be confirmed Ready")
        if review_status in {"ready", "published"}:
            document = await self.refresh_document_duplicate_warnings(job_id, document_id)
            if self.active_duplicate_warnings(document):
                raise ValueError("Resolve possible duplicate warnings before marking or publishing this document")
        changed_at = datetime.utcnow().isoformat()
        actor_user_id = actor.get("user_id")
        actor_session_id = actor.get("session_id")
        metadata = document.get("metadata") if isinstance(document.get("metadata"), dict) else {}
        history = list(metadata.get("review_status_history") or [])
        history.append({
            "from_status": document.get("review_status"),
            "to_status": review_status,
            "reason": reason or "",
            "actor": actor,
            "changed_at": changed_at,
        })
        self.client.table("job_documents").update({
            "review_status": review_status,
            "reviewed_at": changed_at,
            "reviewed_by_user_id": actor_user_id,
            "reviewed_by_session_id": actor_session_id,
            "metadata": {**metadata, "review_status_history": history},
            "updated_at": changed_at,
        }).eq("id", document_id).eq("job_id", job_id).execute()
        if review_status in {"ready", "edited", "failed", "published", "deleted"}:
            self.client.table("document_extractions").update({
                "review_status": review_status,
                "reviewed_at": changed_at,
                "updated_at": changed_at,
            }).eq("document_id", document_id).eq("job_id", job_id).execute()
        refreshed = await self.get_document_review(job_id, document_id)
        return refreshed or {}

    async def store_job_document_detection(
        self,
        document_id: str,
        processing_unit_id: str,
        detection: Dict[str, Any],
        resolved_mode: Optional[str],
    ) -> Dict[str, Any]:
        """Persist classifier output, combining page classifications for one logical document."""
        response = self.client.table("job_documents").select("*").eq("id", document_id).limit(1).execute()
        if not response.data:
            raise ValueError("Document not found")

        document = response.data[0]
        metadata = document.get("metadata") or {}
        if not isinstance(metadata, dict):
            metadata = {}
        detections = [
            item for item in (metadata.get("detections") or [])
            if item.get("processing_unit_id") != processing_unit_id
        ]
        detections.append({
            "processing_unit_id": processing_unit_id,
            "document_type": detection["document_type"],
            "confidence": detection["confidence"],
            "review_reason": detection.get("review_reason") or "",
        })
        routed_modes = {
            item["document_type"]
            for item in detections
            if item["document_type"] != "needs_manual_selection"
        }
        has_unresolved_page = any(item["document_type"] == "needs_manual_selection" for item in detections)
        if has_unresolved_page or len(routed_modes) > 1:
            detected_mode = "needs_manual_selection"
            combined_resolved_mode = None
            review_reason = detection.get("review_reason") or (
                "Pages in this document require a manual extraction mode."
                if has_unresolved_page
                else "Pages in this document were detected as different document types."
            )
        else:
            detected_mode = next(iter(routed_modes), detection["document_type"])
            combined_resolved_mode = resolved_mode or detected_mode
            review_reason = detection.get("review_reason") or ""

        update = {
            "detected_mode": detected_mode,
            "resolved_mode": combined_resolved_mode,
            "detection_confidence": min(float(item.get("confidence", 0)) for item in detections),
            "detection_review_reason": review_reason,
            "metadata": {**metadata, "detections": detections},
            "updated_at": datetime.utcnow().isoformat(),
        }
        updated = self.client.table("job_documents").update(update).eq("id", document_id).execute()
        if not updated.data:
            raise Exception("Supabase returned no updated job_documents row")
        return updated.data[0]

    async def get_job_document(self, job_id: str, document_id: str) -> Optional[Dict[str, Any]]:
        """Fetch one durable document with its extraction units and result files."""
        documents = await self.get_job_documents(job_id)
        return next((document for document in documents if document.get("id") == document_id), None)

    async def override_job_document_mode(
        self,
        job_id: str,
        document_id: str,
        document_mode: str,
        actor: Dict[str, Any],
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Audit a user's manual classification override before re-extraction."""
        document = await self.get_job_document(job_id, document_id)
        if not document:
            raise ValueError("Document not found")
        history = document.get("mode_override_history") or []
        history.append({
            "from_mode": document.get("resolved_mode") or document.get("detected_mode"),
            "to_mode": document_mode,
            "reason": reason or "Manual review selection",
            "actor": actor,
            "changed_at": datetime.utcnow().isoformat(),
        })
        metadata = document.get("metadata") or {}
        if not isinstance(metadata, dict):
            metadata = {}
        update = {
            "resolved_mode": document_mode,
            "status": "processing",
            "mode_override_history": history,
            "metadata": {**metadata, "mode_override_pending": True},
            "updated_at": datetime.utcnow().isoformat(),
        }
        response = self.client.table("job_documents").update(update).eq("id", document_id).eq("job_id", job_id).execute()
        if not response.data:
            raise Exception("Supabase returned no overridden document row")
        return response.data[0]

    async def get_job_documents(self, job_id: str) -> List[Dict[str, Any]]:
        """Retrieve durable source documents, extraction states, and linked result files."""
        response = self.client.table("job_documents")\
            .select("*")\
            .eq("job_id", job_id)\
            .order("created_at")\
            .execute()
        documents = response.data or []
        if not documents:
            return []

        document_ids = [document["id"] for document in documents]
        extraction_response = self.client.table("document_extractions")\
            .select("*")\
            .in_("document_id", document_ids)\
            .order("created_at")\
            .execute()
        file_response = self.client.table("job_files")\
            .select("*")\
            .in_("document_id", document_ids)\
            .order("created_at")\
            .execute()

        extractions_by_document: Dict[str, List[Dict[str, Any]]] = {}
        for extraction in extraction_response.data or []:
            extractions_by_document.setdefault(extraction["document_id"], []).append(extraction)

        files_by_document: Dict[str, List[Dict[str, Any]]] = {}
        for record in file_response.data or []:
            files_by_document.setdefault(record["document_id"], []).append(
                self._job_file_record_to_metadata(record)
            )

        return [
            {
                **document,
                "extractions": extractions_by_document.get(document["id"], []),
                "result_files": files_by_document.get(document["id"], []),
                "duplicate_warnings": self._document_metadata(document).get("duplicate_warnings") or [],
            }
            for document in documents
        ]

    async def upload_job_file(
        self,
        file_data: bytes,
        user_id: str,
        job_id: str,
        filename: str,
        content_type: str = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
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
                    "content-type": content_type,
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
                filename=filename,
                content_type=content_type,
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
                    "content-type": content_type,
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
