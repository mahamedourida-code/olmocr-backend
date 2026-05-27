"""Workspace inbound email ingestion through verified provider webhooks."""

import hashlib
import io
import os
import uuid
from datetime import datetime, timedelta
from email.utils import parseaddr
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request, status
from PIL import Image, UnidentifiedImageError
from svix.webhooks import Webhook, WebhookVerificationError

from app.api.v1.jobs import (
    enforce_batch_limit,
    estimate_parallel_completion,
    persist_queued_documents,
    refresh_uploaded_duplicate_warnings,
    simple_batch_validation,
)
from app.core.config import settings
from app.core.dependencies import enforce_upload_rate_limits, get_current_user
from app.core.limits import get_plan_limits, get_user_plan_type
from app.services.redis_service import RedisService, get_redis_service
from app.services.supabase_service import get_supabase_service
from app.tasks.batch_tasks import process_batch_from_storage
from app.utils.exceptions import ProcessingError
from app.utils.pdf_pages import is_pdf_bytes, render_pdf_pages_to_png

router = APIRouter(prefix="/email-intake", tags=["Email Intake"])

SUPPORTED_ATTACHMENT_TYPES = {
    "application/pdf",
    "image/heic",
    "image/heif",
    "image/jpeg",
    "image/jpg",
    "image/png",
    "image/webp",
}
SUPPORTED_EXTENSION_TYPES = {
    "pdf": "application/pdf",
    "heic": "image/heic",
    "heif": "image/heif",
    "jpeg": "image/jpeg",
    "jpg": "image/jpeg",
    "png": "image/png",
    "webp": "image/webp",
}


def _normalize_address(value: str) -> str:
    return parseaddr(str(value or ""))[1].strip().lower()


def _safe_filename(value: str, fallback: str) -> str:
    filename = os.path.basename(str(value or "").strip()) or fallback
    return filename[:240]


def _rejection(filename: str, reason: str) -> Dict[str, str]:
    return {"filename": filename, "reason": reason}


def _validate_image_bytes(content: bytes) -> bool:
    try:
        try:
            import pillow_heif

            pillow_heif.register_heif_opener()
        except ImportError:
            pass
        with Image.open(io.BytesIO(content)) as image:
            image.verify()
        return True
    except (UnidentifiedImageError, OSError, ValueError):
        return False


async def _download_attachments(email_id: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, str]], int]:
    if not settings.resend_api_key:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Email intake is not configured")

    headers = {"Authorization": f"Bearer {settings.resend_api_key}"}
    accepted: List[Dict[str, Any]] = []
    rejected: List[Dict[str, str]] = []
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(
            f"https://api.resend.com/emails/receiving/{email_id}/attachments",
            headers=headers,
        )
        response.raise_for_status()
        attachments = response.json().get("data") or []
        for index, attachment in enumerate(attachments):
            filename = _safe_filename(attachment.get("filename"), f"attachment_{index + 1}")
            content_type = str(attachment.get("content_type") or "").lower()
            extension = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
            normalized_type = content_type if content_type in SUPPORTED_ATTACHMENT_TYPES else SUPPORTED_EXTENSION_TYPES.get(extension)

            if str(attachment.get("content_disposition") or "").lower() == "inline":
                rejected.append(_rejection(filename, "Inline email images are not imported"))
                continue
            if not normalized_type:
                rejected.append(_rejection(filename, "Unsupported attachment type"))
                continue

            download_url = attachment.get("download_url")
            if not download_url:
                rejected.append(_rejection(filename, "Attachment content is unavailable"))
                continue

            download = await client.get(str(download_url))
            download.raise_for_status()
            content = download.content
            if len(content) < 100:
                rejected.append(_rejection(filename, "Attachment is empty or invalid"))
                continue
            if len(content) > settings.max_file_size_bytes:
                rejected.append(_rejection(filename, "Attachment exceeds the file-size limit"))
                continue
            if normalized_type == "application/pdf":
                if not is_pdf_bytes(content):
                    rejected.append(_rejection(filename, "Attachment is not a valid PDF"))
                    continue
            elif not _validate_image_bytes(content):
                rejected.append(_rejection(filename, "Attachment is not a valid image"))
                continue

            accepted.append({
                "index": index,
                "document_id": str(uuid.uuid4()),
                "filename": filename,
                "content_type": normalized_type,
                "content": content,
                "source_sha256": hashlib.sha256(content).hexdigest(),
            })
    return accepted, rejected, len(attachments)


async def _queue_message_documents(
    intake: Dict[str, Any],
    message: Dict[str, Any],
    accepted_files: List[Dict[str, Any]],
    rejected_attachments: List[Dict[str, str]],
    attachment_count: int,
    redis_service: RedisService,
) -> Dict[str, Any]:
    supabase_service = get_supabase_service()
    owner_user_id = str(intake["owner_user_id"])
    workspace_id = str(intake["workspace_id"])
    user = {"user_id": owner_user_id}
    limits = get_plan_limits(get_user_plan_type(user, supabase_service))
    session_id = f"email:{message['id']}"
    processing_units: List[Dict[str, Any]] = []
    importable_files: List[Dict[str, Any]] = []

    for file_info in accepted_files:
        content = file_info["content"]
        if file_info["content_type"] == "application/pdf":
            try:
                page_images = render_pdf_pages_to_png(content)
            except ProcessingError:
                rejected_attachments.append(_rejection(file_info["filename"], "PDF pages could not be rendered"))
                continue
            base_name = os.path.splitext(file_info["filename"])[0] or "document"
            for page_index, page_bytes in enumerate(page_images, start=1):
                processing_units.append({
                    "id": f"email_{file_info['index']}_page_{page_index}",
                    "document_id": file_info["document_id"],
                    "content": page_bytes,
                    "filename": f"{file_info['index']}_{base_name}_page_{page_index}.png",
                    "content_type": "image/png",
                    "output_format": "xlsx",
                    "document_mode": "auto",
                    "source_filename": file_info["filename"],
                    "source_content_type": "application/pdf",
                    "source_page": page_index,
                    "source_page_count": len(page_images),
                    "source_sha256": hashlib.sha256(page_bytes).hexdigest(),
                })
            importable_files.append(file_info)
            continue

        processing_units.append({
            "id": f"email_{file_info['index']}",
            "document_id": file_info["document_id"],
            "content": content,
            "filename": f"{file_info['index']}_{file_info['filename']}",
            "content_type": file_info["content_type"],
            "output_format": "xlsx",
            "document_mode": "auto",
            "source_filename": file_info["filename"],
            "source_content_type": file_info["content_type"],
            "source_page": None,
            "source_page_count": None,
            "source_sha256": file_info["source_sha256"],
        })
        importable_files.append(file_info)

    if not processing_units:
        return await supabase_service.update_email_intake_message(message["id"], {
            "status": "rejected",
            "attachment_count": attachment_count,
            "accepted_attachment_count": 0,
            "rejected_attachments": rejected_attachments,
        })

    processing_count = len(processing_units)
    simple_batch_validation(processing_count)
    enforce_batch_limit(processing_count, limits)
    await enforce_upload_rate_limits(
        request=None,
        redis_service=redis_service,
        user=user,
        session_id=session_id,
        image_count=processing_count,
        daily_image_limit_override=limits["daily_image_limit"],
        daily_run_limit_override=limits.get("daily_run_limit"),
        enforce_ip_limit=False,
    )

    if not supabase_service.check_and_use_credits(owner_user_id, processing_count):
        raise HTTPException(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            detail={"code": "INSUFFICIENT_CREDITS", "message": "Not enough credits to import email attachments"},
        )

    job_id = str(uuid.uuid4())
    credits_reserved = processing_count
    try:
        stored_images: List[Dict[str, Any]] = []
        for unit in processing_units:
            source_file = await supabase_service.upload_source_file(
                file_data=unit["content"],
                owner_id=owner_user_id,
                job_id=job_id,
                filename=unit["filename"],
                content_type=unit["content_type"],
            )
            stored_images.append({
                "id": unit["id"],
                "document_id": unit["document_id"],
                "storage_path": source_file["storage_path"],
                "filename": unit["filename"],
                "content_type": source_file["content_type"],
                "output_format": "xlsx",
                "document_mode": "auto",
                "size_bytes": source_file["size_bytes"],
                "original_filename": unit["source_filename"],
                "source_content_type": unit["source_content_type"],
                "source_page": unit["source_page"],
                "source_page_count": unit["source_page_count"],
                "source_sha256": unit["source_sha256"],
            })

        document_records: List[Dict[str, Any]] = []
        for source_file_info in importable_files:
            first_unit = next(item for item in stored_images if item["document_id"] == source_file_info["document_id"])
            source_storage_path = first_unit["storage_path"]
            if source_file_info["content_type"] == "application/pdf":
                durable_source = await supabase_service.upload_source_file(
                    file_data=source_file_info["content"],
                    owner_id=owner_user_id,
                    job_id=job_id,
                    filename=f"source_{source_file_info['index']}_{source_file_info['filename']}",
                    content_type="application/pdf",
                )
                source_storage_path = durable_source["storage_path"]
            document_records.append({
                "id": source_file_info["document_id"],
                "job_id": job_id,
                "owner_user_id": owner_user_id,
                "owner_session_id": None,
                "workspace_id": workspace_id,
                "original_filename": source_file_info["filename"],
                "source_storage_path": source_storage_path,
                "source_content_type": source_file_info["content_type"],
                "selected_mode": "auto",
                "resolved_mode": None,
                "status": "queued",
                "metadata": {
                    "origin": "email",
                    "email_intake_message_id": message["id"],
                    "source_email_reference": message["source_email_reference"],
                    "sender": message["sender"],
                    "received_at": message["received_at"],
                    "attachment_filename": source_file_info["filename"],
                    "source_page_count": first_unit.get("source_page_count"),
                    "source_sha256": source_file_info["source_sha256"],
                },
                "expires_at": (datetime.utcnow() + timedelta(hours=settings.file_retention_hours)).isoformat(),
            })

        metadata = {
            "total_images": processing_count,
            "uploaded_files": len(importable_files),
            "consolidation_strategy": "separate",
            "output_format": "xlsx",
            "document_mode": "auto",
            "session_id": session_id,
            "owner_user_id": owner_user_id,
            "owner_session_id": None,
            "workspace_id": workspace_id,
            "origin": "email",
            "email_intake_message_id": message["id"],
            "source_email_reference": message["source_email_reference"],
            "plan": limits["plan"],
            "max_files_per_batch": limits["max_files_per_batch"],
            "credits_reserved": credits_reserved,
            "credits_settled": False,
        }
        await supabase_service.create_job(
            job_id=job_id,
            user_id=owner_user_id,
            filename=f"email_batch_{processing_count}_pages",
            metadata=metadata,
            status="queued",
        )
        await persist_queued_documents(
            supabase_service,
            document_records,
            [{**stored_image, "job_id": job_id} for stored_image in stored_images],
        )
        await refresh_uploaded_duplicate_warnings(supabase_service, document_records)

        job_data = {
            "job_id": job_id,
            "session_id": session_id,
            "user_id": owner_user_id,
            "status": "queued",
            "total_images": processing_count,
            "processed_images": 0,
            "progress": 0,
            "output_format": "xlsx",
            "document_mode": "auto",
            "consolidation_strategy": "separate",
            "images": stored_images,
            "results": [],
            "errors": [],
            "plan": limits["plan"],
            "max_files_per_batch": limits["max_files_per_batch"],
            "credits_reserved": credits_reserved,
            "credits_settled": False,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
        }
        if not await redis_service.create_job(job_id, job_data):
            raise RuntimeError("Failed to store queued intake job")
        async_result = process_batch_from_storage.apply_async(
            args=[job_id, session_id, stored_images, owner_user_id],
            queue="batch_processing",
            priority=6,
        )
        await redis_service.update_job(job_id, {"celery_task_id": async_result.id})
        await supabase_service.update_job_status(
            job_id=job_id,
            status="queued",
            metadata={**metadata, "celery_task_id": async_result.id},
        )
        queued_message = await supabase_service.update_email_intake_message(message["id"], {
            "status": "queued",
            "job_id": job_id,
            "attachment_count": attachment_count,
            "accepted_attachment_count": len(importable_files),
            "rejected_attachments": rejected_attachments,
        })
        queued_message["estimated_completion"] = estimate_parallel_completion(processing_count).isoformat()
        return queued_message
    except Exception:
        supabase_service.refund_credits(owner_user_id, credits_reserved)
        try:
            await redis_service.delete_job(job_id)
        except Exception:
            pass
        raise


@router.get("/address")
async def get_email_intake_address(
    workspace_id: Optional[str] = None,
    user: dict = Depends(get_current_user),
):
    """Return or provision the inbound email address for an owned workspace."""
    try:
        intake = await get_supabase_service().get_or_create_email_intake(
            user["user_id"],
            workspace_id,
            settings.inbound_email_domain,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
    return {
        "id": intake["id"],
        "workspace_id": intake["workspace_id"],
        "address": intake["address"],
        "enabled": intake["enabled"],
        "provider": intake["provider"],
    }


@router.get("/messages")
async def list_email_intake_messages(
    workspace_id: Optional[str] = None,
    limit: int = 50,
    user: dict = Depends(get_current_user),
):
    """Return inbound imports for an owned workspace without exposing provider secrets."""
    try:
        messages = await get_supabase_service().list_owned_email_intake_messages(
            user["user_id"],
            workspace_id,
            limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))
    return {"messages": messages, "total": len(messages)}


@router.post("/resend/webhook")
async def receive_resend_email(
    request: Request,
    redis_service: RedisService = Depends(get_redis_service),
):
    """Verify a Resend receiving event and submit safe attachments through Auto detect."""
    if not settings.resend_webhook_secret:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Email intake is not configured")
    raw_payload = await request.body()
    try:
        event = Webhook(settings.resend_webhook_secret).verify(
            raw_payload,
            {
                "svix-id": request.headers.get("svix-id", ""),
                "svix-timestamp": request.headers.get("svix-timestamp", ""),
                "svix-signature": request.headers.get("svix-signature", ""),
            },
        )
    except WebhookVerificationError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid webhook signature")

    if event.get("type") != "email.received":
        return {"accepted": True, "ignored": True}
    data = event.get("data") or {}
    email_id = str(data.get("email_id") or "").strip()
    if not email_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing provider email ID")

    supabase_service = get_supabase_service()
    existing = await supabase_service.get_email_intake_message(email_id)
    if existing and existing.get("status") in {"queued", "rejected"}:
        return {"accepted": True, "message_id": existing["id"], "status": existing["status"]}

    intake = await supabase_service.find_enabled_email_intake([
        _normalize_address(recipient)
        for recipient in (data.get("to") or [])
    ])
    if not intake:
        return {"accepted": True, "ignored": True, "reason": "Unknown destination"}

    source_reference = str(data.get("message_id") or email_id)
    sender = str(data.get("from") or "").strip()[:320]
    received_at = str(data.get("created_at") or event.get("created_at") or datetime.utcnow().isoformat())
    message = existing or await supabase_service.create_email_intake_message({
        "intake_id": intake["id"],
        "workspace_id": intake["workspace_id"],
        "owner_user_id": intake["owner_user_id"],
        "provider": "resend",
        "provider_email_id": email_id,
        "source_email_reference": source_reference,
        "sender": sender,
        "received_at": received_at,
        "status": "received",
        "attachment_count": 0,
        "accepted_attachment_count": 0,
        "rejected_attachments": [],
        "expires_at": (datetime.utcnow() + timedelta(hours=settings.file_retention_hours)).isoformat(),
    })

    try:
        accepted_files, rejected, attachment_count = await _download_attachments(email_id)
        result = await _queue_message_documents(
            intake,
            message,
            accepted_files,
            rejected,
            attachment_count,
            redis_service,
        )
        return {
            "accepted": True,
            "message_id": result["id"],
            "status": result["status"],
            "job_id": result.get("job_id"),
        }
    except HTTPException as exc:
        if exc.status_code in {status.HTTP_400_BAD_REQUEST, status.HTTP_402_PAYMENT_REQUIRED, status.HTTP_429_TOO_MANY_REQUESTS}:
            await supabase_service.update_email_intake_message(message["id"], {
                "status": "rejected",
                "rejected_attachments": [_rejection("email", "Import could not be accepted under current limits")],
            })
            return {"accepted": True, "message_id": message["id"], "status": "rejected"}
        await supabase_service.update_email_intake_message(message["id"], {"status": "failed"})
        raise
    except Exception:
        await supabase_service.update_email_intake_message(message["id"], {"status": "failed"})
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Email attachment import failed")
