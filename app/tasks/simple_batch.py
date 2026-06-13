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


def _bucket_confidence(fill_ratio: float) -> float:
    """Map a 0..1 row-fill ratio to a confidence score in the 0.6..0.95 range.

    Sparse rows (many empty cells) score lower, dense rows score higher. This is
    a derived signal — not a fabricated number — so the UI can surface a real
    per-row indicator while the model layer evolves toward true logprob-based
    confidence.
    """
    if fill_ratio <= 0:
        return 0.62
    if fill_ratio < 0.35:
        return 0.68
    if fill_ratio < 0.65:
        return 0.80
    if fill_ratio < 0.85:
        return 0.88
    return 0.94


def _row_fill_ratio(values: List[Any]) -> float:
    if not values:
        return 0.0
    filled = 0
    for value in values:
        if value is None:
            continue
        if isinstance(value, str):
            if value.strip():
                filled += 1
        else:
            filled += 1
    return filled / len(values)


def _compute_row_confidence(structured_data: Any, document_mode: str) -> List[float]:
    """Compute per-row confidence for handwritten tabular output.

    Returns an empty list when no per-row signal is available — callers should
    leave the response field as ``None`` in that case so the frontend can
    gracefully fall back to the document-level score.
    """
    if not isinstance(structured_data, dict):
        return []

    rows: List[List[Any]] = []

    if document_mode == 'notes':
        entries = structured_data.get('entries') or structured_data.get('rows') or []
        for entry in entries:
            if isinstance(entry, dict):
                rows.append(list(entry.values()))
            elif isinstance(entry, list):
                rows.append(entry)
            elif isinstance(entry, str) and entry.strip():
                rows.append([entry])
    elif document_mode in ('invoice', 'receipt'):
        line_items = structured_data.get('line_items') or []
        for line in line_items:
            if isinstance(line, dict):
                rows.append(list(line.values()))
    elif document_mode == 'bank_statement':
        transactions = structured_data.get('transactions') or []
        for txn in transactions:
            if isinstance(txn, dict):
                rows.append(list(txn.values()))
    else:
        csv_blob = structured_data.get('csv')
        if isinstance(csv_blob, str) and csv_blob.strip():
            for line in csv_blob.splitlines():
                if not line.strip():
                    continue
                rows.append([cell.strip() for cell in line.split(',')])

    if not rows:
        return []

    return [round(_bucket_confidence(_row_fill_ratio(row)), 3) for row in rows]


def _parse_job_metadata(job_record: Dict[str, Any]) -> Dict[str, Any]:
    metadata = job_record.get('processing_metadata') if job_record else {}
    if isinstance(metadata, dict):
        return metadata
    if isinstance(metadata, str) and metadata:
        try:
            parsed = json.loads(metadata)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


async def _settle_reserved_credits(
    supabase,
    job_id: str,
    user_id: str,
    total_images: int,
    successful_images: int
) -> Dict[str, Any]:
    if not user_id:
        return {
            'credits_reserved': 0,
            'credits_charged': 0,
            'credits_refunded': 0,
            'credits_settled': True
        }

    job_record = await supabase.get_job(job_id)
    metadata = _parse_job_metadata(job_record)
    if metadata.get('credits_settled'):
        return {
            'credits_reserved': metadata.get('credits_reserved', total_images),
            'credits_charged': metadata.get('credits_charged', successful_images),
            'credits_refunded': metadata.get('credits_refunded', 0),
            'credits_settled': True
        }

    reserved_credits = int(metadata.get('credits_reserved') or total_images)
    charged_credits = min(successful_images, reserved_credits)
    refund_credits = max(0, reserved_credits - charged_credits)

    if refund_credits:
        supabase.refund_credits(user_id, refund_credits)

    return {
        'credits_reserved': reserved_credits,
        'credits_charged': charged_credits,
        'credits_refunded': refund_credits,
        'credits_settled': True
    }


async def _restore_image_results_from_supabase(
    redis,
    job_id: str,
    user_id: str = None,
    session_id: str = None
) -> None:
    try:
        supabase = get_supabase_service()
        durable_files = await supabase.get_job_files_for_job(job_id)

        for file_info in durable_files:
            if user_id and file_info.get('user_id') and file_info.get('user_id') != user_id:
                continue
            if not user_id and session_id and file_info.get('session_id') != session_id:
                continue

            image_id = file_info.get('image_id')
            file_id = file_info.get('file_id')
            storage_path = file_info.get('storage_path')
            if not image_id or not file_id or not storage_path:
                continue

            result_data = {
                'status': 'success',
                'image_id': image_id,
                'file_info': file_info,
                'download_url': f"/api/v1/download/{file_id}",
                'skipped': True
            }

            await redis.set_job_image_result(job_id, image_id, result_data, settings.job_expiry_seconds)
            await redis.set_cache(
                f"file:{file_id}",
                {
                    "storage_path": storage_path,
                    "filename": file_info.get('filename', f"{file_id}.xlsx"),
                    "content_type": file_info.get("content_type") or "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
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
    document_id = img.get('document_id')
    source_storage_path = img.get('source_storage_path')
    source_content_type = img.get('source_content_type')
    source_page = img.get('source_page')
    source_page_count = img.get('source_page_count')
    source_sha256 = img.get('source_sha256')

    try:
        existing_result = await redis.get_job_image_result(job_id, image_id)
        if existing_result and existing_result.get('status') == 'success' and not img.get("force_reprocess"):
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
        output_format = str(img.get('output_format') or 'xlsx').lower()
        requested_document_mode = str(img.get('document_mode') or 'table').lower()
        ocr_language = str(img.get('ocr_language') or 'en').strip().lower()[:16] or 'en'
        document_mode = requested_document_mode
        classification_data = None

        # Extract with OlmOCR - pass base64 string directly.
        # No need to decode→encode, olmocr service handles both formats
        # Multi-model OCR: each page is routed to a primary model (round-robin by
        # index) and falls back to the other configured models on failure. Each
        # model has its OWN distributed semaphore, so the model pools provide
        # independent capacity (~N x the OCR slots) and one slow/overloaded model
        # can't starve the whole batch into the task soft-time-limit.
        ocr_models = settings.parsed_ocr_models or [None]
        primary_model = ocr_models[img_index % len(ocr_models)]
        model_order = [primary_model] + [m for m in ocr_models if m != primary_model]
        max_model_attempts = min(len(model_order), 1 + max(0, settings.ocr_failover_attempts))

        async def _ocr_call(make_coro, op_name):
            """One OCR call under a SINGLE global concurrency cap, then routed across
            models with cross-model failover inside the held slot.

            The cap is global (not per-model) on purpose: each concurrent OCR call
            holds a decoded image + model response in memory, and the worker is only
            512MB. Per-model semaphores multiplied total concurrency by the model
            count and OOM-killed the Celery worker, so the slot count must stay = the
            global cap regardless of how many models we round-robin across.
            """
            holder_id = f"{job_id}:{image_id}:{uuid.uuid4().hex}"
            acquired = await redis.acquire_distributed_semaphore(
                name="deepinfra_ocr",
                holder_id=holder_id,
                limit=settings.max_concurrent_ocr_calls,
                lease_seconds=300,
                wait_timeout_seconds=300,
            )
            if not acquired:
                raise RuntimeError("OCR capacity is busy. Please retry shortly.")
            try:
                last_exc = None
                for attempt_model in model_order[:max_model_attempts]:
                    try:
                        return await make_coro(attempt_model)
                    except Exception as exc:
                        last_exc = exc
                        logger.warning(
                            f"[Job {job_id}] {op_name} failed on model {attempt_model} "
                            f"for image {image_id}: {exc}"
                        )
                raise last_exc if last_exc else RuntimeError(f"{op_name} produced no result")
            finally:
                await redis.release_distributed_semaphore("deepinfra_ocr", holder_id)

        try:
            if requested_document_mode == "auto":
                classification_data = await _ocr_call(
                    lambda m: olmocr.classify_document_from_image(image_data, ocr_language=ocr_language, model=m),
                    "classification",
                )
                suggested_mode = classification_data.get("document_type")
                confidence = float(classification_data.get("confidence") or 0)
                if (
                    (source_page_count or 0) > 1
                    or suggested_mode == "needs_manual_selection"
                    or confidence < settings.auto_detection_confidence_threshold
                ):
                    if suggested_mode != "needs_manual_selection":
                        classification_data["suggested_type"] = suggested_mode
                    classification_data["document_type"] = "needs_manual_selection"
                    classification_data["review_reason"] = (
                        "Select one extraction mode for this multi-page document before processing."
                        if (source_page_count or 0) > 1
                        else classification_data.get("review_reason")
                        or "Document type confidence is too low for automatic extraction."
                    )
                    document_mode = "needs_manual_selection"
                else:
                    document_mode = suggested_mode

                if document_id:
                    supabase = get_supabase_service()
                    await supabase.store_job_document_detection(
                        document_id=document_id,
                        processing_unit_id=image_id,
                        detection=classification_data,
                        resolved_mode=None if document_mode == "needs_manual_selection" else document_mode,
                    )

                if document_mode == "needs_manual_selection":
                    review_flags = [{
                        "code": "classification_needs_review",
                        "area": "document_mode",
                        "note": classification_data["review_reason"],
                    }]
                    if document_id:
                        await supabase.upsert_document_extraction({
                            "document_id": document_id,
                            "job_id": job_id,
                            "processing_unit_id": image_id,
                            "status": "needs_review",
                            "structured_data": {"classification": classification_data},
                            "raw_structured_data": {"classification": classification_data},
                            "reviewed_data": {"classification": classification_data},
                            "review_status": "needs_review",
                            "validation_flags": review_flags,
                            "edited": False,
                            "metadata": {
                                "source_storage_path": source_storage_path,
                                "source_content_type": source_content_type,
                                "source_filename": img.get('original_filename') or img.get('filename'),
                                "source_page": source_page,
                                "source_page_count": source_page_count,
                                "source_sha256": source_sha256,
                                "ocr_language": ocr_language,
                                "classification": classification_data,
                            },
                        })
                    return {
                        "status": "needs_review",
                        "image_id": image_id,
                        "filename": img.get("filename"),
                        "document_id": document_id,
                        "document_mode": document_mode,
                        "requires_review": True,
                        "review_flags": review_flags,
                        "classification": classification_data,
                    }

            if document_mode == 'table' and output_format in {'txt', 'text', 'plain_text'}:
                output_format = 'xlsx'
            wants_text_output = output_format in {'txt', 'text', 'plain_text'}
            wants_bank_statement = document_mode == 'bank_statement'
            wants_invoice = document_mode == 'invoice'
            wants_receipt = document_mode == 'receipt'
            wants_notes = document_mode == 'notes'

            if wants_bank_statement:
                bank_statement_data = await _ocr_call(
                    lambda m: olmocr.extract_bank_statement_from_image(image_data, ocr_language=ocr_language, model=m),
                    "bank_statement",
                )
            elif wants_invoice:
                invoice_data = await _ocr_call(
                    lambda m: olmocr.extract_invoice_from_image(image_data, ocr_language=ocr_language, model=m),
                    "invoice",
                )
            elif wants_receipt:
                receipt_data = await _ocr_call(
                    lambda m: olmocr.extract_receipt_from_image(image_data, ocr_language=ocr_language, model=m),
                    "receipt",
                )
            elif wants_notes:
                notes_data = await _ocr_call(
                    lambda m: olmocr.extract_notes_from_image(image_data, ocr_language=ocr_language, model=m),
                    "notes",
                )
            elif wants_text_output:
                text_data = await _ocr_call(
                    lambda m: olmocr.extract_text_from_image(image_data, ocr_language=ocr_language, model=m),
                    "text",
                )
            else:
                csv_data = await _ocr_call(
                    lambda m: olmocr.extract_table_from_image(image_data, ocr_language=ocr_language, model=m),
                    "table",
                )
        finally:
            # Per-model semaphores are acquired/released inside _ocr_call above.
            pass

        # Generate filename
        original_filename = img.get('original_filename') or img.get('filename', f"image_{image_id}")
        base_name = original_filename.split('.')[0] if '.' in original_filename else original_filename
        if wants_bank_statement:
            if output_format == "csv":
                output_data = excel.bank_statement_transactions_to_csv(bank_statement_data)
                output_filename = f"{base_name}_{_safe_filename_part(image_id)}_transactions.csv"
                output_content_type = "text/csv; charset=utf-8"
            else:
                output_data = excel.bank_statement_to_xlsx(bank_statement_data, "Statement")
                output_filename = f"{base_name}_{_safe_filename_part(image_id)}_bank_statement.xlsx"
                output_content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        elif wants_invoice:
            if output_format == "csv":
                output_data = excel.invoice_line_items_to_csv(invoice_data)
                output_filename = f"{base_name}_{_safe_filename_part(image_id)}_invoice_line_items.csv"
                output_content_type = "text/csv; charset=utf-8"
            else:
                output_data = excel.invoice_to_xlsx(invoice_data)
                output_filename = f"{base_name}_{_safe_filename_part(image_id)}_invoice.xlsx"
                output_content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        elif wants_receipt:
            if output_format == "csv":
                output_data = excel.receipt_line_items_to_csv(receipt_data)
                output_filename = f"{base_name}_{_safe_filename_part(image_id)}_receipt.csv"
                output_content_type = "text/csv; charset=utf-8"
            else:
                output_data = excel.receipt_to_xlsx(receipt_data)
                output_filename = f"{base_name}_{_safe_filename_part(image_id)}_receipt.xlsx"
                output_content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        elif wants_notes:
            has_detected_tables = bool(notes_data.get("tables")) if isinstance(notes_data, dict) else False
            if output_format in {"xlsx", "csv"} and not has_detected_tables:
                notes_data.setdefault("review_flags", []).append({
                    "code": "no_detected_table",
                    "area": "tables",
                    "note": "No table was detected in this notes page; readable text was exported instead.",
                })
                output_data = excel.notes_text_to_txt(notes_data)
                output_filename = f"{base_name}_{_safe_filename_part(image_id)}_notes.txt"
                output_content_type = "text/plain; charset=utf-8"
            elif output_format == "csv":
                output_data = excel.notes_tables_to_csv(notes_data)
                output_filename = f"{base_name}_{_safe_filename_part(image_id)}_notes_tables.csv"
                output_content_type = "text/csv; charset=utf-8"
            elif output_format == "xlsx":
                output_data = excel.notes_tables_to_xlsx(notes_data)
                output_filename = f"{base_name}_{_safe_filename_part(image_id)}_notes_tables.xlsx"
                output_content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            else:
                output_data = excel.notes_text_to_txt(notes_data)
                output_filename = f"{base_name}_{_safe_filename_part(image_id)}_notes.txt"
                output_content_type = "text/plain; charset=utf-8"
        elif wants_text_output:
            output_data = text_data.encode('utf-8')
            output_filename = f"{base_name}_{_safe_filename_part(image_id)}_text.txt"
            output_content_type = "text/plain; charset=utf-8"
        else:
            if document_mode == "table" and output_format == "csv":
                output_data = excel.table_to_csv(csv_data)
                output_filename = f"{base_name}_{_safe_filename_part(image_id)}_table.csv"
                output_content_type = "text/csv; charset=utf-8"
            else:
                output_data = excel.csv_to_xlsx(csv_data, f"Table_{image_id}")
                suffix = document_mode if document_mode in {'invoice', 'receipt', 'invoice_receipt'} else "processed"
                output_filename = f"{base_name}_{_safe_filename_part(image_id)}_{suffix}.xlsx"
                output_content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

        # Save file with a deterministic file_id so retries overwrite, not duplicate.
        file_id = storage.save_result_file_sync(
            session_id,
            output_filename,
            output_data,
            file_id=file_id,
            update_session_metadata=False
        )

        # Upload result to durable storage so downloads work from separate workers.
        supabase_url = None
        supabase_storage_path = None
        try:
            supabase = get_supabase_service()
            storage_owner_id = user_id or session_id
            upload_result = await supabase.upload_file_to_storage(
                file_data=output_data,
                file_path=f"users/{storage_owner_id}/jobs/{job_id}/{output_filename}",
                user_id=storage_owner_id,
                job_id=job_id,
                filename=output_filename,
                content_type=output_content_type
            )
            supabase_url = upload_result.get('signed_url')
            supabase_storage_path = upload_result.get('storage_path')
            if supabase_storage_path:
                file_metadata = {
                    "storage_path": supabase_storage_path,
                    "filename": output_filename,
                    "content_type": output_content_type,
                    "job_id": job_id,
                    "document_id": document_id,
                    "session_id": session_id,
                    "user_id": user_id or "",
                    "image_id": image_id,
                    "file_id": file_id,
                    "size_bytes": len(output_data),
                    "document_mode": document_mode,
                    "selected_mode": requested_document_mode,
                    "ocr_language": ocr_language,
                    "detected_mode": classification_data.get("document_type") if classification_data else None,
                    "detection_confidence": classification_data.get("confidence") if classification_data else None,
                    "detection_review_reason": classification_data.get("review_reason") if classification_data else None,
                    "completed_at": datetime.utcnow().isoformat()
                }
                await redis.set_cache(f"file:{file_id}", file_metadata, settings.file_retention_seconds)
            logger.info(f"[Job {job_id}] Uploaded result to Supabase: {supabase_storage_path}")
        except Exception as upload_error:
            logger.warning(f"[Job {job_id}] Supabase result upload failed: {upload_error}")

        review_flags = (
            bank_statement_data.get("review_flags", [])
            if wants_bank_statement and isinstance(bank_statement_data, dict)
            else invoice_data.get("review_flags", [])
            if wants_invoice and isinstance(invoice_data, dict)
            else receipt_data.get("review_flags", [])
            if wants_receipt and isinstance(receipt_data, dict)
            else notes_data.get("review_flags", [])
            if wants_notes and isinstance(notes_data, dict)
            else []
        )
        requires_review = bool(review_flags)
        structured_data = (
            bank_statement_data
            if wants_bank_statement
            else invoice_data
            if wants_invoice
            else receipt_data
            if wants_receipt
            else notes_data
            if wants_notes
            else {"text": text_data}
            if wants_text_output
            else {"csv": csv_data}
        )
        if classification_data:
            structured_data = {
                **structured_data,
                "_classification": classification_data,
            }

        # P1 — Handwritten specialist signals.
        # is_handwritten: derived from the resolved document_mode. Notes mode is
        # always handwritten in the current product flow; future backends can
        # override this by emitting an explicit signal on classification_data.
        classification_handwritten = bool(classification_data.get("is_handwritten")) if classification_data else False
        is_handwritten = bool(wants_notes or classification_handwritten)

        # row_confidence: per-row sparseness heuristic for tabular handwritten
        # output. Real derived signal — fill ratio across the row maps to a
        # confidence bucket. Set to None when we don't have row-structured data
        # so the frontend gracefully falls back to the document-level score.
        row_confidence: Any = None
        if is_handwritten:
            computed_rows = _compute_row_confidence(structured_data, document_mode)
            row_confidence = computed_rows if computed_rows else None

        file_record = {
            'file_id': file_id,
            'job_id': job_id,
            'document_id': document_id,
            'filename': output_filename,
            'original_filename': original_filename,
            'image_id': image_id,
            'size_bytes': len(output_data),
            'supabase_url': supabase_url,
            'storage_path': supabase_storage_path,
            'session_id': session_id,
            'user_id': user_id or "",
            'content_type': output_content_type,
            'document_mode': document_mode,
            'selected_mode': requested_document_mode,
            'ocr_language': ocr_language,
            'detected_mode': classification_data.get("document_type") if classification_data else None,
            'detection_confidence': classification_data.get("confidence") if classification_data else None,
            'detection_review_reason': classification_data.get("review_reason") if classification_data else None,
            'status': "completed",
            'requires_review': requires_review,
            'review_flags': review_flags,
            'confidence_score': 72 if requires_review else 92,
            'is_handwritten': is_handwritten,
            'row_confidence': row_confidence,
            'source_page': source_page,
            'source_page_count': source_page_count,
            'expires_at': (datetime.utcnow() + timedelta(hours=settings.file_retention_hours)).isoformat(),
            'completed_at': datetime.utcnow().isoformat()
        }

        if supabase_storage_path:
            try:
                durable_file = await supabase.upsert_job_file(file_record)
                file_record.update(durable_file)
                if document_id:
                    await supabase.upsert_document_extraction({
                        "document_id": document_id,
                        "job_id": job_id,
                        "processing_unit_id": image_id,
                        "result_file_id": file_id,
                        "status": "completed",
                        "structured_data": structured_data,
                        "raw_structured_data": structured_data,
                        "reviewed_data": structured_data,
                        "review_status": "needs_review" if requires_review else "ready",
                        "validation_flags": review_flags,
                        "edited": False,
                        "metadata": {
                            "source_storage_path": source_storage_path,
                            "source_content_type": source_content_type,
                            "source_filename": original_filename,
                            "source_page": source_page,
                            "source_page_count": source_page_count,
                            "source_sha256": source_sha256,
                            "output_filename": output_filename,
                            "output_content_type": output_content_type,
                            "selected_mode": requested_document_mode,
                            "resolved_mode": document_mode,
                            "ocr_language": ocr_language,
                            "classification": classification_data,
                        },
                    })
                    await supabase.finalize_job_document_statuses(job_id)
                    await supabase.refresh_document_duplicate_warnings(job_id, document_id)
            except Exception as metadata_error:
                logger.error(f"[Job {job_id}] Failed to store durable file metadata for {file_id}: {metadata_error}")
                raise

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
                    'image_results': completed_results,
                    'session_id': session_id,
                    'owner_user_id': user_id,
                    'owner_session_id': None if user_id else session_id
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
            filename=output_filename,
            image_id=image_id,
            document_id=document_id,
            source_page=source_page,
            source_page_count=source_page_count,
            size_bytes=len(output_data),
            status="completed",
            document_mode=document_mode,
            requires_review=requires_review,
            confidence_score=file_record.get("confidence_score"),
            review_flags=review_flags
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
            'filename': output_filename,
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

        if document_id:
            try:
                supabase = get_supabase_service()
                await supabase.upsert_document_extraction({
                    "document_id": document_id,
                    "job_id": job_id,
                    "processing_unit_id": image_id,
                    "status": "failed",
                    "structured_data": {},
                    "raw_structured_data": {},
                    "reviewed_data": {},
                    "review_status": "needs_review",
                    "validation_flags": [{"code": "processing_failed", "message": error_msg}],
                    "edited": False,
                    "metadata": {
                        "source_storage_path": source_storage_path,
                        "source_content_type": source_content_type,
                        "source_filename": img.get('original_filename') or img.get('filename'),
                        "source_page": source_page,
                        "source_page_count": source_page_count,
                        "source_sha256": source_sha256,
                    },
                })
            except Exception as metadata_error:
                logger.debug(f"Failed to persist extraction failure state: {metadata_error}")

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
    1. Processes all images concurrently using the configured OCR cap
    2. Updates the SAME job_id in Redis
    3. Sends completion message when done

    Simple and straightforward.
    """
    redis = await get_redis_service()
    storage = FileStorageManager()
    start_time = datetime.utcnow()

    logger.info(f"[Job {job_id}] Starting batch processing: {len(images)} images")
    await _restore_image_results_from_supabase(redis, job_id, user_id, session_id)
    
    # Update job status to "processing" in Supabase at start.
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
                'image_results': existing_results,
                'session_id': session_id,
                'owner_user_id': user_id,
                'owner_session_id': None if user_id else session_id
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

        # Local cap prevents one Celery task from creating excessive waiters.
        # Redis enforces the same cap globally across all worker machines.
        local_ocr_limit = max(1, settings.max_concurrent_ocr_calls)
        semaphore = asyncio.Semaphore(local_ocr_limit)
        logger.info(f"[Job {job_id}] Using OCR concurrency cap: {local_ocr_limit}")

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

        # Update job status in Supabase database (CRITICAL for dashboard and recovery)
        try:
            supabase = get_supabase_service()
            credit_metadata = await _settle_reserved_credits(
                supabase=supabase,
                job_id=job_id,
                user_id=user_id,
                total_images=len(images),
                successful_images=len(successful_results)
            )
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
                    'image_results': completed_image_results,
                    'session_id': session_id,
                    'owner_user_id': user_id,
                    'owner_session_id': None if user_id else session_id,
                    **credit_metadata
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
                document_id=file_data.get('document_id'),
                source_page=file_data.get('source_page'),
                source_page_count=file_data.get('source_page_count'),
                size_bytes=file_data.get('size_bytes'),
                status=file_data.get('status'),
                document_mode=file_data.get('document_mode'),
                requires_review=file_data.get('requires_review'),
                confidence_score=file_data.get('confidence_score'),
                review_flags=file_data.get('review_flags') or []
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
        try:
            supabase = get_supabase_service()
            credit_metadata = await _settle_reserved_credits(
                supabase=supabase,
                job_id=job_id,
                user_id=user_id,
                total_images=len(images),
                successful_images=0
            )
            await supabase.update_job_status(
                job_id=job_id,
                status='failed',
                error_message=str(e),
                metadata={
                    'session_id': session_id,
                    'owner_user_id': user_id,
                    'owner_session_id': None if user_id else session_id,
                    'failed_at': datetime.utcnow().isoformat(),
                    **credit_metadata
                }
            )
            logger.info(f"[Job {job_id}] Updated failed status in Supabase")
        except Exception as supabase_error:
            logger.error(f"[Job {job_id}] Failed to update Supabase status: {supabase_error}")
