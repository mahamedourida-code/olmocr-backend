import base64
import asyncio
import csv
import logging
import time
import random
import io
import json
import re
from decimal import Decimal, InvalidOperation
from typing import Optional, List, Dict, Any, Union
from openai import OpenAI
from openai import (
    APIConnectionError,
    APITimeoutError,
    InternalServerError,
    RateLimitError,
)
from openai.types.chat import ChatCompletion
from PIL import Image

# Transient provider failures worth retrying on the SAME model: request timeouts,
# connection resets, 429 rate limits, and 5xx server errors. Everything else
# (bad request / auth / unprocessable) is non-retryable and must fail fast so the
# model-failover layer or the page-level failure path can take over.
_RETRYABLE_OCR_ERRORS = (
    APITimeoutError,
    APIConnectionError,
    RateLimitError,
    InternalServerError,
)

from app.core.config import settings
from app.utils.exceptions import OlmOCRError

# Register HEIC/HEIF support
try:
    from pillow_heif import register_heif_opener
    register_heif_opener()
    HEIC_SUPPORT = True
except ImportError:
    HEIC_SUPPORT = False

logger = logging.getLogger(__name__)


class OlmOCRService:
    """Service for interacting with the OlmOCR API via DeepInfra."""
    
    def __init__(self):
        try:
            self.client = OpenAI(
                api_key=settings.olmocr_api_key,
                base_url=settings.olmocr_base_url,
                timeout=settings.ocr_request_timeout,
                max_retries=settings.ocr_client_max_retries
            )
        except Exception as e:
            logger.error(f"Failed to initialize OpenAI client: {e}")
            raise OlmOCRError(f"Failed to initialize OlmOCR service: {e}")
        
        self.model = settings.olmocr_model
        self.models = getattr(settings, "parsed_ocr_models", None) or [self.model]
        self.max_tokens = 2000
        self._last_request_time = 0.0
        self._request_count = 0

    def _is_pdf_bytes(self, file_data: bytes) -> bool:
        return file_data[:5] == b"%PDF-"

    def _render_pdf_pages_to_png(self, pdf_data: bytes) -> List[bytes]:
        try:
            import pymupdf
        except ImportError as exc:
            raise OlmOCRError("PDF support is not installed on this worker") from exc

        page_images: List[bytes] = []
        try:
            with pymupdf.open(stream=pdf_data, filetype="pdf") as document:
                if document.page_count == 0:
                    raise OlmOCRError("PDF has no pages to process")

                for page_number in range(document.page_count):
                    page = document.load_page(page_number)
                    pixmap = page.get_pixmap(dpi=180, alpha=False)
                    page_images.append(pixmap.tobytes(output="png"))

            logger.info(f"Rendered PDF into {len(page_images)} page image(s)")
            return page_images
        except OlmOCRError:
            raise
        except Exception as exc:
            raise OlmOCRError(f"Failed to render PDF for OCR: {exc}") from exc

    def _downscale_for_ocr(self, image_bytes: bytes, max_dim: int = 2048, quality: int = 85) -> bytes:
        """Downscale oversized photos before the vision call to cut latency + memory.

        Big phone photos (3-5 MB, 3000-4000 px) cost upload time, vision-token
        prefill, and worker RAM with no OCR accuracy gain. We cap the longest side
        and re-encode as JPEG. Images already within the cap (including 180-DPI PDF
        page renders, which are ~1980px) pass through untouched, so PDF quality is
        never degraded. Any failure falls back to the original bytes.
        """
        try:
            with Image.open(io.BytesIO(image_bytes)) as img:
                if max(img.size) <= max_dim:
                    return image_bytes
                scale = max_dim / float(max(img.size))
                new_size = (max(1, round(img.width * scale)), max(1, round(img.height * scale)))
                try:
                    resample = Image.Resampling.LANCZOS
                except AttributeError:
                    resample = Image.LANCZOS
                resized = img.convert("RGB").resize(new_size, resample)
                buffer = io.BytesIO()
                resized.save(buffer, format="JPEG", quality=quality, optimize=True)
                downscaled = buffer.getvalue()
                logger.info(
                    f"Downscaled OCR image {img.size}->{new_size} "
                    f"({len(image_bytes)//1024}KB->{len(downscaled)//1024}KB)"
                )
                return downscaled
        except Exception as exc:
            logger.debug(f"Image downscale skipped: {exc}")
            return image_bytes

    @staticmethod
    def _ocr_language_instruction(ocr_language: Optional[str]) -> str:
        code = str(ocr_language or "en").strip().lower()
        language_names = {
            "ar": "Arabic",
            "de": "German",
            "en": "English",
            "es": "Spanish",
            "fr": "French",
            "it": "Italian",
            "pt": "Portuguese",
            "zh": "Chinese",
        }
        language_name = language_names.get(code)
        if not language_name or code == "en":
            return ""
        return (
            f" The user selected {language_name} as the OCR language. "
            "Use that preference to interpret printed labels, reading order, and common terms, "
            "while preserving visible text, dates, numbers, currencies, and field values exactly as written."
        )

    async def _extract_table_from_pdf(self, pdf_data: bytes, ocr_language: Optional[str] = "en", model: Optional[str] = None) -> str:
        page_images = self._render_pdf_pages_to_png(pdf_data)
        page_outputs: List[str] = []

        for index, page_image in enumerate(page_images):
            logger.info(f"Processing PDF page {index + 1}/{len(page_images)}")
            page_csv = await self.extract_table_from_image(page_image, ocr_language=ocr_language, model=model)
            if page_csv and page_csv.strip():
                page_outputs.append(page_csv.strip())

        if not page_outputs:
            raise OlmOCRError("No table data extracted from PDF")

        return "\n".join(page_outputs)

    async def _extract_text_from_pdf(self, pdf_data: bytes, ocr_language: Optional[str] = "en", model: Optional[str] = None) -> str:
        page_images = self._render_pdf_pages_to_png(pdf_data)
        page_outputs: List[str] = []

        for index, page_image in enumerate(page_images):
            logger.info(f"Processing PDF page {index + 1}/{len(page_images)} as text")
            page_text = await self.extract_text_from_image(page_image, ocr_language=ocr_language, model=model)
            if page_text and page_text.strip():
                page_outputs.append(page_text.strip())

        if not page_outputs:
            raise OlmOCRError("No text extracted from PDF")

        return "\n\n".join(page_outputs)
    
    async def _apply_rate_limiting(self):
        """
        Apply intelligent rate limiting with exponential backoff and jitter.
        
        This method ensures that API requests are spaced out appropriately
        to avoid overwhelming the OlmOCR API service.
        """
        current_time = time.time()
        base_delay = max(0.0, settings.olmocr_base_delay_seconds)
        if base_delay <= 0:
            self._last_request_time = current_time
            return

        self._request_count += 1
        
        # Calculate delay based on configuration
        if settings.olmocr_exponential_backoff:
            # Exponential backoff: base_delay * 2^(request_count % 4)
            delay = base_delay * (2 ** (self._request_count % 4))
            delay = min(delay, settings.olmocr_max_delay_seconds)
        else:
            # Fixed delay
            delay = base_delay
        
        # Add jitter to prevent thundering herd
        jitter = random.uniform(0, settings.olmocr_jitter_factor * delay)
        total_delay = delay + jitter
        
        # Ensure minimum time between requests
        time_since_last = current_time - self._last_request_time
        if time_since_last < total_delay:
            sleep_time = total_delay - time_since_last
            logger.info(f"Rate limiting: sleeping for {sleep_time:.2f}s (request #{self._request_count})")
            await asyncio.sleep(sleep_time)
        
        self._last_request_time = time.time()
    
    async def extract_table_from_image(self, image_data: Union[bytes, str], ocr_language: Optional[str] = "en", model: Optional[str] = None) -> str:
        """
        Extract table data from image using OlmOCR API.

        Args:
            image_data: Binary image data (bytes) or base64 encoded string

        Returns:
            Extracted table data as CSV string

        Raises:
            OlmOCRError: If API call fails or returns invalid response
        """
        try:
            # Handle both bytes and base64 string input
            if isinstance(image_data, bytes):
                image_bytes = image_data
            else:
                # Decode base64 string to bytes
                if image_data.startswith('data:'):
                    img_b64_str = image_data.split(',', 1)[1]
                else:
                    img_b64_str = image_data
                image_bytes = base64.b64decode(img_b64_str)

            if self._is_pdf_bytes(image_bytes):
                return await self._extract_table_from_pdf(image_bytes, ocr_language=ocr_language, model=model)

            # Apply rate limiting before making the image OCR API call
            await self._apply_rate_limiting()
            
            # Check if this is a HEIC/HEIF image and convert to JPEG if needed
            try:
                # Try to open the image with PIL
                img = Image.open(io.BytesIO(image_bytes))
                
                # Check if it's HEIC/HEIF format
                if hasattr(img, 'format') and img.format and img.format.lower() in ['heif', 'heic']:
                    logger.info(f"Detected HEIC/HEIF image (format: {img.format}), converting to JPEG...")
                    
                    # Convert to RGB if necessary (HEIC might be in different mode)
                    if img.mode not in ('RGB', 'RGBA'):
                        img = img.convert('RGB')
                    elif img.mode == 'RGBA':
                        # Convert RGBA to RGB (JPEG doesn't support transparency)
                        rgb_img = Image.new('RGB', img.size, (255, 255, 255))
                        rgb_img.paste(img, mask=img.split()[3] if len(img.split()) > 3 else None)
                        img = rgb_img
                    
                    # Save as JPEG to bytes
                    output_buffer = io.BytesIO()
                    img.save(output_buffer, format='JPEG', quality=95)
                    output_buffer.seek(0)
                    converted_bytes = output_buffer.read()
                    
                    logger.info(f"Successfully converted HEIC to JPEG ({len(image_bytes)} bytes -> {len(converted_bytes)} bytes)")
                    image_bytes = converted_bytes
                    
                # Close the image
                img.close()
                    
            except Exception as e:
                # If PIL can't open it, it might be a valid format that doesn't need conversion
                logger.debug(f"PIL processing note: {str(e)} - proceeding with original image")
            
            # Convert to base64 for API
            image_bytes = self._downscale_for_ocr(image_bytes)
            img_b64 = base64.b64encode(image_bytes).decode("utf-8")
            language_instruction = self._ocr_language_instruction(ocr_language)
            
            # Prepare the request
            messages = [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/png;base64,{img_b64}"
                            }
                        },
                        {
                            "type": "text",
                            "text": (
                                "Attached is one page of a document that you must process as a table. "
                                "Return only the best structured table you can read. Preserve the visible row labels, "
                                "column labels, amounts, dates, signs, and hierarchy. Do not summarize, explain, or add prose. "
                                "Use an empty cell when a value is unclear. Prefer one markdown table; if there are multiple "
                                "tables, return them one after another separated by one blank line. Do not say you are unable "
                                "to extract the table unless there is no readable table at all. Return markdown table content only."
                            ) + language_instruction
                        }
                    ]
                }
            ]
            
            # Make the API call
            logger.info("Making OlmOCR API request")
            response: ChatCompletion = await self._make_api_call(messages, model=model)
            
            # Extract the response content
            if not response.choices or not response.choices[0].message.content:
                raise OlmOCRError("Empty response from OlmOCR API")
            
            raw_content = response.choices[0].message.content.strip()
            logger.info(f"Received OCR response, length: {len(raw_content)}")
            logger.debug(f"Raw OCR response (first 500 chars): {raw_content[:500]}...")
            
            # Clean the response (remove code blocks, etc.)
            cleaned_content = self._clean_ocr_response(raw_content)

            json_csv_content = self._extract_csv_from_json_response(cleaned_content)
            if self._validate_csv_content(json_csv_content):
                csv_content = json_csv_content
            else:
                # Check if response contains HTML table format
                if '<table' in cleaned_content.lower() or '<tr' in cleaned_content.lower():
                    logger.info("Detected HTML table format in response, converting to CSV")
                    table_content = self._parse_html_table(cleaned_content)
                else:
                    # Extract table data from markdown response
                    table_content = self._extract_csv_from_response(cleaned_content)

                # Convert to CSV format (handles both pipe-separated and structured data)
                csv_content = self._convert_pipe_to_csv(table_content)
            
            # Validate that we got meaningful content
            if not self._validate_csv_content(csv_content):
                logger.warning(f"Could not extract valid table structure. Attempting fallback processing...")
                # Try direct processing of the cleaned content
                csv_content = self._convert_pipe_to_csv(cleaned_content)
                
                if not self._validate_csv_content(csv_content):
                    # Last resort: treat entire content as single-column data
                    logger.warning("Treating content as single-column data")
                    lines = cleaned_content.split('\n')
                    csv_content = '\n'.join([line.strip() for line in lines if line.strip()])
            
            logger.info(f"Successfully processed OCR data: {len(csv_content.split(chr(10)))} rows")
            return csv_content
            
        except OlmOCRError:
            raise
        except Exception as e:
            logger.error(f"OlmOCR API error: {str(e)}")
            raise OlmOCRError(f"Failed to process image: {str(e)}")

    async def extract_text_from_image(self, image_data: Union[bytes, str], ocr_language: Optional[str] = "en", model: Optional[str] = None) -> str:
        """Extract plain text from an image or rendered PDF page."""
        try:
            if isinstance(image_data, bytes):
                image_bytes = image_data
            else:
                img_b64_str = image_data.split(',', 1)[1] if image_data.startswith('data:') else image_data
                image_bytes = base64.b64decode(img_b64_str)

            if self._is_pdf_bytes(image_bytes):
                return await self._extract_text_from_pdf(image_bytes, ocr_language=ocr_language, model=model)

            await self._apply_rate_limiting()

            try:
                img = Image.open(io.BytesIO(image_bytes))
                if hasattr(img, 'format') and img.format and img.format.lower() in ['heif', 'heic']:
                    logger.info(f"Detected HEIC/HEIF image (format: {img.format}), converting to JPEG...")
                    if img.mode not in ('RGB', 'RGBA'):
                        img = img.convert('RGB')
                    elif img.mode == 'RGBA':
                        rgb_img = Image.new('RGB', img.size, (255, 255, 255))
                        rgb_img.paste(img, mask=img.split()[3] if len(img.split()) > 3 else None)
                        img = rgb_img

                    output_buffer = io.BytesIO()
                    img.save(output_buffer, format='JPEG', quality=95)
                    output_buffer.seek(0)
                    image_bytes = output_buffer.read()

                img.close()
            except Exception as e:
                logger.debug(f"PIL processing note: {str(e)} - proceeding with original image")

            image_bytes = self._downscale_for_ocr(image_bytes)
            img_b64 = base64.b64encode(image_bytes).decode("utf-8")
            language_instruction = self._ocr_language_instruction(ocr_language)
            messages = [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/png;base64,{img_b64}"
                            }
                        },
                        {
                            "type": "text",
                            "text": (
                                "Extract all readable text from this document image. "
                                "Return plain text only. Preserve line breaks, labels, and reading order. "
                                "Do not summarize. Do not convert the result into CSV, JSON, or an Excel table."
                                + language_instruction
                            )
                        }
                    ]
                }
            ]

            logger.info("Making OlmOCR text extraction request")
            response: ChatCompletion = await self._make_api_call(messages, max_tokens=5000, model=model)

            if not response.choices or not response.choices[0].message.content:
                raise OlmOCRError("Empty response from OlmOCR API")

            raw_content = response.choices[0].message.content.strip()
            return self._clean_ocr_response(raw_content)

        except OlmOCRError:
            raise
        except Exception as e:
            logger.error(f"OlmOCR text extraction error: {str(e)}")
            raise OlmOCRError(f"Failed to extract text: {str(e)}")

    async def classify_document_from_image(self, image_data: Union[bytes, str], ocr_language: Optional[str] = "en", model: Optional[str] = None) -> Dict[str, Any]:
        """Classify a document for extractor routing without inventing an accounting type."""
        try:
            if isinstance(image_data, bytes):
                image_bytes = image_data
            else:
                img_b64_str = image_data.split(',', 1)[1] if image_data.startswith('data:') else image_data
                image_bytes = base64.b64decode(img_b64_str)

            if self._is_pdf_bytes(image_bytes):
                page_images = self._render_pdf_pages_to_png(image_bytes)
                image_bytes = page_images[0]

            await self._apply_rate_limiting()
            image_bytes = self._downscale_for_ocr(image_bytes)
            img_b64 = base64.b64encode(image_bytes).decode("utf-8")
            language_instruction = self._ocr_language_instruction(ocr_language)
            messages = [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {"url": f"data:image/png;base64,{img_b64}"}
                        },
                        {
                            "type": "text",
                            "text": (
                                "Classify this uploaded document for extraction routing only. "
                                "Choose exactly one document_type: invoice, receipt, bank_statement, table, notes, "
                                "or needs_manual_selection. Use invoice only for supplier bills with invoice fields; "
                                "receipt for proof of purchase, point-of-sale receipts, restaurant receipts, and card slips, "
                                "even when some fields are small or partly cropped; bank_statement only for bank account transaction statements; "
                                "table for a visible grid/list intended as rows and columns; notes for narrative handwriting or prose. "
                                "Only choose needs_manual_selection when the document type is genuinely impossible to determine. "
                                "confidence must be between 0 and 1. review_reason must briefly explain uncertainty or be empty "
                                "when the type is clear. Do not extract fields and do not infer accounting treatment."
                                + language_instruction
                            )
                        }
                    ]
                }
            ]
            classification_schema = {
                "type": "json_schema",
                "json_schema": {
                    "name": "document_classification",
                    "strict": True,
                    "schema": {
                        "type": "object",
                        "properties": {
                            "document_type": {
                                "type": "string",
                                "enum": ["invoice", "receipt", "bank_statement", "table", "notes", "needs_manual_selection"]
                            },
                            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                            "review_reason": {"type": "string"}
                        },
                        "required": ["document_type", "confidence", "review_reason"],
                        "additionalProperties": False
                    }
                }
            }

            try:
                response: ChatCompletion = await self._make_api_call(
                    messages,
                    max_tokens=300,
                    response_format=classification_schema,
                    temperature=0,
                    model=model,
                )
            except Exception as schema_error:
                if "response_format" not in str(schema_error).lower() and "schema" not in str(schema_error).lower():
                    raise
                response = await self._make_api_call(
                    messages,
                    max_tokens=300,
                    response_format={"type": "json_object"},
                    temperature=0,
                    model=model,
                )

            if not response.choices or not response.choices[0].message.content:
                raise OlmOCRError("Empty document classification response")

            return self._parse_document_classification(
                self._clean_ocr_response(response.choices[0].message.content.strip())
            )
        except OlmOCRError:
            raise
        except Exception as e:
            logger.error(f"Document classification error: {str(e)}")
            raise OlmOCRError(f"Failed to classify document: {str(e)}")

    async def extract_notes_from_image(self, image_data: Union[bytes, str], ocr_language: Optional[str] = "en", model: Optional[str] = None) -> Dict[str, Any]:
        """Extract handwritten narrative text and any visibly detected tables."""
        try:
            if isinstance(image_data, bytes):
                image_bytes = image_data
            else:
                img_b64_str = image_data.split(',', 1)[1] if image_data.startswith('data:') else image_data
                image_bytes = base64.b64decode(img_b64_str)

            if self._is_pdf_bytes(image_bytes):
                page_results = []
                for page_number, page_image in enumerate(self._render_pdf_pages_to_png(image_bytes), start=1):
                    page_result = await self.extract_notes_from_image(page_image, ocr_language=ocr_language, model=model)
                    page_result["page"] = page_number
                    page_results.append(page_result)
                return self._merge_notes_pages(page_results)

            await self._apply_rate_limiting()
            try:
                img = Image.open(io.BytesIO(image_bytes))
                if hasattr(img, 'format') and img.format and img.format.lower() in ['heif', 'heic']:
                    if img.mode not in ('RGB', 'RGBA'):
                        img = img.convert('RGB')
                    elif img.mode == 'RGBA':
                        rgb_img = Image.new('RGB', img.size, (255, 255, 255))
                        rgb_img.paste(img, mask=img.split()[3] if len(img.split()) > 3 else None)
                        img = rgb_img
                    output_buffer = io.BytesIO()
                    img.save(output_buffer, format='JPEG', quality=95)
                    output_buffer.seek(0)
                    image_bytes = output_buffer.read()
                img.close()
            except Exception as e:
                logger.debug(f"PIL processing note: {str(e)} - proceeding with original image")

            image_bytes = self._downscale_for_ocr(image_bytes)
            img_b64 = base64.b64encode(image_bytes).decode("utf-8")
            language_instruction = self._ocr_language_instruction(ocr_language)
            messages = [{
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{img_b64}"}},
                    {
                        "type": "text",
                        "text": (
                            "Extract handwritten or typed notes for human review only. "
                            "Return the readable narrative in reading order, preserving headings and line breaks. "
                            "If a visible table exists, extract only its visible columns and rows. "
                            "Do not invent a table from prose, summarize, classify accounting, or post data anywhere. "
                            "Use empty arrays when no table is visible and add review notes only for unclear text or cells."
                            + language_instruction
                        ),
                    },
                ],
            }]
            notes_schema = {
                "type": "json_schema",
                "json_schema": {
                    "name": "notes_extraction",
                    "strict": True,
                    "schema": {
                        "type": "object",
                        "properties": {
                            "readable_text": {"type": "string"},
                            "tables": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "title": {"type": "string"},
                                        "columns": {"type": "array", "items": {"type": "string"}},
                                        "rows": {
                                            "type": "array",
                                            "items": {
                                                "type": "object",
                                                "properties": {"cells": {"type": "array", "items": {"type": "string"}}},
                                                "required": ["cells"],
                                                "additionalProperties": False,
                                            },
                                        },
                                    },
                                    "required": ["title", "columns", "rows"],
                                    "additionalProperties": False,
                                },
                            },
                            "review_notes": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {"area": {"type": "string"}, "note": {"type": "string"}},
                                    "required": ["area", "note"],
                                    "additionalProperties": False,
                                },
                            },
                        },
                        "required": ["readable_text", "tables", "review_notes"],
                        "additionalProperties": False,
                    },
                },
            }

            logger.info("Making OlmOCR notes extraction request")
            try:
                response: ChatCompletion = await self._make_api_call(
                    messages,
                    max_tokens=5000,
                    response_format=notes_schema,
                    temperature=0,
                    model=model,
                )
            except Exception as structured_error:
                error_text = str(structured_error).lower()
                if "response_format" not in error_text and "json_schema" not in error_text and "schema" not in error_text:
                    raise
                logger.warning("Notes schema response format unavailable; retrying dedicated JSON request")
                response = await self._make_api_call(
                    messages,
                    max_tokens=5000,
                    response_format={"type": "json_object"},
                    temperature=0,
                    model=model,
                )

            if not response.choices or not response.choices[0].message.content:
                raise OlmOCRError("Empty response from OlmOCR API")
            return self._parse_notes_json(self._clean_ocr_response(response.choices[0].message.content.strip()))
        except OlmOCRError:
            raise
        except Exception as e:
            logger.error(f"OlmOCR notes extraction error: {str(e)}")
            raise OlmOCRError(f"Failed to extract notes: {str(e)}")

    def _parse_document_classification(self, content: str) -> Dict[str, Any]:
        allowed_types = {"invoice", "receipt", "bank_statement", "table", "notes", "needs_manual_selection"}
        try:
            parsed = json.loads(content)
        except json.JSONDecodeError as exc:
            # Raise instead of silently returning needs_manual_selection: a bad reply
            # from one model must trigger cross-model failover, not skip extraction.
            raise OlmOCRError("Document classification response was not valid JSON") from exc

        document_type = str(parsed.get("document_type") or "needs_manual_selection").strip().lower()
        if document_type not in allowed_types:
            document_type = "needs_manual_selection"
        try:
            confidence = min(1.0, max(0.0, float(parsed.get("confidence", 0.0))))
        except (TypeError, ValueError):
            confidence = 0.0
        review_reason = str(parsed.get("review_reason") or "").strip()
        return {
            "document_type": document_type,
            "confidence": confidence,
            "review_reason": review_reason,
        }

    async def extract_bank_statement_from_image(self, image_data: Union[bytes, str], ocr_language: Optional[str] = "en", model: Optional[str] = None) -> Dict[str, Any]:
        """Extract visible bank statement text and transactions into a structured review object."""
        try:
            if isinstance(image_data, bytes):
                image_bytes = image_data
            else:
                img_b64_str = image_data.split(',', 1)[1] if image_data.startswith('data:') else image_data
                image_bytes = base64.b64decode(img_b64_str)

            if self._is_pdf_bytes(image_bytes):
                page_images = self._render_pdf_pages_to_png(image_bytes)
                page_results = []
                for index, page_image in enumerate(page_images, start=1):
                    result = await self.extract_bank_statement_from_image(page_image, ocr_language=ocr_language, model=model)
                    result["page"] = index
                    page_results.append(result)
                return self._merge_bank_statement_pages(page_results)

            await self._apply_rate_limiting()

            try:
                img = Image.open(io.BytesIO(image_bytes))
                if hasattr(img, 'format') and img.format and img.format.lower() in ['heif', 'heic']:
                    if img.mode not in ('RGB', 'RGBA'):
                        img = img.convert('RGB')
                    elif img.mode == 'RGBA':
                        rgb_img = Image.new('RGB', img.size, (255, 255, 255))
                        rgb_img.paste(img, mask=img.split()[3] if len(img.split()) > 3 else None)
                        img = rgb_img
                    output_buffer = io.BytesIO()
                    img.save(output_buffer, format='JPEG', quality=95)
                    output_buffer.seek(0)
                    image_bytes = output_buffer.read()
                img.close()
            except Exception as e:
                logger.debug(f"PIL processing note: {str(e)} - proceeding with original image")

            image_bytes = self._downscale_for_ocr(image_bytes)
            img_b64 = base64.b64encode(image_bytes).decode("utf-8")
            language_instruction = self._ocr_language_instruction(ocr_language)
            messages = [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {"url": f"data:image/png;base64,{img_b64}"}
                        },
                        {
                            "type": "text",
                            "text": (
                                "You are extracting a bank statement for OCR review only. "
                                "Do not reconcile, categorize, post transactions, infer missing transactions, or give advice. "
                                "Return JSON matching the supplied schema. Use empty strings when values or printed pagination are not visible. "
                                "Keep every visible transaction row in document order. "
                                "Set readable to false only when a row cannot be reliably transcribed, and describe the issue in review_note. "
                                "Use review_notes only for visible OCR uncertainty such as an unreadable row or a visibly missing statement page. "
                                "Preserve original wording and number formatting."
                                + language_instruction
                            )
                        }
                    ]
                }
            ]
            bank_statement_schema = {
                "type": "json_schema",
                "json_schema": {
                    "name": "bank_statement_extraction",
                    "strict": True,
                    "schema": {
                        "type": "object",
                        "properties": {
                            "account_holder": {"type": "string"},
                            "bank_name": {"type": "string"},
                            "account_number": {"type": "string"},
                            "period": {"type": "string"},
                            "currency": {"type": "string"},
                            "opening_balance": {"type": "string"},
                            "closing_balance": {"type": "string"},
                            "printed_page_number": {"type": "string"},
                            "printed_page_count": {"type": "string"},
                            "transactions": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "date": {"type": "string"},
                                        "description": {"type": "string"},
                                        "reference": {"type": "string"},
                                        "debit": {"type": "string"},
                                        "credit": {"type": "string"},
                                        "balance": {"type": "string"},
                                        "readable": {"type": "boolean"},
                                        "review_note": {"type": "string"},
                                    },
                                    "required": [
                                        "date", "description", "reference", "debit",
                                        "credit", "balance", "readable", "review_note",
                                    ],
                                    "additionalProperties": False,
                                },
                            },
                            "raw_text": {"type": "string"},
                            "review_notes": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "area": {"type": "string"},
                                        "note": {"type": "string"},
                                    },
                                    "required": ["area", "note"],
                                    "additionalProperties": False,
                                },
                            },
                        },
                        "required": [
                            "account_holder", "bank_name", "account_number", "period",
                            "currency", "opening_balance", "closing_balance",
                            "printed_page_number", "printed_page_count", "transactions",
                            "raw_text", "review_notes",
                        ],
                        "additionalProperties": False,
                    },
                },
            }

            logger.info("Making OlmOCR bank statement extraction request")
            try:
                response: ChatCompletion = await self._make_api_call(
                    messages,
                    max_tokens=5000,
                    response_format=bank_statement_schema,
                    temperature=0,
                    model=model,
                )
            except Exception as structured_error:
                error_text = str(structured_error).lower()
                if "response_format" not in error_text and "json_schema" not in error_text and "schema" not in error_text:
                    raise
                logger.warning("Bank statement schema response format unavailable; retrying dedicated JSON request")
                response = await self._make_api_call(
                    messages,
                    max_tokens=5000,
                    response_format={"type": "json_object"},
                    temperature=0,
                    model=model,
                )

            if not response.choices or not response.choices[0].message.content:
                raise OlmOCRError("Empty response from OlmOCR API")

            raw_content = self._clean_ocr_response(response.choices[0].message.content.strip())
            return self._parse_bank_statement_json(raw_content)
        except OlmOCRError:
            raise
        except Exception as e:
            logger.error(f"OlmOCR bank statement extraction error: {str(e)}")
            raise OlmOCRError(f"Failed to extract bank statement: {str(e)}")

    async def extract_invoice_from_image(self, image_data: Union[bytes, str], ocr_language: Optional[str] = "en", model: Optional[str] = None) -> Dict[str, Any]:
        """Extract visible invoice fields and line items for review and export."""
        try:
            if isinstance(image_data, bytes):
                image_bytes = image_data
            else:
                img_b64_str = image_data.split(',', 1)[1] if image_data.startswith('data:') else image_data
                image_bytes = base64.b64decode(img_b64_str)

            if self._is_pdf_bytes(image_bytes):
                page_results = []
                for page_number, page_image in enumerate(self._render_pdf_pages_to_png(image_bytes), start=1):
                    page_result = await self.extract_invoice_from_image(page_image, ocr_language=ocr_language, model=model)
                    page_result["page"] = page_number
                    page_results.append(page_result)
                return self._merge_invoice_pages(page_results)

            await self._apply_rate_limiting()

            try:
                img = Image.open(io.BytesIO(image_bytes))
                if hasattr(img, 'format') and img.format and img.format.lower() in ['heif', 'heic']:
                    if img.mode not in ('RGB', 'RGBA'):
                        img = img.convert('RGB')
                    elif img.mode == 'RGBA':
                        rgb_img = Image.new('RGB', img.size, (255, 255, 255))
                        rgb_img.paste(img, mask=img.split()[3] if len(img.split()) > 3 else None)
                        img = rgb_img
                    output_buffer = io.BytesIO()
                    img.save(output_buffer, format='JPEG', quality=95)
                    output_buffer.seek(0)
                    image_bytes = output_buffer.read()
                img.close()
            except Exception as e:
                logger.debug(f"PIL processing note: {str(e)} - proceeding with original image")

            image_bytes = self._downscale_for_ocr(image_bytes)
            img_b64 = base64.b64encode(image_bytes).decode("utf-8")
            language_instruction = self._ocr_language_instruction(ocr_language)
            messages = [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {"url": f"data:image/png;base64,{img_b64}"}
                        },
                        {
                            "type": "text",
                            "text": (
                                "Extract only visible invoice data for human review. "
                                "Do not infer missing values, calculate missing totals, or classify accounting. "
                                "Return JSON matching the supplied schema. Use empty strings when fields are not visible. "
                                "Preserve visible number and date formatting. For line_items, include one item per visible invoice row."
                                + language_instruction
                            )
                        }
                    ]
                }
            ]
            invoice_schema = {
                "type": "json_schema",
                "json_schema": {
                    "name": "invoice_extraction",
                    "strict": True,
                    "schema": {
                        "type": "object",
                        "properties": {
                            "vendor_name": {"type": "string"},
                            "supplier_tax_vat_id": {"type": "string"},
                            "invoice_number": {"type": "string"},
                            "invoice_date": {"type": "string"},
                            "due_date": {"type": "string"},
                            "po_reference": {"type": "string"},
                            "subtotal": {"type": "string"},
                            "tax_vat_amount": {"type": "string"},
                            "total": {"type": "string"},
                            "currency": {"type": "string"},
                            "line_items": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "description": {"type": "string"},
                                        "quantity": {"type": "string"},
                                        "unit_price": {"type": "string"},
                                        "tax_rate": {"type": "string"},
                                        "line_total": {"type": "string"},
                                    },
                                    "required": ["description", "quantity", "unit_price", "tax_rate", "line_total"],
                                    "additionalProperties": False,
                                },
                            },
                        },
                        "required": [
                            "vendor_name", "supplier_tax_vat_id", "invoice_number",
                            "invoice_date", "due_date", "po_reference", "subtotal",
                            "tax_vat_amount", "total", "currency", "line_items",
                        ],
                        "additionalProperties": False,
                    },
                },
            }

            logger.info("Making OlmOCR invoice extraction request")
            try:
                response: ChatCompletion = await self._make_api_call(
                    messages,
                    max_tokens=5000,
                    response_format=invoice_schema,
                    temperature=0,
                    model=model,
                )
            except Exception as structured_error:
                error_text = str(structured_error).lower()
                if "response_format" not in error_text and "json_schema" not in error_text and "schema" not in error_text:
                    raise
                logger.warning("Invoice schema response format unavailable; retrying dedicated invoice JSON request")
                response = await self._make_api_call(
                    messages,
                    max_tokens=5000,
                    response_format={"type": "json_object"},
                    temperature=0,
                    model=model,
                )

            if not response.choices or not response.choices[0].message.content:
                raise OlmOCRError("Empty response from OlmOCR API")

            raw_content = self._clean_ocr_response(response.choices[0].message.content.strip())
            return self._parse_invoice_json(raw_content)
        except OlmOCRError:
            raise
        except Exception as e:
            logger.error(f"OlmOCR invoice extraction error: {str(e)}")
            raise OlmOCRError(f"Failed to extract invoice: {str(e)}")

    async def extract_receipt_from_image(self, image_data: Union[bytes, str], ocr_language: Optional[str] = "en", model: Optional[str] = None) -> Dict[str, Any]:
        """Extract visible receipt fields and line items for expense review."""
        try:
            if isinstance(image_data, bytes):
                image_bytes = image_data
            else:
                img_b64_str = image_data.split(',', 1)[1] if image_data.startswith('data:') else image_data
                image_bytes = base64.b64decode(img_b64_str)

            if self._is_pdf_bytes(image_bytes):
                page_results = []
                for page_number, page_image in enumerate(self._render_pdf_pages_to_png(image_bytes), start=1):
                    page_result = await self.extract_receipt_from_image(page_image, ocr_language=ocr_language, model=model)
                    page_result["page"] = page_number
                    page_results.append(page_result)
                return self._merge_receipt_pages(page_results)

            await self._apply_rate_limiting()

            try:
                img = Image.open(io.BytesIO(image_bytes))
                if hasattr(img, 'format') and img.format and img.format.lower() in ['heif', 'heic']:
                    if img.mode not in ('RGB', 'RGBA'):
                        img = img.convert('RGB')
                    elif img.mode == 'RGBA':
                        rgb_img = Image.new('RGB', img.size, (255, 255, 255))
                        rgb_img.paste(img, mask=img.split()[3] if len(img.split()) > 3 else None)
                        img = rgb_img
                    output_buffer = io.BytesIO()
                    img.save(output_buffer, format='JPEG', quality=95)
                    output_buffer.seek(0)
                    image_bytes = output_buffer.read()
                img.close()
            except Exception as e:
                logger.debug(f"PIL processing note: {str(e)} - proceeding with original image")

            image_bytes = self._downscale_for_ocr(image_bytes)
            img_b64 = base64.b64encode(image_bytes).decode("utf-8")
            language_instruction = self._ocr_language_instruction(ocr_language)
            messages = [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {"url": f"data:image/png;base64,{img_b64}"}
                        },
                        {
                            "type": "text",
                            "text": (
                                "Read the receipt image, then extract every visible receipt field for human expense review. "
                                "Prefer a partial result over empty fields: if a merchant, date, payment method, subtotal, tax, "
                                "total, currency, or item row is visible, include it. Do not infer missing values, categorize "
                                "expenses, create bills, or calculate missing totals. Merchant can be the store, restaurant, "
                                "retailer, supplier, or seller name. Payment method can be card, cash, tender type, or visible "
                                "card brand/last digits. Tax can appear as tax, VAT, GST, HST, sales tax, or service tax. "
                                "Total can appear as total, amount paid, amount due, balance, or grand total. Currency can be "
                                "a symbol or code. Preserve visible number and date formatting. Put the best plain OCR text "
                                "you can read in raw_text. Return JSON with keys merchant, date, payment_method, currency, "
                                "subtotal, tax_vat_amount, total, discount, tip, raw_text, review_notes, and line_items. "
                                "Each line_items row should use description, quantity, unit_price, tax_rate, and line_total. "
                                "Use empty strings only for fields that are not visible."
                                + language_instruction
                            )
                        }
                    ]
                }
            ]
            receipt_schema = {
                "type": "json_schema",
                "json_schema": {
                    "name": "receipt_extraction",
                    "strict": False,
                    "schema": {
                        "type": "object",
                        "properties": {
                            "merchant": {"type": "string"},
                            "date": {"type": "string"},
                            "payment_method": {"type": "string"},
                            "currency": {"type": "string"},
                            "subtotal": {"type": "string"},
                            "tax_vat_amount": {"type": "string"},
                            "total": {"type": "string"},
                            "discount": {"type": "string"},
                            "tip": {"type": "string"},
                            "raw_text": {"type": "string"},
                            "review_notes": {"type": "array", "items": {"type": "string"}},
                            "line_items": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "description": {"type": "string"},
                                        "quantity": {"type": "string"},
                                        "unit_price": {"type": "string"},
                                        "tax_rate": {"type": "string"},
                                        "line_total": {"type": "string"},
                                    },
                                    "required": ["description", "quantity", "unit_price", "tax_rate", "line_total"],
                                    "additionalProperties": False,
                                },
                            },
                        },
                        "required": [
                            "merchant", "date", "payment_method", "currency", "subtotal",
                            "tax_vat_amount", "total", "discount", "tip", "raw_text",
                            "review_notes", "line_items",
                        ],
                        "additionalProperties": True,
                    },
                },
            }

            logger.info("Making OlmOCR receipt extraction request")
            try:
                response: ChatCompletion = await self._make_api_call(
                    messages,
                    max_tokens=5000,
                    response_format=receipt_schema,
                    temperature=0,
                    model=model,
                )
            except Exception as structured_error:
                error_text = str(structured_error).lower()
                if "response_format" not in error_text and "json_schema" not in error_text and "schema" not in error_text:
                    raise
                logger.warning("Receipt schema response format unavailable; retrying dedicated receipt JSON request")
                response = await self._make_api_call(
                    messages,
                    max_tokens=5000,
                    response_format={"type": "json_object"},
                    temperature=0,
                    model=model,
                )

            if not response.choices or not response.choices[0].message.content:
                raise OlmOCRError("Empty response from OlmOCR API")

            raw_content = self._clean_ocr_response(response.choices[0].message.content.strip())
            return self._parse_receipt_json(raw_content)
        except OlmOCRError:
            raise
        except Exception as e:
            logger.error(f"OlmOCR receipt extraction error: {str(e)}")
            raise OlmOCRError(f"Failed to extract receipt: {str(e)}")

    def _parse_bank_statement_json(self, content: str) -> Dict[str, Any]:
        parsed: Dict[str, Any] = {}
        parse_failed = False
        try:
            loaded = json.loads(content)
            parsed = loaded if isinstance(loaded, dict) else {}
        except json.JSONDecodeError:
            match = re.search(r"\{.*\}", content, flags=re.DOTALL)
            if match:
                try:
                    loaded = json.loads(match.group(0))
                    parsed = loaded if isinstance(loaded, dict) else {}
                except json.JSONDecodeError:
                    parse_failed = True
            else:
                parse_failed = True

        legacy_summary = parsed.get("summary") if isinstance(parsed.get("summary"), dict) else {}
        field_aliases = {
            "account_holder": ["account_holder", "holder_name"],
            "bank_name": ["bank_name", "bank"],
            "account_number": ["account_number", "account_no"],
            "period": ["period", "statement_period"],
            "currency": ["currency"],
            "opening_balance": ["opening_balance"],
            "closing_balance": ["closing_balance"],
            "printed_page_number": ["printed_page_number", "page_number"],
            "printed_page_count": ["printed_page_count", "page_count", "total_pages"],
        }
        normalized: Dict[str, Any] = {}
        for field, aliases in field_aliases.items():
            value = next(
                (
                    parsed.get(alias)
                    for alias in aliases
                    if parsed.get(alias) not in (None, "")
                ),
                "",
            )
            if value in (None, ""):
                value = next(
                    (
                        legacy_summary.get(alias)
                        for alias in aliases
                        if legacy_summary.get(alias) not in (None, "")
                    ),
                    "",
                )
            normalized[field] = str(value or "").strip()

        normalized["transactions"] = []
        for row in parsed.get("transactions") or []:
            if not isinstance(row, dict):
                continue
            readable_value = row.get("readable", True)
            readable = readable_value if isinstance(readable_value, bool) else str(readable_value).lower() not in {"false", "no", "0"}
            normalized["transactions"].append({
                "date": str(row.get("date") or "").strip(),
                "description": str(row.get("description") or "").strip(),
                "reference": str(row.get("reference") or "").strip(),
                "debit": str(row.get("debit") or "").strip(),
                "credit": str(row.get("credit") or "").strip(),
                "balance": str(row.get("balance") or "").strip(),
                "readable": readable,
                "review_note": str(row.get("review_note") or row.get("note") or "").strip(),
            })
        normalized["raw_text"] = str(parsed.get("raw_text") or (content if parse_failed else "")).strip()
        review_notes = parsed.get("review_notes") or parsed.get("review") or []
        normalized["review_notes"] = [
            {
                "area": str(item.get("area") or "statement").strip(),
                "note": str(item.get("note") or "").strip(),
            }
            for item in review_notes
            if isinstance(item, dict) and str(item.get("note") or "").strip()
        ]
        normalized["summary"] = {
            "account_holder": normalized["account_holder"],
            "bank_name": normalized["bank_name"],
            "account_number": normalized["account_number"],
            "statement_period": normalized["period"],
            "currency": normalized["currency"],
            "opening_balance": normalized["opening_balance"],
            "closing_balance": normalized["closing_balance"],
        }
        normalized["review_flags"] = self._validate_bank_statement_data(normalized)
        if parse_failed:
            normalized["review_flags"].append({
                "code": "unstructured_statement_response",
                "area": "bank_statement",
                "note": "Structured bank statement response could not be parsed. Review the source document.",
            })
        return normalized

    def _merge_bank_statement_pages(self, page_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        fields = [
            "account_holder", "bank_name", "account_number", "period",
            "currency", "opening_balance", "closing_balance",
        ]
        merged: Dict[str, Any] = {field: "" for field in fields}
        transactions: List[Dict[str, Any]] = []
        raw_text_parts: List[str] = []
        review_notes: List[Dict[str, Any]] = []
        page_review_flags: List[Dict[str, str]] = []
        printed_page_numbers: List[int] = []
        printed_page_count: Optional[int] = None

        for result in page_results:
            page = result.get("page")
            for field in fields:
                if result.get(field) and not merged[field]:
                    merged[field] = result[field]
            for row in result.get("transactions") or []:
                if isinstance(row, dict):
                    transactions.append({"page": page, **row})
            raw_text = result.get("raw_text")
            if raw_text:
                raw_text_parts.append(f"Page {page}\n{raw_text}")
            for item in result.get("review_notes") or []:
                if isinstance(item, dict):
                    review_notes.append({"page": page, **item})
            for flag in result.get("review_flags") or []:
                if isinstance(flag, dict) and flag.get("code") and flag.get("note"):
                    page_review_flags.append({
                        "code": str(flag["code"]),
                        "area": str(flag.get("area") or "statement"),
                        "note": str(flag["note"]),
                    })
            detected_page = self._parse_positive_integer(result.get("printed_page_number"))
            detected_count = self._parse_positive_integer(result.get("printed_page_count"))
            if detected_page is not None:
                printed_page_numbers.append(detected_page)
            if detected_count is not None:
                printed_page_count = max(printed_page_count or 0, detected_count)

        merged["printed_page_number"] = ""
        merged["printed_page_count"] = str(printed_page_count or "")
        merged["transactions"] = transactions
        merged["raw_text"] = "\n\n".join(raw_text_parts)
        merged["review_notes"] = review_notes
        merged["summary"] = {
            "account_holder": merged["account_holder"],
            "bank_name": merged["bank_name"],
            "account_number": merged["account_number"],
            "statement_period": merged["period"],
            "currency": merged["currency"],
            "opening_balance": merged["opening_balance"],
            "closing_balance": merged["closing_balance"],
        }
        merged["review_flags"] = self._validate_bank_statement_data(
            merged,
            available_printed_pages=printed_page_numbers,
        )
        for flag in page_review_flags:
            self._append_unique_review_flag(
                merged["review_flags"],
                flag["code"],
                flag["area"],
                flag["note"],
            )
        return merged

    def _validate_bank_statement_data(
        self,
        statement_data: Dict[str, Any],
        available_printed_pages: Optional[List[int]] = None,
    ) -> List[Dict[str, str]]:
        flags: List[Dict[str, str]] = []

        for note in statement_data.get("review_notes") or []:
            if not isinstance(note, dict):
                continue
            area = str(note.get("area") or "statement").strip()
            note_text = str(note.get("note") or "").strip()
            if not note_text:
                continue
            lowered = f"{area} {note_text}".lower()
            if "unread" in lowered or "illegib" in lowered:
                code = "unreadable_row"
            elif "missing" in lowered and "page" in lowered:
                code = "missing_pages"
            elif "balance" in lowered and any(word in lowered for word in ("mismatch", "inconsisten", "does not match")):
                code = "balance_sequence_inconsistency"
            else:
                code = "statement_review_note"
            self._append_unique_review_flag(flags, code, area, note_text)

        for index, row in enumerate(statement_data.get("transactions") or [], start=1):
            if not isinstance(row, dict):
                continue
            if row.get("readable") is False:
                note = str(row.get("review_note") or "Transaction row could not be read reliably.").strip()
                self._append_unique_review_flag(flags, "unreadable_row", f"transaction_{index}", note)

        expected_pages = self._parse_positive_integer(statement_data.get("printed_page_count"))
        if expected_pages and available_printed_pages is not None:
            detected_pages = {page for page in available_printed_pages if 1 <= page <= expected_pages}
            missing_pages = [str(page) for page in range(1, expected_pages + 1) if page not in detected_pages]
            if missing_pages:
                self._append_unique_review_flag(
                    flags,
                    "missing_pages",
                    "pages",
                    f"Statement indicates missing page(s): {', '.join(missing_pages)}.",
                )

        transactions = [
            row for row in statement_data.get("transactions") or []
            if isinstance(row, dict) and row.get("readable", True) is not False
        ]
        previous_balance = self._parse_invoice_amount(statement_data.get("opening_balance"))
        for index, row in enumerate(transactions, start=1):
            balance = self._parse_invoice_amount(row.get("balance"))
            debit = self._parse_invoice_amount(row.get("debit"))
            credit = self._parse_invoice_amount(row.get("credit"))
            if previous_balance is not None and balance is not None and (debit is not None or credit is not None):
                expected_balance = previous_balance - (debit or Decimal("0")) + (credit or Decimal("0"))
                if abs(expected_balance - balance) > Decimal("0.02"):
                    self._append_unique_review_flag(
                        flags,
                        "balance_sequence_inconsistency",
                        f"transaction_{index}",
                        "Visible debit, credit, and running balance values do not follow the displayed sequence.",
                    )
            if balance is not None:
                previous_balance = balance

        closing_balance = self._parse_invoice_amount(statement_data.get("closing_balance"))
        if previous_balance is not None and closing_balance is not None and transactions:
            if abs(previous_balance - closing_balance) > Decimal("0.02"):
                self._append_unique_review_flag(
                    flags,
                    "balance_sequence_inconsistency",
                    "closing_balance",
                    "The last visible running balance does not match the visible closing balance.",
                )
        return flags

    def _append_unique_review_flag(
        self,
        flags: List[Dict[str, str]],
        code: str,
        area: str,
        note: str,
    ) -> None:
        candidate = {"code": code, "area": area, "note": note}
        if candidate not in flags:
            flags.append(candidate)

    def _parse_positive_integer(self, value: Any) -> Optional[int]:
        match = re.search(r"\d+", str(value or ""))
        if not match:
            return None
        parsed = int(match.group(0))
        return parsed if parsed > 0 else None

    def _parse_notes_json(self, content: str) -> Dict[str, Any]:
        parsed: Dict[str, Any] = {}
        parse_failed = False
        try:
            loaded = json.loads(content)
            parsed = loaded if isinstance(loaded, dict) else {}
        except json.JSONDecodeError:
            match = re.search(r"\{.*\}", content, flags=re.DOTALL)
            if match:
                try:
                    loaded = json.loads(match.group(0))
                    parsed = loaded if isinstance(loaded, dict) else {}
                except json.JSONDecodeError:
                    parse_failed = True
            else:
                parse_failed = True

        tables: List[Dict[str, Any]] = []
        for table in parsed.get("tables") or []:
            if not isinstance(table, dict):
                continue
            columns = [str(cell or "").strip() for cell in table.get("columns") or []]
            rows = []
            for row in table.get("rows") or []:
                raw_cells = row.get("cells") if isinstance(row, dict) else row
                if isinstance(raw_cells, list):
                    rows.append([str(cell or "").strip() for cell in raw_cells])
            if columns or rows:
                tables.append({
                    "title": str(table.get("title") or "").strip(),
                    "columns": columns,
                    "rows": rows,
                })
        normalized = {
            "readable_text": str(parsed.get("readable_text") or (content if parse_failed else "")).strip(),
            "tables": tables,
            "review_notes": [
                {
                    "area": str(note.get("area") or "notes").strip(),
                    "note": str(note.get("note") or "").strip(),
                }
                for note in parsed.get("review_notes") or []
                if isinstance(note, dict) and str(note.get("note") or "").strip()
            ],
        }
        normalized["review_flags"] = self._validate_notes_data(normalized)
        if parse_failed:
            normalized["review_flags"].append({
                "code": "unstructured_notes_response",
                "area": "notes",
                "note": "Structured notes response could not be parsed. Review the readable text output.",
            })
        return normalized

    def _merge_notes_pages(self, page_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        text_parts: List[str] = []
        tables: List[Dict[str, Any]] = []
        review_notes: List[Dict[str, str]] = []
        page_flags: List[Dict[str, str]] = []
        for result in page_results:
            page = result.get("page")
            if result.get("readable_text"):
                text_parts.append(f"Page {page}\n{result['readable_text']}")
            for table in result.get("tables") or []:
                if isinstance(table, dict):
                    tables.append({"page": page, **table})
            for note in result.get("review_notes") or []:
                if isinstance(note, dict):
                    review_notes.append({"area": f"page_{page}:{note.get('area', 'notes')}", "note": str(note.get("note") or "")})
            for flag in result.get("review_flags") or []:
                if isinstance(flag, dict) and flag.get("code") and flag.get("note"):
                    page_flags.append(flag)
        merged = {
            "readable_text": "\n\n".join(text_parts),
            "tables": tables,
            "review_notes": review_notes,
        }
        merged["review_flags"] = self._validate_notes_data(merged)
        for flag in page_flags:
            self._append_unique_review_flag(merged["review_flags"], str(flag["code"]), str(flag.get("area") or "notes"), str(flag["note"]))
        return merged

    def _validate_notes_data(self, notes_data: Dict[str, Any]) -> List[Dict[str, str]]:
        flags: List[Dict[str, str]] = []
        for note in notes_data.get("review_notes") or []:
            if not isinstance(note, dict):
                continue
            area = str(note.get("area") or "notes").strip()
            message = str(note.get("note") or "").strip()
            if not message:
                continue
            lowered = f"{area} {message}".lower()
            code = "unreadable_notes_content" if any(term in lowered for term in ("unread", "unclear", "illegib")) else "notes_review_note"
            self._append_unique_review_flag(flags, code, area, message)
        return flags

    def _parse_invoice_json(self, content: str) -> Dict[str, Any]:
        parsed: Dict[str, Any] = {}
        parse_failed = False
        try:
            loaded = json.loads(content)
            parsed = loaded if isinstance(loaded, dict) else {}
        except json.JSONDecodeError:
            match = re.search(r"\{.*\}", content, flags=re.DOTALL)
            if match:
                try:
                    loaded = json.loads(match.group(0))
                    parsed = loaded if isinstance(loaded, dict) else {}
                except json.JSONDecodeError:
                    parse_failed = True
            else:
                parse_failed = True

        field_aliases = {
            "vendor_name": ["vendor_name", "vendor", "supplier_name"],
            "supplier_tax_vat_id": ["supplier_tax_vat_id", "supplier_tax_id", "vat_id", "tax_id"],
            "invoice_number": ["invoice_number", "invoice_no", "invoice_id"],
            "invoice_date": ["invoice_date", "date"],
            "due_date": ["due_date"],
            "po_reference": ["po_reference", "po_number", "reference"],
            "subtotal": ["subtotal", "sub_total"],
            "tax_vat_amount": ["tax_vat_amount", "tax_amount", "vat_amount", "tax"],
            "total": ["total", "invoice_total", "grand_total"],
            "currency": ["currency"],
        }
        normalized = {
            field: next((str(parsed.get(alias) or "").strip() for alias in aliases if parsed.get(alias) not in (None, "")), "")
            for field, aliases in field_aliases.items()
        }
        normalized["line_items"] = []
        for row in parsed.get("line_items") or []:
            if not isinstance(row, dict):
                continue
            normalized["line_items"].append({
                "description": str(row.get("description") or "").strip(),
                "quantity": str(row.get("quantity") or "").strip(),
                "unit_price": str(row.get("unit_price") or "").strip(),
                "tax_rate": str(row.get("tax_rate") or row.get("tax") or "").strip(),
                "line_total": str(row.get("line_total") or row.get("total") or "").strip(),
            })
        normalized["review_flags"] = self._validate_invoice_data(normalized)
        if parse_failed:
            normalized["review_flags"].append({
                "code": "unstructured_invoice_response",
                "area": "invoice",
                "note": "Structured invoice response could not be parsed. Review the source document.",
            })
        return normalized

    def _merge_invoice_pages(self, page_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        fields = [
            "vendor_name", "supplier_tax_vat_id", "invoice_number", "invoice_date",
            "due_date", "po_reference", "subtotal", "tax_vat_amount", "total", "currency",
        ]
        merged: Dict[str, Any] = {field: "" for field in fields}
        merged["line_items"] = []
        for result in page_results:
            page = result.get("page")
            for field in fields:
                if result.get(field) and not merged[field]:
                    merged[field] = result[field]
            for row in result.get("line_items") or []:
                if isinstance(row, dict):
                    merged["line_items"].append({"page": page, **row})
        merged["review_flags"] = self._validate_invoice_data(merged)
        return merged

    def _validate_invoice_data(self, invoice_data: Dict[str, Any]) -> List[Dict[str, str]]:
        flags: List[Dict[str, str]] = []
        required_fields = [
            ("vendor_name", "missing_vendor", "Vendor name was not detected."),
            ("invoice_number", "missing_invoice_number", "Invoice number was not detected."),
            ("invoice_date", "missing_invoice_date", "Invoice date was not detected."),
            ("total", "missing_total", "Invoice total was not detected."),
        ]
        for field, code, note in required_fields:
            if not str(invoice_data.get(field) or "").strip():
                flags.append({"code": code, "area": field, "note": note})

        subtotal = self._parse_invoice_amount(invoice_data.get("subtotal"))
        tax_amount = self._parse_invoice_amount(invoice_data.get("tax_vat_amount"))
        total = self._parse_invoice_amount(invoice_data.get("total"))
        if subtotal is not None and tax_amount is not None and total is not None:
            if abs((subtotal + tax_amount) - total) > Decimal("0.02"):
                flags.append({
                    "code": "subtotal_tax_mismatch",
                    "area": "total",
                    "note": "Subtotal plus tax/VAT does not match the visible total.",
                })
        return flags

    def _parse_invoice_amount(self, value: Any) -> Optional[Decimal]:
        raw = str(value or "").strip()
        if not raw:
            return None
        sanitized = re.sub(r"[^\d,.\-]", "", raw)
        if "," in sanitized and "." in sanitized:
            if sanitized.rfind(",") > sanitized.rfind("."):
                sanitized = sanitized.replace(".", "").replace(",", ".")
            else:
                sanitized = sanitized.replace(",", "")
        elif "," in sanitized:
            last_group = sanitized.rsplit(",", 1)[-1]
            sanitized = sanitized.replace(",", ".") if len(last_group) in {1, 2} else sanitized.replace(",", "")
        try:
            return Decimal(sanitized)
        except InvalidOperation:
            return None

    def _parse_receipt_json(self, content: str) -> Dict[str, Any]:
        parsed: Dict[str, Any] = {}
        parse_failed = False
        try:
            loaded = json.loads(content)
            parsed = loaded if isinstance(loaded, dict) else {}
        except json.JSONDecodeError:
            match = re.search(r"\{.*\}", content, flags=re.DOTALL)
            if match:
                try:
                    loaded = json.loads(match.group(0))
                    parsed = loaded if isinstance(loaded, dict) else {}
                except json.JSONDecodeError:
                    parse_failed = True
            else:
                parse_failed = True

        def normalize_key(value: str) -> str:
            return re.sub(r"[^a-z0-9]", "", value.lower())

        def scalar(value: Any) -> str:
            if value in (None, "") or isinstance(value, (dict, list)):
                return ""
            return str(value).strip()

        candidates: List[Dict[str, Any]] = []

        def add_candidate(value: Any) -> None:
            if isinstance(value, dict) and value not in candidates:
                candidates.append(value)

        add_candidate(parsed)
        for key in ("receipt", "data", "result", "summary", "expense", "document", "extraction"):
            add_candidate(parsed.get(key))
        for key in ("receipts", "documents"):
            nested_list = parsed.get(key)
            if isinstance(nested_list, list):
                for item in nested_list:
                    add_candidate(item)

        field_entries: List[Dict[str, Any]] = []
        for source in candidates:
            fields = source.get("fields")
            if isinstance(fields, dict):
                field_entries.extend({"label": key, "value": value} for key, value in fields.items())
            elif isinstance(fields, list):
                for entry in fields:
                    if not isinstance(entry, dict):
                        continue
                    label = entry.get("name") or entry.get("label") or entry.get("field") or entry.get("key")
                    value = entry.get("value") or entry.get("text") or entry.get("content")
                    if label:
                        field_entries.append({"label": label, "value": value})

        def first_value(aliases: List[str]) -> str:
            alias_keys = {normalize_key(alias) for alias in aliases}
            for source in candidates:
                for key, value in source.items():
                    if normalize_key(str(key)) in alias_keys:
                        text = scalar(value)
                        if text:
                            return text
            for entry in field_entries:
                label = normalize_key(str(entry.get("label") or ""))
                if label in alias_keys or any(alias in label or label in alias for alias in alias_keys):
                    text = scalar(entry.get("value"))
                    if text:
                        return text
            return ""

        field_aliases = {
            "merchant": [
                "merchant", "merchant_name", "vendor_name", "vendor", "store_name", "store",
                "business_name", "supplier", "seller", "retailer", "restaurant", "shop",
            ],
            "date": ["date", "receipt_date", "transaction_date", "purchase_date", "transaction_datetime", "datetime"],
            "payment_method": [
                "payment_method", "payment", "tender", "tender_type", "payment_type", "paid_by",
                "card", "card_type", "card_brand", "payment_card",
            ],
            "currency": ["currency", "currency_code", "currency_symbol"],
            "subtotal": ["subtotal", "sub_total", "sub_total_amount", "net", "net_amount", "before_tax"],
            "tax_vat_amount": [
                "tax_vat_amount", "tax_amount", "vat_amount", "tax", "vat", "gst", "hst",
                "sales_tax", "service_tax", "tax_total", "taxes",
            ],
            "total": [
                "total", "grand_total", "receipt_total", "total_amount", "amount", "amount_paid",
                "amount_due", "balance", "balance_due", "paid", "grand_total_amount",
            ],
            "discount": ["discount", "discount_amount", "savings"],
            "tip": ["tip", "tip_amount", "gratuity", "service_charge"],
        }
        normalized = {field: first_value(aliases) for field, aliases in field_aliases.items()}

        raw_text = first_value(["raw_text", "ocr_text", "full_text", "receipt_text", "text", "content"])
        if not raw_text and parse_failed:
            raw_text = content.strip()
        normalized["raw_text"] = raw_text

        review_notes: List[str] = []
        for source in candidates:
            notes = source.get("review_notes") or source.get("notes") or source.get("warnings")
            if isinstance(notes, list):
                review_notes.extend(str(note).strip() for note in notes if str(note).strip())
            elif scalar(notes):
                review_notes.append(scalar(notes))
        normalized["review_notes"] = review_notes

        normalized["line_items"] = []
        line_item_keys = {normalize_key(key) for key in ("line_items", "items", "products", "purchases", "charges", "rows", "entries")}
        line_rows: List[Any] = []
        for source in candidates:
            for key, value in source.items():
                if normalize_key(str(key)) in line_item_keys and isinstance(value, list):
                    line_rows = value
                    break
            if line_rows:
                break

        def row_value(row: Dict[str, Any], aliases: List[str]) -> str:
            alias_keys = {normalize_key(alias) for alias in aliases}
            for key, value in row.items():
                if normalize_key(str(key)) in alias_keys:
                    text = scalar(value)
                    if text:
                        return text
            return ""

        for row in line_rows:
            if not isinstance(row, dict):
                continue
            normalized["line_items"].append({
                "description": row_value(row, ["description", "item", "name", "label", "product", "details", "line_description"]),
                "quantity": row_value(row, ["quantity", "qty", "count"]),
                "unit_price": row_value(row, ["unit_price", "price", "rate", "unit_cost"]),
                "tax_rate": row_value(row, ["tax_rate", "tax", "vat", "gst", "hst"]),
                "line_total": row_value(row, ["line_total", "total", "amount", "price", "total_price", "extended_price"]),
            })

        if raw_text:
            def labeled_amount(labels: List[str]) -> str:
                for line in raw_text.splitlines():
                    line_lower = line.lower()
                    if any(label in line_lower for label in labels):
                        matches = re.findall(r"[$\u20ac\u00a3]?\s*-?\d[\d,]*(?:[.,]\d{1,2})?", line)
                        if matches:
                            return matches[-1].strip()
                return ""

            normalized["subtotal"] = normalized["subtotal"] or labeled_amount(["subtotal", "sub total", "net"])
            normalized["tax_vat_amount"] = normalized["tax_vat_amount"] or labeled_amount(["tax", "vat", "gst", "hst"])
            normalized["total"] = normalized["total"] or labeled_amount(["total", "amount paid", "amount due", "balance"])
            if not normalized["date"]:
                date_match = re.search(r"\b\d{1,4}[/-]\d{1,2}[/-]\d{1,4}\b", raw_text)
                normalized["date"] = date_match.group(0) if date_match else ""
            if not normalized["currency"]:
                if "$" in raw_text:
                    normalized["currency"] = "$"
                elif "\u20ac" in raw_text:
                    normalized["currency"] = "EUR"
                elif "\u00a3" in raw_text:
                    normalized["currency"] = "GBP"
            if not normalized["merchant"]:
                excluded = ("total", "subtotal", "tax", "vat", "visa", "mastercard", "cash", "change", "date", "time", "receipt")
                for line in raw_text.splitlines()[:8]:
                    candidate = line.strip()
                    if 2 < len(candidate) < 80 and re.search(r"[A-Za-z]", candidate) and not any(term in candidate.lower() for term in excluded):
                        normalized["merchant"] = candidate
                        break

        normalized["review_flags"] = self._validate_receipt_data(normalized)
        if parse_failed:
            normalized["review_flags"].append({
                "code": "unstructured_receipt_response",
                "area": "receipt",
                "note": "Structured receipt response could not be parsed. Review the source document.",
            })
        return normalized

    def _merge_receipt_pages(self, page_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        fields = [
            "merchant", "date", "payment_method", "currency", "subtotal",
            "tax_vat_amount", "total", "discount", "tip",
        ]
        merged: Dict[str, Any] = {field: "" for field in fields}
        merged["line_items"] = []
        for result in page_results:
            page = result.get("page")
            for field in fields:
                if result.get(field) and not merged[field]:
                    merged[field] = result[field]
            for row in result.get("line_items") or []:
                if isinstance(row, dict):
                    merged["line_items"].append({"page": page, **row})
        merged["review_flags"] = self._validate_receipt_data(merged)
        return merged

    def _validate_receipt_data(self, receipt_data: Dict[str, Any]) -> List[Dict[str, str]]:
        flags: List[Dict[str, str]] = []
        required_fields = [
            ("merchant", "missing_merchant", "Merchant was not detected."),
            ("date", "missing_receipt_date", "Receipt date was not detected."),
            ("total", "missing_total", "Receipt total was not detected."),
        ]
        for field, code, note in required_fields:
            if not str(receipt_data.get(field) or "").strip():
                flags.append({"code": code, "area": field, "note": note})
        if not str(receipt_data.get("tax_vat_amount") or "").strip():
            flags.append({
                "code": "unclear_tax",
                "area": "tax_vat_amount",
                "note": "Tax/VAT is not clearly visible on the receipt.",
            })

        line_items = [row for row in receipt_data.get("line_items") or [] if isinstance(row, dict)]
        line_totals = [self._parse_invoice_amount(row.get("line_total")) for row in line_items]
        subtotal = self._parse_invoice_amount(receipt_data.get("subtotal"))
        tax_amount = self._parse_invoice_amount(receipt_data.get("tax_vat_amount"))
        total = self._parse_invoice_amount(receipt_data.get("total"))
        discount = self._parse_invoice_amount(receipt_data.get("discount")) or Decimal("0")
        tip = self._parse_invoice_amount(receipt_data.get("tip")) or Decimal("0")
        has_complete_line_totals = bool(line_items) and all(amount is not None for amount in line_totals)

        if has_complete_line_totals and total is not None and tax_amount is not None:
            line_items_subtotal = sum((amount for amount in line_totals if amount is not None), Decimal("0"))
            visible_subtotal = subtotal if subtotal is not None else line_items_subtotal
            line_items_disagree = subtotal is not None and abs(line_items_subtotal - subtotal) > Decimal("0.02")
            total_disagrees = abs((visible_subtotal + tax_amount - discount + tip) - total) > Decimal("0.02")
            if line_items_disagree or total_disagrees:
                flags.append({
                    "code": "line_items_total_mismatch",
                    "area": "total",
                    "note": "Visible receipt line items and adjustments do not match the visible total.",
                })
        return flags

    def _retry_after_seconds(self, exc: RateLimitError) -> Optional[float]:
        """Read a Retry-After header off a 429 and clamp it to a sane ceiling."""
        try:
            response = getattr(exc, "response", None)
            headers = getattr(response, "headers", None)
            if not headers:
                return None
            raw = headers.get("retry-after") or headers.get("Retry-After")
            if raw is None:
                return None
            delay = float(str(raw).strip())
        except (TypeError, ValueError):
            return None
        if delay < 0:
            return None
        return min(delay, settings.ocr_retry_after_cap_seconds)

    async def _make_api_call(
        self,
        messages: List[Dict[str, Any]],
        max_tokens: Optional[int] = None,
        response_format: Optional[Dict[str, Any]] = None,
        temperature: Optional[float] = None,
        model: Optional[str] = None,
    ) -> ChatCompletion:
        """
        Make the actual API call to OlmOCR with the single authoritative retry layer.

        Retries ONLY transient provider failures (timeouts / connection resets /
        429 / 5xx) on the same model, with jittered exponential backoff and a
        capped Retry-After honor on 429s. Non-retryable errors raise immediately so
        the caller's model-failover loop (or the page failure path) takes over.
        The OpenAI client's own transport retry is left at ocr_client_max_retries
        (0 by default) so retries do not multiply.
        """
        loop = asyncio.get_event_loop()
        request_options: Dict[str, Any] = {
            "model": model or self.model,
            "messages": messages,
            "max_tokens": max_tokens or self.max_tokens,
        }
        if response_format is not None:
            request_options["response_format"] = response_format
        if temperature is not None:
            request_options["temperature"] = temperature

        max_attempts = max(1, settings.ocr_max_attempts_per_model)
        last_exc: Optional[Exception] = None
        for attempt in range(1, max_attempts + 1):
            try:
                return await loop.run_in_executor(
                    None,
                    lambda: self.client.chat.completions.create(**request_options),
                )
            except _RETRYABLE_OCR_ERRORS as exc:
                last_exc = exc
                if attempt >= max_attempts:
                    break
                # Jittered exponential backoff; honor a capped Retry-After on 429.
                backoff_delay = min(
                    settings.ocr_retry_max_delay_seconds,
                    settings.ocr_retry_base_delay_seconds * (2 ** (attempt - 1)),
                )
                delay = backoff_delay + random.uniform(0, backoff_delay)
                if isinstance(exc, RateLimitError):
                    retry_after = self._retry_after_seconds(exc)
                    if retry_after is not None:
                        delay = max(delay, retry_after)
                logger.warning(
                    "Transient OCR error on model %s (attempt %d/%d): %s — retrying in %.2fs",
                    request_options["model"], attempt, max_attempts, exc, delay,
                )
                await asyncio.sleep(delay)
        # Exhausted retries on a transient error — surface it for model failover.
        assert last_exc is not None
        raise last_exc
    
    def _parse_html_table(self, content: str) -> str:
        """
        Parse HTML table format and convert to CSV-compatible format.

        Args:
            content: HTML content containing table tags

        Returns:
            CSV-formatted string extracted from HTML table
        """
        import re

        if not content:
            return ""

        csv_rows = []

        # Extract all table rows (both <tr> and </tr> variations)
        # This regex finds content between <tr> and </tr> tags
        tr_pattern = re.compile(r'<tr[^>]*>(.*?)</tr>', re.IGNORECASE | re.DOTALL)
        rows = tr_pattern.findall(content)

        for row in rows:
            cells = []

            # Extract header cells (<th>)
            th_pattern = re.compile(r'<th[^>]*>(.*?)</th>', re.IGNORECASE | re.DOTALL)
            headers = th_pattern.findall(row)

            # Extract data cells (<td>)
            td_pattern = re.compile(r'<td[^>]*>(.*?)</td>', re.IGNORECASE | re.DOTALL)
            data_cells = td_pattern.findall(row)

            # Combine headers and data cells
            all_cells = headers + data_cells

            # Clean each cell (remove nested HTML tags, trim whitespace)
            for cell in all_cells:
                # Remove any remaining HTML tags
                clean_cell = re.sub(r'<[^>]+>', '', cell)
                # Decode HTML entities
                clean_cell = clean_cell.replace('&nbsp;', ' ')
                clean_cell = clean_cell.replace('&amp;', '&')
                clean_cell = clean_cell.replace('&lt;', '<')
                clean_cell = clean_cell.replace('&gt;', '>')
                clean_cell = clean_cell.replace('&quot;', '"')
                # Strip whitespace
                clean_cell = clean_cell.strip()

                # Handle CSV escaping if cell contains comma or quotes
                if ',' in clean_cell or '"' in clean_cell or '\n' in clean_cell:
                    clean_cell = '"' + clean_cell.replace('"', '""') + '"'

                cells.append(clean_cell)

            # Only add non-empty rows
            if cells and any(cell.strip() for cell in cells):
                csv_rows.append(','.join(cells))

        result = '\n'.join(csv_rows)
        logger.info(f"Parsed HTML table: {len(csv_rows)} rows extracted")
        return result

    def _clean_ocr_response(self, content: str) -> str:
        """
        Clean the OCR response to remove any non-table content.

        Args:
            content: Raw response content (likely in markdown format)

        Returns:
            Cleaned table content
        """
        if not content:
            return ""

        # Remove common prefixes/suffixes that might be added by the model
        content = content.strip()

        # Remove markdown code blocks if present
        if content.startswith("```"):
            lines = content.split('\n')
            # Remove first line (```markdown or similar)
            if lines[0].strip().startswith("```"):
                lines = lines[1:]
            # Remove last line if it's ```
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            content = '\n'.join(lines)

        return content

    @staticmethod
    def _stringify_table_cell(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return str(value).strip()

    @classmethod
    def _rows_to_csv(cls, rows: List[List[Any]]) -> str:
        output = io.StringIO()
        writer = csv.writer(output)
        for row in rows:
            writer.writerow([cls._stringify_table_cell(cell) for cell in row])
        return output.getvalue().strip()

    def _extract_json_candidate(self, content: str) -> Optional[Any]:
        if not content:
            return None

        candidates = [content.strip()]
        candidates.extend(
            match.strip()
            for match in re.findall(r"```(?:json)?\s*(.*?)```", content, flags=re.IGNORECASE | re.DOTALL)
            if match.strip()
        )

        starts = [index for index in (content.find("{"), content.find("[")) if index >= 0]
        if starts:
            start = min(starts)
            for end_char in ("}", "]"):
                end = content.rfind(end_char)
                if end > start:
                    candidates.append(content[start:end + 1].strip())

        for candidate in candidates:
            try:
                return json.loads(candidate)
            except (TypeError, json.JSONDecodeError):
                continue
        return None

    def _json_table_rows(self, payload: Any) -> List[List[Any]]:
        if isinstance(payload, dict):
            tables = payload.get("tables")
            if isinstance(tables, list):
                combined: List[List[Any]] = []
                for table in tables:
                    rows = self._json_table_rows(table)
                    if rows:
                        combined.extend(rows)
                return combined

            table = payload.get("table")
            if isinstance(table, (dict, list)):
                return self._json_table_rows(table)

            columns = payload.get("columns") or payload.get("headers")
            rows = payload.get("rows") or payload.get("data")
            if isinstance(rows, list):
                normalized: List[List[Any]] = []
                column_names = [self._stringify_table_cell(column) for column in columns] if isinstance(columns, list) else []
                if column_names:
                    normalized.append(column_names)
                for row in rows:
                    if isinstance(row, dict):
                        keys = column_names or list(row.keys())
                        normalized.append([row.get(key, "") for key in keys])
                    elif isinstance(row, list):
                        normalized.append(row)
                    elif row is not None:
                        normalized.append([row])
                return normalized

            dict_values = [value for value in payload.values() if isinstance(value, list)]
            if len(dict_values) == 1:
                return self._json_table_rows(dict_values[0])

        if isinstance(payload, list):
            if not payload:
                return []
            if all(isinstance(row, dict) for row in payload):
                columns: List[str] = []
                for row in payload:
                    for key in row.keys():
                        if key not in columns:
                            columns.append(key)
                return [columns] + [[row.get(column, "") for column in columns] for row in payload]
            if all(isinstance(row, list) for row in payload):
                return payload
            return [[item] for item in payload if item is not None]

        return []

    def _extract_csv_from_json_response(self, content: str) -> str:
        payload = self._extract_json_candidate(content)
        if payload is None:
            return ""

        rows = self._json_table_rows(payload)
        rows = [row for row in rows if any(self._stringify_table_cell(cell) for cell in row)]
        if not rows:
            return ""
        return self._rows_to_csv(rows)
    
    def _extract_csv_from_response(self, content: str) -> str:
        """
        Extract table data from markdown response, handling both normal and handwritten tables.
        
        Args:
            content: Response content in markdown format
            
        Returns:
            CSV formatted content
        """
        if not content:
            return ""
        
        lines = content.split('\n')
        table_lines = []
        in_table = False
        
        for line in lines:
            stripped = line.strip()
            
            # Detect markdown table (contains pipes)
            if '|' in stripped:
                in_table = True
                # Skip separator lines (like |---|---|)
                if not all(c in '|-: ' for c in stripped):
                    table_lines.append(stripped)
            # Handle continuation of table data without pipes (common in handwritten OCR)
            elif in_table and stripped and not stripped.startswith('#'):
                # Check if this looks like table data
                if not any(indicator in stripped.lower() for indicator in ['```', '**', '##']):
                    table_lines.append(stripped)
            # End of table detection
            elif in_table and (not stripped or stripped.startswith('#')):
                in_table = False
        
        # If no markdown tables found, try to extract structured data
        if not table_lines:
            return self._extract_structured_data(content)
        
        return '\n'.join(table_lines)
    
    def _extract_structured_data(self, content: str) -> str:
        """
        Extract structured data from content when no markdown tables are found.
        Particularly useful for handwritten data that may not be formatted as tables.
        
        Args:
            content: Raw markdown content
            
        Returns:
            Structured data in CSV-like format
        """
        if not content:
            return ""
        
        lines = content.split('\n')
        structured_lines = []
        
        for line in lines:
            stripped = line.strip()
            
            # Skip empty lines and markdown headers
            if not stripped or stripped.startswith('#'):
                continue
            
            # Skip markdown formatting
            if any(indicator in stripped for indicator in ['```', '**', '---']):
                continue
            
            # Remove bullet points and lists markers
            if stripped.startswith(('- ', '* ', '+ ', '• ')):
                stripped = stripped[2:].strip()
            
            # Remove numbering (1., 2., etc.)
            import re
            stripped = re.sub(r'^\d+\.\s+', '', stripped)
            
            if stripped:
                structured_lines.append(stripped)
        
        # Try to detect if this is columnar data by looking for patterns
        if structured_lines:
            # Check if lines have consistent delimiters (spaces, tabs, etc.)
            return self._detect_and_format_columns(structured_lines)
        
        return '\n'.join(structured_lines)
    
    def _detect_and_format_columns(self, lines: List[str]) -> str:
        """
        Detect if lines contain columnar data and format appropriately.
        Enhanced to better handle handwritten data with irregular spacing.
        
        Args:
            lines: List of text lines
            
        Returns:
            CSV formatted string
        """
        if not lines:
            return ""
        
        import re
        
        # Try to detect column patterns
        formatted_lines = []
        
        # First, analyze the first line (likely headers) to detect column positions
        if lines:
            # Look for consistent spacing patterns across all lines
            has_multi_columns = any('  ' in line or '\t' in line for line in lines)
            
            if has_multi_columns:
                for line in lines:
                    # Smart column detection: preserve single spaces within data,
                    # but treat 2+ spaces as column separators
                    # This helps with names like "John Doe" vs column gaps
                    parts = re.split(r'\s{2,}|\t+', line)
                    # Clean up each part
                    parts = [part.strip() for part in parts if part.strip()]
                    formatted_lines.append(','.join(parts))
            else:
                # Single column data or tightly spaced data
                for line in lines:
                    formatted_lines.append(line)
        
        # Check if we have consistent column counts
        column_counts = []
        for line in formatted_lines:
            column_counts.append(len(line.split(',')))
        
        # If column counts vary too much, it's probably not tabular data
        if column_counts:
            most_common_count = max(set(column_counts), key=column_counts.count)
            
            # Normalize all lines to have the same number of columns
            normalized_lines = []
            for line in formatted_lines:
                parts = line.split(',')
                while len(parts) < most_common_count:
                    parts.append('')
                normalized_lines.append(','.join(parts[:most_common_count]))
            
            return '\n'.join(normalized_lines)
        
        return '\n'.join(formatted_lines)
    
    def _validate_csv_content(self, content: str) -> bool:
        """
        Validate that the content looks like CSV.
        
        Args:
            content: Content to validate
            
        Returns:
            True if content appears to be valid CSV
        """
        if not content:
            return False
        
        lines = content.strip().split('\n')
        if len(lines) < 1:  # At least one line
            return False
        
        # Check if at least some lines contain commas or look like single-column data
        has_csv_structure = False
        for line in lines:
            line = line.strip()
            if not line:
                continue
            # Either has commas (multi-column) or looks like data (single column)
            if ',' in line or (line and not any(token_indicator in line.lower() for token_indicator in [
                'token', 'tokenization', '[', ']', 'debug', 'processing'
            ])):
                has_csv_structure = True
                break
        
        return has_csv_structure
    
    def _convert_pipe_to_csv(self, content: str) -> str:
        """
        Convert pipe-separated markdown table content to CSV format.
        Enhanced to handle both normal and handwritten table structures.
        
        Args:
            content: Pipe-separated or structured content
            
        Returns:
            CSV formatted content
        """
        if not content:
            return ""
        
        lines = content.strip().split('\n')
        rows: List[List[Any]] = []
        max_columns = 0
        
        # First pass: detect if we have pipe-separated content
        has_pipes = any('|' in line for line in lines)
        
        if has_pipes:
            # Process markdown table format
            for line in lines:
                line = line.strip()
                
                # Skip separator lines
                if not line or all(c in '|-: ' for c in line):
                    continue
                
                # Process pipe-separated line
                if '|' in line:
                    # Remove leading/trailing pipes
                    if line.startswith('|'):
                        line = line[1:]
                    if line.endswith('|'):
                        line = line[:-1]
                    
                    # Split by pipes and clean each cell
                    cells = [cell.strip() for cell in line.split('|')]
                    max_columns = max(max_columns, len(cells))
                    rows.append(cells)
                else:
                    # Line without pipes in a table context - treat as single column continuation
                    if line and rows:
                        # Append to the last cell of the previous row
                        rows[-1][-1] = f"{rows[-1][-1]} {line}".strip()
        else:
            # No pipes found - process as structured data
            # This handles cases where handwritten data might not have clear delimiters
            for line in lines:
                line = line.strip()
                if line:
                    # Try to detect natural column separators (multiple spaces, tabs)
                    if '  ' in line or '\t' in line:
                        rows.append([cell.strip() for cell in re.split(r'\s{2,}|\t+', line) if cell.strip()])
                    elif ',' in line:
                        rows.append(next(csv.reader([line])))
                    else:
                        rows.append([line])
        
        # Ensure all rows have the same number of columns if we detected a table
        if max_columns > 1 and rows:
            normalized_rows = []
            for parts in rows:
                while len(parts) < max_columns:
                    parts.append('')
                normalized_rows.append(parts[:max_columns])
            return self._rows_to_csv(normalized_rows)
        
        return self._rows_to_csv(rows)
    
    async def process_batch_images(
        self, 
        images_data: List[bytes], 
        progress_callback: Optional[callable] = None
    ) -> List[Dict[str, Any]]:
        """
        Process multiple images in batch.
        
        Args:
            images_data: List of binary image data
            progress_callback: Optional callback for progress updates
            
        Returns:
            List of results with success/error status
        """
        results = []
        
        for i, image_data in enumerate(images_data):
            try:
                # Rate limiting is already handled in extract_table_from_image
                csv_content = await self.extract_table_from_image(image_data)
                results.append({
                    "index": i,
                    "success": True,
                    "data": csv_content,
                    "error": None
                })
                
                if progress_callback:
                    await progress_callback(i + 1, len(images_data))
                    
            except Exception as e:
                logger.error(f"Failed to process image {i}: {str(e)}")
                results.append({
                    "index": i,
                    "success": False,
                    "data": None,
                    "error": str(e)
                })
                
                if progress_callback:
                    await progress_callback(i + 1, len(images_data))
        
        return results
    
    async def health_check(self) -> bool:
        """
        Check if the OlmOCR API is accessible.
        
        Returns:
            True if API is accessible, False otherwise
        """
        try:
            # Create a minimal test request
            test_image = base64.b64encode(b"test").decode("utf-8")
            messages = [
                {
                    "role": "user", 
                    "content": [
                        {
                            "type": "text",
                            "text": "Test"
                        }
                    ]
                }
            ]
            
            # Try to make a request (expect it to fail, but connection should work)
            await self._make_api_call(messages)
            return True
            
        except Exception as e:
            logger.warning(f"OlmOCR health check failed: {str(e)}")
            return False


# Global OlmOCR service instance - lazily initialized
_olmocr_service = None

def get_olmocr_service() -> OlmOCRService:
    """Get or create the global OlmOCR service instance."""
    global _olmocr_service
    if _olmocr_service is None:
        _olmocr_service = OlmOCRService()
    return _olmocr_service
