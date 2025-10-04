import base64
import asyncio
import logging
import time
import random
from typing import Optional, List, Dict, Any
from openai import OpenAI
from openai.types.chat import ChatCompletion
import backoff

from app.core.config import settings
from app.utils.exceptions import OlmOCRError

logger = logging.getLogger(__name__)


class OlmOCRService:
    """Service for interacting with the OlmOCR API via DeepInfra."""
    
    def __init__(self):
        try:
            self.client = OpenAI(
                api_key=settings.olmocr_api_key,
                base_url=settings.olmocr_base_url,
                timeout=30.0,
                max_retries=2
            )
        except Exception as e:
            logger.error(f"Failed to initialize OpenAI client: {e}")
            raise OlmOCRError(f"Failed to initialize OlmOCR service: {e}")
        
        self.model = settings.olmocr_model
        self.max_tokens = 2000
        self._last_request_time = 0.0
        self._request_count = 0
    
    async def _apply_rate_limiting(self):
        """
        Apply intelligent rate limiting with exponential backoff and jitter.
        
        This method ensures that API requests are spaced out appropriately
        to avoid overwhelming the OlmOCR API service.
        """
        current_time = time.time()
        self._request_count += 1
        
        # Calculate delay based on configuration
        if settings.olmocr_exponential_backoff:
            # Exponential backoff: base_delay * 2^(request_count % 4)
            delay = settings.olmocr_base_delay_seconds * (2 ** (self._request_count % 4))
            delay = min(delay, settings.olmocr_max_delay_seconds)
        else:
            # Fixed delay
            delay = settings.olmocr_base_delay_seconds
        
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
    
    @backoff.on_exception(
        backoff.expo,
        (Exception,),
        max_tries=3,
        max_time=30
    )
    async def extract_table_from_image(self, image_data: bytes) -> str:
        """
        Extract table data from image using OlmOCR API.
        
        Args:
            image_data: Binary image data
            
        Returns:
            Extracted table data as CSV string
            
        Raises:
            OlmOCRError: If API call fails or returns invalid response
        """
        try:
            # Apply rate limiting before making the API call
            await self._apply_rate_limiting()
            # Convert image to base64
            img_b64 = base64.b64encode(image_data).decode("utf-8")
            
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
                                "Extract the table data from this image and convert it to CSV format. "
                                "Rules:\n"
                                "1. Return ONLY clean CSV data, no explanations or extra text\n"
                                "2. Use commas to separate columns\n"
                                "3. Put each row on a new line\n"
                                "4. Include column headers as the first row\n"
                                "5. For empty cells, leave them blank or use 'none'\n"
                                "6. Do NOT include any tokenization information, model metadata, or debugging output\n"
                                "7. Start your response directly with the CSV data\n\n"
                                "Example format:\n"
                                "Header1,Header2,Header3\n"
                                "Value1,Value2,Value3\n"
                                "Value4,none,Value6"
                            )
                        }
                    ]
                }
            ]
            
            # Make the API call
            logger.info("Making OlmOCR API request")
            response: ChatCompletion = await self._make_api_call(messages)
            
            # Extract the response content
            if not response.choices or not response.choices[0].message.content:
                raise OlmOCRError("Empty response from OlmOCR API")
            
            raw_content = response.choices[0].message.content.strip()
            
            # Clean and process the response
            csv_content = self._clean_ocr_response(raw_content)
            
            # Validate that we got CSV-like content
            if not self._validate_csv_content(csv_content):
                logger.warning(f"Invalid CSV format in API response. Raw content: {raw_content[:200]}...")
                # Try to extract CSV from the response if it contains other text
                csv_content = self._extract_csv_from_response(raw_content)
                
                if not self._validate_csv_content(csv_content):
                    raise OlmOCRError(f"Could not extract valid CSV from API response: {raw_content[:100]}...")
            
            logger.info("Successfully extracted table data from image")
            return csv_content
            
        except OlmOCRError:
            raise
        except Exception as e:
            logger.error(f"OlmOCR API error: {str(e)}")
            raise OlmOCRError(f"Failed to process image: {str(e)}")
    
    async def _make_api_call(self, messages: List[Dict[str, Any]]) -> ChatCompletion:
        """
        Make the actual API call to OlmOCR.
        
        Args:
            messages: Chat completion messages
            
        Returns:
            API response
        """
        # Run the synchronous OpenAI call in a thread pool
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                max_tokens=self.max_tokens
            )
        )
        return response
    
    def _clean_ocr_response(self, content: str) -> str:
        """
        Clean the OCR response to remove any non-CSV content.
        
        Args:
            content: Raw response content
            
        Returns:
            Cleaned CSV content
        """
        if not content:
            return ""
        
        # Remove common prefixes/suffixes that might be added by the model
        content = content.strip()
        
        # Remove markdown code blocks if present
        if content.startswith("```"):
            lines = content.split('\n')
            # Remove first line (```csv or similar)
            if lines[0].strip().startswith("```"):
                lines = lines[1:]
            # Remove last line if it's ```
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            content = '\n'.join(lines)
        
        # Remove any lines that look like tokenization info or debugging
        lines = content.split('\n')
        clean_lines = []
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # Skip lines that look like tokenization or debugging info
            if any(token_indicator in line.lower() for token_indicator in [
                'token', 'tokenization', '[', ']', 'debug', 'processing',
                'model output', 'response:', 'extracted:', 'result:'
            ]):
                continue
                
            # Skip lines that don't look like CSV data
            if line.startswith('#') or line.startswith('//'):
                continue
                
            clean_lines.append(line)
        
        return '\n'.join(clean_lines)
    
    def _extract_csv_from_response(self, content: str) -> str:
        """
        Try to extract CSV data from a response that might contain other text.
        
        Args:
            content: Response content that might contain CSV
            
        Returns:
            Extracted CSV content
        """
        if not content:
            return ""
        
        lines = content.split('\n')
        csv_lines = []
        found_csv_start = False
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Look for lines that look like CSV (contain commas or are headers)
            if ',' in line or (not found_csv_start and any(char.isalpha() for char in line)):
                found_csv_start = True
                csv_lines.append(line)
            elif found_csv_start and not any(token_indicator in line.lower() for token_indicator in [
                'token', 'tokenization', '[', ']', 'debug', 'processing'
            ]):
                csv_lines.append(line)
        
        return '\n'.join(csv_lines)
    
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