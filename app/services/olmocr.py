import base64
import asyncio
import logging
import time
import random
from typing import Optional, List, Dict, Any, Union
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
    async def extract_table_from_image(self, image_data: Union[bytes, str]) -> str:
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
            # Apply rate limiting before making the API call
            await self._apply_rate_limiting()

            # Handle both bytes and base64 string input
            if isinstance(image_data, bytes):
                # Convert binary image to base64
                img_b64 = base64.b64encode(image_data).decode("utf-8")
            else:
                # Already base64 string - use directly (optimization!)
                # Remove data URL prefix if present
                if image_data.startswith('data:'):
                    img_b64 = image_data.split(',', 1)[1]
                else:
                    img_b64 = image_data
            
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
                            "text": "Attached is one page of a document that you must process. Just return the plain text representation of this document as if you were reading it naturally. Convert equations to LateX and tables to markdown."
                                    "Return your output as markdown"
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
            logger.info(f"Received OCR response, length: {len(raw_content)}")
            logger.debug(f"Raw OCR response (first 500 chars): {raw_content[:500]}...")
            
            # Clean the response (remove code blocks, etc.)
            cleaned_content = self._clean_ocr_response(raw_content)
            
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
        csv_lines = []
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
                    
                    # Convert to CSV format
                    csv_cells = []
                    for cell in cells:
                        # Handle empty cells
                        if not cell:
                            csv_cells.append('')
                        elif ',' in cell or '"' in cell or '\n' in cell:
                            # Escape quotes and wrap in quotes
                            cell = '"' + cell.replace('"', '""') + '"'
                            csv_cells.append(cell)
                        else:
                            csv_cells.append(cell)
                    
                    csv_lines.append(','.join(csv_cells))
                else:
                    # Line without pipes in a table context - treat as single column continuation
                    if line and csv_lines:
                        # Append to the last cell of the previous row
                        csv_lines[-1] += ' ' + line
        else:
            # No pipes found - process as structured data
            # This handles cases where handwritten data might not have clear delimiters
            for line in lines:
                line = line.strip()
                if line:
                    # Try to detect natural column separators (multiple spaces, tabs)
                    import re
                    if '  ' in line or '\t' in line:
                        # Replace multiple spaces/tabs with comma
                        line = re.sub(r'\s{2,}|\t+', ',', line)
                    csv_lines.append(line)
        
        # Ensure all rows have the same number of columns if we detected a table
        if max_columns > 1 and csv_lines:
            normalized_lines = []
            for line in csv_lines:
                parts = line.split(',')
                while len(parts) < max_columns:
                    parts.append('')
                normalized_lines.append(','.join(parts[:max_columns]))
            return '\n'.join(normalized_lines)
        
        return '\n'.join(csv_lines)
    
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