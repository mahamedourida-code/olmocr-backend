import base64
import imghdr
from typing import List, Optional, Tuple
from PIL import Image
import io
from app.core.config import settings
from app.utils.exceptions import FileSizeError, FileFormatError


SUPPORTED_FORMATS = {"png", "jpeg", "jpg", "webp"}
SUPPORTED_MIME_TYPES = {
    "image/png": "png",
    "image/jpeg": "jpg", 
    "image/jpg": "jpg",
    "image/webp": "webp"
}


def validate_base64_image(base64_data: str, filename: Optional[str] = None) -> Tuple[bytes, str]:
    """
    Validate base64 encoded image data.
    
    Args:
        base64_data: Base64 encoded image string
        filename: Optional filename for format validation
        
    Returns:
        Tuple of (image_bytes, image_format)
        
    Raises:
        FileFormatError: If image format is not supported
        FileSizeError: If image size exceeds limits
    """
    try:
        # Decode base64 data
        image_bytes = base64.b64decode(base64_data)
    except Exception:
        raise FileFormatError("Invalid base64 encoded image data")
    
    # Check file size
    if len(image_bytes) > settings.max_file_size_bytes:
        raise FileSizeError(
            f"File size ({len(image_bytes)} bytes) exceeds maximum allowed "
            f"size ({settings.max_file_size_bytes} bytes)"
        )
    
    # Detect image format
    image_format = imghdr.what(None, h=image_bytes)
    if not image_format or image_format not in SUPPORTED_FORMATS:
        raise FileFormatError(
            f"Unsupported image format. Supported formats: {', '.join(SUPPORTED_FORMATS).upper()}"
        )
    
    # Additional validation with PIL
    try:
        with Image.open(io.BytesIO(image_bytes)) as img:
            # Check image dimensions
            width, height = img.size
            if width < 100 or height < 100:
                raise FileFormatError("Image dimensions too small (minimum 100x100 pixels)")
            if width > 8000 or height > 8000:
                raise FileFormatError("Image dimensions too large (maximum 8000x8000 pixels)")
    except Exception as e:
        if isinstance(e, FileFormatError):
            raise
        raise FileFormatError("Invalid or corrupted image file")
    
    return image_bytes, image_format


def validate_batch_size(batch_size: int) -> None:
    """
    Validate batch size against maximum allowed.
    
    Args:
        batch_size: Number of images in batch
        
    Raises:
        FileFormatError: If batch size exceeds maximum
    """
    if batch_size > settings.max_batch_size:
        raise FileFormatError(
            f"Batch size ({batch_size}) exceeds maximum allowed "
            f"size ({settings.max_batch_size})"
        )
    
    if batch_size < 1:
        raise FileFormatError("Batch size must be at least 1")


def validate_filename(filename: str) -> str:
    """
    Validate and sanitize filename.
    
    Args:
        filename: Original filename
        
    Returns:
        Sanitized filename
    """
    if not filename:
        return "image"
    
    # Remove path components and dangerous characters
    import os
    filename = os.path.basename(filename)
    
    # Remove extension for processing
    name, ext = os.path.splitext(filename)
    
    # Sanitize name
    sanitized_name = "".join(c for c in name if c.isalnum() or c in "._-")
    if not sanitized_name:
        sanitized_name = "image"
    
    return sanitized_name


def validate_content_type(content_type: str) -> str:
    """
    Validate HTTP content type for image uploads.
    
    Args:
        content_type: HTTP content type header
        
    Returns:
        Image format string
        
    Raises:
        FileFormatError: If content type is not supported
    """
    if content_type not in SUPPORTED_MIME_TYPES:
        raise FileFormatError(
            f"Unsupported content type: {content_type}. "
            f"Supported types: {', '.join(SUPPORTED_MIME_TYPES.keys())}"
        )
    
    return SUPPORTED_MIME_TYPES[content_type]