from typing import List, Optional, Literal
from pydantic import BaseModel, Field, validator


class ImageData(BaseModel):
    """Model for individual image data in requests."""
    
    image: str = Field(..., description="Base64 encoded image data")
    filename: Optional[str] = Field(None, description="Original filename")
    
    @validator("image")
    def validate_base64(cls, v):
        """Validate that image is base64 encoded (supports both data URLs and raw base64)."""
        if not v:
            raise ValueError("Image data cannot be empty")

        import base64

        # Handle data URL format (data:image/png;base64,{base64_data})
        if v.startswith('data:'):
            try:
                # Extract base64 part after comma
                base64_part = v.split(',', 1)[1]
                decoded = base64.b64decode(base64_part, validate=True)
                if len(decoded) < 100:  # Minimum reasonable image size
                    raise ValueError("Image data appears to be too small (minimum 100 bytes)")
            except ValueError as e:
                # Re-raise our custom ValueError
                raise e
            except Exception:
                raise ValueError("Invalid data URL format or base64 encoding")
        else:
            # Handle raw base64 data (no data URL prefix)
            try:
                decoded = base64.b64decode(v, validate=True)
                if len(decoded) < 100:  # Minimum reasonable image size
                    raise ValueError("Image data appears to be too small (minimum 100 bytes)")
            except ValueError as e:
                # Re-raise our custom ValueError
                raise e
            except Exception:
                raise ValueError("Invalid base64 encoding")

        return v


class BatchOptions(BaseModel):
    """Options for batch processing."""
    
    output_type: Literal["consolidated", "separate", "concatenated"] = Field(
        "consolidated", 
        description="Output type: consolidated (multiple sheets), separate (individual files), concatenated (single sheet)"
    )
    sheet_naming: Literal["filename", "auto", "custom"] = Field(
        "auto", 
        description="Sheet naming strategy"
    )
    include_source: bool = Field(
        True, 
        description="Include source image reference in output"
    )


class BatchConvertRequest(BaseModel):
    """Request model for batch image conversion."""
    
    images: List[ImageData] = Field(..., description="List of images to process")
    output_format: Literal["xlsx"] = Field("xlsx", description="Output format (currently only XLSX)")
    consolidation_strategy: Literal["consolidated", "separate", "concatenated"] = Field(
        "consolidated", 
        description="How to consolidate results: consolidated (multiple sheets), separate (individual files), concatenated (single sheet)"
    )
    batch_options: Optional[BatchOptions] = Field(
        default_factory=BatchOptions, 
        description="Batch processing options"
    )
    
    @validator("images")
    def validate_images_list(cls, v):
        """Validate images list."""
        if not v:
            raise ValueError("Images list cannot be empty")
        
        if len(v) > 10:  # This should match settings.max_batch_size
            raise ValueError("Too many images in batch (maximum 10)")
        
        return v