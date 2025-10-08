from typing import List, Optional, Dict, Any, Literal
from datetime import datetime
from pydantic import BaseModel, Field


class ConvertResponse(BaseModel):
    """Response model for single image conversion."""
    
    success: bool = Field(..., description="Whether the conversion was successful")
    job_id: Optional[str] = Field(None, description="Unique job identifier for download (same as file_id for single conversions)")
    download_url: Optional[str] = Field(None, description="URL to download the converted file")
    expires_at: Optional[datetime] = Field(None, description="Expiration time for the download")
    processing_time: Optional[float] = Field(None, description="Processing time in seconds")
    session_id: Optional[str] = Field(None, description="Session identifier")
    error: Optional[str] = Field(None, description="Error message if conversion failed")


class JobProgress(BaseModel):
    """Model for job progress information."""
    
    total_images: int = Field(..., description="Total number of images to process")
    processed_images: int = Field(..., description="Number of images processed so far")
    current_image: Optional[str] = Field(None, description="Currently processing image filename")
    percentage: float = Field(..., description="Completion percentage")


class ProcessedFile(BaseModel):
    """Model for individual processed file information."""
    
    file_id: str = Field(..., description="Unique file identifier for download")
    download_url: str = Field(..., description="Direct download URL for this file")
    filename: str = Field(..., description="Generated filename (e.g., 'products_processed.xlsx')")
    original_image: str = Field(..., description="Original image filename that was processed")
    size_bytes: Optional[int] = Field(None, description="File size in bytes")
    created_at: datetime = Field(..., description="File creation timestamp")
    
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "file_id": "ac4d45936141403087068c6e2fc8caa1",
                    "download_url": "/api/v1/download/ac4d45936141403087068c6e2fc8caa1",
                    "filename": "products_processed.xlsx",
                    "original_image": "products.png",
                    "size_bytes": 5023,
                    "created_at": "2025-09-27T13:23:59.701370Z"
                }
            ]
        }
    }


class BatchJobResults(BaseModel):
    """Model for batch job completion results."""
    
    # Summary statistics
    total_images: int = Field(..., description="Total number of images submitted for processing")
    successful_images: int = Field(..., description="Number of images successfully processed")
    failed_images: int = Field(..., description="Number of images that failed processing")
    
    # File information
    files: List[ProcessedFile] = Field(default_factory=list, description="List of successfully generated files")
    total_files: int = Field(..., description="Total number of files generated")
    
    # Download information
    primary_download: Optional[str] = Field(None, description="Primary download URL (first file) for convenience")
    expires_at: datetime = Field(..., description="When all files expire and become unavailable")
    
    # Processing information
    processing_time_seconds: float = Field(..., description="Total time taken to process all images")
    completed_at: datetime = Field(..., description="When the job completed")
    
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "total_images": 2,
                    "successful_images": 2,
                    "failed_images": 0,
                    "files": [
                        {
                            "file_id": "ac4d45936141403087068c6e2fc8caa1",
                            "download_url": "/api/v1/download/ac4d45936141403087068c6e2fc8caa1",
                            "filename": "products_processed.xlsx",
                            "original_image": "products.png",
                            "size_bytes": 5023,
                            "created_at": "2025-09-27T13:23:59.701370Z"
                        }
                    ],
                    "total_files": 2,
                    "primary_download": "/api/v1/download/ac4d45936141403087068c6e2fc8caa1",
                    "expires_at": "2025-09-28T13:23:59.701370Z",
                    "processing_time_seconds": 4.2,
                    "completed_at": "2025-09-27T13:23:59.701370Z"
                }
            ]
        }
    }


class BatchConvertResponse(BaseModel):
    """Response model for batch image conversion."""
    
    success: bool = Field(..., description="Whether the batch job was created successfully")
    job_id: str = Field(..., description="Unique job identifier")
    estimated_completion: Optional[datetime] = Field(None, description="Estimated completion time")
    status_url: str = Field(..., description="URL to check job status")
    session_id: str = Field(..., description="Session identifier")


class JobStatusResponse(BaseModel):
    """
    Response model for job status check.
    
    This response provides complete information about a batch processing job,
    including its current status, progress, and results when completed.
    """
    
    # Job identification
    job_id: str = Field(..., description="Unique job identifier")
    status: Literal["queued", "processing", "completed", "failed", "partially_completed"] = Field(
        ..., 
        description="Current job status"
    )
    
    # Progress information (available during processing)
    progress: Optional[JobProgress] = Field(
        None, 
        description="Processing progress information (available when status is 'processing')"
    )
    
    # Results information (available when completed)
    results: Optional[BatchJobResults] = Field(
        None, 
        description="Job completion results (available when status is 'completed' or 'partially_completed')"
    )
    
    # Error information
    errors: List[str] = Field(
        default_factory=list, 
        description="List of error messages (if any failures occurred)"
    )
    
    # Timestamps
    created_at: datetime = Field(..., description="When the job was created")
    updated_at: datetime = Field(..., description="When the job was last updated")
    estimated_completion: Optional[datetime] = Field(
        None, 
        description="Estimated completion time (available for queued/processing jobs)"
    )
    
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "job_id": "17ed3aba-37c0-41d0-b6f4-239393d43d54",
                    "status": "completed",
                    "progress": {
                        "total_images": 2,
                        "processed_images": 2,
                        "current_image": "products.png",
                        "percentage": 100.0
                    },
                    "results": {
                        "total_images": 2,
                        "successful_images": 2,
                        "failed_images": 0,
                        "files": [
                            {
                                "file_id": "ac4d45936141403087068c6e2fc8caa1",
                                "download_url": "/api/v1/download/ac4d45936141403087068c6e2fc8caa1",
                                "filename": "products_processed.xlsx",
                                "original_image": "products.png",
                                "size_bytes": 5023,
                                "created_at": "2025-09-27T13:23:59.701370Z"
                            },
                            {
                                "file_id": "254fd6aa2a9f4a1c91ab6ef08604cf5c",
                                "download_url": "/api/v1/download/254fd6aa2a9f4a1c91ab6ef08604cf5c",
                                "filename": "employees_processed.xlsx",
                                "original_image": "employees.png",
                                "size_bytes": 4856,
                                "created_at": "2025-09-27T13:23:59.702370Z"
                            }
                        ],
                        "total_files": 2,
                        "primary_download": "/api/v1/download/ac4d45936141403087068c6e2fc8caa1",
                        "expires_at": "2025-09-28T13:23:59.701370Z",
                        "processing_time_seconds": 16.2,
                        "completed_at": "2025-09-27T13:23:59.702370Z"
                    },
                    "errors": [],
                    "created_at": "2025-09-27T13:23:43.686018Z",
                    "updated_at": "2025-09-27T13:23:59.702370Z",
                    "estimated_completion": None
                }
            ]
        }
    }


class HealthCheckResponse(BaseModel):
    """Response model for health check."""
    
    status: Literal["healthy", "degraded", "unhealthy"] = Field(..., description="Overall system status")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Health check timestamp")
    version: str = Field("1.0.0", description="Application version")
    uptime_seconds: float = Field(..., description="Application uptime in seconds")
    
    # Service status
    services: Dict[str, Dict[str, Any]] = Field(
        default_factory=dict, 
        description="Status of individual services"
    )
    
    # System metrics
    system: Dict[str, Any] = Field(
        default_factory=dict, 
        description="System metrics"
    )

    class Config:
        json_encoders = {
            datetime: lambda dt: dt.isoformat()
        }


class ErrorResponse(BaseModel):
    """Response model for API errors."""
    
    error: str = Field(..., description="Error type")
    message: str = Field(..., description="Error message")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional error details")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Error timestamp")
    request_id: Optional[str] = Field(None, description="Request identifier for debugging")


class FileInfoResponse(BaseModel):
    """Response model for file information."""
    
    file_id: str = Field(..., description="File identifier")
    filename: str = Field(..., description="Original filename")
    size_bytes: int = Field(..., description="File size in bytes")
    content_type: str = Field(..., description="File content type")
    created_at: datetime = Field(..., description="File creation timestamp")
    expires_at: datetime = Field(..., description="File expiration timestamp")
    download_count: int = Field(0, description="Number of times file was downloaded")


class SessionInfoResponse(BaseModel):
    """Response model for session information."""
    
    session_id: str = Field(..., description="Session identifier")
    created_at: datetime = Field(..., description="Session creation timestamp")
    expires_at: datetime = Field(..., description="Session expiration timestamp")
    uploads_count: int = Field(0, description="Number of uploaded files")
    processing_count: int = Field(0, description="Number of files being processed")
    results_count: int = Field(0, description="Number of result files")
    is_expired: bool = Field(False, description="Whether the session has expired")