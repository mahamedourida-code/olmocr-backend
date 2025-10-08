from typing import Optional, Dict, Any, Literal
from datetime import datetime
from pydantic import BaseModel, Field


class JobStatus(BaseModel):
    """Model for batch job status and tracking."""
    
    job_id: str = Field(..., description="Unique job identifier")
    session_id: str = Field(..., description="Session identifier")
    status: Literal["queued", "processing", "completed", "failed"] = Field(
        ..., description="Current job status"
    )
    
    # Progress tracking
    total_images: int = Field(..., description="Total number of images to process")
    processed_images: int = Field(0, description="Number of images processed")
    successful_count: int = Field(0, description="Number of successfully processed images")
    failed_count: int = Field(0, description="Number of failed processing attempts")
    current_image: Optional[str] = Field(None, description="Currently processing image")
    
    # Timing
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Job creation time")
    started_at: Optional[datetime] = Field(None, description="Job start time")
    completed_at: Optional[datetime] = Field(None, description="Job completion time")
    estimated_completion: Optional[datetime] = Field(None, description="Estimated completion time")
    
    # Results
    result_file_id: Optional[str] = Field(None, description="Generated result file ID")
    errors: list[str] = Field(default_factory=list, description="List of error messages")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional job metadata")
    
    @property
    def percentage(self) -> float:
        """Calculate completion percentage."""
        if self.total_images == 0:
            return 0.0
        return (self.processed_images / self.total_images) * 100
    
    @property
    def is_active(self) -> bool:
        """Check if job is actively processing."""
        return self.status in ["queued", "processing"]
    
    @property
    def is_completed(self) -> bool:
        """Check if job is completed (successfully or with failures)."""
        return self.status in ["completed", "failed"]


class ImageProcessingResult(BaseModel):
    """Model for individual image processing result."""
    
    image_filename: str = Field(..., description="Original image filename")
    success: bool = Field(..., description="Whether processing was successful")
    extracted_data: Optional[str] = Field(None, description="Extracted CSV data")
    error_message: Optional[str] = Field(None, description="Error message if processing failed")
    processing_time: float = Field(..., description="Processing time in seconds")
    confidence_score: Optional[float] = Field(None, description="OCR confidence score")
    
    
class BatchProcessingContext(BaseModel):
    """Model for batch processing context and configuration."""
    
    job_id: str = Field(..., description="Job identifier")
    session_id: str = Field(..., description="Session identifier")
    output_format: Literal["individual", "consolidated"] = Field(
        "individual", description="Output format for batch results"
    )
    consolidation_strategy: Optional[Literal["separate_sheets", "single_sheet"]] = Field(
        None, description="Strategy for consolidating results"
    )
    max_retries: int = Field(3, description="Maximum retry attempts per image")
    timeout_seconds: int = Field(300, description="Processing timeout per image")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Context creation time")


class FileMetadata(BaseModel):
    """Model for file metadata and tracking."""
    
    file_id: str = Field(..., description="Unique file identifier")
    session_id: str = Field(..., description="Session identifier")
    filename: str = Field(..., description="Original filename")
    content_type: str = Field(..., description="File content type")
    size_bytes: int = Field(..., description="File size in bytes")
    
    # Lifecycle
    created_at: datetime = Field(default_factory=datetime.utcnow, description="File creation time")
    last_accessed: Optional[datetime] = Field(None, description="Last access time")
    expires_at: datetime = Field(..., description="File expiration time")
    download_count: int = Field(0, description="Number of downloads")
    
    # Processing info
    source_type: Literal["upload", "result"] = Field(..., description="File source type")
    job_id: Optional[str] = Field(None, description="Associated job ID if result file")
    processing_status: Optional[Literal["pending", "processing", "completed", "failed"]] = Field(
        None, description="Processing status for uploaded files"
    )
    
    @property
    def is_expired(self) -> bool:
        """Check if file has expired."""
        return datetime.utcnow() > self.expires_at
    
    @property
    def age_hours(self) -> float:
        """Get file age in hours."""
        return (datetime.utcnow() - self.created_at).total_seconds() / 3600


class SessionMetadata(BaseModel):
    """Model for session metadata and tracking."""
    
    session_id: str = Field(..., description="Unique session identifier")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Session creation time")
    last_activity: datetime = Field(default_factory=datetime.utcnow, description="Last activity time")
    expires_at: datetime = Field(..., description="Session expiration time")
    
    # Activity counters
    uploads_count: int = Field(0, description="Number of files uploaded")
    jobs_count: int = Field(0, description="Number of batch jobs created")
    downloads_count: int = Field(0, description="Number of downloads performed")
    
    # File tracking
    uploaded_files: list[str] = Field(default_factory=list, description="List of uploaded file IDs")
    result_files: list[str] = Field(default_factory=list, description="List of result file IDs")
    active_jobs: list[str] = Field(default_factory=list, description="List of active job IDs")
    
    @property
    def is_expired(self) -> bool:
        """Check if session has expired."""
        return datetime.utcnow() > self.expires_at
    
    @property
    def is_active(self) -> bool:
        """Check if session has recent activity."""
        # Consider session active if last activity was within 1 hour
        return (datetime.utcnow() - self.last_activity).total_seconds() < 3600
    
    def update_activity(self) -> None:
        """Update last activity timestamp."""
        self.last_activity = datetime.utcnow()