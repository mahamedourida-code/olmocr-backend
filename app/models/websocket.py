"""
WebSocket-specific Pydantic models for real-time messa    error_code: Optional[str] = Field(None, description="Error code")
    retry_count: Optional[int] = Field(None, description="Current retry count")
    max_retries: Optional[int] = Field(None, description="Maximum retry attempts")


class ConnectionMessage(WebSocketMessage):ule defines models for WebSocket communication including
job status updates, progress notifications, and error messages.
"""

from typing import Optional, Dict, Any, List, Literal, Union
from datetime import datetime
from pydantic import BaseModel, Field


class WebSocketMessage(BaseModel):
    """Base WebSocket message model."""
    
    type: str = Field(..., description="Message type identifier")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Message timestamp")
    session_id: Optional[str] = Field(None, description="Session identifier")


class JobProgressUpdate(WebSocketMessage):
    """WebSocket message for job progress updates."""
    
    type: Literal["job_progress"] = "job_progress"
    job_id: str = Field(..., description="Job identifier")
    status: Literal["queued", "processing", "completed", "failed", "partially_completed"] = Field(
        ..., description="Current job status"
    )
    progress: int = Field(..., ge=0, le=100, description="Progress percentage (0-100)")
    total_images: int = Field(..., ge=0, description="Total number of images to process")
    processed_images: int = Field(..., ge=0, description="Number of images processed")
    current_image: Optional[str] = Field(None, description="Currently processing image")
    processing_time: Optional[float] = Field(None, description="Processing time in seconds")
    estimated_completion: Optional[datetime] = Field(None, description="Estimated completion time")


class ProcessedFileInfo(BaseModel):
    """Information about a processed file."""
    file_id: str = Field(..., description="File identifier")
    download_url: str = Field(..., description="Download URL for the file")
    filename: str = Field(..., description="Original filename")
    image_id: Optional[str] = Field(None, description="Associated image identifier")
    size_bytes: Optional[int] = Field(None, description="File size in bytes")


class JobCompletedMessage(WebSocketMessage):
    """WebSocket message for job completion."""

    type: Literal["job_completed"] = "job_completed"
    job_id: str = Field(..., description="Job identifier")
    status: Literal["completed", "failed", "partially_completed"] = Field(
        ..., description="Final job status"
    )
    successful_images: int = Field(..., ge=0, description="Number of successfully processed images")
    failed_images: int = Field(..., ge=0, description="Number of failed images")
    files: List[ProcessedFileInfo] = Field(default_factory=list, description="List of processed files with metadata")
    download_urls: List[str] = Field(default_factory=list, description="List of download URLs (legacy)")
    primary_download_url: Optional[str] = Field(None, description="Primary download URL")
    processing_time: float = Field(..., description="Total processing time in seconds")
    expires_at: datetime = Field(..., description="File expiration time")
    errors: List[str] = Field(default_factory=list, description="List of error messages")


class SingleFileCompletedMessage(WebSocketMessage):
    """WebSocket message for individual file completion (progressive results)."""

    type: Literal["file_ready"] = "file_ready"
    job_id: str = Field(..., description="Job identifier")
    file_info: ProcessedFileInfo = Field(..., description="Completed file information")
    image_number: int = Field(..., description="Image number in batch (1-indexed)")
    total_images: int = Field(..., description="Total images in batch")


class JobErrorMessage(WebSocketMessage):
    """WebSocket message for job errors."""

    type: Literal["job_error"] = "job_error"
    job_id: str = Field(..., description="Job identifier")
    error: str = Field(..., description="Error message")
    error_code: Optional[str] = Field(None, description="Error code")
    retry_count: Optional[int] = Field(None, description="Current retry count")
    max_retries: Optional[int] = Field(None, description="Maximum retry attempts")


class ConnectionMessage(WebSocketMessage):
    """WebSocket message for connection events."""
    
    type: Literal["connection"] = "connection"
    event: Literal["connected", "authenticated", "disconnected"] = Field(..., description="Connection event")
    message: str = Field(..., description="Human-readable message")
    client_id: Optional[str] = Field(None, description="Client identifier")


class SubscriptionMessage(WebSocketMessage):
    """WebSocket message for subscription events."""
    
    type: Literal["subscription"] = "subscription"
    event: Literal["subscribed", "unsubscribed"] = Field(..., description="Subscription event")
    topics: List[str] = Field(..., description="List of topics")
    message: str = Field(..., description="Human-readable message")


class SystemMessage(WebSocketMessage):
    """WebSocket message for system notifications."""
    
    type: Literal["system"] = "system"
    level: Literal["info", "warning", "error"] = Field(..., description="Message level")
    message: str = Field(..., description="System message")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional details")


class WebSocketSubscriptionRequest(BaseModel):
    """Request model for WebSocket subscriptions."""
    
    action: Literal["subscribe", "unsubscribe"] = Field(..., description="Subscription action")
    topics: List[str] = Field(..., description="List of topics to subscribe/unsubscribe to")
    job_ids: Optional[List[str]] = Field(None, description="Specific job IDs to monitor")


class WebSocketAuthRequest(BaseModel):
    """Request model for WebSocket authentication."""
    
    action: Literal["authenticate"] = "authenticate"
    session_id: Optional[str] = Field(None, description="Session identifier")
    token: Optional[str] = Field(None, description="Authentication token")


# Union type for all WebSocket messages
WebSocketMessageType = Union[
    JobProgressUpdate,
    SingleFileCompletedMessage,
    JobCompletedMessage,
    JobErrorMessage,
    ConnectionMessage,
    SubscriptionMessage,
    SystemMessage
]


class WebSocketTopics:
    """Constants for WebSocket pub/sub topics."""

    # Job-specific topics
    JOB_PROGRESS = "job_progress"
    JOB_COMPLETED = "job_completed"
    JOB_ERROR = "job_error"
    SINGLE_IMAGE_COMPLETED = "single_image_completed"

    # Session-specific topics
    SESSION_PREFIX = "session"  # session:{session_id}

    # System topics
    SYSTEM_NOTIFICATIONS = "system_notifications"
    
    @staticmethod
    def job_topic(job_id: str) -> str:
        """Generate job-specific topic."""
        return f"job:{job_id}"
    
    @staticmethod
    def session_topic(session_id: str) -> str:
        """Generate session-specific topic."""
        return f"session:{session_id}"
    
    @staticmethod
    def job_status_topic(job_id: str) -> str:
        """Generate job status topic."""
        return f"job_status:{job_id}"


class WebSocketClientInfo(BaseModel):
    """Information about connected WebSocket clients."""
    
    client_id: str = Field(..., description="Unique client identifier")
    session_id: Optional[str] = Field(None, description="Associated session ID")
    connected_at: datetime = Field(default_factory=datetime.utcnow, description="Connection timestamp")
    last_activity: datetime = Field(default_factory=datetime.utcnow, description="Last activity timestamp")
    subscriptions: List[str] = Field(default_factory=list, description="List of subscribed topics")
    user_agent: Optional[str] = Field(None, description="Client user agent")
    ip_address: Optional[str] = Field(None, description="Client IP address")
    
    def update_activity(self) -> None:
        """Update last activity timestamp."""
        self.last_activity = datetime.utcnow()
    
    def add_subscription(self, topic: str) -> None:
        """Add a subscription topic."""
        if topic not in self.subscriptions:
            self.subscriptions.append(topic)
    
    def remove_subscription(self, topic: str) -> None:
        """Remove a subscription topic."""
        if topic in self.subscriptions:
            self.subscriptions.remove(topic)
