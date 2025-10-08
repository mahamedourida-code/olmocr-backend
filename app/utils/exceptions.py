from fastapi import HTTPException, status


class OlmOCRError(HTTPException):
    """Base exception for OlmOCR API errors."""
    
    def __init__(self, detail: str, status_code: int = status.HTTP_502_BAD_GATEWAY):
        super().__init__(status_code=status_code, detail=detail)


class FileProcessingError(HTTPException):
    """Exception for file processing errors."""
    
    def __init__(self, detail: str, status_code: int = status.HTTP_422_UNPROCESSABLE_ENTITY):
        super().__init__(status_code=status_code, detail=detail)


class FileSizeError(HTTPException):
    """Exception for file size validation errors."""
    
    def __init__(self, detail: str = "File size exceeds maximum allowed limit"):
        super().__init__(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail=detail)


class FileFormatError(HTTPException):
    """Exception for invalid file format errors."""
    
    def __init__(self, detail: str = "Invalid file format. Supported formats: PNG, JPG, JPEG, WEBP"):
        super().__init__(status_code=status.HTTP_400_BAD_REQUEST, detail=detail)


class SessionNotFoundError(HTTPException):
    """Exception for session not found errors."""
    
    def __init__(self, detail: str = "Session not found or expired"):
        super().__init__(status_code=status.HTTP_404_NOT_FOUND, detail=detail)


class FileNotFoundError(HTTPException):
    """Exception for file not found errors."""
    
    def __init__(self, detail: str = "File not found or expired"):
        super().__init__(status_code=status.HTTP_404_NOT_FOUND, detail=detail)


class BatchProcessingError(HTTPException):
    """Exception for batch processing errors."""
    
    def __init__(self, detail: str, status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR):
        super().__init__(status_code=status_code, detail=detail)


class JobNotFoundError(HTTPException):
    """Exception for job not found errors."""
    
    def __init__(self, detail: str = "Job not found or expired"):
        super().__init__(status_code=status.HTTP_404_NOT_FOUND, detail=detail)


class ProcessingError(HTTPException):
    """Exception for general processing errors."""
    
    def __init__(self, detail: str, status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR):
        super().__init__(status_code=status_code, detail=detail)


class ValidationError(HTTPException):
    """Exception for validation errors."""
    
    def __init__(self, detail: str, status_code: int = status.HTTP_422_UNPROCESSABLE_ENTITY):
        super().__init__(status_code=status_code, detail=detail)