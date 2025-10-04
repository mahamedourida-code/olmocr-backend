from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from fastapi.encoders import jsonable_encoder
from contextlib import asynccontextmanager
import asyncio
import logging
import time
from datetime import datetime

from app.core.config import get_settings
from app.api.v1 import api_router
from app.services.storage import FileStorageManager
from app.utils.exceptions import ProcessingError, ValidationError


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager.
    
    Handles startup and shutdown tasks:
    - Initialize services
    - Graceful shutdown
    """
    settings = get_settings()
    logger.info("Starting OlmOCR Backend Service")
    
    # Initialize storage service
    storage_service = FileStorageManager()
    
    # Store references in app state
    app.state.storage_service = storage_service
    
    logger.info("Application startup complete")
    
    yield
    
    # Shutdown
    logger.info("Shutting down application...")
    logger.info("Application shutdown complete")


# Create FastAPI application
app = FastAPI(
    title="OlmOCR Backend Service",
    description="""
    A FastAPI backend service that converts table screenshots and document images 
    into downloadable XLSX files with batch processing capabilities.
    
    ## Features
    
    * **Single Image Conversion**: Convert individual images to XLSX
    * **Batch Processing**: Process multiple images simultaneously
    * **Session Management**: Secure file access with automatic cleanup
    * **OCR Integration**: Advanced text extraction using OlmOCR API
    * **File Management**: Temporary storage with configurable retention
    
    ## Workflow
    
    1. Upload image(s) containing table data
    2. OCR extraction and CSV parsing
    3. XLSX generation with formatting
    4. Secure download with expiration
    
    ## Rate Limiting
    
    API endpoints are rate-limited to ensure fair usage and system stability.
    """,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# Get settings
settings = get_settings()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

# Add trusted host middleware for security
# if not settings.debug:
#     app.add_middleware(
#         TrustedHostMiddleware,
#         allowed_hosts=settings.allowed_hosts
#     )


# Custom exception handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Handle HTTP exceptions with consistent error format."""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": "HTTP_ERROR",
            "message": exc.detail,
            "status_code": exc.status_code,
            "timestamp": datetime.utcnow().isoformat(),
            "path": request.url.path
        }
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle request validation errors."""
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": "VALIDATION_ERROR",
            "message": "Request validation failed",
            "details": exc.errors(),
            "timestamp": datetime.utcnow().isoformat(),
            "path": request.url.path
        }
    )


@app.exception_handler(ProcessingError)
async def processing_exception_handler(request: Request, exc: ProcessingError):
    """Handle custom processing errors."""
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": "PROCESSING_ERROR",
            "message": str(exc),
            "timestamp": datetime.utcnow().isoformat(),
            "path": request.url.path
        }
    )


@app.exception_handler(ValidationError)
async def validation_error_handler(request: Request, exc: ValidationError):
    """Handle custom validation errors."""
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={
            "error": "VALIDATION_ERROR",
            "message": str(exc),
            "timestamp": datetime.utcnow().isoformat(),
            "path": request.url.path
        }
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle unexpected errors."""
    try:
        logger.error(f"Unexpected error: {str(exc)}", exc_info=True)
        
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "error": "INTERNAL_ERROR",
                "message": "An unexpected error occurred",
                "timestamp": datetime.utcnow().isoformat(),
                "path": str(request.url.path)
            }
        )
    except Exception as handler_exc:
        logger.error(f"Error in exception handler: {str(handler_exc)}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "error": "CRITICAL_ERROR",
                "message": "Error in error handler"
            }
        )


# Request logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log all incoming requests."""
    start_time = time.time()
    
    # Log request
    logger.info(
        f"Request: {request.method} {request.url.path} "
        f"from {request.client.host if request.client else 'unknown'}"
    )
    
    # Process request
    response = await call_next(request)
    
    # Log response
    duration = time.time() - start_time
    logger.info(
        f"Response: {response.status_code} "
        f"({duration:.3f}s)"
    )
    
    return response


# Include API routes
app.include_router(api_router, prefix="/api/v1")


# Root endpoint
@app.get("/", tags=["Root"])
async def root():
    """
    API root endpoint.
    
    Returns basic information about the service and available endpoints.
    """
    return {
        "service": "OlmOCR Backend Service",
        "version": "1.0.0",
        "status": "running",
        "timestamp": datetime.utcnow().isoformat(),
        "docs_url": "/docs",
        "health_check": "/api/v1/health",
        "endpoints": {
            "single_conversion": "/api/v1/convert/single",
            "batch_conversion": "/api/v1/jobs/batch", 
            "job_status": "/api/v1/jobs/{job_id}/status",
            "file_download": "/api/v1/download/{file_id}",
            "health_check": "/api/v1/health"
        }
    }


# Export the app for deployment
if __name__ == "__main__":
    import uvicorn
    
    # Run the application
    uvicorn.run(
        "app.main_temp:app",
        host="0.0.0.0",
        port=int(settings.port),
        reload=settings.debug,
        log_level="info" if not settings.debug else "debug"
    )