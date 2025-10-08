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
from app.services.websocket_service import get_websocket_manager
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
    - Start background cleanup tasks
    - Graceful shutdown
    """
    settings = get_settings()
    logger.info("Starting OlmOCR Backend Service")
    
    # Initialize storage service
    storage_service = FileStorageManager()
    
    # Initialize WebSocket manager
    websocket_manager = await get_websocket_manager()
    
    # Start WebSocket Redis pub/sub listener
    await websocket_manager.start_redis_pubsub_listener()
    
    # Start background cleanup task
    cleanup_task = asyncio.create_task(
        periodic_cleanup(storage_service, settings.cleanup_interval_hours)
    )
    
    # Start WebSocket cleanup task
    websocket_cleanup_task = asyncio.create_task(
        periodic_websocket_cleanup(websocket_manager)
    )
    
    # Store references in app state
    app.state.storage_service = storage_service
    app.state.websocket_manager = websocket_manager
    app.state.cleanup_task = cleanup_task
    app.state.websocket_cleanup_task = websocket_cleanup_task
    
    logger.info("Application startup complete")
    
    yield
    
    # Shutdown
    logger.info("Shutting down application...")
    
    # Stop WebSocket Redis pub/sub listener
    await websocket_manager.stop_redis_pubsub_listener()
    
    # Cancel background tasks
    cleanup_task.cancel()
    websocket_cleanup_task.cancel()
    
    try:
        await cleanup_task
    except asyncio.CancelledError:
        pass
    
    try:
        await websocket_cleanup_task
    except asyncio.CancelledError:
        pass
    
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

# Log CORS configuration for debugging
logger.info(f"CORS Configuration:")
logger.info(f"  - Allowed Origins: {settings.parsed_allowed_origins}")
logger.info(f"  - Allow Credentials: {settings.cors_allow_credentials}")
logger.info(f"  - Allowed Methods: {settings.parsed_cors_allow_methods}")
logger.info(f"  - Allowed Headers: {settings.parsed_cors_allow_headers}")
if settings.cors_allow_origin_regex:
    logger.info(f"  - Origin Regex: {settings.cors_allow_origin_regex}")
if settings.parsed_cors_expose_headers:
    logger.info(f"  - Expose Headers: {settings.parsed_cors_expose_headers}")
logger.info(f"  - Max Age: {settings.cors_max_age}")

# Add CORS middleware with comprehensive configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.parsed_allowed_origins,
    allow_origin_regex=settings.cors_allow_origin_regex,
    allow_credentials=settings.cors_allow_credentials,
    allow_methods=settings.parsed_cors_allow_methods,
    allow_headers=settings.parsed_cors_allow_headers,
    expose_headers=settings.parsed_cors_expose_headers,
    max_age=settings.cors_max_age,
)

# Add trusted host middleware for security
if not settings.debug:
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=settings.allowed_hosts
    )


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
    from fastapi.encoders import jsonable_encoder
    try:
        settings = get_settings()
        logger.error(f"Unexpected error: {str(exc)}", exc_info=True)
        
        error_response = {
            "error": "INTERNAL_ERROR",
            "message": "An unexpected error occurred",
            "timestamp": datetime.utcnow().isoformat(),
            "path": request.url.path
        }
        
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


async def periodic_cleanup(storage_service: FileStorageManager, interval_hours: int):
    """
    Background task for periodic cleanup of expired files and sessions.
    
    Runs continuously to maintain storage hygiene and prevent disk space issues.
    """
    while True:
        try:
            logger.info("Starting periodic cleanup...")
            
            # Clean up expired files and sessions
            cleanup_count = storage_service.cleanup_expired_files()
            
            logger.info(
                f"Cleanup complete: {cleanup_count} items cleaned up"
            )
            
        except Exception as e:
            logger.error(f"Error during periodic cleanup: {e}")
        
        # Wait for next cleanup cycle
        await asyncio.sleep(interval_hours * 3600)


async def periodic_websocket_cleanup(websocket_manager):
    """
    Background task for periodic cleanup of inactive WebSocket connections.
    
    Runs continuously to clean up disconnected clients and maintain connection health.
    """
    while True:
        try:
            logger.info("Starting periodic WebSocket cleanup...")
            
            # Clean up inactive connections
            cleaned_count = websocket_manager.cleanup_inactive_connections()
            
            if cleaned_count > 0:
                logger.info(f"WebSocket cleanup complete: {cleaned_count} connections cleaned up")
            else:
                logger.debug("WebSocket cleanup complete: no inactive connections found")
            
        except Exception as e:
            logger.error(f"Error during WebSocket cleanup: {e}")
        
        # Wait for next cleanup cycle (every 5 minutes)
        await asyncio.sleep(300)


# Export the app for deployment
if __name__ == "__main__":
    import uvicorn
    
    # Run the application
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=int(settings.port),
        reload=settings.debug,
        log_level="info" if not settings.debug else "debug"
    )