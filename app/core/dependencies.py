from fastapi import Depends, HTTPException, Request, status, Cookie, WebSocket
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Optional
import time
import jwt
import logging

from app.core.config import Settings, get_settings
from app.services.storage import FileStorageManager
from app.models.jobs import SessionMetadata

logger = logging.getLogger(__name__)
security = HTTPBearer(auto_error=False)


def get_storage_service(settings: Settings = Depends(get_settings)) -> FileStorageManager:
    """Get storage service instance."""
    return FileStorageManager()


def get_session_id(
    request: Request, 
    session_id_cookie: Optional[str] = Cookie(None, alias="session_id")
) -> str:
    """Extract or create session ID from request."""
    import logging
    logger = logging.getLogger(__name__)
    
    # Try to get session ID from cookie first (most reliable)
    if session_id_cookie:
        logger.info(f"Session ID from cookie: {session_id_cookie}")
        return session_id_cookie
        
    # Try to get session ID from header
    session_id = request.headers.get("x-session-id")
    
    if not session_id:
        # Try to get from query parameter
        session_id = request.query_params.get("session_id")
    
    if not session_id:
        # Generate new session ID
        import uuid
        session_id = str(uuid.uuid4())
        logger.info(f"Generated new session ID: {session_id}")
    else:
        logger.info(f"Session ID from header/query: {session_id}")
    
    return session_id


async def get_or_create_session(
    request: Request,
    storage: FileStorageManager = Depends(get_storage_service),
    session_id_cookie: Optional[str] = Cookie(None, alias="session_id")
) -> SessionMetadata:
    """Get existing session or create new one."""
    import logging
    logger = logging.getLogger(__name__)
    
    session_id = get_session_id(request, session_id_cookie)
    logger.info(f"Getting or creating session: {session_id}")
    
    # Try to get existing session
    session = await storage.get_session_metadata(session_id)
    
    if not session:
        # Create new session
        logger.info(f"Creating new session: {session_id}")
        session = await storage.create_session(session_id)
    elif session.is_expired:
        # Session expired, creating new one
        logger.info(f"Session expired, creating new: {session_id}")
        await storage.cleanup_session(session_id)
        session = await storage.create_session(session_id)
    else:
        # Update activity
        logger.info(f"Found existing session: {session_id}, result_files: {session.result_files}")
        session.update_activity()
        await storage.update_session_metadata(session)
    
    return session


def validate_file_upload(
    file_size: int,
    content_type: str,
    settings: Settings = Depends(get_settings)
) -> None:
    """Validate uploaded file parameters."""
    # Check file size
    if file_size > settings.max_file_size:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"File size ({file_size} bytes) exceeds maximum allowed size ({settings.max_file_size} bytes)"
        )
    
    # Check content type
    allowed_types = [
        "image/jpeg", "image/jpg", "image/png", "image/gif", 
        "image/bmp", "image/tiff", "image/webp"
    ]
    
    if content_type not in allowed_types:
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail=f"Unsupported file type: {content_type}. Allowed types: {', '.join(allowed_types)}"
        )


def validate_batch_request(
    image_count: int,
    settings: Settings = Depends(get_settings)
) -> None:
    """Validate batch processing request."""
    if image_count == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No images provided for batch processing"
        )
    
    if image_count > settings.max_batch_size:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Batch size ({image_count}) exceeds maximum allowed ({settings.max_batch_size})"
        )


class RateLimiter:
    """Simple in-memory rate limiter."""
    
    def __init__(self):
        self.requests = {}
    
    def is_allowed(self, client_ip: str, max_requests: int, window_seconds: int) -> bool:
        """Check if request is allowed based on rate limit."""
        now = time.time()
        window_start = now - window_seconds
        
        # Clean old requests
        if client_ip in self.requests:
            self.requests[client_ip] = [
                req_time for req_time in self.requests[client_ip] 
                if req_time > window_start
            ]
        else:
            self.requests[client_ip] = []
        
        # Check if under limit
        if len(self.requests[client_ip]) >= max_requests:
            return False
        
        # Add current request
        self.requests[client_ip].append(now)
        return True


# Global rate limiter instance
rate_limiter = RateLimiter()


def check_rate_limit(
    request: Request,
    settings: Settings = Depends(get_settings)
) -> None:
    """Check rate limiting for the request."""
    client_ip = request.client.host if request.client else "unknown"
    
    if not rate_limiter.is_allowed(
        client_ip, 
        settings.rate_limit_requests, 
        settings.rate_limit_window
    ):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded. Please try again later."
        )


def get_client_ip(request: Request) -> str:
    """Get client IP address from request."""
    # Check for forwarded headers (when behind proxy)
    forwarded_for = request.headers.get("x-forwarded-for")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    
    real_ip = request.headers.get("x-real-ip")
    if real_ip:
        return real_ip
    
    # Fallback to direct client IP
    return request.client.host if request.client else "unknown"


async def verify_job_ownership(
    job_id: str,
    session: SessionMetadata = Depends(get_or_create_session),
    storage: FileStorageManager = Depends(get_storage_service)
) -> None:
    """Verify that the job belongs to the current session."""
    job_status = await storage.get_job_status(job_id)
    
    if not job_status:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found"
        )
    
    if job_status.session_id != session.session_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied to this job"
        )


async def verify_file_ownership(
    file_id: str,
    session: SessionMetadata = Depends(get_or_create_session),
    storage: FileStorageManager = Depends(get_storage_service)
) -> None:
    """Verify that the file belongs to the current session."""
    file_metadata = await storage.get_file_metadata(file_id)
    
    if not file_metadata:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found"
        )
    
    if file_metadata.session_id != session.session_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied to this file"
        )
    
    if file_metadata.is_expired:
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail="File has expired and is no longer available"
        )


async def get_websocket_session(
    websocket: WebSocket,
    session_id: Optional[str] = None,
    storage: FileStorageManager = Depends(get_storage_service)
) -> Optional[SessionMetadata]:
    """
    Get session metadata for WebSocket connections.
    
    Args:
        websocket: WebSocket connection
        session_id: Optional session identifier
        storage: Storage service
    
    Returns:
        SessionMetadata if session exists, None otherwise
    """
    import logging
    logger = logging.getLogger(__name__)
    
    if not session_id:
        logger.warning("WebSocket session requested without session_id")
        return None
    
    try:
        # Try to get existing session
        session = await storage.get_session_metadata(session_id)
        
        if session and not session.is_expired:
            session.update_activity()
            await storage.update_session_metadata(session)
            logger.info(f"WebSocket session found: {session_id}")
            return session
        else:
            logger.warning(f"WebSocket session not found or expired: {session_id}")
            return None
            
    except Exception as e:
        logger.error(f"Error getting WebSocket session {session_id}: {e}")
        return None


def get_websocket_client_info(websocket: WebSocket) -> dict:
    """
    Extract client information from WebSocket connection.

    Args:
        websocket: WebSocket connection

    Returns:
        Dictionary with client information
    """
    return {
        "ip_address": websocket.client.host if websocket.client else None,
        "user_agent": websocket.headers.get("user-agent"),
        "origin": websocket.headers.get("origin"),
        "host": websocket.headers.get("host")
    }


async def verify_supabase_token(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    settings: Settings = Depends(get_settings)
) -> dict:
    """
    Verify Supabase JWT token from Authorization header.

    Args:
        credentials: HTTP Bearer token
        settings: Application settings

    Returns:
        Dictionary with user information from token

    Raises:
        HTTPException: If token is invalid or missing
    """
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authorization token",
            headers={"WWW-Authenticate": "Bearer"}
        )

    try:
        token = credentials.credentials

        # Get JWT secret from settings
        jwt_secret = settings.supabase_jwt_secret

        if jwt_secret:
            # Verify signature with Supabase JWT secret (production mode)
            try:
                payload = jwt.decode(
                    token,
                    jwt_secret,
                    algorithms=["HS256"],
                    options={
                        "verify_signature": True,
                        "verify_exp": True,
                        "verify_aud": False,  # Supabase tokens don't always have aud
                        "verify_iss": False   # Skip issuer verification for compatibility
                    }
                )
                logger.info(f"JWT signature verified successfully for user: {payload.get('sub')}")
            except jwt.ExpiredSignatureError:
                logger.error("JWT token has expired")
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Token has expired. Please sign in again.",
                    headers={"WWW-Authenticate": "Bearer"}
                )
            except jwt.InvalidSignatureError:
                logger.error("JWT signature verification failed")
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid token signature. Please sign in again.",
                    headers={"WWW-Authenticate": "Bearer"}
                )
            except jwt.InvalidTokenError as e:
                logger.error(f"Invalid JWT token: {e}")
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid token format. Please sign in again.",
                    headers={"WWW-Authenticate": "Bearer"}
                )
        else:
            # Development mode: skip signature verification but still validate structure
            logger.warning("JWT_SECRET not set - skipping signature verification (development mode only)")
            try:
                payload = jwt.decode(
                    token,
                    options={"verify_signature": False, "verify_exp": True}
                )
            except jwt.ExpiredSignatureError:
                logger.error("JWT token has expired (dev mode)")
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Token has expired. Please sign in again.",
                    headers={"WWW-Authenticate": "Bearer"}
                )

        # Extract user information
        user_id = payload.get("sub")
        email = payload.get("email")

        if not user_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token: missing user ID"
            )

        logger.info(f"Verified token for user: {user_id}")

        return {
            "user_id": user_id,
            "email": email,
            "raw_payload": payload
        }

    except jwt.InvalidTokenError as e:
        logger.error(f"Invalid JWT token: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid token: {str(e)}",
            headers={"WWW-Authenticate": "Bearer"}
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Token verification error: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token verification failed",
            headers={"WWW-Authenticate": "Bearer"}
        )


async def get_current_user(
    user_info: dict = Depends(verify_supabase_token)
) -> dict:
    """
    Get current authenticated user information.

    Args:
        user_info: Verified user information from JWT token

    Returns:
        User information dictionary
    """
    return user_info


async def get_optional_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    settings: Settings = Depends(get_settings)
) -> Optional[dict]:
    """
    Get user information if token is provided, otherwise return None.
    Useful for endpoints that work with or without authentication.

    Args:
        credentials: Optional HTTP Bearer token
        settings: Application settings

    Returns:
        User information dictionary or None
    """
    if not credentials:
        return None

    try:
        token = credentials.credentials
        jwt_secret = settings.supabase_jwt_secret

        if jwt_secret:
            # Verify signature in production
            payload = jwt.decode(
                token,
                jwt_secret,
                algorithms=["HS256"],
                options={
                    "verify_signature": True,
                    "verify_exp": True,
                    "verify_aud": False
                }
            )
        else:
            # Development mode: skip verification
            payload = jwt.decode(
                token,
                options={"verify_signature": False}
            )

        user_id = payload.get("sub")
        if user_id:
            return {
                "user_id": user_id,
                "email": payload.get("email"),
                "raw_payload": payload
            }
    except:
        # Silently fail for optional auth
        pass

    return None