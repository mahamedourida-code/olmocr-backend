"""
Session management endpoints for batch file sharing.
"""

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from pydantic import BaseModel, Field
import logging
import random
import string
import zipfile
import io
import json

from app.core.dependencies import get_current_user, get_optional_user
from app.services.supabase_service import get_supabase_service, SupabaseService
from app.services.storage import FileStorageManager, get_storage_service
from app.core.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

router = APIRouter(prefix="/sessions", tags=["sessions"])


class SessionCreateRequest(BaseModel):
    """Request model for creating a share session."""
    file_ids: List[str] = Field(..., description="List of file IDs to include in session")
    title: Optional[str] = Field(None, description="Optional title for the session")
    description: Optional[str] = Field(None, description="Optional description")
    expires_in_days: int = Field(7, ge=1, le=30, description="Number of days until expiration")


class SessionCreateResponse(BaseModel):
    """Response model for session creation."""
    session_id: str = Field(..., description="Unique session identifier")
    share_url: str = Field(..., description="Full shareable URL")
    expires_at: Optional[datetime] = Field(None, description="Expiration timestamp")


class SessionFile(BaseModel):
    """Model for file information in a session."""
    file_id: str
    filename: str
    size_bytes: Optional[int] = None
    created_at: Optional[datetime] = None


class SessionDetailsResponse(BaseModel):
    """Response model for session details."""
    session_id: str
    title: Optional[str]
    description: Optional[str]
    files: List[SessionFile]
    created_at: datetime
    expires_at: Optional[datetime]
    access_count: int


def generate_session_id(length: int = 10) -> str:
    """Generate a random session ID."""
    # Use uppercase letters and digits for readability
    chars = string.ascii_uppercase + string.digits
    # Avoid confusing characters
    chars = chars.replace('O', '').replace('0', '').replace('I', '').replace('1', '')
    return ''.join(random.choice(chars) for _ in range(length))


@router.post("/create", response_model=SessionCreateResponse)
async def create_share_session(
    request: SessionCreateRequest,
    user = Depends(get_optional_user),
    supabase: SupabaseService = Depends(get_supabase_service)
) -> SessionCreateResponse:
    """
    Create a shareable session for batch files.
    
    This endpoint creates a unique session ID that can be shared to provide
    access to multiple files at once.
    """
    logger.info(f"Creating share session for user: {user.get('id') if user else 'anonymous'}")
    logger.info(f"Request data: file_ids={request.file_ids}, title={request.title}")
    
    try:
        # Generate unique session ID
        max_attempts = 10
        session_id = None
        
        for _ in range(max_attempts):
            candidate_id = generate_session_id()
            # Check if ID already exists
            existing = await supabase.get_share_session(candidate_id)
            if not existing:
                session_id = candidate_id
                break
        
        if not session_id:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to generate unique session ID"
            )
        
        # Calculate expiration
        expires_at = None
        if request.expires_in_days:
            expires_at = datetime.utcnow() + timedelta(days=request.expires_in_days)
        
        # Create session in database
        # Fix: Only pass user_id if user exists and has an id
        user_id = None
        if user and user.get('id'):
            user_id = str(user.get('id'))
        
        session_data = await supabase.create_share_session(
            session_id=session_id,
            user_id=user_id,
            file_ids=request.file_ids,
            title=request.title or f"Batch of {len(request.file_ids)} files",
            description=request.description,
            expires_at=expires_at
        )
        
        # Build share URL
        frontend_url = settings.allowed_origins[0] if isinstance(settings.allowed_origins, list) else "https://exceletto.vercel.app"
        share_url = f"{frontend_url}/share/{session_id}"
        
        logger.info(f"Created share session {session_id} with {len(request.file_ids)} files")
        
        return SessionCreateResponse(
            session_id=session_id,
            share_url=share_url,
            expires_at=expires_at
        )
        
    except Exception as e:
        logger.error(f"Failed to create share session: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create share session: {str(e)}"
        )


@router.get("/{session_id}", response_model=SessionDetailsResponse)
async def get_session_details(
    session_id: str,
    supabase: SupabaseService = Depends(get_supabase_service),
    storage: FileStorageManager = Depends(get_storage_service)
) -> SessionDetailsResponse:
    """
    Get session details and file information.
    
    This endpoint retrieves information about a share session including
    all associated files. No authentication required for active sessions.
    """
    try:
        # Fetch session
        session = await supabase.get_share_session(session_id)
        
        if not session:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Session not found"
            )
        
        # Check if session is active and not expired
        if not session.get('is_active'):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Session is no longer active"
            )
        
        expires_at = session.get('expires_at')
        if expires_at:
            expiry_time = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
            if datetime.utcnow() > expiry_time:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Session has expired"
                )
        
        # Increment access count
        await supabase.increment_session_access(session_id)
        
        # Get file details
        file_ids = session.get('file_ids', [])
        files = []
        
        for file_id in file_ids:
            # Try to get file info from storage
            file_info = await storage.get_file_info(file_id)
            if file_info:
                files.append(SessionFile(
                    file_id=file_id,
                    filename=file_info.get('filename', f"{file_id}.xlsx"),
                    size_bytes=file_info.get('size_bytes'),
                    created_at=file_info.get('created_at')
                ))
            else:
                # Fallback if file info not found
                files.append(SessionFile(
                    file_id=file_id,
                    filename=f"{file_id}.xlsx"
                ))
        
        return SessionDetailsResponse(
            session_id=session_id,
            title=session.get('title'),
            description=session.get('description'),
            files=files,
            created_at=datetime.fromisoformat(session['created_at'].replace('Z', '+00:00')),
            expires_at=datetime.fromisoformat(expires_at.replace('Z', '+00:00')) if expires_at else None,
            access_count=session.get('access_count', 0)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get session details: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve session details: {str(e)}"
        )


@router.get("/{session_id}/download-all")
async def download_all_session_files(
    session_id: str,
    supabase: SupabaseService = Depends(get_supabase_service),
    storage: FileStorageManager = Depends(get_storage_service)
):
    """
    Download all files in a session as a ZIP archive.
    
    This endpoint creates a ZIP file containing all files associated
    with the share session and returns it as a download.
    """
    try:
        # Fetch session
        session = await supabase.get_share_session(session_id)
        
        if not session or not session.get('is_active'):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Session not found or inactive"
            )
        
        # Check expiration
        expires_at = session.get('expires_at')
        if expires_at:
            expiry_time = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
            if datetime.utcnow() > expiry_time:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Session has expired"
                )
        
        file_ids = session.get('file_ids', [])
        
        if not file_ids:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No files found in session"
            )
        
        # Create ZIP in memory
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for i, file_id in enumerate(file_ids):
                try:
                    # Download file from storage
                    file_data = await storage.download_file(file_id)
                    
                    if file_data:
                        # Get filename or use default
                        file_info = await storage.get_file_info(file_id)
                        filename = f"{file_id}.xlsx"
                        if file_info and 'filename' in file_info:
                            filename = file_info['filename']
                        elif file_info and 'original_filename' in file_info:
                            filename = file_info['original_filename']
                        
                        # Ensure unique filenames in ZIP
                        if i > 0:
                            name_parts = filename.rsplit('.', 1)
                            if len(name_parts) == 2:
                                filename = f"{name_parts[0]}_{i+1}.{name_parts[1]}"
                            else:
                                filename = f"{filename}_{i+1}"
                        
                        # Add file to ZIP
                        zip_file.writestr(filename, file_data)
                        logger.info(f"Added {filename} to ZIP")
                        
                except Exception as e:
                    logger.error(f"Failed to add file {file_id} to ZIP: {str(e)}")
                    # Continue with other files even if one fails
        
        # Reset buffer position
        zip_buffer.seek(0)
        
        # Increment access count
        await supabase.increment_session_access(session_id)
        
        # Return ZIP as download
        return StreamingResponse(
            zip_buffer,
            media_type="application/zip",
            headers={
                "Content-Disposition": f"attachment; filename=batch_{session_id}.zip",
                "Content-Length": str(len(zip_buffer.getvalue()))
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create ZIP download: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create ZIP download: {str(e)}"
        )


@router.delete("/{session_id}")
async def deactivate_session(
    session_id: str,
    user = Depends(get_current_user),
    supabase: SupabaseService = Depends(get_supabase_service)
):
    """
    Deactivate a share session.
    
    This endpoint allows users to deactivate their own share sessions,
    making them inaccessible via the share link.
    """
    try:
        # Fetch session to verify ownership
        session = await supabase.get_share_session(session_id)
        
        if not session:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Session not found"
            )
        
        # Verify ownership
        if str(session.get('user_id')) != str(user.get('id')):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You don't have permission to deactivate this session"
            )
        
        # Deactivate session
        await supabase.deactivate_share_session(session_id)
        
        logger.info(f"Deactivated share session {session_id}")
        
        return {"message": "Session deactivated successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to deactivate session: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to deactivate session: {str(e)}"
        )
