"""
Session management endpoints for batch file sharing.
"""

from fastapi import APIRouter, Depends, HTTPException, status, Request
from fastapi.responses import StreamingResponse
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta, timezone
from pydantic import BaseModel, Field
from urllib.parse import quote
import logging
import random
import string
import zipfile
import io
import json

from app.core.dependencies import (
    get_current_user,
    get_optional_user,
    get_or_create_session,
    verify_file_metadata_access,
)
from app.services.supabase_service import get_supabase_service, SupabaseService
from app.models.jobs import SessionMetadata
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
    download_url: Optional[str] = None
    office_viewer_url: Optional[str] = None


class SessionDetailsResponse(BaseModel):
    """Response model for session details."""
    session_id: str
    title: Optional[str]
    description: Optional[str]
    files: List[SessionFile]
    created_at: datetime
    expires_at: Optional[datetime]
    access_count: int
    download_all_url: Optional[str] = None


def generate_session_id(length: int = 10) -> str:
    """Generate a random session ID."""
    # Use uppercase letters and digits for readability
    chars = string.ascii_uppercase + string.digits
    # Avoid confusing characters
    chars = chars.replace('O', '').replace('0', '').replace('I', '').replace('1', '')
    return ''.join(random.choice(chars) for _ in range(length))


def _parse_optional_datetime(value: Optional[Any]) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    except ValueError:
        return None


def _get_share_files_from_session(session_data: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
    metadata = session_data.get('metadata') or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except json.JSONDecodeError:
            metadata = {}

    files = metadata.get("files") if isinstance(metadata, dict) else None
    return files if isinstance(files, list) else None


def _ensure_share_session_available(session_data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not session_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Session not found"
        )

    if not session_data.get('is_active'):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Session is no longer active"
        )

    expires_at = session_data.get('expires_at')
    if expires_at:
        expiry_time = datetime.fromisoformat(str(expires_at).replace('Z', '+00:00'))
        current_time = datetime.now(timezone.utc)
        if current_time > expiry_time:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Session has expired"
            )

    return session_data


async def _get_share_session_files(
    session_data: Dict[str, Any],
    supabase: SupabaseService
) -> List[Dict[str, Any]]:
    file_ids = session_data.get('file_ids', [])
    durable_files = _get_share_files_from_session(session_data)
    if not durable_files:
        durable_files = await supabase.get_job_files_by_ids(file_ids)
    return durable_files or []


def _share_file_name(file_info: Dict[str, Any]) -> str:
    file_id = file_info.get('file_id') or 'download'
    return (
        file_info.get('filename')
        or file_info.get('original_filename')
        or f"{file_id}.xlsx"
    )


def _share_file_response(
    file_info: Dict[str, Any],
    session_id: str,
    request: Request
) -> SessionFile:
    file_id = file_info.get('file_id')
    filename = _share_file_name(file_info)
    download_url = str(request.url_for(
        "download_session_file",
        session_id=session_id,
        file_id=file_id
    ))
    office_viewer_url = (
        "https://view.officeapps.live.com/op/view.aspx"
        f"?src={quote(download_url, safe='')}"
    )

    return SessionFile(
        file_id=file_id,
        filename=filename,
        size_bytes=file_info.get('size_bytes'),
        created_at=_parse_optional_datetime(file_info.get('created_at')),
        download_url=download_url,
        office_viewer_url=office_viewer_url
    )


@router.post("/create", response_model=SessionCreateResponse)
async def create_share_session(
    request: SessionCreateRequest,
    user = Depends(get_optional_user),
    session: SessionMetadata = Depends(get_or_create_session),
    supabase: SupabaseService = Depends(get_supabase_service)
) -> SessionCreateResponse:
    """
    Create a shareable session for batch files.
    
    This endpoint creates a unique session ID that can be shared to provide
    access to multiple files at once.
    """
    logger.info(f"Creating share session for user: {user.get('user_id') if user else 'anonymous'}")
    logger.info(f"Request data: file_ids={request.file_ids}, title={request.title}")
    
    try:
        durable_files = await supabase.get_job_files_by_ids(request.file_ids)
        found_file_ids = {file_info.get("file_id") for file_info in durable_files}
        missing_file_ids = [file_id for file_id in request.file_ids if file_id not in found_file_ids]
        if missing_file_ids:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="One or more files are unavailable for sharing"
            )

        for file_info in durable_files:
            verify_file_metadata_access(file_info, user, session.session_id)

        file_snapshot = [
            {
                "file_id": file_info.get("file_id"),
                "job_id": file_info.get("job_id"),
                "filename": file_info.get("filename"),
                "original_filename": file_info.get("original_filename"),
                "storage_path": file_info.get("storage_path"),
                "size_bytes": file_info.get("size_bytes"),
                "created_at": file_info.get("created_at"),
                "content_type": file_info.get("content_type"),
                "status": file_info.get("status"),
                "expires_at": file_info.get("expires_at"),
            }
            for file_info in durable_files
        ]

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
            # Use timezone-aware datetime for consistency
            expires_at = datetime.now(timezone.utc) + timedelta(days=request.expires_in_days)
        
        # Create session in database
        user_id = None
        if user and user.get('user_id'):
            user_id = str(user.get('user_id'))
        
        session_data = await supabase.create_share_session(
            session_id=session_id,
            user_id=user_id,
            file_ids=request.file_ids,
            title=request.title or f"Batch of {len(request.file_ids)} files",
            description=request.description,
            expires_at=expires_at,
            metadata={"files": file_snapshot}
        )
        
        frontend_url = settings.frontend_url.rstrip("/")
        share_url = f"{frontend_url}/share/{session_id}"
        
        logger.info(f"Created share session {session_id} with {len(request.file_ids)} files")
        
        return SessionCreateResponse(
            session_id=session_id,
            share_url=share_url,
            expires_at=expires_at
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create share session: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create share session: {str(e)}"
        )


@router.get("/{session_id}", response_model=SessionDetailsResponse)
async def get_session_details(
    request: Request,
    session_id: str,
    supabase: SupabaseService = Depends(get_supabase_service)
) -> SessionDetailsResponse:
    """
    Get session details and file information.
    
    This endpoint retrieves information about a share session including
    all associated files. No authentication required for active sessions.
    """
    try:
        session = await supabase.get_share_session(session_id)
        session = _ensure_share_session_available(session)
        expires_at = session.get('expires_at')

        await supabase.increment_session_access(session_id)

        durable_files = await _get_share_session_files(session, supabase)

        files = [
            _share_file_response(file_info, session_id, request)
            for file_info in durable_files
            if file_info.get('file_id')
        ]
        
        return SessionDetailsResponse(
            session_id=session_id,
            title=session.get('title'),
            description=session.get('description'),
            files=files,
            created_at=datetime.fromisoformat(session['created_at'].replace('Z', '+00:00')),
            expires_at=datetime.fromisoformat(expires_at.replace('Z', '+00:00')) if expires_at else None,
            access_count=session.get('access_count', 0),
            download_all_url=str(request.url_for(
                "download_all_session_files",
                session_id=session_id
            ))
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get session details: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve session details: {str(e)}"
        )


@router.get("/{session_id}/download/{file_id}", name="download_session_file")
async def download_session_file(
    session_id: str,
    file_id: str,
    supabase: SupabaseService = Depends(get_supabase_service)
):
    """
    Download one file through an active share session.

    Single-file downloads use the same durable share metadata as ZIP downloads,
    so public share links never need to expose legacy local file paths.
    """
    try:
        session = await supabase.get_share_session(session_id)
        session = _ensure_share_session_available(session)

        durable_files = await _get_share_session_files(session, supabase)
        file_info = next(
            (
                candidate
                for candidate in durable_files
                if candidate.get("file_id") == file_id
            ),
            None
        )

        if not file_info or not file_info.get("storage_path"):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="File not found in this share session"
            )

        file_data = await supabase.download_file_from_storage(file_info["storage_path"])
        if not file_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="File is no longer available"
            )

        await supabase.increment_session_access(session_id)

        filename = _share_file_name(file_info)
        media_type = file_info.get("content_type") or "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

        return StreamingResponse(
            io.BytesIO(file_data),
            media_type=media_type,
            headers={
                "Content-Disposition": f"attachment; filename*=UTF-8''{quote(filename, safe='')}",
                "Content-Length": str(len(file_data))
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to download session file: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to download file: {str(e)}"
        )


@router.get("/{session_id}/download-all", name="download_all_session_files")
async def download_all_session_files(
    session_id: str,
    supabase: SupabaseService = Depends(get_supabase_service)
):
    """
    Download all files in a session as a ZIP archive.
    
    This endpoint creates a ZIP file containing all files associated
    with the share session and returns it as a download.
    """
    try:
        session = await supabase.get_share_session(session_id)
        session = _ensure_share_session_available(session)
        
        file_ids = session.get('file_ids', [])
        
        if not file_ids:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No files found in session"
            )

        durable_files = await _get_share_session_files(session, supabase)

        valid_files = [
            file_info
            for file_info in durable_files
            if file_info.get("file_id") and file_info.get("storage_path")
        ]

        if not valid_files:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No downloadable files found in session"
            )
        
        # Create ZIP in memory
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            added_files = 0
            for i, file_info in enumerate(valid_files):
                try:
                    file_id = file_info.get("file_id")
                    storage_path = file_info.get("storage_path")

                    file_data = await supabase.download_file_from_storage(storage_path)
                    
                    if file_data:
                        filename = f"{file_id}.xlsx"
                        if file_info and file_info.get('filename'):
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
                        added_files += 1
                        logger.info(f"Added {filename} to ZIP")
                        
                except Exception as e:
                    logger.error(f"Failed to add file {file_info.get('file_id')} to ZIP: {str(e)}")
                    # Continue with other files even if one fails

            if added_files == 0:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="No downloadable files could be added to the archive"
                )
        
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
        if str(session.get('user_id')) != str(user.get('user_id')):
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
