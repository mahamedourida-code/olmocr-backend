"""
API endpoints for exporting Excel files to user's Google Sheets via OAuth.
"""
import logging
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Header
from pydantic import BaseModel

from app.core.dependencies import get_current_user, get_storage_service
from app.services.storage import FileStorageManager
from app.services.google_oauth_service import GoogleOAuthSheetsService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/google", tags=["Google OAuth Export"])


class GoogleOAuthExportRequest(BaseModel):
    """Request model for OAuth sheets export."""
    file_id: str
    filename: Optional[str] = None
    google_access_token: str  # User's Google OAuth access token


class GoogleOAuthExportResponse(BaseModel):
    """Response model for OAuth sheets export."""
    success: bool
    spreadsheet_url: Optional[str] = None
    spreadsheet_id: Optional[str] = None
    message: Optional[str] = None
    error: Optional[str] = None


@router.post("/oauth/export-sheets", response_model=GoogleOAuthExportResponse)
async def export_to_user_sheets(
    request: GoogleOAuthExportRequest,
    current_user: dict = Depends(get_current_user),
    storage: FileStorageManager = Depends(get_storage_service)
):
    """
    Export an Excel file to the user's Google Sheets using their OAuth token.
    
    This endpoint:
    1. Receives the user's Google OAuth access token
    2. Uses it to create sheets in THEIR Google Drive
    3. Returns the Google Sheets URL
    
    The user must have authenticated with Google and granted Sheets/Drive permissions.
    """
    try:
        # Validate Google token is provided
        if not request.google_access_token:
            raise HTTPException(
                status_code=400,
                detail="Google access token is required. Please sign in with Google."
            )
        
        # Get file path from storage
        file_path = storage.get_download_file_path(request.file_id)
        
        if not file_path.exists():
            raise HTTPException(
                status_code=404,
                detail=f"File {request.file_id} not found"
            )
        
        # Use provided filename or generate one
        filename = request.filename or f"Export_{request.file_id}"
        if not filename.endswith('.xlsx'):
            filename = filename.replace('.xlsx', '')  # Remove if exists
            # Don't add .xlsx to the Google Sheets name
        
        logger.info(f"Exporting file {request.file_id} to user's Google Sheets")
        
        # Create service with user's OAuth token
        oauth_service = GoogleOAuthSheetsService(request.google_access_token)
        
        # Convert to Google Sheets in user's Drive
        result = await oauth_service.excel_to_user_sheets(
            excel_path=file_path,
            filename=filename
        )
        
        if result["success"]:
            logger.info(f"Successfully exported to user's Google Sheets: {result.get('spreadsheet_id')}")
            return GoogleOAuthExportResponse(
                success=True,
                spreadsheet_url=result["spreadsheet_url"],
                spreadsheet_id=result["spreadsheet_id"],
                message=result.get("message", "Exported successfully!")
            )
        else:
            logger.error(f"Failed to export: {result.get('error')}")
            return GoogleOAuthExportResponse(
                success=False,
                error=result.get("error", "Unknown error occurred")
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error during OAuth sheets export: {e}")
        return GoogleOAuthExportResponse(
            success=False,
            error=f"Internal error: {str(e)}"
        )


@router.get("/oauth/config")
async def get_oauth_config():
    """
    Get Google OAuth configuration for the frontend.
    Frontend needs this to initiate OAuth flow.
    """
    import os
    
    client_id = os.getenv('GOOGLE_OAUTH_CLIENT_ID')
    
    if not client_id:
        return {
            "configured": False,
            "message": "Google OAuth not configured. Please set GOOGLE_OAUTH_CLIENT_ID in environment."
        }
    
    return {
        "configured": True,
        "client_id": client_id,
        "scopes": [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.file"
        ],
        "discovery_docs": [
            "https://sheets.googleapis.com/$discovery/rest?version=v4",
            "https://www.googleapis.com/discovery/v1/apis/drive/v3/rest"
        ]
    }
