"""
API endpoints for exporting Excel files to Google Sheets.
"""
import logging
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.core.dependencies import get_current_user, get_storage_service
from app.services.storage import FileStorageManager
from app.services.google_sheets_service import google_sheets_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/sheets", tags=["Google Sheets Export"])


class SheetsExportRequest(BaseModel):
    """Request model for sheets export."""
    file_id: str
    filename: Optional[str] = None


class SheetsExportResponse(BaseModel):
    """Response model for sheets export."""
    success: bool
    spreadsheet_url: Optional[str] = None
    spreadsheet_id: Optional[str] = None
    error: Optional[str] = None


@router.post("/export", response_model=SheetsExportResponse)
async def export_to_sheets(
    request: SheetsExportRequest,
    current_user: dict = Depends(get_current_user),
    storage: FileStorageManager = Depends(get_storage_service)
):
    """
    Export an Excel file to Google Sheets.
    
    Takes an existing Excel file from storage and converts it to Google Sheets,
    then returns the Google Sheets URL.
    """
    try:
        # Get file path from storage
        file_path = storage.get_download_file_path(request.file_id)
        
        if not file_path.exists():
            raise HTTPException(
                status_code=404,
                detail=f"File {request.file_id} not found"
            )
        
        # Use provided filename or generate one
        filename = request.filename or f"Export_{request.file_id}"
        
        # Get user email from current user
        user_email = current_user.get('email')
        
        logger.info(f"Exporting file {request.file_id} to Google Sheets for user {user_email}")
        
        # Convert to Google Sheets
        result = await google_sheets_service.excel_to_sheets(
            excel_path=file_path,
            filename=filename,
            user_email=user_email
        )
        
        if result["success"]:
            logger.info(f"Successfully exported to Google Sheets: {result.get('spreadsheet_id')}")
            return SheetsExportResponse(
                success=True,
                spreadsheet_url=result["spreadsheet_url"],
                spreadsheet_id=result["spreadsheet_id"]
            )
        else:
            logger.error(f"Failed to export: {result.get('error')}")
            return SheetsExportResponse(
                success=False,
                error=result.get("error", "Unknown error occurred")
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error during sheets export: {e}")
        return SheetsExportResponse(
            success=False,
            error=f"Internal error: {str(e)}"
        )


@router.get("/health")
async def sheets_health():
    """Check if Google Sheets API is configured."""
    is_configured = google_sheets_service.credentials is not None
    
    return {
        "configured": is_configured,
        "message": "Google Sheets API is configured" if is_configured else "Google Sheets API not configured"
    }
