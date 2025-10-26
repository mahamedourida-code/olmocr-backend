"""
Google Sheets export service for converting Excel files to Google Sheets.
"""
import os
import logging
import json
import base64
from pathlib import Path
from typing import Optional, Dict, Any
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)


class GoogleSheetsService:
    """Service for exporting Excel files to Google Sheets."""
    
    def __init__(self):
        self.credentials = self._load_credentials()
        if self.credentials:
            self.sheets_service = build('sheets', 'v4', credentials=self.credentials)
            self.drive_service = build('drive', 'v3', credentials=self.credentials)
        else:
            self.sheets_service = None
            self.drive_service = None
    
    def _load_credentials(self):
        """Load Google service account credentials from environment."""
        try:
            # Try base64 encoded credentials first (more reliable for complex JSON)
            creds_base64 = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON_BASE64')
            if creds_base64:
                creds_json = base64.b64decode(creds_base64).decode('utf-8')
                creds_dict = json.loads(creds_json)
                return service_account.Credentials.from_service_account_info(
                    creds_dict,
                    scopes=[
                        'https://www.googleapis.com/auth/spreadsheets',
                        'https://www.googleapis.com/auth/drive'
                    ]
                )
            
            # Fallback to direct JSON string
            creds_json = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON')
            if creds_json:
                creds_dict = json.loads(creds_json)
                return service_account.Credentials.from_service_account_info(
                    creds_dict,
                    scopes=[
                        'https://www.googleapis.com/auth/spreadsheets',
                        'https://www.googleapis.com/auth/drive'
                    ]
                )
            
            logger.warning("No Google credentials found in environment")
            return None
            
        except Exception as e:
            logger.error(f"Failed to load Google credentials: {e}")
            return None
    
    async def excel_to_sheets(self, excel_path: Path, filename: str, user_email: Optional[str] = None) -> Dict[str, Any]:
        """
        Convert an Excel file to Google Sheets.
        
        Args:
            excel_path: Path to the Excel file
            filename: Name for the Google Sheets file
            user_email: Email to share the sheet with (optional)
            
        Returns:
            Dict with success status and sheet URL or error message
        """
        if not self.credentials:
            return {
                "success": False,
                "error": "Google Sheets API not configured. Please set up credentials."
            }
        
        try:
            # Read Excel file
            excel_file = pd.ExcelFile(excel_path)
            sheet_names = excel_file.sheet_names
            
            # Create Google Sheets using Sheets API directly
            spreadsheet_body = {
                'properties': {
                    'title': filename
                },
                'sheets': [{
                    'properties': {
                        'title': sheet_names[0] if sheet_names else 'Sheet1'
                    }
                }]
            }
            
            spreadsheet = self.sheets_service.spreadsheets().create(
                body=spreadsheet_body,
                fields='spreadsheetId'
            ).execute()
            
            spreadsheet_id = spreadsheet.get('spreadsheetId')
            logger.info(f"Created Google Sheets: {spreadsheet_id}")
            
            # If multiple sheets, add the remaining sheets
            if len(sheet_names) > 1:
                batch_requests = []
                
                # Add remaining sheets (first sheet already created)
                for sheet_name in sheet_names[1:]:
                    batch_requests.append({
                        'addSheet': {
                            'properties': {
                                'title': sheet_name
                            }
                        }
                    })
                
                # Execute batch update
                self.sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={'requests': batch_requests}
                ).execute()
            
            # Populate each sheet with data
            for sheet_name in sheet_names:
                df = pd.read_excel(excel_path, sheet_name=sheet_name)
                
                # Convert DataFrame to list of lists
                values = [df.columns.tolist()] + df.values.tolist()
                
                # Handle NaN and convert to strings
                for i, row in enumerate(values):
                    values[i] = [str(cell) if pd.notna(cell) else '' for cell in row]
                
                # Update sheet with data
                self.sheets_service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=f"'{sheet_name}'!A1",
                    valueInputOption='RAW',
                    body={'values': values}
                ).execute()
            
            # Share with user - REQUIRED for access
            if user_email:
                try:
                    # Create permission for user with writer access
                    permission = {
                        'type': 'user',
                        'role': 'writer',
                        'emailAddress': user_email
                    }
                    
                    self.drive_service.permissions().create(
                        fileId=spreadsheet_id,
                        body=permission,
                        sendNotificationEmail=True,
                        fields='id'
                    ).execute()
                    
                    logger.info(f"Successfully shared sheet with {user_email}")
                    
                    # Also make the file discoverable by anyone with the link (optional)
                    # This ensures users can always access it even if email fails
                    link_permission = {
                        'type': 'anyone',
                        'role': 'reader'
                    }
                    
                    self.drive_service.permissions().create(
                        fileId=spreadsheet_id,
                        body=link_permission
                    ).execute()
                    
                    logger.info(f"Made sheet accessible via link")
                    
                except HttpError as e:
                    error_content = e.content.decode('utf-8') if hasattr(e, 'content') else str(e)
                    logger.error(f"Failed to share sheet: {error_content}")
                    # Don't fail the export, just log the error
            else:
                logger.warning("No user email provided - sheet will only be accessible to service account")
            
            # Build the URL
            sheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"
            
            return {
                "success": True,
                "spreadsheet_url": sheet_url,
                "spreadsheet_id": spreadsheet_id
            }
            
        except HttpError as e:
            error_content = e.content.decode('utf-8') if hasattr(e, 'content') else str(e)
            logger.error(f"Google API HTTP error: Status={e.resp.status if hasattr(e, 'resp') else 'unknown'}, Content={error_content}")
            
            # Parse error details
            error_message = str(e)
            
            # Provide helpful error messages based on status code
            if hasattr(e, 'resp') and e.resp.status == 403:
                if "Drive API has not been used" in error_content:
                    return {
                        "success": False,
                        "error": "Google Drive API is not enabled. Please enable it in Google Cloud Console."
                    }
                elif "Sheets API has not been used" in error_content:
                    return {
                        "success": False,
                        "error": "Google Sheets API is not enabled. Please enable it in Google Cloud Console."
                    }
                elif "does not have storage.objects.create access" in error_content:
                    return {
                        "success": False,
                        "error": "Service account lacks permissions. Please grant 'Editor' role to the service account."
                    }
                else:
                    return {
                        "success": False,
                        "error": f"Permission denied: {error_content[:200]}"
                    }
            elif hasattr(e, 'resp') and e.resp.status == 400:
                return {
                    "success": False,
                    "error": f"Bad request: {error_content[:200]}"
                }
            
            return {
                "success": False,
                "error": f"Google API error (Status {e.resp.status if hasattr(e, 'resp') else 'unknown'}): {error_content[:200]}"
            }
            
        except Exception as e:
            logger.error(f"Failed to convert to Google Sheets: {e}")
            return {
                "success": False,
                "error": f"Failed to export: {str(e)}"
            }


# Global instance
google_sheets_service = GoogleSheetsService()
