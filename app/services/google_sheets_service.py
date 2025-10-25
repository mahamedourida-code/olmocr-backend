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
            
            # Create Google Sheets file via Drive API
            file_metadata = {
                'name': filename,
                'mimeType': 'application/vnd.google-apps.spreadsheet'
            }
            
            drive_file = self.drive_service.files().create(
                body=file_metadata,
                fields='id'
            ).execute()
            
            spreadsheet_id = drive_file.get('id')
            logger.info(f"Created Google Sheets: {spreadsheet_id}")
            
            # If multiple sheets, add them to the spreadsheet
            if len(sheet_names) > 1:
                # Rename first sheet and add others
                batch_requests = []
                
                # Rename the default sheet to first sheet name
                batch_requests.append({
                    'updateSheetProperties': {
                        'properties': {
                            'sheetId': 0,
                            'title': sheet_names[0]
                        },
                        'fields': 'title'
                    }
                })
                
                # Add remaining sheets
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
            
            # Share with user if email provided
            if user_email:
                try:
                    self.drive_service.permissions().create(
                        fileId=spreadsheet_id,
                        body={
                            'type': 'user',
                            'role': 'writer',
                            'emailAddress': user_email
                        },
                        sendNotificationEmail=True
                    ).execute()
                    logger.info(f"Shared sheet with {user_email}")
                except HttpError as e:
                    logger.warning(f"Failed to share with {user_email}: {e}")
            
            # Build the URL
            sheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"
            
            return {
                "success": True,
                "spreadsheet_url": sheet_url,
                "spreadsheet_id": spreadsheet_id
            }
            
        except HttpError as e:
            logger.error(f"Google API error: {e}")
            error_message = str(e)
            
            # Provide helpful error messages
            if "403" in error_message:
                if "Drive API has not been used" in error_message:
                    return {
                        "success": False,
                        "error": "Google Drive API is not enabled. Please enable it in Google Cloud Console."
                    }
                elif "Sheets API has not been used" in error_message:
                    return {
                        "success": False,
                        "error": "Google Sheets API is not enabled. Please enable it in Google Cloud Console."
                    }
                else:
                    return {
                        "success": False,
                        "error": "Permission denied. Please check service account permissions."
                    }
            
            return {
                "success": False,
                "error": f"Google API error: {error_message}"
            }
            
        except Exception as e:
            logger.error(f"Failed to convert to Google Sheets: {e}")
            return {
                "success": False,
                "error": f"Failed to export: {str(e)}"
            }


# Global instance
google_sheets_service = GoogleSheetsService()
