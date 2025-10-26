"""
Google OAuth2 service for user authentication and Sheets export.
Users authenticate with their Google account to export to their own Drive.
"""
import os
import logging
import json
from pathlib import Path
from typing import Optional, Dict, Any
import pandas as pd
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)


class GoogleOAuthSheetsService:
    """Service for exporting to user's Google Sheets using their OAuth token."""
    
    def __init__(self, access_token: str):
        """
        Initialize with user's Google OAuth access token.
        
        Args:
            access_token: User's Google OAuth2 access token
        """
        self.credentials = Credentials(token=access_token)
        self.sheets_service = build('sheets', 'v4', credentials=self.credentials)
        self.drive_service = build('drive', 'v3', credentials=self.credentials)
    
    async def excel_to_user_sheets(self, excel_path: Path, filename: str) -> Dict[str, Any]:
        """
        Convert an Excel file to Google Sheets in the user's Drive.
        
        Args:
            excel_path: Path to the Excel file
            filename: Name for the Google Sheets file
            
        Returns:
            Dict with success status and sheet URL or error message
        """
        try:
            # Read Excel file
            excel_file = pd.ExcelFile(excel_path)
            sheet_names = excel_file.sheet_names
            
            # Create Google Sheets using Sheets API in user's Drive
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
                fields='spreadsheetId,spreadsheetUrl'
            ).execute()
            
            spreadsheet_id = spreadsheet.get('spreadsheetId')
            spreadsheet_url = spreadsheet.get('spreadsheetUrl')
            
            # If no URL returned, construct it
            if not spreadsheet_url:
                spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"
            
            logger.info(f"Created Google Sheets in user's Drive: {spreadsheet_id}")
            
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
                if batch_requests:
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
            
            logger.info(f"Successfully populated Google Sheets: {spreadsheet_id}")
            
            return {
                "success": True,
                "spreadsheet_url": spreadsheet_url,
                "spreadsheet_id": spreadsheet_id,
                "message": "Exported to your Google Sheets successfully!"
            }
            
        except HttpError as e:
            error_content = e.content.decode('utf-8') if hasattr(e, 'content') else str(e)
            logger.error(f"Google API HTTP error: Status={e.resp.status if hasattr(e, 'resp') else 'unknown'}, Content={error_content}")
            
            # Parse error for user-friendly messages
            if hasattr(e, 'resp'):
                if e.resp.status == 401:
                    return {
                        "success": False,
                        "error": "Google authentication expired. Please sign in with Google again."
                    }
                elif e.resp.status == 403:
                    if "insufficient" in error_content.lower():
                        return {
                            "success": False,
                            "error": "Please grant permission to access Google Sheets when signing in."
                        }
                    return {
                        "success": False,
                        "error": "Permission denied. Please ensure you granted Sheets access."
                    }
                elif e.resp.status == 404:
                    return {
                        "success": False,
                        "error": "Google Sheets API not found. Please try again."
                    }
            
            return {
                "success": False,
                "error": f"Google API error: {error_content[:200]}"
            }
            
        except Exception as e:
            logger.error(f"Failed to convert to Google Sheets: {e}")
            return {
                "success": False,
                "error": f"Failed to export: {str(e)}"
            }
