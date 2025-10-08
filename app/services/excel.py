import io
import pandas as pd
from typing import List, Dict, Any, Optional
import logging
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils.dataframe import dataframe_to_rows

from app.utils.exceptions import FileProcessingError

logger = logging.getLogger(__name__)


class ExcelService:
    """Service for converting CSV data to XLSX format."""
    
    def __init__(self):
        self.header_font = Font(bold=True, color="FFFFFF")
        self.header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
        self.alignment = Alignment(horizontal="left", vertical="top")
    
    def csv_to_xlsx(self, csv_content: str, sheet_name: str = "Sheet1") -> bytes:
        """
        Convert CSV content to XLSX format.
        
        Args:
            csv_content: CSV data as string
            sheet_name: Name for the Excel sheet
            
        Returns:
            XLSX file as bytes
            
        Raises:
            FileProcessingError: If CSV parsing fails
        """
        try:
            # Log the CSV content for debugging
            logger.info(f"Converting CSV to XLSX. Content length: {len(csv_content)}")
            logger.info(f"CSV content preview (first 200 chars): {csv_content[:200]}")
            
            # Handle empty or whitespace-only content
            if not csv_content or not csv_content.strip():
                logger.warning("CSV content is empty or whitespace only")
                # Create a minimal Excel file with a message
                return self._create_empty_xlsx_with_message("No data extracted from image", sheet_name)
            
            # Parse CSV content into DataFrame
            df = pd.read_csv(io.StringIO(csv_content))
            
            # Check if DataFrame is empty
            if df.empty:
                logger.warning("Parsed DataFrame is empty")
                return self._create_empty_xlsx_with_message("No tabular data found in image", sheet_name)
            
            # Handle empty cells
            df = df.fillna("")
            
            # Convert to XLSX
            xlsx_bytes = self._dataframe_to_xlsx_bytes(df, sheet_name)
            
            logger.info(f"Successfully converted CSV to XLSX: {len(df)} rows, {len(df.columns)} columns")
            return xlsx_bytes
            
        except pd.errors.EmptyDataError:
            logger.warning("CSV EmptyDataError - creating empty Excel file")
            return self._create_empty_xlsx_with_message("No data extracted from image", sheet_name)
        except pd.errors.ParserError as e:
            logger.warning(f"CSV ParserError: {str(e)} - attempting to create raw text Excel")
            return self._create_raw_text_xlsx(csv_content, sheet_name)
        except Exception as e:
            logger.error(f"Excel conversion error: {str(e)}")
            # Instead of raising an error, create an Excel file with the error message
            return self._create_empty_xlsx_with_message(f"Error processing image: {str(e)}", sheet_name)
    
    def create_batch_xlsx(
        self, 
        batch_results: List[Dict[str, Any]], 
        output_type: str = "separate"
    ) -> bytes:
        """
        Create XLSX file(s) from batch processing results.
        
        Args:
            batch_results: List of batch processing results
            output_type: "separate", "consolidated", or "concatenated"
            
        Returns:
            XLSX file as bytes
        """
        if output_type == "consolidated":
            return self._create_consolidated_xlsx(batch_results)
        elif output_type == "concatenated":
            return self._create_concatenated_xlsx(batch_results)
        else:  # separate
            return self._create_separate_xlsx(batch_results)
    
    def _create_consolidated_xlsx(self, batch_results: List[Dict[str, Any]]) -> bytes:
        """
        Create a single XLSX file with multiple sheets.
        
        Args:
            batch_results: List of batch processing results
            
        Returns:
            XLSX file as bytes
        """
        try:
            workbook = Workbook()
            # Remove default sheet
            workbook.remove(workbook.active)
            
            successful_sheets = 0
            
            for i, result in enumerate(batch_results):
                sheet_name = f"Image_{i+1}"
                worksheet = workbook.create_sheet(title=sheet_name)
                
                if result["success"] and result["data"]:
                    try:
                        # Log the data being processed
                        logger.info(f"Processing sheet {sheet_name}, data length: {len(result['data'])}")
                        logger.info(f"Data preview: {result['data'][:200]}...")
                        
                        df = pd.read_csv(io.StringIO(result["data"]))
                        df = df.fillna("")
                        
                        if not df.empty:
                            # Add data to worksheet
                            for r in dataframe_to_rows(df, index=False, header=True):
                                worksheet.append(r)
                            
                            # Style the header row
                            self._style_header_row(worksheet)
                            successful_sheets += 1
                            logger.info(f"Successfully added sheet {sheet_name} with {len(df)} rows")
                        else:
                            # Empty dataframe
                            worksheet.append(["Status", "No data extracted from image"])
                            worksheet.append(["Raw Data", result["data"] if result["data"] else "No raw data"])
                        
                    except Exception as e:
                        logger.warning(f"Failed to parse CSV for sheet {sheet_name}: {str(e)}")
                        logger.warning(f"Raw data that failed: {result['data'][:500]}...")
                        # Add error information to sheet
                        worksheet.append(["Error", f"Failed to parse data: {str(e)}"])
                        worksheet.append(["Raw Data", result["data"] if result["data"] else "No data"])
                        if result["data"]:
                            # Split raw data into lines and add them
                            lines = result["data"].split('\n')[:10]  # First 10 lines
                            for j, line in enumerate(lines):
                                worksheet.append([f"Line {j+1}", line])
                else:
                    # Failed result
                    error_msg = result.get("error", "Unknown error")
                    worksheet.append(["Status", "Processing Failed"])
                    worksheet.append(["Error", error_msg])
                    logger.warning(f"Sheet {sheet_name} failed: {error_msg}")
            
            # Add a summary sheet
            summary_sheet = workbook.create_sheet(title="Summary", index=0)
            summary_sheet.append(["Batch Processing Summary"])
            summary_sheet.append(["Total Images", str(len(batch_results))])
            
            successful_count = sum(1 for r in batch_results if r["success"])
            summary_sheet.append(["Successfully Processed", str(successful_count)])
            summary_sheet.append(["Failed Processing", str(len(batch_results) - successful_count)])
            summary_sheet.append(["Sheets with Valid Data", str(successful_sheets)])
            
            # Style the summary sheet
            for row in summary_sheet['A1:B5']:
                for cell in row:
                    if cell.column == 1:  # First column
                        cell.font = self.header_font
                        cell.fill = self.header_fill
            
            # If no successful sheets were added, add debugging info
            if successful_sheets == 0:
                summary_sheet.append([])
                summary_sheet.append(["Debug Information"])
                for i, result in enumerate(batch_results):
                    summary_sheet.append([f"Image {i+1} Status", "Success" if result["success"] else "Failed"])
                    if result.get("data"):
                        summary_sheet.append([f"Image {i+1} Data Length", str(len(result["data"]))])
                        summary_sheet.append([f"Image {i+1} Data Preview", result["data"][:100]])
            
            # Save to bytes
            output = io.BytesIO()
            workbook.save(output)
            output.seek(0)
            
            return output.getvalue()
            
        except Exception as e:
            logger.error(f"Failed to create consolidated XLSX: {str(e)}")
            raise FileProcessingError(f"Failed to create consolidated Excel file: {str(e)}")
    
    def _create_concatenated_xlsx(self, batch_results: List[Dict[str, Any]]) -> bytes:
        """
        Create a single XLSX file with concatenated data.
        
        Args:
            batch_results: List of batch processing results
            
        Returns:
            XLSX file as bytes
        """
        try:
            all_dataframes = []
            
            for i, result in enumerate(batch_results):
                if result["success"] and result["data"]:
                    try:
                        df = pd.read_csv(io.StringIO(result["data"]))
                        df = df.fillna("")
                        
                        # Add source column
                        df.insert(0, "Source", f"Image_{i+1}")
                        all_dataframes.append(df)
                        
                    except Exception as e:
                        logger.warning(f"Failed to process result {i} for concatenation: {str(e)}")
            
            if not all_dataframes:
                raise FileProcessingError("No valid data to concatenate")
            
            # Concatenate all dataframes
            combined_df = pd.concat(all_dataframes, ignore_index=True, sort=False)
            
            # Convert to XLSX
            return self._dataframe_to_xlsx_bytes(combined_df, "Combined_Data")
            
        except Exception as e:
            logger.error(f"Failed to create concatenated XLSX: {str(e)}")
            raise FileProcessingError(f"Failed to create concatenated Excel file: {str(e)}")
    
    def _create_separate_xlsx(self, batch_results: List[Dict[str, Any]]) -> bytes:
        """
        Create a ZIP file containing separate XLSX files.
        Note: For now, returns the first successful result as XLSX.
        TODO: Implement ZIP creation for multiple files.
        
        Args:
            batch_results: List of batch processing results
            
        Returns:
            XLSX file as bytes
        """
        # For now, return the first successful result
        # In a full implementation, this would create a ZIP file
        for i, result in enumerate(batch_results):
            if result["success"] and result["data"]:
                return self.csv_to_xlsx(result["data"], f"Table_{i+1}")
        
        raise FileProcessingError("No successful results to process")
    
    def _dataframe_to_xlsx_bytes(self, df: pd.DataFrame, sheet_name: str) -> bytes:
        """
        Convert pandas DataFrame to XLSX bytes with styling.
        
        Args:
            df: DataFrame to convert
            sheet_name: Name for the Excel sheet
            
        Returns:
            XLSX file as bytes
        """
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            # Get the workbook and worksheet
            workbook = writer.book
            worksheet = writer.sheets[sheet_name]
            
            # Style the header row
            self._style_header_row(worksheet)
            
            # Auto-adjust column widths
            self._auto_adjust_columns(worksheet, df)
        
        output.seek(0)
        return output.getvalue()
    
    def _style_header_row(self, worksheet) -> None:
        """
        Style the header row of a worksheet.
        
        Args:
            worksheet: openpyxl worksheet object
        """
        if worksheet.max_row > 0:
            for cell in worksheet[1]:  # First row
                cell.font = self.header_font
                cell.fill = self.header_fill
                cell.alignment = self.alignment
    
    def _auto_adjust_columns(self, worksheet, df: pd.DataFrame) -> None:
        """
        Auto-adjust column widths based on content.
        
        Args:
            worksheet: openpyxl worksheet object
            df: DataFrame for width calculation
        """
        for i, column in enumerate(df.columns):
            # Calculate max width needed
            max_length = max(
                len(str(column)),  # Header length
                df[column].astype(str).map(len).max() if not df.empty else 0  # Content length
            )
            
            # Set column width (with some padding)
            adjusted_width = min(max_length + 2, 50)  # Max width of 50
            worksheet.column_dimensions[chr(65 + i)].width = adjusted_width
    
    def _create_empty_xlsx_with_message(self, message: str, sheet_name: str) -> bytes:
        """
        Create an Excel file with a message when no data is available.
        
        Args:
            message: Message to display in the Excel file
            sheet_name: Name for the Excel sheet
            
        Returns:
            XLSX file as bytes
        """
        workbook = Workbook()
        worksheet = workbook.active
        worksheet.title = sheet_name
        
        # Add message
        worksheet['A1'] = "Status"
        worksheet['B1'] = message
        
        # Add some basic styling
        worksheet['A1'].font = self.header_font
        worksheet['A1'].fill = self.header_fill
        worksheet['B1'].font = Font(color="FF0000")  # Red text
        
        # Adjust column widths
        worksheet.column_dimensions['A'].width = 15
        worksheet.column_dimensions['B'].width = 50
        
        # Save to bytes
        output = io.BytesIO()
        workbook.save(output)
        output.seek(0)
        return output.getvalue()
    
    def _create_raw_text_xlsx(self, content: str, sheet_name: str) -> bytes:
        """
        Create an Excel file with raw text content when CSV parsing fails.
        
        Args:
            content: Raw text content to include
            sheet_name: Name for the Excel sheet
            
        Returns:
            XLSX file as bytes
        """
        workbook = Workbook()
        worksheet = workbook.active
        worksheet.title = sheet_name
        
        # Add headers
        worksheet['A1'] = "Raw OCR Output"
        worksheet['A1'].font = self.header_font
        worksheet['A1'].fill = self.header_fill
        
        # Split content into lines and add to Excel
        lines = content.split('\n')
        for i, line in enumerate(lines, start=2):
            if line.strip():  # Only add non-empty lines
                worksheet[f'A{i}'] = line.strip()
        
        # Adjust column width
        worksheet.column_dimensions['A'].width = 80
        
        # Save to bytes
        output = io.BytesIO()
        workbook.save(output)
        output.seek(0)
        return output.getvalue()

    def create_multi_sheet_xlsx(self, results: List[Dict[str, Any]]) -> bytes:
        """
        Create an XLSX file with multiple sheets from batch results.
        
        Args:
            results: List of result dictionaries with 'sheet_name', 'csv_data', and optionally 'error'
            
        Returns:
            XLSX file as bytes
        """
        try:
            workbook = Workbook()
            # Remove default sheet
            workbook.remove(workbook.active)
            
            for i, result in enumerate(results):
                sheet_name = result.get('sheet_name', f'Sheet_{i+1}')
                # Clean sheet name (Excel has restrictions)
                sheet_name = sheet_name.replace('/', '_').replace('\\', '_')[:31]
                
                worksheet = workbook.create_sheet(title=sheet_name)
                
                if result.get('error'):
                    # Add error information
                    worksheet['A1'] = "Error"
                    worksheet['A1'].font = self.header_font
                    worksheet['A1'].fill = PatternFill(start_color="FF0000", end_color="FF0000", fill_type="solid")
                    worksheet['A2'] = f"Failed to process: {result['error']}"
                else:
                    csv_data = result.get('csv_data', '')
                    if csv_data and csv_data.strip():
                        # Parse CSV and add to worksheet
                        try:
                            df = pd.read_csv(io.StringIO(csv_data))
                            if not df.empty:
                                # Add headers
                                for col_num, column_name in enumerate(df.columns, 1):
                                    cell = worksheet.cell(row=1, column=col_num, value=column_name)
                                    cell.font = self.header_font
                                    cell.fill = self.header_fill
                                    cell.alignment = self.alignment
                                
                                # Add data
                                for row_num, row_data in df.iterrows():
                                    for col_num, value in enumerate(row_data, 1):
                                        worksheet.cell(row=row_num + 2, column=col_num, value=value)
                                
                                # Auto-adjust column widths
                                for column in worksheet.columns:
                                    max_length = 0
                                    column_letter = column[0].column_letter
                                    for cell in column:
                                        try:
                                            if len(str(cell.value)) > max_length:
                                                max_length = len(str(cell.value))
                                        except:
                                            pass
                                    adjusted_width = min(max_length + 2, 50)
                                    worksheet.column_dimensions[column_letter].width = adjusted_width
                            else:
                                worksheet['A1'] = "No data extracted"
                        except Exception as e:
                            worksheet['A1'] = f"Parse error: {str(e)}"
                    else:
                        worksheet['A1'] = "No data extracted from image"
            
            # If no sheets were created, add a default one
            if not workbook.worksheets:
                worksheet = workbook.create_sheet(title="Results")
                worksheet['A1'] = "No results to display"
            
            # Save to bytes
            output = io.BytesIO()
            workbook.save(output)
            output.seek(0)
            return output.getvalue()
            
        except Exception as e:
            logger.error(f"Error creating multi-sheet XLSX: {str(e)}")
            # Return a basic error sheet
            return self._create_empty_xlsx_with_message(f"Error creating results: {str(e)}", "Error")

    def create_concatenated_xlsx(self, results: List[Dict[str, Any]]) -> bytes:
        """
        Create an XLSX file with all results concatenated in a single sheet.
        
        Args:
            results: List of result dictionaries with 'sheet_name', 'csv_data', and optionally 'error'
            
        Returns:
            XLSX file as bytes
        """
        try:
            workbook = Workbook()
            worksheet = workbook.active
            worksheet.title = "Concatenated_Results"
            
            current_row = 1
            
            for i, result in enumerate(results):
                sheet_name = result.get('sheet_name', f'Image_{i+1}')
                
                # Add section header
                header_cell = worksheet.cell(row=current_row, column=1, value=f"=== {sheet_name} ===")
                header_cell.font = Font(bold=True, size=14)
                header_cell.fill = PatternFill(start_color="D3D3D3", end_color="D3D3D3", fill_type="solid")
                current_row += 1
                
                if result.get('error'):
                    error_cell = worksheet.cell(row=current_row, column=1, value=f"Error: {result['error']}")
                    error_cell.font = Font(color="FF0000")
                    current_row += 2
                else:
                    csv_data = result.get('csv_data', '')
                    if csv_data and csv_data.strip():
                        try:
                            df = pd.read_csv(io.StringIO(csv_data))
                            if not df.empty:
                                # Add headers
                                for col_num, column_name in enumerate(df.columns, 1):
                                    cell = worksheet.cell(row=current_row, column=col_num, value=column_name)
                                    cell.font = self.header_font
                                    cell.fill = self.header_fill
                                current_row += 1
                                
                                # Add data rows
                                for _, row_data in df.iterrows():
                                    for col_num, value in enumerate(row_data, 1):
                                        worksheet.cell(row=current_row, column=col_num, value=value)
                                    current_row += 1
                            else:
                                worksheet.cell(row=current_row, column=1, value="No data extracted")
                                current_row += 1
                        except Exception as e:
                            worksheet.cell(row=current_row, column=1, value=f"Parse error: {str(e)}")
                            current_row += 1
                    else:
                        worksheet.cell(row=current_row, column=1, value="No data extracted")
                        current_row += 1
                
                # Add spacing between sections
                current_row += 1
            
            # Auto-adjust column widths
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width
            
            # Save to bytes
            output = io.BytesIO()
            workbook.save(output)
            output.seek(0)
            return output.getvalue()
            
        except Exception as e:
            logger.error(f"Error creating concatenated XLSX: {str(e)}")
            return self._create_empty_xlsx_with_message(f"Error creating results: {str(e)}", "Error")

    def _create_empty_xlsx_with_message(self, message: str, sheet_name: str = "Results") -> bytes:
        """
        Create an empty XLSX file with a single message.
        
        Args:
            message: Message to display
            sheet_name: Name of the sheet
            
        Returns:
            XLSX file as bytes
        """
        workbook = Workbook()
        worksheet = workbook.active
        worksheet.title = sheet_name
        
        cell = worksheet['A1']
        cell.value = message
        cell.font = self.header_font
        cell.fill = PatternFill(start_color="FFAAAA", end_color="FFAAAA", fill_type="solid")
        
        # Save to bytes
        output = io.BytesIO()
        workbook.save(output)
        output.seek(0)
        return output.getvalue()


# Global Excel service instance
excel_service = ExcelService()