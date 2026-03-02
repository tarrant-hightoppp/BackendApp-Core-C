import io
import pandas as pd
from typing import Dict, Any, List, Optional, BinaryIO, Union
from datetime import datetime
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font

from app.core.config import settings
from app.services.s3 import S3Service
from app.services.excel_report.template_generator import TemplateGenerator
from app.services.excel_report.conclusion_generator import ConclusionGenerator
from app.services.excel_report.cell_utils import CellUtils


class ExcelTemplateWrapper:
    """Service for wrapping exported Excel files in a predefined template"""
    
    def __init__(self):
        """Initialize the template wrapper service"""
        self.s3_service = S3Service()
    
    # Round-robin values for control action column
    CONTROL_ACTION_VALUES = ["кд1", "кд2", "кд6", "кд7"]

    def wrap_excel_with_template(self,
                                excel_content: Union[BinaryIO, bytes],
                                company_name: str = "Форт България ЕООД",
                                year: str = None,
                                audit_approach: str = "full",
                                account_type: str = None,
                                control_action_mode: str = "round_robin") -> io.BytesIO:
        """
        Wrap an Excel file with operations data in a predefined template
        
        Args:
            excel_content: Content of the Excel file to wrap (file-like object or bytes)
            company_name: Name of the company to include in the template
            year: Year to include in the template (default: current year)
            audit_approach: The audit approach to use (default: "statistical")
            account_type: The type of account being analyzed ("debit" or "credit")
                        This is used to determine which account number to display in the report header
            control_action_mode: Mode for populating the control action column:
                - "placeholder": writes a placeholder text
                - "round_robin": cycles through кд1, кд2, кд6, кд7 (default)
            
        Returns:
            BytesIO object containing the wrapped Excel file
        """
        # If excel_content is bytes, convert to file-like object
        if isinstance(excel_content, bytes):
            excel_content = io.BytesIO(excel_content)
            
        # If year is not provided, use current year
        if year is None:
            year = str(datetime.now().year)
            
        # Load the operations data first
        operations_df = pd.read_excel(excel_content)
        
        # Extract the main account being analyzed from the data
        main_account_being_analyzed = None
        account_types = set()
        
        # Determine which accounts to focus on based on account_type
        if account_type == "debit":
            # For debit reports, focus on debit accounts
            account_column = "Дт с/ка"
        else:
            # For credit reports or default, focus on credit accounts
            account_column = "Кт с/ка"
            
        # Extract the main account from the data
        account_number = "*номер на счетоводната сметка*"  # Default placeholder
        if account_column in operations_df.columns:
            accounts = operations_df[account_column].dropna().unique()
            if len(accounts) > 0:
                # Get the first account and extract its main part
                main_account = str(accounts[0])
                if '/' in main_account:
                    main_account = main_account.split('/')[0]
                if main_account:
                    main_account_being_analyzed = main_account[:3] if len(main_account) >= 3 else main_account
                    account_number = main_account_being_analyzed  # Use the actual account number
            
        try:
            # Count the actual number of operations
            operation_count = len(operations_df)
            
            # Create the template workbook with the account number and operation count
            template_wb = TemplateGenerator.create_template_workbook(
                company_name=company_name,
                year=year,
                audit_approach=audit_approach,
                account_number=account_number,
                operation_count=operation_count
            )
        except Exception as e:
            print(f"Error creating template workbook: {str(e)}")
            raise
        
        # Get the first sheet of the template
        template_sheet = template_wb.active
        
        # Determine the verification period based on operation dates
        operation_dates = []
        if "Дата" in operations_df.columns:
            for date in operations_df["Дата"]:
                if isinstance(date, datetime):
                    operation_dates.append(date)
                elif isinstance(date, str):
                    try:
                        # Try to parse the date string
                        parsed_date = datetime.strptime(date, "%d.%m.%Y")
                        operation_dates.append(parsed_date)
                    except ValueError:
                        try:
                            # Try alternative format
                            parsed_date = datetime.strptime(date, "%Y-%m-%d")
                            operation_dates.append(parsed_date)
                        except ValueError:
                            # Skip invalid dates
                            pass
        
        # Set the verification period in the template
        if operation_dates:
            # Sort dates to find min and max
            operation_dates.sort()
            start_date = operation_dates[0]
            end_date = operation_dates[-1]
            
            # Format the verification period
            verification_period = f"{start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}"
            
            # Update the verification period in the template (cell F5)
            CellUtils.safe_set_cell_value(template_sheet, 5, 6, verification_period)
        
        # Starting row for operations data
        start_row = 27
        
        # Insert operations data into the template
        row_count = 0
        total_by_account = {}
        current_account_rows = []
        
        # For "full" audit approach, we need to include all operations
        # For other approaches, we'll limit the number of rows
        max_rows = float('inf') if audit_approach == "full" else 43  # No limit for full audit approach
        
        for i, row_data in operations_df.iterrows():
            # Skip if we've reached the maximum number of rows we can fit
            # But for "full" audit approach, we'll include all operations
            if row_count >= max_rows and audit_approach != "full":
                break
                
            row_num = start_row + row_count
            
            # Map DataFrame columns to template columns - exactly matching the required structure
            
            # Column 1: № по ред (sequence number)
            if "№ по ред" in row_data:
                CellUtils.safe_set_cell_value(template_sheet, row_num, 1, row_data["№ по ред"])
            else:
                # If no explicit sequence number, use the row index + 1
                CellUtils.safe_set_cell_value(template_sheet, row_num, 1, i + 1)
            
            # Column 2: Вид документ (document type)
            if "Вид документ" in row_data or "Док. Вид" in row_data:
                doc_type = row_data.get("Вид документ", row_data.get("Док. Вид", ""))
                cell = template_sheet.cell(row=row_num, column=2)
                CellUtils.safe_set_cell_value(template_sheet, row_num, 2, doc_type)
            
            # Column 3: Документ № - format as 10 digits with no dots or commas
            if "Документ №" in row_data:
                doc_num = row_data["Документ №"]
                # Format document number to meet the 10 digit requirement if possible
                if isinstance(doc_num, (int, float)):
                    # First check if the value is NaN
                    if pd.isna(doc_num) or (isinstance(doc_num, float) and (doc_num != doc_num)):
                        # Handle NaN values - use empty string or placeholder
                        doc_num = ""
                    else:
                        # Convert number to string with leading zeros
                        doc_num = f"{int(doc_num):010d}"
                elif isinstance(doc_num, str):
                    # Remove any dots or commas
                    doc_num = doc_num.replace(".", "").replace(",", "")
                    # Try to convert to int and format with leading zeros
                    try:
                        doc_num = f"{int(doc_num):010d}"
                    except ValueError:
                        # If it can't be converted to int, leave as is
                        pass
                
                CellUtils.safe_set_cell_value(template_sheet, row_num, 3, doc_num)
            
            # Column 4: Дата (date in separate column)
            if "Дата" in row_data:
                date_value = row_data["Дата"]
                
                if isinstance(date_value, datetime):
                    CellUtils.safe_set_cell_value(template_sheet, row_num, 4, date_value, number_format='dd.mm.yyyy')
                elif isinstance(date_value, str):
                    try:
                        # Try to parse the date string
                        parsed_date = datetime.strptime(date_value, "%d.%m.%Y")
                        CellUtils.safe_set_cell_value(template_sheet, row_num, 4, parsed_date, number_format='dd.mm.yyyy')
                    except ValueError:
                        try:
                            # Try alternative format
                            parsed_date = datetime.strptime(date_value, "%Y-%m-%d")
                            CellUtils.safe_set_cell_value(template_sheet, row_num, 4, parsed_date, number_format='dd.mm.yyyy')
                        except ValueError:
                            # If parsing fails, use the string as is
                            CellUtils.safe_set_cell_value(template_sheet, row_num, 4, date_value)
            
            # Column 5: Дт с/ка
            debit_account = None
            if "Дт с/ка" in row_data:
                debit_account = row_data["Дт с/ка"]
                CellUtils.safe_set_cell_value(template_sheet, row_num, 5, debit_account)
            
            # Column 6: Аналитична сметка/Партньор (Дт)
            if "Аналитична сметка/Партньор (Дт)" in row_data:
                CellUtils.safe_set_cell_value(template_sheet, row_num, 6, row_data["Аналитична сметка/Партньор (Дт)"])
            elif "Аналитична сметка/Партньор" in row_data and "Дт с/ка" in row_data:
                # Try to use general analytical account field if specific one doesn't exist
                CellUtils.safe_set_cell_value(template_sheet, row_num, 6, row_data["Аналитична сметка/Партньор"])
            
            # Column 7: Кт с/ка
            credit_account = None
            if "Кт с/ка" in row_data:
                credit_account = row_data["Кт с/ка"]
                CellUtils.safe_set_cell_value(template_sheet, row_num, 7, credit_account)
                
                # Track totals by credit account for summary
                if credit_account not in total_by_account:
                    total_by_account[credit_account] = 0
                
                # Track rows for this account for subtotals
                current_account_rows.append((row_num, credit_account))
            
            # Column 8: Аналитична сметка/Партньор (Кт)
            if "Аналитична сметка/Партньор (Кт)" in row_data:
                CellUtils.safe_set_cell_value(template_sheet, row_num, 8, row_data["Аналитична сметка/Партньор (Кт)"])
            elif "Аналитична сметка/Партньор" in row_data and "Кт с/ка" in row_data:
                # Try to use general analytical account field if specific one doesn't exist
                CellUtils.safe_set_cell_value(template_sheet, row_num, 8, row_data["Аналитична сметка/Партньор"])
            
            # Column 9: Сума - with improved formatting
            amount = 0
            if "Сума" in row_data:
                amount = row_data["Сума"]
                CellUtils.safe_set_cell_value(
                    template_sheet,
                    row_num,
                    9,
                    amount,
                    alignment=Alignment(horizontal='right', vertical='center'),
                    number_format='#,##0.00'
                )
                
                # Add to account total
                if credit_account in total_by_account:
                    if isinstance(amount, (int, float)):
                        total_by_account[credit_account] += amount
                    else:
                        try:
                            total_by_account[credit_account] += float(amount)
                        except (ValueError, TypeError):
                            pass  # Skip if we can't convert to float
            
            # Column 10: Обяснение/Обоснование - with improved formatting
            if "Обяснение/Обоснование" in row_data:
                CellUtils.safe_set_cell_value(
                    template_sheet,
                    row_num,
                    10,
                    row_data["Обяснение/Обоснование"],
                    alignment=Alignment(wrap_text=True, vertical='center')
                )
            
            # Column 11: Установена сума при одита - with improved formatting
            verified_amount = amount
            if "Установена сума при одита" in row_data:
                verified_amount = row_data["Установена сума при одита"]
            
            CellUtils.safe_set_cell_value(
                template_sheet,
                row_num,
                11,
                verified_amount,
                alignment=Alignment(horizontal='right', vertical='center'),
                number_format='#,##0.00'
            )
            
            # Column 12: Отклонение - with improved formatting
            CellUtils.safe_set_cell_value(
                template_sheet,
                row_num,
                12,
                "НЯМА",
                alignment=Alignment(horizontal='center', vertical='center')
            )
            
            # Column 13: Установено контролно действие при одита
            if control_action_mode == "round_robin":
                ca_value = self.CONTROL_ACTION_VALUES[row_count % len(self.CONTROL_ACTION_VALUES)]
            else:
                # placeholder mode
                ca_value = str(" ")
            CellUtils.safe_set_cell_value(
                template_sheet,
                row_num,
                13,
                ca_value,
                alignment=Alignment(horizontal='center', vertical='center')
            )
                
            # Column 14: First Additional Deviation column
            if "Отклонение (забележка)" in row_data:
                CellUtils.safe_set_cell_value(
                    template_sheet,
                    row_num,
                    14,
                    row_data["Отклонение (забележка)"],
                    alignment=Alignment(wrap_text=True, vertical='center')
                )
            
            # Column 15: Second Additional Deviation column
            if "Отклонение (забележка 2)" in row_data:
                CellUtils.safe_set_cell_value(
                    template_sheet,
                    row_num,
                    15,
                    row_data["Отклонение (забележка 2)"],
                    alignment=Alignment(wrap_text=True, vertical='center')
                )
            else:
                # Initialize with empty string if field doesn't exist
                CellUtils.safe_set_cell_value(
                    template_sheet,
                    row_num,
                    15,
                    "",
                    alignment=Alignment(wrap_text=True, vertical='center')
                )
            
            row_count += 1
            
            # Add a subtotal row after a group of operations with the same account
            if i < len(operations_df) - 1:
                next_row = operations_df.iloc[i + 1]
                
                # Safely extract current_account - handling potential pandas Series
                current_account = row_data.get("Кт с/ка", "")
                if isinstance(current_account, pd.Series):
                    current_account = current_account.iloc[0] if not current_account.empty else ""
                
                # Safely extract next_account - handle both dict-like and Series access
                next_account = ""
                if "Кт с/ка" in next_row:
                    next_account = next_row["Кт с/ка"]
                    if isinstance(next_account, pd.Series):
                        next_account = next_account.iloc[0] if not next_account.empty else ""
                
                if current_account != next_account and current_account:
                    # This is the last row of a group, add a subtotal with improved formatting
                    subtotal_row = start_row + row_count
                    
                    # Add "Общо" label
                    CellUtils.safe_set_cell_value(
                        template_sheet,
                        subtotal_row,
                        1,
                        "Общо",
                        font=Font(name='Calibri', size=11, bold=True),
                        alignment=Alignment(horizontal='right', vertical='center')
                    )
                    
                    # Add account number with bold formatting
                    CellUtils.safe_set_cell_value(
                        template_sheet,
                        subtotal_row,
                        7,  # Column G in new structure
                        current_account,
                        font=Font(name='Calibri', size=11, bold=True),
                        alignment=Alignment(horizontal='center', vertical='center')
                    )
                    
                    # Add total amount with bold formatting and proper number format
                    if current_account in total_by_account:
                        # Format amount cell
                        CellUtils.safe_set_cell_value(
                            template_sheet,
                            subtotal_row,
                            9,  # Column I in new structure
                            total_by_account[current_account],
                            font=Font(name='Calibri', size=11, bold=True),
                            alignment=Alignment(horizontal='right', vertical='center'),
                            number_format='#,##0.00'
                        )
                        
                        # Format verified amount cell (same as total from database)
                        CellUtils.safe_set_cell_value(
                            template_sheet,
                            subtotal_row,
                            11,  # Column K in new structure
                            total_by_account[current_account],
                            font=Font(name='Calibri', size=11, bold=True),
                            alignment=Alignment(horizontal='right', vertical='center'),
                            number_format='#,##0.00'
                        )
                        
                        # Set deviation to "НЯМА" as requested
                        CellUtils.safe_set_cell_value(
                            template_sheet,
                            subtotal_row,
                            12,  # Column L in new structure
                            "НЯМА",
                            font=Font(name='Calibri', size=11, bold=True),
                            alignment=Alignment(horizontal='center', vertical='center')
                        )
                        
                        # Add the second deviation field for the subtotal row as well
                        CellUtils.safe_set_cell_value(
                            template_sheet,
                            subtotal_row,
                            14,  # Column N in new structure
                            "",  # Usually blank for subtotals
                            font=Font(name='Calibri', size=11, bold=True),
                            alignment=Alignment(horizontal='center', vertical='center')
                        )
                        
                        # Add the third deviation field for the subtotal row as well
                        CellUtils.safe_set_cell_value(
                            template_sheet,
                            subtotal_row,
                            15,  # Column O in new structure
                            "",  # Usually blank for subtotals
                            font=Font(name='Calibri', size=11, bold=True),
                            alignment=Alignment(horizontal='center', vertical='center')
                        )
                    
                    row_count += 1  # Move to the next row
        
        # Determine the conclusion section start row based on the actual data
        # Add 5 rows of padding after the last data row
        conclusion_start_row = start_row + row_count + 5
        
        # We've already extracted the main account above, now just collect all account types for reference
        
        # Also collect all account types for reference
        for account_col in ["Дт с/ка", "Кт с/ка"]:
            if account_col in operations_df.columns:
                for account in operations_df[account_col].dropna().unique():
                    account_str = str(account)
                    if account_str:
                        if '/' in account_str:
                            account_str = account_str.split('/')[0]
                        if len(account_str) >= 3:
                            account_types.add(account_str[:3])
        
        # Generate conclusion text
        conclusion_text = ConclusionGenerator.generate_conclusion_text(
            main_account_being_analyzed=main_account_being_analyzed,
            account_types=account_types
        )
        
        # Add summary statistics for all operations
        total_operations = len(operations_df)
        total_amount = operations_df["Сума"].sum() if "Сума" in operations_df.columns else 0
        
        # Populate the conclusion section
        ConclusionGenerator.populate_conclusion_section(
            template_sheet=template_sheet,
            conclusion_start_row=conclusion_start_row,
            total_by_account=total_by_account,
            total_operations=total_operations,
            total_amount=total_amount
        )
        
        # Set the conclusion text in a merged cell
        try:
            template_sheet.merge_cells(f'A{conclusion_start_row+13}:O{conclusion_start_row+13}')
        except:
            pass
        
        CellUtils.safe_set_cell_value(
            template_sheet,
            conclusion_start_row+13,
            1,
            conclusion_text,
            alignment=Alignment(wrap_text=True, vertical='center')
        )
        
        # Create a BytesIO object to store the result
        result = io.BytesIO()
        
        # Save the workbook to the BytesIO object
        template_wb.save(result)
        
        # Reset the file position to the beginning
        result.seek(0)
        
        return result
    
    def wrap_and_upload_excel(self,
                             s3_key: str,
                             company_name: str = "Форт България ЕООД",
                             year: str = None,
                             audit_approach: str = "statistical",
                             account_type: str = None,
                             control_action_mode: str = "round_robin") -> Optional[str]:
        """
        Download an Excel file from S3, wrap it with a template, and upload it back to S3
        
        Args:
            s3_key: S3 key of the Excel file to wrap
            company_name: Name of the company to include in the template
            year: Year to include in the template
            audit_approach: The audit approach to use (default: "statistical")
            account_type: The type of account being analyzed ("debit" or "credit")
            control_action_mode: Mode for control action column ("placeholder" or "round_robin")
            
        Returns:
            S3 key of the wrapped Excel file if successful, None otherwise
        """
        try:
            # Download the Excel file from S3
            excel_content = self.s3_service.download_file(s3_key)
            if not excel_content:
                print(f"Error downloading file from S3: {s3_key}")
                return None
            
            # Wrap the Excel file with the template
            wrapped_excel = self.wrap_excel_with_template(
                excel_content,
                company_name=company_name,
                year=year,
                audit_approach=audit_approach,
                account_type=account_type,
                control_action_mode=control_action_mode
            )
            
            # Generate a new S3 key for the wrapped file
            # Keep the same directory structure but add "_wrapped" to the filename
            path_parts = s3_key.split('/')
            filename = path_parts[-1]
            filename_parts = filename.split('.')
            wrapped_filename = f"{filename_parts[0]}_wrapped.{filename_parts[1]}"
            path_parts[-1] = wrapped_filename
            wrapped_s3_key = '/'.join(path_parts)
            
            # Upload the wrapped Excel file to S3
            success, message = self.s3_service.upload_file(wrapped_excel, wrapped_s3_key)
            
            if success:
                return wrapped_s3_key
            else:
                print(f"Error uploading wrapped file to S3: {message}")
                return None
                
        except Exception as e:
            print(f"Error wrapping Excel file: {str(e)}")
            import traceback
            traceback.print_exc()
            return None