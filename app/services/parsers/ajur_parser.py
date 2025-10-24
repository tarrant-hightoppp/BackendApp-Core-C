import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime
from io import BytesIO

from app.services.parsers.base_parser import BaseExcelParser


class AjurParser(BaseExcelParser):
    """Parser for AJUR Excel format - supports both standard, chronological and audit templates"""
    
    def parse(self, file_path: str, file_id: int, import_uuid: str = None) -> List[Dict[str, Any]]:
        """
        Parse the AJUR Excel file and extract accounting operations
        
        For AJUR format:
        - вид (Document type)
        - номер (Document number)
        - дата (Date)
        - дебит (Debit account)
        - аналитична (Analytical for debit)
        - кредит (Credit account)
        - аналитична (Analytical for credit)
        - сума (Amount)
        - обяснение (Description)
        
        Args:
            file_path: Path to the Excel file
            file_id: ID of the uploaded file in the database
            import_uuid: UUID of the import batch this file belongs to
            
        Returns:
            List of dictionaries containing accounting operations data
        """
        try:
            # Read Excel file
            df = pd.read_excel(file_path)
            
            # First, print the original dataframe columns and sample data to understand structure
            print(f"[DEBUG] AJUR parser - Original DataFrame columns: {list(df.columns)}")
            print(f"[DEBUG] AJUR parser - Original DataFrame shape: {df.shape}")
            print(f"[DEBUG] AJUR parser - First 3 rows sample (original):")
            for i in range(min(3, len(df))):
                print(f"[DEBUG] Original Row {i}: {list(df.iloc[i].values)}")
            
            # Try to identify the table structure
            column_map = self._detect_columns(df)
            print(f"[DEBUG] AJUR parser - Detected column mapping: {column_map}")
            
            # Force correct credit column for AJUR format if incorrectly detected
            if column_map.get('credit') == 16 and len(df.columns) > 15:
                # Check if column 15 is actually "Кт с/ка"
                if 'кт' in str(df.columns[15]).lower() and 'с/ка' in str(df.columns[15]).lower():
                    print(f"[DEBUG] AJUR parser - Correcting credit column from 16 to 15")
                    column_map['credit'] = 15
            
            # Fix column detection based on actual column names for AJUR format
            if column_map['amount'] == 0 or column_map['amount'] is None:
                print(f"[DEBUG] AJUR parser - Trying to find better column mappings from DataFrame columns")
                # Check for specific column names that match the AJUR format
                for i, col_name in enumerate(df.columns):
                    col_str = str(col_name).lower().strip()
                    
                    # Specific checks for the Ajur format in the sample file
                    if 'потр' in col_str:  # "Потр." column (user/operator identifier)
                        print(f"[DEBUG] AJUR parser - Found operator column by name: {col_name} at index {i}")
                        column_map['operator'] = i
                    elif 'опер' in col_str and ('no' in col_str or '№' in col_str):  # "Опер. No" column
                        print(f"[DEBUG] AJUR parser - Found operation number column by name: {col_name} at index {i}")
                        column_map['operation_number'] = i
                        column_map['sequence_number'] = i  # Also use it as sequence number
                    elif 'сума' in col_str:
                        print(f"[DEBUG] AJUR parser - Found amount column by name: {col_name} at index {i}")
                        column_map['amount'] = i
                    elif 'дт' in col_str and 'с/ка' in col_str and 'аналитична' not in col_str:
                        print(f"[DEBUG] AJUR parser - Found MAIN debit column (Дт с/ка) by name: {col_name} at index {i}")
                        column_map['debit'] = i
                        # This is the EXACT main account (Дт с/ка) column that we need to audit
                        print(f"[DEBUG] AJUR parser - Main debit account for audit found at {i}")
                    elif 'кт' in col_str and 'с/ка' in col_str and 'аналитична' not in col_str:
                        print(f"[DEBUG] AJUR parser - Found MAIN credit column (Кт с/ка) by name: {col_name} at index {i}")
                        column_map['credit'] = i
                        # This is the EXACT main account (Кт с/ка) column that we need to audit
                        print(f"[DEBUG] AJUR parser - Main credit account for audit found at {i}")
                    elif 'дата' in col_str and 'рег' in col_str:  # "Дата рег." column
                        print(f"[DEBUG] AJUR parser - Found registration date column by name: {col_name} at index {i}")
                        column_map['date'] = i
                    elif 'дата' in col_str:
                        # Only set date if we haven't found a more specific one
                        if column_map['date'] is None:
                            print(f"[DEBUG] AJUR parser - Found date column by name: {col_name} at index {i}")
                            column_map['date'] = i
                    elif 'вид' in col_str and 'док' in col_str:
                        print(f"[DEBUG] AJUR parser - Found doc_type column by name: {col_name} at index {i}")
                        column_map['doc_type'] = i
                    elif 'документ' in col_str and ('no' in col_str or 'дата' in col_str):
                        print(f"[DEBUG] AJUR parser - Found doc_number column by name: {col_name} at index {i}")
                        column_map['doc_number'] = i
                    elif 'аналитична' in col_str and 'сметка' in col_str:
                        # If we've already found the debit account but not the analytical debit
                        if column_map['debit'] is not None and column_map['analytical_debit'] is None and i > column_map['debit']:
                            print(f"[DEBUG] AJUR parser - Found analytical_debit column by name: {col_name} at index {i}")
                            column_map['analytical_debit'] = i
                        # If we've already found the credit account but not the analytical credit
                        elif column_map['credit'] is not None and column_map['analytical_credit'] is None and i > column_map['credit']:
                            print(f"[DEBUG] AJUR parser - Found analytical_credit column by name: {col_name} at index {i}")
                            column_map['analytical_credit'] = i
                        # If we haven't found analytical_debit yet
                        elif column_map['analytical_debit'] is None:
                            print(f"[DEBUG] AJUR parser - Found analytical_debit column by name: {col_name} at index {i}")
                            column_map['analytical_debit'] = i
                        # If we've found analytical_debit but not analytical_credit
                        elif column_map['analytical_credit'] is None:
                            print(f"[DEBUG] AJUR parser - Found analytical_credit column by name: {col_name} at index {i}")
                            column_map['analytical_credit'] = i
                    elif 'обяснителен' in col_str or 'текст' in col_str:
                        print(f"[DEBUG] AJUR parser - Found description column by name: {col_name} at index {i}")
                        column_map['description'] = i
            
            # If still not found, use hardcoded values for this specific AJUR format from the sample file
            if column_map['amount'] == 0 or column_map['amount'] is None:
                # In the sample file, 'Сума' is at column 24
                if len(df.columns) >= 25:
                    print(f"[DEBUG] AJUR parser - Using default AJUR column mapping for 'Сума' at index 24")
                    column_map['amount'] = 24
                # Fall back to older format if needed
                elif len(df.columns) >= 13:
                    print(f"[DEBUG] AJUR parser - Using older default AJUR column mapping for 'Сума' at index 12")
                    column_map['amount'] = 12
            
            if column_map['debit'] is None:
                # In the sample file, 'Дт с/ка' is at column 6 - THIS IS THE MAIN ACCOUNT COLUMN TO AUDIT
                if len(df.columns) >= 7:
                    print(f"[DEBUG] AJUR parser - Using default AJUR column mapping for main debit 'Дт с/ка' at index 6")
                    column_map['debit'] = 6
                    # Print headers around this position for verification
                    for j in range(max(0, 6-2), min(len(df.columns), 6+3)):
                        print(f"[DEBUG] Column {j}: {df.columns[j]}")
                elif len(df.columns) >= 6:
                    print(f"[DEBUG] AJUR parser - Using older default AJUR column mapping for 'Дт с/ка' at index 5")
                    column_map['debit'] = 5
                
            if column_map['credit'] is None:
                # In the sample file, 'Кт с/ка' is at column 15 - THIS IS THE MAIN ACCOUNT COLUMN TO AUDIT
                if len(df.columns) >= 16:
                    print(f"[DEBUG] AJUR parser - Using default AJUR column mapping for main credit 'Кт с/ка' at index 15")
                    column_map['credit'] = 15
                    # Print headers around this position for verification
                    for j in range(max(0, 15-2), min(len(df.columns), 15+3)):
                        print(f"[DEBUG] Column {j}: {df.columns[j]}")
                elif len(df.columns) >= 9:
                    print(f"[DEBUG] AJUR parser - Using older default AJUR column mapping for 'Кт с/ка' at index 8")
                    column_map['credit'] = 8
                
            if column_map['date'] is None:
                # In the sample file, 'Дата рег.' is at column 2
                if len(df.columns) >= 3:
                    print(f"[DEBUG] AJUR parser - Using default AJUR column mapping for 'Дата рег.' at index 2")
                    column_map['date'] = 2
                elif len(df.columns) >= 2:
                    print(f"[DEBUG] AJUR parser - Using older default AJUR column mapping for 'Дата' at index 1")
                    column_map['date'] = 1
                    
            if column_map['analytical_debit'] is None and len(df.columns) >= 8:
                print(f"[DEBUG] AJUR parser - Using default AJUR column mapping for analytical debit at index 7")
                column_map['analytical_debit'] = 7
                
            if column_map['analytical_credit'] is None and len(df.columns) >= 17:
                print(f"[DEBUG] AJUR parser - Using default AJUR column mapping for analytical credit at index 16")
                column_map['analytical_credit'] = 16
                
            if column_map['doc_type'] is None and len(df.columns) >= 4:
                print(f"[DEBUG] AJUR parser - Using default AJUR column mapping for document type at index 3")
                column_map['doc_type'] = 3
                
            if column_map['doc_number'] is None and len(df.columns) >= 5:
                print(f"[DEBUG] AJUR parser - Using default AJUR column mapping for document number at index 4")
                column_map['doc_number'] = 4
                
            if column_map['description'] is None and len(df.columns) >= 26:
                print(f"[DEBUG] AJUR parser - Using default AJUR column mapping for description at index 25")
                column_map['description'] = 25
            
            # Skip header rows if necessary
            # Detect the start of actual data
            data_start_row = self._find_data_start_row(df, column_map)
            print(f"[DEBUG] AJUR parser - Data start row detected at: {data_start_row}")
            
            if data_start_row > 0:
                df = df.iloc[data_start_row:]
                df = df.reset_index(drop=True)
                print(f"[DEBUG] AJUR parser - After skipping headers, DataFrame shape: {df.shape}")
            
            # Print first few rows to see the structure after header removal
            print(f"[DEBUG] AJUR parser - First 3 rows sample after header removal:")
            for i in range(min(3, len(df))):
                row_values = list(df.iloc[i].values)
                print(f"[DEBUG] Row {i}: {row_values}")
                
                # Print key columns with their indices for easier debugging
                for col_name, col_idx in column_map.items():
                    if col_idx is not None and col_idx < len(row_values):
                        print(f"  {col_name} (col {col_idx}): {row_values[col_idx]}")
            
            operations = []
            
            # Debug logging to inspect first few rows for better understanding
            for i in range(min(3, len(df))):
                try:
                    raw_data = df.iloc[i].to_dict()
                    sanitized = self._sanitize_json_data(raw_data)
                    print(f"[DEBUG] AJUR parser - Row {i} sanitization sample:")
                    
                    # Check for NaN values in original data
                    import math
                    import numpy as np
                    # pandas should be available from the top-level import
                    for key, value in raw_data.items():
                        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                            print(f"  [BEFORE] Found NaN/Inf at key '{key}': {value}")
                        elif pd.isna(value):
                            print(f"  [BEFORE] Found pandas NA at key '{key}': {value}")
                    
                    # Check the sanitized version
                    print(f"  [AFTER] First few sanitized keys: {list(sanitized.keys())[:5]}")
                except Exception as e:
                    print(f"[DEBUG] Error in debug sanitization for row {i}: {e}")
            
            # Process each row
            for idx, row in df.iterrows():
                if idx % 20 == 0:  # Reduce verbosity by only logging every 20th row
                    print(f"[DEBUG] AJUR parser - Processing row {idx} of {len(df)}")
                
                try:
                    # Get values using column map instead of fixed indices
                    amount_idx = column_map.get('amount')
                    debit_idx = column_map.get('debit')
                    credit_idx = column_map.get('credit')
                    
                    # Check for required columns
                    if amount_idx is None or (debit_idx is None and credit_idx is None):
                        if idx == 0:  # Only show this error once
                            print(f"[DEBUG] AJUR parser - Critical columns not detected in the file structure")
                            print(f"[DEBUG] AJUR parser - Column map: {column_map}")
                            
                            # Force use of column 12 for amount, 5 for debit, 8 for credit as last resort
                            if len(row) >= 13:
                                amount_idx = 12  # Сума column in AJUR format
                                debit_idx = 5    # Дт с/ка in AJUR format
                                credit_idx = 8   # Кт с/ка in AJUR format
                                date_idx = 1     # Дата column in AJUR format
                                
                                print(f"[DEBUG] AJUR parser - Forced use of AJUR standard columns: amount=12, debit=5, credit=8")
                            else:
                                continue
                        else:
                            continue
                    
                    # Extract values with index safety checks - FOCUSED ON MAIN ACCOUNTS WE SHOULD AUDIT
                    amount_value = row.iloc[amount_idx] if amount_idx is not None and amount_idx < len(row) else None
                    
                    # Extract Дт с/ка - THE EXACT MAIN DEBIT ACCOUNT WE NEED FOR AUDITING
                    debit_value = None
                    if debit_idx is not None and debit_idx < len(row):
                        debit_value = row.iloc[debit_idx]
                        if idx < 3:  # Debug first few rows
                            print(f"[DEBUG] AJUR parser - Row {idx} extracted from Дт с/ка column ({debit_idx}): '{debit_value}'")
                    
                    # Extract Кт с/ка - THE EXACT MAIN CREDIT ACCOUNT WE NEED FOR AUDITING
                    credit_value = None
                    if credit_idx is not None and credit_idx < len(row):
                        credit_value = row.iloc[credit_idx]
                        if idx < 3:  # Debug first few rows
                            print(f"[DEBUG] AJUR parser - Row {idx} extracted from Кт с/ка column ({credit_idx}): '{credit_value}'")
                    
                    # Log every credit_value for debugging, especially in the first few rows
                    if idx < 10 or idx % 100 == 0:  # Log first 10 rows and then every 100th row
                        print(f"[DEBUG] AJUR parser - Row {idx} Raw values: amount={amount_value}, debit={debit_value}, credit={credit_value}")
                        print(f"[DEBUG] AJUR parser - Credit column index: {credit_idx}, Type: {type(credit_value)}")
                    
                    # Skip rows that don't have amount or both debit and credit accounts
                    if pd.isna(amount_value) or (pd.isna(debit_value) and pd.isna(credit_value)):
                        if idx < 10 or idx % 100 == 0:  # More detailed logging for troubleshooting
                            print(f"[DEBUG] AJUR parser - Skipping row {idx} - missing required data")
                            if pd.isna(amount_value):
                                print(f"[DEBUG] AJUR parser - Amount is NaN/None")
                            if pd.isna(debit_value):
                                print(f"[DEBUG] AJUR parser - Debit is NaN/None")
                            if pd.isna(credit_value):
                                print(f"[DEBUG] AJUR parser - Credit is NaN/None")
                        continue
                    
                    # Extract other fields using the column map
                    date_idx = column_map.get('date')
                    doc_type_idx = column_map.get('doc_type')
                    doc_num_idx = column_map.get('doc_number')
                    analytical_debit_idx = column_map.get('analytical_debit')
                    analytical_credit_idx = column_map.get('analytical_credit')
                    desc_idx = column_map.get('description')
                    
                    # Extract and clean date safely with special handling for document type codes
                    raw_date_value = row.iloc[date_idx] if date_idx is not None and date_idx < len(row) else None
                    
                    # Handle the date conversion with special handling for document type codes
                    operation_date = self.convert_ajur_date(raw_date_value)
                    
                    document_type = self.clean_string(
                        row.iloc[doc_type_idx] if doc_type_idx is not None and doc_type_idx < len(row) else None
                    )
                    
                    document_number = self.clean_string(
                        row.iloc[doc_num_idx] if doc_num_idx is not None and doc_num_idx < len(row) else None
                    )
                    
                    # Process main debit account (Дт с/ка) - CRITICAL FOR AUDITING
                    debit_account = self.clean_string(debit_value)
                    if idx < 5 or idx % 50 == 0:  # Log frequently for verification
                        print(f"[DEBUG] AJUR parser - Row {idx} MAIN DEBIT ACCOUNT (Дт с/ка): '{debit_account}'")
                    
                    analytical_debit = self.clean_string(
                        row.iloc[analytical_debit_idx] if analytical_debit_idx is not None and analytical_debit_idx < len(row) else None
                    )
                    
                    # Process main credit account (Кт с/ка) - CRITICAL FOR AUDITING
                    credit_account = self.clean_string(credit_value)
                    if idx < 5 or idx % 50 == 0:  # Log frequently for verification
                        print(f"[DEBUG] AJUR parser - Row {idx} MAIN CREDIT ACCOUNT (Кт с/ка): '{credit_account}'")
                    
                    analytical_credit = self.clean_string(
                        row.iloc[analytical_credit_idx] if analytical_credit_idx is not None and analytical_credit_idx < len(row) else None
                    )
                    
                    # Extract amount with special handling
                    amount = None
                    try:
                        # First try direct conversion if it's a number
                        if isinstance(amount_value, (int, float)) and not pd.isna(amount_value):
                            amount = float(amount_value)
                            if idx % 100 == 0:
                                print(f"[DEBUG] AJUR parser - Extracted numeric amount: {amount}")
                        # Then try string cleaning if it's a string
                        elif isinstance(amount_value, str) and amount_value.strip():
                            # Check for [object Object] which indicates corrupted data
                            if '[object Object]' in amount_value:
                                print(f"[DEBUG] AJUR parser - Found [object Object] in amount field, row {idx}")
                                # Try to recover from previous valid rows with similar accounts
                                amount = self._recover_corrupted_amount(df, idx, debit_account, credit_account, amount_idx, column_map)
                            else:
                                # Regular string cleaning
                                cleaned = amount_value.replace(' ', '').replace(',', '.').strip()
                                if cleaned and any(c.isdigit() for c in cleaned):
                                    try:
                                        amount = float(cleaned)
                                        if idx % 100 == 0:
                                            print(f"[DEBUG] AJUR parser - Extracted amount from string: {amount}")
                                    except ValueError:
                                        pass
                        
                        # Fall back to the clean_numeric method
                        if amount is None:
                            amount = self.clean_numeric(amount_value)
                            
                        # Apply a sanity check - amount should be reasonably large for a financial transaction
                        # This helps filter out row numbers mistakenly identified as amounts, but allow small valid transactions
                        if amount is not None and abs(amount) < 0.001:  # Using absolute value to handle both positive and negative small amounts
                            if idx % 100 == 0:
                                print(f"[DEBUG] AJUR parser - Amount too small, might be a row number: {amount}")
                            amount = None
                    except Exception as e:
                        print(f"[DEBUG] AJUR parser - Error processing amount '{amount_value}': {str(e)}")
                        amount = None
                    
                    description = self.clean_string(
                        row.iloc[desc_idx] if desc_idx is not None and desc_idx < len(row) else None
                    )
                    
                    # Skip if we don't have necessary data
                    # Allow operations without a date if we have amount and account info
                    if not amount:
                        print(f"[DEBUG] AJUR parser - Skipping row {idx} - missing amount")
                        continue
                    
                    # Ensure we have at least one account
                    if not debit_account and not credit_account:
                        print(f"[DEBUG] AJUR parser - Skipping row {idx} - missing both debit and credit accounts")
                        continue
                        
                    # If operation date is missing but we have account info and amount,
                    # try to use a fallback date or current date
                    if not operation_date:
                        print(f"[DEBUG] AJUR parser - Row {idx} is missing date but has account info - using fallback date")
                        # Try to extract date from document type if it contains a date format (e.g., "/ 31.12.2023")
                        if document_type and "/" in document_type:
                            parts = document_type.split("/")
                            if len(parts) >= 2:
                                date_part = parts[1].strip()
                                try:
                                    operation_date = datetime.strptime(date_part, "%d.%m.%Y").date()
                                    print(f"[DEBUG] AJUR parser - Extracted date {operation_date} from document type")
                                except:
                                    # If extraction fails, continue with other methods
                                    pass
                        
                        # If still no date, try to extract from document number if available
                        if not operation_date and document_number and "/" in document_number:
                            date_part = document_number.split("/")[1].strip()
                            try:
                                operation_date = datetime.strptime(date_part, "%d.%m.%Y").date()
                                print(f"[DEBUG] AJUR parser - Extracted date {operation_date} from document number")
                            except:
                                # If extraction fails, use current date as fallback
                                operation_date = datetime.now().date()
                                print(f"[DEBUG] AJUR parser - Using current date {operation_date} as fallback")
                        # If still no date, use current date as fallback
                        elif not operation_date:
                            operation_date = datetime.now().date()
                            print(f"[DEBUG] AJUR parser - Using current date {operation_date} as fallback")
                except Exception as extract_error:
                    print(f"[DEBUG] AJUR parser - Error extracting data from row {idx}: {str(extract_error)}")
                    continue
                
                # Create operation dictionary
                # First convert row to dictionary, then sanitize to handle NaN values
                row_dict = row.to_dict()
                sanitized_raw_data = self._sanitize_json_data(row_dict)
                
                # Extract sequence number if available
                # Check if there's a column that might contain sequence numbers
                sequence_number = None
                # In many accounting files, the first column contains a sequence number
                if 0 in row_dict and isinstance(row_dict[0], (int, float)) and not pd.isna(row_dict[0]):
                    try:
                        sequence_number = int(row_dict[0])
                    except (ValueError, TypeError):
                        sequence_number = None
                
                # Extract structured analytical account data
                analytical_debit_structured = self._parse_analytical_account(analytical_debit)
                analytical_credit_structured = self._parse_analytical_account(analytical_credit)
                
                # Extract partner name from analytical account data
                partner_name = None
                # First try to get partner from credit analytical account
                if analytical_credit_structured and 'entity_name' in analytical_credit_structured:
                    partner_name = analytical_credit_structured['entity_name']
                # If not found, try from debit analytical account
                elif analytical_debit_structured and 'entity_name' in analytical_debit_structured:
                    partner_name = analytical_debit_structured['entity_name']
                
                # Enhance description by including more context from analytical accounts
                enhanced_description = description
                if not enhanced_description and (analytical_debit or analytical_credit):
                    # If no description but we have analytical data, create a description
                    if analytical_debit_structured and 'description' in analytical_debit_structured:
                        enhanced_description = analytical_debit_structured['description']
                    elif analytical_credit_structured and 'description' in analytical_credit_structured:
                        enhanced_description = analytical_credit_structured['description']
                
                # Extract audit-specific fields if available
                audit_finding = None
                deviation = None
                control_action = None
                control_finding = None
                
                audit_finding_idx = column_map.get('audit_finding')
                if audit_finding_idx is not None and audit_finding_idx < len(row):
                    audit_finding = self.clean_string(row.iloc[audit_finding_idx])
                    
                deviation_idx = column_map.get('deviation')
                if deviation_idx is not None and deviation_idx < len(row):
                    deviation = self.clean_string(row.iloc[deviation_idx])
                    
                control_action_idx = column_map.get('control_action')
                if control_action_idx is not None and control_action_idx < len(row):
                    control_action = self.clean_string(row.iloc[control_action_idx])
                    
                control_finding_idx = column_map.get('control_finding')
                if control_finding_idx is not None and control_finding_idx < len(row):
                    control_finding = self.clean_string(row.iloc[control_finding_idx])
                
                # Simplified operation data with only essential fields supported by AccountingOperation model
                operation = {
                    "file_id": file_id,
                    "operation_date": operation_date,
                    "document_type": document_type,
                    "document_number": document_number,
                    "debit_account": debit_account,
                    "credit_account": credit_account,
                    "amount": amount,
                    "description": enhanced_description,
                    "partner_name": partner_name,
                    "analytical_debit": analytical_debit,
                    "analytical_credit": analytical_credit,
                    "analytical_debit_structured": analytical_debit_structured,
                    "analytical_credit_structured": analytical_credit_structured,
                    "template_type": "ajur",
                    "raw_data": sanitized_raw_data,
                    "import_uuid": import_uuid,
                    "sequence_number": sequence_number
                }
                
                operations.append(operation)
                if idx < 10 or len(operations) % 20 == 0:
                    print(f"[DEBUG] AJUR parser - Successfully added operation from row {idx}")
                    print(f"[DEBUG] AJUR parser - Operation details: Date={operation_date}, Type={document_type}, Amount={amount}")
                    print(f"[DEBUG] AJUR parser - Accounts: Debit={debit_account}, Credit={credit_account}")
            
            print(f"[DEBUG] AJUR parser - Total operations extracted: {len(operations)}")
            return operations
            
        except Exception as e:
            print(f"Error parsing AJUR Excel file: {e}")
            return []
    
    def parse_memory(self, file_obj: BytesIO, file_id: int, import_uuid: str = None) -> List[Dict[str, Any]]:
        """
        Parse the AJUR Excel file from memory and extract accounting operations
        
        Args:
            file_obj: BytesIO object containing the Excel file
            file_id: ID of the uploaded file in the database
            import_uuid: UUID of the import batch this file belongs to
            
        Returns:
            List of dictionaries containing accounting operations data
        """
        try:
            # Reset file pointer to beginning
            file_obj.seek(0)
            
            # Read Excel file from memory
            df = pd.read_excel(file_obj)
            
            # First, print the original dataframe columns and sample data to understand structure
            print(f"[DEBUG] AJUR parser (memory) - Original DataFrame columns: {list(df.columns)}")
            print(f"[DEBUG] AJUR parser (memory) - Original DataFrame shape: {df.shape}")
            print(f"[DEBUG] AJUR parser (memory) - First 3 rows sample (original):")
            for i in range(min(3, len(df))):
                print(f"[DEBUG] Original Row {i}: {list(df.iloc[i].values)}")
            
            # Try to identify the table structure
            column_map = self._detect_columns(df)
            print(f"[DEBUG] AJUR parser (memory) - Detected column mapping: {column_map}")
            
            # Force correct credit column for AJUR format if incorrectly detected
            if column_map.get('credit') == 16 and len(df.columns) > 15:
                # Check if column 15 is actually "Кт с/ка"
                if 'кт' in str(df.columns[15]).lower() and 'с/ка' in str(df.columns[15]).lower():
                    print(f"[DEBUG] AJUR parser (memory) - Correcting credit column from 16 to 15")
                    column_map['credit'] = 15
            
            # Fix column detection based on actual column names for AJUR format
            if column_map['amount'] == 0 or column_map['amount'] is None:
                print(f"[DEBUG] AJUR parser (memory) - Trying to find better column mappings from DataFrame columns")
                # Check for specific column names that match the AJUR format
                for i, col_name in enumerate(df.columns):
                    col_str = str(col_name).lower().strip()
                    
                    # Specific checks for the Ajur format in the sample file
                    if 'потр' in col_str:  # "Потр." column (user/operator identifier)
                        print(f"[DEBUG] AJUR parser (memory) - Found operator column by name: {col_name} at index {i}")
                        column_map['operator'] = i
                    elif 'опер' in col_str and ('no' in col_str or '№' in col_str):  # "Опер. No" column
                        print(f"[DEBUG] AJUR parser (memory) - Found operation number column by name: {col_name} at index {i}")
                        column_map['operation_number'] = i
                        column_map['sequence_number'] = i  # Also use it as sequence number
                    elif 'сума' in col_str:
                        print(f"[DEBUG] AJUR parser (memory) - Found amount column by name: {col_name} at index {i}")
                        column_map['amount'] = i
                    elif 'дт' in col_str and 'с/ка' in col_str and 'аналитична' not in col_str:
                        print(f"[DEBUG] AJUR parser (memory) - Found MAIN debit column (Дт с/ка) by name: {col_name} at index {i}")
                        column_map['debit'] = i
                        # This is the EXACT main account (Дт с/ка) column that we need to audit
                        print(f"[DEBUG] AJUR parser (memory) - Main debit account for audit found at {i}")
                    elif 'кт' in col_str and 'с/ка' in col_str and 'аналитична' not in col_str:
                        print(f"[DEBUG] AJUR parser (memory) - Found MAIN credit column (Кт с/ка) by name: {col_name} at index {i}")
                        column_map['credit'] = i
                        # This is the EXACT main account (Кт с/ка) column that we need to audit
                        print(f"[DEBUG] AJUR parser (memory) - Main credit account for audit found at {i}")
                    elif 'дата' in col_str and 'рег' in col_str:  # "Дата рег." column
                        print(f"[DEBUG] AJUR parser (memory) - Found registration date column by name: {col_name} at index {i}")
                        column_map['date'] = i
                    elif 'дата' in col_str:
                        # Only set date if we haven't found a more specific one
                        if column_map['date'] is None:
                            print(f"[DEBUG] AJUR parser (memory) - Found date column by name: {col_name} at index {i}")
                            column_map['date'] = i
                    elif 'вид' in col_str and 'док' in col_str:
                        print(f"[DEBUG] AJUR parser (memory) - Found doc_type column by name: {col_name} at index {i}")
                        column_map['doc_type'] = i
                    elif 'документ' in col_str and ('no' in col_str or 'дата' in col_str):
                        print(f"[DEBUG] AJUR parser (memory) - Found doc_number column by name: {col_name} at index {i}")
                        column_map['doc_number'] = i
                    elif 'аналитична' in col_str and 'сметка' in col_str:
                        # If we've already found the debit account but not the analytical debit
                        if column_map['debit'] is not None and column_map['analytical_debit'] is None and i > column_map['debit']:
                            print(f"[DEBUG] AJUR parser (memory) - Found analytical_debit column by name: {col_name} at index {i}")
                            column_map['analytical_debit'] = i
                        # If we've already found the credit account but not the analytical credit
                        elif column_map['credit'] is not None and column_map['analytical_credit'] is None and i > column_map['credit']:
                            print(f"[DEBUG] AJUR parser (memory) - Found analytical_credit column by name: {col_name} at index {i}")
                            column_map['analytical_credit'] = i
                        # If we haven't found analytical_debit yet
                        elif column_map['analytical_debit'] is None:
                            print(f"[DEBUG] AJUR parser (memory) - Found analytical_debit column by name: {col_name} at index {i}")
                            column_map['analytical_debit'] = i
                        # If we've found analytical_debit but not analytical_credit
                        elif column_map['analytical_credit'] is None:
                            print(f"[DEBUG] AJUR parser (memory) - Found analytical_credit column by name: {col_name} at index {i}")
                            column_map['analytical_credit'] = i
                    elif 'обяснителен' in col_str or 'текст' in col_str:
                        print(f"[DEBUG] AJUR parser (memory) - Found description column by name: {col_name} at index {i}")
                        column_map['description'] = i
            
            # If still not found, use hardcoded values for this specific AJUR format from the sample file
            if column_map['amount'] == 0 or column_map['amount'] is None:
                # In the sample file, 'Сума' is at column 24
                if len(df.columns) >= 25:
                    print(f"[DEBUG] AJUR parser (memory) - Using default AJUR column mapping for 'Сума' at index 24")
                    column_map['amount'] = 24
                # Fall back to older format if needed
                elif len(df.columns) >= 13:
                    print(f"[DEBUG] AJUR parser (memory) - Using older default AJUR column mapping for 'Сума' at index 12")
                    column_map['amount'] = 12
            
            if column_map['debit'] is None:
                # In the sample file, 'Дт с/ка' is at column 6
                if len(df.columns) >= 7:
                    print(f"[DEBUG] AJUR parser (memory) - Using default AJUR column mapping for 'Дт с/ка' at index 6")
                    column_map['debit'] = 6
                elif len(df.columns) >= 6:
                    print(f"[DEBUG] AJUR parser (memory) - Using older default AJUR column mapping for 'Дт с/ка' at index 5")
                    column_map['debit'] = 5
                
            if column_map['credit'] is None:
                # In the sample file, 'Кт с/ка' is at column 15
                if len(df.columns) >= 16:
                    print(f"[DEBUG] AJUR parser (memory) - Using default AJUR column mapping for 'Кт с/ка' at index 15")
                    column_map['credit'] = 15
                elif len(df.columns) >= 9:
                    print(f"[DEBUG] AJUR parser (memory) - Using older default AJUR column mapping for 'Кт с/ка' at index 8")
                    column_map['credit'] = 8
                
            if column_map['date'] is None:
                # In the sample file, 'Дата рег.' is at column 2
                if len(df.columns) >= 3:
                    print(f"[DEBUG] AJUR parser (memory) - Using default AJUR column mapping for 'Дата рег.' at index 2")
                    column_map['date'] = 2
                elif len(df.columns) >= 2:
                    print(f"[DEBUG] AJUR parser (memory) - Using older default AJUR column mapping for 'Дата' at index 1")
                    column_map['date'] = 1
                    
            if column_map['analytical_debit'] is None and len(df.columns) >= 8:
                print(f"[DEBUG] AJUR parser (memory) - Using default AJUR column mapping for analytical debit at index 7")
                column_map['analytical_debit'] = 7
                
            if column_map['analytical_credit'] is None and len(df.columns) >= 17:
                print(f"[DEBUG] AJUR parser (memory) - Using default AJUR column mapping for analytical credit at index 16")
                column_map['analytical_credit'] = 16
                
            if column_map['doc_type'] is None and len(df.columns) >= 4:
                print(f"[DEBUG] AJUR parser (memory) - Using default AJUR column mapping for document type at index 3")
                column_map['doc_type'] = 3
                
            if column_map['doc_number'] is None and len(df.columns) >= 5:
                print(f"[DEBUG] AJUR parser (memory) - Using default AJUR column mapping for document number at index 4")
                column_map['doc_number'] = 4
                
            if column_map['description'] is None and len(df.columns) >= 26:
                print(f"[DEBUG] AJUR parser (memory) - Using default AJUR column mapping for description at index 25")
                column_map['description'] = 25
            
            # Skip header rows if necessary
            # Detect the start of actual data
            data_start_row = self._find_data_start_row(df, column_map)
            print(f"[DEBUG] AJUR parser (memory) - Data start row detected at: {data_start_row}")
            
            if data_start_row > 0:
                df = df.iloc[data_start_row:]
                df = df.reset_index(drop=True)
                print(f"[DEBUG] AJUR parser (memory) - After skipping headers, DataFrame shape: {df.shape}")
            
            # Print first few rows to see the structure after header removal
            print(f"[DEBUG] AJUR parser (memory) - First 3 rows sample after header removal:")
            for i in range(min(3, len(df))):
                row_values = list(df.iloc[i].values)
                print(f"[DEBUG] Row {i}: {row_values}")
                
                # Print key columns with their indices for easier debugging
                for col_name, col_idx in column_map.items():
                    if col_idx is not None and col_idx < len(row_values):
                        print(f"  {col_name} (col {col_idx}): {row_values[col_idx]}")
            
            operations = []
            
            # Debug logging to inspect first few rows for better understanding
            for i in range(min(3, len(df))):
                try:
                    raw_data = df.iloc[i].to_dict()
                    sanitized = self._sanitize_json_data(raw_data)
                    print(f"[DEBUG] AJUR parser (memory) - Row {i} sanitization sample:")
                    
                    # Check for NaN values in original data
                    import math
                    import numpy as np
                    # pandas should be available from the top-level import
                    for key, value in raw_data.items():
                        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                            print(f"  [BEFORE] Found NaN/Inf at key '{key}': {value}")
                        elif pd.isna(value):
                            print(f"  [BEFORE] Found pandas NA at key '{key}': {value}")
                    
                    # Check the sanitized version
                    print(f"  [AFTER] First few sanitized keys: {list(sanitized.keys())[:5]}")
                except Exception as e:
                    print(f"[DEBUG] Error in debug sanitization for row {i}: {e}")
                    
            # Process each row
            for idx, row in df.iterrows():
                if idx % 20 == 0:  # Reduce verbosity by only logging every 20th row
                    print(f"[DEBUG] AJUR parser (memory) - Processing row {idx} of {len(df)}")
                
                try:
                    # Get values using column map instead of fixed indices
                    amount_idx = column_map.get('amount')
                    debit_idx = column_map.get('debit')
                    credit_idx = column_map.get('credit')
                    
                    # Check for required columns
                    if amount_idx is None or (debit_idx is None and credit_idx is None):
                        if idx == 0:  # Only show this error once
                            print(f"[DEBUG] AJUR parser (memory) - Critical columns not detected in the file structure")
                            print(f"[DEBUG] AJUR parser (memory) - Column map: {column_map}")
                            
                            # Force use of column 12 for amount, 5 for debit, 8 for credit as last resort
                            if len(row) >= 13:
                                amount_idx = 12  # Сума column in AJUR format
                                debit_idx = 5    # Дт с/ка in AJUR format
                                credit_idx = 8   # Кт с/ка in AJUR format
                                date_idx = 1     # Дата column in AJUR format
                                
                                print(f"[DEBUG] AJUR parser (memory) - Forced use of AJUR standard columns: amount=12, debit=5, credit=8")
                            else:
                                continue
                        else:
                            continue
                    
                    # Extract values with index safety checks
                    amount_value = row.iloc[amount_idx] if amount_idx is not None and amount_idx < len(row) else None
                    debit_value = row.iloc[debit_idx] if debit_idx is not None and debit_idx < len(row) else None
                    credit_value = row.iloc[credit_idx] if credit_idx is not None and credit_idx < len(row) else None
                    
                    # Log every credit_value for debugging, especially in the first few rows
                    if idx < 10 or idx % 100 == 0:  # Log first 10 rows and then every 100th row
                        print(f"[DEBUG] AJUR parser (memory) - Row {idx} Raw values: amount={amount_value}, debit={debit_value}, credit={credit_value}")
                        print(f"[DEBUG] AJUR parser (memory) - Credit column index: {credit_idx}, Type: {type(credit_value)}")
                    
                    # Skip rows that don't have amount or both debit and credit accounts
                    if pd.isna(amount_value) or (pd.isna(debit_value) and pd.isna(credit_value)):
                        if idx < 10 or idx % 100 == 0:  # More detailed logging for troubleshooting
                            print(f"[DEBUG] AJUR parser (memory) - Skipping row {idx} - missing required data")
                            if pd.isna(amount_value):
                                print(f"[DEBUG] AJUR parser (memory) - Amount is NaN/None")
                            if pd.isna(debit_value):
                                print(f"[DEBUG] AJUR parser (memory) - Debit is NaN/None")
                            if pd.isna(credit_value):
                                print(f"[DEBUG] AJUR parser (memory) - Credit is NaN/None")
                        continue
                    
                    # Extract other fields using the column map
                    date_idx = column_map.get('date')
                    doc_type_idx = column_map.get('doc_type')
                    doc_num_idx = column_map.get('doc_number')
                    analytical_debit_idx = column_map.get('analytical_debit')
                    analytical_credit_idx = column_map.get('analytical_credit')
                    desc_idx = column_map.get('description')
                    
                    # Extract and clean date safely with special handling for document type codes
                    raw_date_value = row.iloc[date_idx] if date_idx is not None and date_idx < len(row) else None
                    
                    # Handle the date conversion with special handling for document type codes
                    operation_date = self.convert_ajur_date(raw_date_value)
                    
                    document_type = self.clean_string(
                        row.iloc[doc_type_idx] if doc_type_idx is not None and doc_type_idx < len(row) else None
                    )
                    
                    document_number = self.clean_string(
                        row.iloc[doc_num_idx] if doc_num_idx is not None and doc_num_idx < len(row) else None
                    )
                    
                    debit_account = self.clean_string(debit_value)
                    
                    analytical_debit = self.clean_string(
                        row.iloc[analytical_debit_idx] if analytical_debit_idx is not None and analytical_debit_idx < len(row) else None
                    )
                    
                    # Process main credit account (Кт с/ка) - CRITICAL FOR AUDITING
                    credit_account = self.clean_string(credit_value)
                    if idx < 5 or idx % 50 == 0:  # Log frequently for verification
                        print(f"[DEBUG] AJUR parser (memory) - Row {idx} MAIN CREDIT ACCOUNT (Кт с/ка): '{credit_account}'")
                    
                    analytical_credit = self.clean_string(
                        row.iloc[analytical_credit_idx] if analytical_credit_idx is not None and analytical_credit_idx < len(row) else None
                    )
                    
                    # Extract amount with special handling
                    amount = None
                    try:
                        # First try direct conversion if it's a number
                        if isinstance(amount_value, (int, float)) and not pd.isna(amount_value):
                            amount = float(amount_value)
                            if idx % 100 == 0:
                                print(f"[DEBUG] AJUR parser (memory) - Extracted numeric amount: {amount}")
                        # Then try string cleaning if it's a string
                        elif isinstance(amount_value, str) and amount_value.strip():
                            # Check for [object Object] which indicates corrupted data
                            if '[object Object]' in amount_value:
                                print(f"[DEBUG] AJUR parser (memory) - Found [object Object] in amount field, row {idx}")
                                # Try to recover from previous valid rows with similar accounts
                                amount = self._recover_corrupted_amount(df, idx, debit_account, credit_account, amount_idx, column_map)
                            else:
                                # Regular string cleaning
                                cleaned = amount_value.replace(' ', '').replace(',', '.').strip()
                                if cleaned and any(c.isdigit() for c in cleaned):
                                    try:
                                        amount = float(cleaned)
                                        if idx % 100 == 0:
                                            print(f"[DEBUG] AJUR parser (memory) - Extracted amount from string: {amount}")
                                    except ValueError:
                                        pass
                        
                        # Fall back to the clean_numeric method
                        if amount is None:
                            amount = self.clean_numeric(amount_value)
                            
                        # Apply a sanity check - amount should be reasonably large for a financial transaction
                        # This helps filter out row numbers mistakenly identified as amounts, but allow small valid transactions
                        if amount is not None and abs(amount) < 0.001:  # Using absolute value to handle both positive and negative small amounts
                            if idx % 100 == 0:
                                print(f"[DEBUG] AJUR parser (memory) - Amount too small, might be a row number: {amount}")
                            amount = None
                    except Exception as e:
                        print(f"[DEBUG] AJUR parser (memory) - Error processing amount '{amount_value}': {str(e)}")
                        amount = None
                    
                    description = self.clean_string(
                        row.iloc[desc_idx] if desc_idx is not None and desc_idx < len(row) else None
                    )
                    
                    # Skip if we don't have necessary data
                    # Allow operations without a date if we have amount and account info
                    if not amount:
                        print(f"[DEBUG] AJUR parser (memory) - Skipping row {idx} - missing amount")
                        continue
                    
                    # Ensure we have at least one account
                    if not debit_account and not credit_account:
                        print(f"[DEBUG] AJUR parser (memory) - Skipping row {idx} - missing both debit and credit accounts")
                        continue
                        
                    # If operation date is missing but we have account info and amount,
                    # try to use a fallback date or current date
                    if not operation_date:
                        print(f"[DEBUG] AJUR parser (memory) - Row {idx} is missing date but has account info - using fallback date")
                        # Try to extract date from document type if it contains a date format (e.g., "/ 31.12.2023")
                        if document_type and "/" in document_type:
                            parts = document_type.split("/")
                            if len(parts) >= 2:
                                date_part = parts[1].strip()
                                try:
                                    operation_date = datetime.strptime(date_part, "%d.%m.%Y").date()
                                    print(f"[DEBUG] AJUR parser (memory) - Extracted date {operation_date} from document type")
                                except:
                                    # If extraction fails, continue with other methods
                                    pass
                        
                        # If still no date, try to extract from document number if available
                        if not operation_date and document_number and "/" in document_number:
                            date_part = document_number.split("/")[1].strip()
                            try:
                                operation_date = datetime.strptime(date_part, "%d.%m.%Y").date()
                                print(f"[DEBUG] AJUR parser (memory) - Extracted date {operation_date} from document number")
                            except:
                                # If extraction fails, use current date as fallback
                                operation_date = datetime.now().date()
                                print(f"[DEBUG] AJUR parser (memory) - Using current date {operation_date} as fallback")
                        # If still no date, use current date as fallback
                        elif not operation_date:
                            operation_date = datetime.now().date()
                            print(f"[DEBUG] AJUR parser (memory) - Using current date {operation_date} as fallback")
                except Exception as extract_error:
                    print(f"[DEBUG] AJUR parser (memory) - Error extracting data from row {idx}: {str(extract_error)}")
                    continue
                
                # Create operation dictionary
                # First convert row to dictionary, then sanitize to handle NaN values
                row_dict = row.to_dict()
                sanitized_raw_data = self._sanitize_json_data(row_dict)
                
                # Extract sequence number if available
                # Check if there's a column that might contain sequence numbers
                sequence_number = None
                # In many accounting files, the first column contains a sequence number
                if 0 in row_dict and isinstance(row_dict[0], (int, float)) and not pd.isna(row_dict[0]):
                    try:
                        sequence_number = int(row_dict[0])
                    except (ValueError, TypeError):
                        sequence_number = None
                
                # Extract structured analytical account data
                analytical_debit_structured = self._parse_analytical_account(analytical_debit)
                analytical_credit_structured = self._parse_analytical_account(analytical_credit)
                
                # Extract audit-specific fields if available
                audit_finding = None
                deviation = None
                control_action = None
                control_finding = None
                
                audit_finding_idx = column_map.get('audit_finding')
                if audit_finding_idx is not None and audit_finding_idx < len(row):
                    audit_finding = self.clean_string(row.iloc[audit_finding_idx])
                    
                deviation_idx = column_map.get('deviation')
                if deviation_idx is not None and deviation_idx < len(row):
                    deviation = self.clean_string(row.iloc[deviation_idx])
                    
                control_action_idx = column_map.get('control_action')
                if control_action_idx is not None and control_action_idx < len(row):
                    control_action = self.clean_string(row.iloc[control_action_idx])
                    
                control_finding_idx = column_map.get('control_finding')
                if control_finding_idx is not None and control_finding_idx < len(row):
                    control_finding = self.clean_string(row.iloc[control_finding_idx])
                
                # Extract partner name from analytical account data
                partner_name = None
                # First try to get partner from credit analytical account
                if analytical_credit_structured and 'entity_name' in analytical_credit_structured:
                    partner_name = analytical_credit_structured['entity_name']
                # If not found, try from debit analytical account
                elif analytical_debit_structured and 'entity_name' in analytical_debit_structured:
                    partner_name = analytical_debit_structured['entity_name']
                
                # Enhance description by including more context from analytical accounts
                enhanced_description = description
                if not enhanced_description and (analytical_debit or analytical_credit):
                    # If no description but we have analytical data, create a description
                    if analytical_debit_structured and 'description' in analytical_debit_structured:
                        enhanced_description = analytical_debit_structured['description']
                    elif analytical_credit_structured and 'description' in analytical_credit_structured:
                        enhanced_description = analytical_credit_structured['description']

                # Simplified operation data with only essential fields supported by AccountingOperation model
                operation = {
                    "file_id": file_id,
                    "operation_date": operation_date,
                    "document_type": document_type,
                    "document_number": document_number,
                    "debit_account": debit_account,
                    "credit_account": credit_account,
                    "amount": amount,
                    "description": enhanced_description,
                    "partner_name": partner_name,
                    "analytical_debit": analytical_debit,
                    "analytical_credit": analytical_credit,
                    "analytical_debit_structured": analytical_debit_structured,
                    "analytical_credit_structured": analytical_credit_structured,
                    "template_type": "ajur",
                    "raw_data": sanitized_raw_data,
                    "import_uuid": import_uuid,
                    "sequence_number": sequence_number
                }
                
                operations.append(operation)
                if idx < 10 or len(operations) % 20 == 0:
                    print(f"[DEBUG] AJUR parser (memory) - Successfully added operation from row {idx}")
                    print(f"[DEBUG] AJUR parser (memory) - Operation details: Date={operation_date}, Type={document_type}, Amount={amount}")
                    print(f"[DEBUG] AJUR parser (memory) - Accounts: Debit={debit_account}, Credit={credit_account}")
            
            print(f"[DEBUG] AJUR parser (memory) - Total operations extracted: {len(operations)}")
            return operations
            
        except Exception as e:
            print(f"Error parsing AJUR Excel file from memory: {e}")
            return []
    
    def _detect_ajur_template_type(self, df: pd.DataFrame) -> str:
        """
        Detect the AJUR template type based on column headers and content patterns
        
        Args:
            df: DataFrame with the Excel content
            
        Returns:
            String indicating the template type: "audit", "chronological", or "standard"
        """
        # Check column headers and first few rows
        for i in range(min(10, len(df))):
            row_values = [str(val).lower() if not pd.isna(val) else "" for val in df.iloc[i].values]
            row_text = " ".join(row_values)
            
            # Check for audit-specific columns
            if "установено при одита" in row_text or "тествани на контролни действия" in row_text:
                print(f"[DEBUG] AJUR parser - Detected audit template type at row {i}")
                return "audit"
                
            # Check for chronological format with user/operation columns
            if "потр" in row_text and (("опер" in row_text and "no" in row_text) or ("опер" in row_text and "№" in row_text)):
                print(f"[DEBUG] AJUR parser - Detected chronological template type at row {i}")
                return "chronological"
        
        # If columns count is approximately 18, it's probably audit type
        if len(df.columns) >= 17:
            print(f"[DEBUG] AJUR parser - Detected possible audit template type based on column count ({len(df.columns)})")
            return "audit"
            
        # If columns count is approximately 12, it's probably chronological type
        if len(df.columns) >= 11 and len(df.columns) <= 13:
            print(f"[DEBUG] AJUR parser - Detected possible chronological template type based on column count ({len(df.columns)})")
            return "chronological"
        
        # Default to standard AJUR if can't determine
        print(f"[DEBUG] AJUR parser - Using default standard template type")
        return "standard"
    
    def _detect_columns(self, df: pd.DataFrame) -> Dict[str, Optional[int]]:
        """
        Enhanced column detection for multiple Ajur formats
        Supports both chronological and audit formats with more flexible name matching
        
        Args:
            df: DataFrame with the Excel content
            
        Returns:
            Dictionary mapping column types to their indices
        """
        # Detect template type first
        template_type = self._detect_ajur_template_type(df)
        print(f"[DEBUG] AJUR parser - Working with detected template type: {template_type}")
        
        # Initialize column map with all None values - extended with format-specific columns
        column_map = {
            'doc_type': None,      # Document type column
            'doc_number': None,    # Document number column
            'date': None,          # Date column
            'debit': None,         # Debit account column
            'analytical_debit': None,  # Analytical debit column
            'credit': None,        # Credit account column
            'analytical_credit': None, # Analytical credit column
            'amount': None,        # Amount column
            'description': None,   # Description column
            # Format-specific columns
            'operator': None,           # User/operator ID (chronological format)
            'operation_number': None,   # Operation number (chronological format)
            'sequence_number': None,    # Sequence/row number
            # Audit-specific columns
            'audit_finding': None,      # "Установено при одита"
            'deviation': None,          # "Отклонение"
            'control_action': None,     # "Тествани на контролни действия"
            'control_finding': None,    # "Установено наличие на контролно действие при одита"
            # Additional columns for second format
            'quantity': None,           # "Количество 1 по Кт"
            'measure': None             # "Мярка 1 по Кт"
        }
        
        # First, check column names directly
        for i, col_name in enumerate(df.columns):
            col_str = str(col_name).lower().strip()
            
            # Look for various column identifiers across both formats
            if i == 0 and (col_str == "№" or "потр" in col_str):
                # Either sequence number (№) or operator (Потр) can be in first column
                if "№" in col_str or "no" in col_str or "номер" in col_str:
                    column_map['sequence_number'] = i
                    print(f"[DEBUG] AJUR parser - Found sequence number column at index {i}: {col_str}")
                else:
                    column_map['operator'] = i
                    print(f"[DEBUG] AJUR parser - Found operator column at index {i}: {col_str}")
                    
            elif "опер" in col_str and ("no" in col_str or "№" in col_str):
                column_map['operation_number'] = i
                print(f"[DEBUG] AJUR parser - Found operation number column at index {i}: {col_str}")
                
            elif "сума" in col_str:
                column_map['amount'] = i
                print(f"[DEBUG] AJUR parser - Found amount column at index {i}: {col_str}")
                
            elif ("дт" in col_str or "дебит" in col_str) and "с/ка" in col_str and "аналитична" not in col_str:
                column_map['debit'] = i
                print(f"[DEBUG] AJUR parser - Found debit account column at index {i}: {col_str}")
                
            elif ("кт" in col_str or "кредит" in col_str) and "с/ка" in col_str and "аналитична" not in col_str:
                column_map['credit'] = i
                print(f"[DEBUG] AJUR parser - Found credit account column at index {i}: {col_str}")
                
            elif "аналитична" in col_str and "сметка" in col_str:
                # Determine if this is debit or credit analytical based on position relative to main accounts
                if column_map['debit'] is not None and i > column_map['debit'] and column_map['analytical_debit'] is None:
                    column_map['analytical_debit'] = i
                    print(f"[DEBUG] AJUR parser - Found analytical debit column at index {i}: {col_str}")
                elif column_map['credit'] is not None and i > column_map['credit'] and column_map['analytical_credit'] is None:
                    column_map['analytical_credit'] = i
                    print(f"[DEBUG] AJUR parser - Found analytical credit column at index {i}: {col_str}")
                elif column_map['analytical_debit'] is None:
                    column_map['analytical_debit'] = i
                    print(f"[DEBUG] AJUR parser - Found analytical debit column at index {i}: {col_str}")
                else:
                    column_map['analytical_credit'] = i
                    print(f"[DEBUG] AJUR parser - Found analytical credit column at index {i}: {col_str}")
                    
            elif "дата" in col_str:
                if "рег" in col_str:
                    column_map['date'] = i
                    print(f"[DEBUG] AJUR parser - Found registration date column at index {i}: {col_str}")
                elif column_map['date'] is None:
                    column_map['date'] = i
                    print(f"[DEBUG] AJUR parser - Found date column at index {i}: {col_str}")
                    
            elif ("вид" in col_str and "док" in col_str):
                column_map['doc_type'] = i
                print(f"[DEBUG] AJUR parser - Found document type column at index {i}: {col_str}")
                
            elif "документ" in col_str and ("no" in col_str or "дата" in col_str or "№" in col_str):
                column_map['doc_number'] = i
                print(f"[DEBUG] AJUR parser - Found document number column at index {i}: {col_str}")
                
            elif "обяснителен" in col_str or "текст" in col_str:
                column_map['description'] = i
                print(f"[DEBUG] AJUR parser - Found description column at index {i}: {col_str}")
                
            # Handle audit-specific columns from the second format
            elif "установено при одита" in col_str:
                column_map['audit_finding'] = i
                print(f"[DEBUG] AJUR parser - Found audit finding column at index {i}: {col_str}")
                
            elif "отклонение" in col_str:
                column_map['deviation'] = i
                print(f"[DEBUG] AJUR parser - Found deviation column at index {i}: {col_str}")
                
            elif "тествани" in col_str and "контролни" in col_str:
                column_map['control_action'] = i
                print(f"[DEBUG] AJUR parser - Found control action column at index {i}: {col_str}")
                
            elif "установено наличие" in col_str and "контролно действие" in col_str:
                column_map['control_finding'] = i
                print(f"[DEBUG] AJUR parser - Found control finding column at index {i}: {col_str}")
                
            elif "количество" in col_str:
                column_map['quantity'] = i
                print(f"[DEBUG] AJUR parser - Found quantity column at index {i}: {col_str}")
                
            elif "мярка" in col_str:
                column_map['measure'] = i
                print(f"[DEBUG] AJUR parser - Found measure column at index {i}: {col_str}")
        
        # Check the first 30 rows for potential headers if direct column name check wasn't enough
        if column_map['amount'] is None or column_map['debit'] is None or column_map['credit'] is None:
            print("[DEBUG] AJUR parser - Checking row values for column headers")
            for i in range(min(30, len(df))):
                row_values = [str(val).lower() if not pd.isna(val) else "" for val in df.iloc[i].values]
                
                # Look for column headers by keywords
                for col_idx, val in enumerate(row_values):
                    if not val:  # Skip empty values
                        continue
                        
                    # Check for different column types - common for all formats
                    if any(keyword in val for keyword in ["вид", "тип", "type", "документ"]) and "номер" not in val:
                        column_map['doc_type'] = col_idx
                    elif any(keyword in val for keyword in ["номер", "no.", "number"]):
                        column_map['doc_number'] = col_idx
                    elif any(keyword in val for keyword in ["дата", "date"]):
                        column_map['date'] = col_idx
                    elif ("дебит" in val or "дт" in val or "dt" in val or "debit" in val) and "аналитична" not in val:
                        column_map['debit'] = col_idx
                    elif ("кредит" in val or "кт" in val or "kt" in val or "credit" in val) and "аналитична" not in val:
                        column_map['credit'] = col_idx
                    elif any(keyword in val for keyword in ["сума", "amount", "value", "стойност"]):
                        column_map['amount'] = col_idx
                    elif any(keyword in val for keyword in ["аналитична", "analytics", "analytic"]) and column_map['analytical_debit'] is None:
                        column_map['analytical_debit'] = col_idx
                    elif any(keyword in val for keyword in ["аналитична", "analytics", "analytic"]) and column_map['analytical_debit'] is not None:
                        column_map['analytical_credit'] = col_idx
                    elif any(keyword in val for keyword in ["обяснение", "описание", "description", "details", "основание", "текст"]):
                        column_map['description'] = col_idx
                
                # If we found most of the important columns, consider this a header row
                critical_columns = [column_map['debit'], column_map['credit'], column_map['amount']]
                if sum(1 for col in critical_columns if col is not None) >= 2:
                    print(f"[DEBUG] _detect_columns - Found header row at {i}")
                    break
        
        # Fallback for amount column - try both format positions
        if column_map['amount'] is None:
            print("[DEBUG] _detect_columns - Amount column not found, trying alternative detection")
            # Try to find a column with numeric values that could be amounts
            for col_idx in range(len(df.columns)):
                numeric_count = 0
                total_value = 0
                
                # Skip the first column (usually row numbers)
                if col_idx == 0:
                    continue
                    
                for row_idx in range(min(50, len(df))):
                    try:
                        val = df.iloc[row_idx, col_idx]
                        if isinstance(val, (int, float)) and not pd.isna(val) and val > 0:
                            numeric_count += 1
                            total_value += float(val)
                    except:
                        continue
                
                # If we found a column with several numeric values, it might be the amount
                # But we need to make sure it's not just row numbers
                if numeric_count >= 5:
                    # Calculate average value - should be reasonably large for financial transactions
                    avg_value = total_value / max(1, numeric_count)
                    if avg_value > 50:  # Transactions likely have larger amounts than 50
                        print(f"[DEBUG] _detect_columns - Potential amount column found at index {col_idx}, avg value: {avg_value}")
                        column_map['amount'] = col_idx
                        break
                    else:
                        print(f"[DEBUG] _detect_columns - Column {col_idx} has numeric values but avg ({avg_value}) is too small, likely not amounts")
            
            # Try specific positions for known formats
            if column_map['amount'] is None:
                # Try format 1 position (column ~11)
                amount_candidates = [11, 12]  # Common positions for amount in both formats
                for pos in amount_candidates:
                    if pos < len(df.columns):
                        # Check if column has numeric values
                        numeric_count = sum(1 for i in range(min(10, len(df)))
                                        if isinstance(df.iloc[i, pos], (int, float))
                                        and not pd.isna(df.iloc[i, pos]))
                        if numeric_count >= 3:  # If at least 3 rows have numeric values
                            column_map['amount'] = pos
                            print(f"[DEBUG] AJUR parser - Using fallback amount column at position {pos}")
                            break
        
        # Fallback for account columns
        if column_map['debit'] is None and len(df.columns) >= 7:
            # Try typical positions for debit account
            for pos in [5, 6]:
                if pos < len(df.columns):
                    column_map['debit'] = pos
                    print(f"[DEBUG] AJUR parser - Using fallback debit account column at position {pos}")
                    break
                    
        if column_map['credit'] is None and len(df.columns) >= 9:
            # Try typical positions for credit account
            for pos in [8, 9]:
                if pos < len(df.columns):
                    column_map['credit'] = pos
                    print(f"[DEBUG] AJUR parser - Using fallback credit account column at position {pos}")
                    break
        
        # Handle the case when "[object Object]" values are detected
        # Check a sample of rows for this pattern
        sample_size = min(10, len(df))
        object_placeholders_found = False
        for i in range(sample_size):
            try:
                row = df.iloc[i]
                for col_idx in range(len(row)):
                    cell_value = row.iloc[col_idx]
                    if isinstance(cell_value, str) and '[object Object]' in cell_value:
                        object_placeholders_found = True
                        print(f"[DEBUG] AJUR parser - Found [object Object] placeholder in column {col_idx}, row {i}")
                        # If this is in the amount column, mark it for special handling
                        if column_map['amount'] == col_idx:
                            print(f"[DEBUG] AJUR parser - Amount column contains [object Object] placeholders - will apply special recovery")
            except:
                continue
        
        # Try to infer missing columns based on typical layout
        if all(v is None for v in [column_map['debit'], column_map['credit'], column_map['amount']]):
            print("[DEBUG] _detect_columns - Critical columns not detected, using default AJUR layout")
            # Default Ajur layout (based on typical structure)
            column_map = {
                'doc_type': 0,
                'doc_number': 1,
                'date': 2,
                'debit': 3,
                'analytical_debit': 4,
                'credit': 5,
                'analytical_credit': 6,
                'amount': 7,
                'description': 8
            }
        
        return column_map
        
    def _find_data_start_row(self, df: pd.DataFrame, column_map: Dict[str, Optional[int]] = None) -> int:
        """
        Find the row where actual data starts
        
        Args:
            df: DataFrame with the Excel content
            column_map: Dictionary mapping column types to their indices
            
        Returns:
            Row index where data starts (0-based)
        """
        # If no column_map provided, create a default one
        if column_map is None:
            column_map = {
                'doc_type': 0,
                'doc_number': 1,
                'date': 2,
                'debit': 3,
                'analytical_debit': 4,
                'credit': 5,
                'analytical_credit': 6,
                'amount': 7,
                'description': 8
            }
        
        # Look for rows that contain typical header values
        for i in range(min(30, len(df))):  # Increased to check more rows
            row_values = [str(val).lower() for val in df.iloc[i].values if not pd.isna(val)]
            print(f"[DEBUG] _find_data_start_row - Row {i} values: {row_values}")
            
            # Check if the row contains keywords that suggest it's a header
            header_keywords = ["вид", "номер", "дата", "дебит", "кредит", "аналитична", "сума", "обяснение", "счетоводна", "операция"]
            matches = sum(any(keyword in val for keyword in header_keywords) for val in row_values)
            print(f"[DEBUG] _find_data_start_row - Row {i} keyword matches: {matches}")
            
            if matches >= 3:  # Looking for at least 3 matches
                print(f"[DEBUG] _find_data_start_row - Found header row at {i}, data starts at {i+1}")
                return i + 1  # Data starts in the next row
        
        # Alternative strategy: look for a row where we can find values in our detected columns
        amount_idx = column_map.get('amount')
        debit_idx = column_map.get('debit')
        credit_idx = column_map.get('credit')
        
        if amount_idx is not None:
            for i in range(min(50, len(df))):
                try:
                    # Check if this row has an amount value
                    if amount_idx < len(df.columns):
                        val = df.iloc[i, amount_idx]
                        if not pd.isna(val) and (isinstance(val, (int, float)) or
                                              (isinstance(val, str) and any(c.isdigit() for c in val))):
                            # Check if we also have a debit or credit account
                            has_account = False
                            if debit_idx is not None and debit_idx < len(df.columns):
                                debit_val = df.iloc[i, debit_idx]
                                if not pd.isna(debit_val) and str(debit_val).strip():
                                    has_account = True
                            if not has_account and credit_idx is not None and credit_idx < len(df.columns):
                                credit_val = df.iloc[i, credit_idx]
                                if not pd.isna(credit_val) and str(credit_val).strip():
                                    has_account = True
                            
                            if has_account:
                                print(f"[DEBUG] _find_data_start_row - Found first data row at {i}")
                                return i
                except Exception as e:
                    print(f"[DEBUG] _find_data_start_row - Error checking row {i}: {str(e)}")
                    continue
        
        # Fall back to the original alternative strategy
        print("[DEBUG] _find_data_start_row - Could not find data row using column map, trying fallback strategy")
        for i in range(min(30, len(df))):
            try:
                if len(df.columns) > 7:
                    val = df.iloc[i, 7]  # Check column index 7 (typically amount)
                    if not pd.isna(val) and (isinstance(val, (int, float)) or (isinstance(val, str) and any(c.isdigit() for c in val))):
                        print(f"[DEBUG] _find_data_start_row - Found first row with numeric amount at {i}")
                        return max(0, i-1)  # Start from this row or the one before
            except Exception as e:
                print(f"[DEBUG] _find_data_start_row - Error checking row {i}: {str(e)}")
                continue
        
        print("[DEBUG] _find_data_start_row - Could not determine data start row, assuming 0")
        return 0  # If no header found, assume data starts at row 0
        
    def _sanitize_json_data(self, data):
        """
        Sanitize data to make it JSON-compatible
        
        Args:
            data: Dictionary or JSON string that may contain non-JSON-compatible values
            
        Returns:
            Sanitized dictionary or JSON string with all values compatible with JSON
        """
        import math
        import json
        import numpy as np
        # Use global pandas import
        
        # If data is a string, try to parse it as JSON
        if isinstance(data, str):
            try:
                # Parse the JSON string to a dictionary
                data_dict = json.loads(data)
                # Sanitize the dictionary
                sanitized_dict = self._sanitize_json_dict(data_dict)
                # Convert back to JSON string
                return json.dumps(sanitized_dict)
            except json.JSONDecodeError:
                # If we can't parse as JSON, manually replace NaN values
                return data.replace(': NaN', ': null').replace(':NaN', ':null')
        
        # If data is a dictionary, sanitize it directly
        if isinstance(data, dict):
            return self._sanitize_json_dict(data)
            
        # Return the original data if it's neither a string nor a dictionary
        return data
        
    def _sanitize_json_dict(self, data_dict):
        """
        Sanitize a dictionary to make it JSON-compatible
        
        Args:
            data_dict: Dictionary that may contain non-JSON-compatible values
            
        Returns:
            Sanitized dictionary with all values compatible with JSON
        """
        import math
        import numpy as np
        # Use global pandas import
        
        result = {}
        
        for key, value in data_dict.items():
            # Handle NaN, Infinity and -Infinity values
            if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                result[key] = None
            elif isinstance(value, (np.number, np.bool_)):
                # Convert NumPy types to Python native types
                result[key] = value.item()
            elif pd.isna(value):
                # Handle any other NA/NaN values from pandas
                result[key] = None
            # Recursively handle nested dictionaries
            elif isinstance(value, dict):
                result[key] = self._sanitize_json_dict(value)
            # Handle lists by sanitizing each item
            elif isinstance(value, list):
                result[key] = [self._sanitize_json_dict(item) if isinstance(item, dict) else
                              (None if (isinstance(item, float) and (math.isnan(item) or math.isinf(item))) else item)
                              for item in value]
            else:
                result[key] = value
                
        return result
        
    def convert_ajur_date(self, date_value: Any) -> Optional[datetime.date]:
        """
        Convert AJUR date formats to standard date with special handling for document type codes
        
        Args:
            date_value: Date value in various formats (string, datetime, etc.)
            
        Returns:
            Standardized date object or None if conversion fails
        """
        if pd.isna(date_value):
            return None
            
        # Special handling for known document type codes in Bulgarian accounting
        # These are not dates but document type codes like "МО", "КДИ", "СчС", etc.
        document_type_codes = ["МО", "КДИ", "СчС", "БИ", "ФД", "СчС", "Орд", "Ф-ра"]
        if isinstance(date_value, str) and date_value.strip() in document_type_codes:
            print(f"[DEBUG] AJUR parser - Detected document type code '{date_value}' in date field, treating as non-date")
            # This is a document type code, not a date
            # We'll use current date as a fallback or try to extract date from other fields
            return None
                
        # If not a document type code, proceed with normal date conversion
        try:
            if isinstance(date_value, datetime):
                return date_value.date()
            elif isinstance(date_value, str):
                # Try different date formats common in Bulgarian accounting
                for fmt in ["%d.%m.%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"]:
                    try:
                        return datetime.strptime(date_value, fmt).date()
                    except ValueError:
                        continue
                
                # If we got here, none of our specific formats matched
                # Try to extract date parts using splitting (for formats like "БИ 2/ 05.01.2023")
                if "/" in date_value:
                    parts = date_value.split("/")
                    if len(parts) >= 2:
                        # Try to parse the second part as a date
                        date_part = parts[1].strip()
                        try:
                            for fmt in ["%d.%m.%Y", "%d-%m-%Y", "%Y-%m-%d"]:
                                try:
                                    return datetime.strptime(date_part, fmt).date()
                                except ValueError:
                                    continue
                        except:
                            pass
                
                # If still no match, try a more flexible approach with pandas
                try:
                    return pd.to_datetime(date_value, errors='raise').date()
                except:
                    # Last resort - return None for this date
                    print(f"[DEBUG] AJUR parser - Could not parse date value: {date_value}")
                    return None
            else:
                # Try using pandas to convert numeric or other types
                try:
                    return pd.to_datetime(date_value, errors='coerce').date()
                except:
                    return None
        except Exception as e:
            print(f"[DEBUG] AJUR parser - Error converting date {date_value}: {e}")
            return None
    
    def _parse_analytical_account(self, analytical_value: str) -> Dict[str, Any]:
        """
        Parse semicolon-delimited analytical account information into structured data
        
        Args:
            analytical_value: String containing the analytical account information
            
        Returns:
            Dictionary with structured analytical account data
        """
        if not analytical_value or pd.isna(analytical_value):
            return {}
            
        parts = analytical_value.split(';')
        result = {}
        
        # Common patterns in analytical data
        if len(parts) >= 2:
            # Code;Description format
            result['code'] = parts[0].strip()
            result['description'] = parts[1].strip()
        
        # AJUR format typically has: ID;PARTNER_NAME;DOC_NUM;DOC_DATE
        # Example: "20;ЕКОНТ Експрес ООД;1220700737;11.01.2023"
        if len(parts) >= 2:
            result['entity_id'] = parts[0].strip()
            result['entity_name'] = parts[1].strip()
            
        if len(parts) >= 4:
            result['document_number'] = parts[2].strip() if parts[2].strip() else None
            result['document_date'] = parts[3].strip() if parts[3].strip() else None
            
        # More specific patterns based on data observed in AJUR files
        if len(parts) >= 6:
            # Format: ID;Name;DocNum;Date;Type;Description
            if 'entity_id' not in result:
                result['entity_id'] = parts[0].strip()
            if 'entity_name' not in result:
                result['entity_name'] = parts[1].strip()
            if 'document_number' not in result:
                result['document_number'] = parts[2].strip() if len(parts) > 2 else None
            if 'document_date' not in result:
                result['document_date'] = parts[3].strip() if len(parts) > 3 else None
            result['type_code'] = parts[4].strip() if len(parts) > 4 else None
            result['type_description'] = parts[5].strip() if len(parts) > 5 else None
        
        return result
    
    def _recover_corrupted_amount(self, df: pd.DataFrame, current_row_idx: int,
                                debit_account: str, credit_account: str, amount_idx: int, column_map: Dict[str, Optional[int]] = None) -> Optional[float]:
        """
        Enhanced recovery of corrupted amount values (e.g. [object Object])
        Supports both Ajur formats with improved recovery strategies
        
        Args:
            df: DataFrame with all operations
            current_row_idx: Index of the current row with corrupted amount
            debit_account: Debit account of the current operation
            credit_account: Credit account of the current operation
            amount_idx: Column index of the amount field
            column_map: Map of column names to indices
            
        Returns:
            Recovered amount value if possible, None otherwise
        """
        print(f"[DEBUG] AJUR parser - Attempting to recover corrupted amount for row {current_row_idx}")
        
        # Strategy 1: Look at nearby rows with similar accounts (expanded search range)
        search_range = 20  # Look at more rows for better matching
        for idx in range(max(0, current_row_idx - search_range), min(current_row_idx + search_range, len(df))):
            if idx == current_row_idx:
                continue  # Skip the current row
                
            try:
                row = df.iloc[idx]
                # Get debit and credit accounts using column map
                debit_idx = column_map.get('debit')
                credit_idx = column_map.get('credit')
                
                if debit_idx is not None and debit_idx < len(row):
                    row_debit = self.clean_string(row.iloc[debit_idx])
                else:
                    continue  # Can't compare accounts
                    
                if credit_idx is not None and credit_idx < len(row):
                    row_credit = self.clean_string(row.iloc[credit_idx])
                else:
                    continue  # Can't compare accounts
                
                # Check if accounts match or are similar
                if row_debit == debit_account and row_credit == credit_account:
                    # Try to get a valid amount from this row
                    if amount_idx < len(row):
                        amount_value = row.iloc[amount_idx]
                        if isinstance(amount_value, (int, float)) and not pd.isna(amount_value):
                            recovered_amount = float(amount_value)
                            print(f"[DEBUG] AJUR parser - Recovered amount {recovered_amount} from row {idx} with matching accounts")
                            return recovered_amount
                        elif isinstance(amount_value, str) and '[object Object]' not in amount_value:
                            # Try to parse string amount
                            cleaned = amount_value.replace(' ', '').replace(',', '.').strip()
                            if cleaned and any(c.isdigit() for c in cleaned):
                                try:
                                    recovered_amount = float(cleaned)
                                    print(f"[DEBUG] AJUR parser - Recovered amount {recovered_amount} from row {idx} with matching accounts")
                                    return recovered_amount
                                except ValueError:
                                    pass
            except Exception as e:
                print(f"[DEBUG] AJUR parser - Error in account matching recovery for row {idx}: {e}")
                continue
        
        # Strategy 2: Scan all numeric columns in the current row
        try:
            row = df.iloc[current_row_idx]
            # Examine each column for potential amount values
            for col_idx in range(len(row)):
                if col_idx == amount_idx:
                    continue  # Skip the corrupted column itself
                    
                cell_value = row.iloc[col_idx]
                # Check numeric values
                if isinstance(cell_value, (int, float)) and not pd.isna(cell_value):
                    # Filter for reasonable financial amounts (avoid very small values that might be row numbers)
                    if abs(float(cell_value)) >= 0.01 and abs(float(cell_value)) < 10000000:
                        recovered_amount = float(cell_value)
                        print(f"[DEBUG] AJUR parser - Recovered amount {recovered_amount} from column {col_idx} in same row")
                        return recovered_amount
                # Check string values that might contain numbers
                elif isinstance(cell_value, str) and any(c.isdigit() for c in cell_value) and '[object Object]' not in cell_value:
                    # Try to parse numeric strings
                    try:
                        # Remove non-numeric chars except decimal point
                        cleaned = ''.join([c for c in cell_value if c.isdigit() or c == '.' or c == ',' or c == '-'])
                        cleaned = cleaned.replace(',', '.').strip()
                        if cleaned and any(c.isdigit() for c in cleaned):
                            value = float(cleaned)
                            if abs(value) >= 0.01 and abs(value) < 10000000:
                                print(f"[DEBUG] AJUR parser - Recovered amount {value} from string in column {col_idx}")
                                return value
                    except ValueError:
                        pass
        except Exception as e:
            print(f"[DEBUG] AJUR parser - Error in column scan recovery: {e}")
        
        # Strategy 3: Handle [object Object] by trying to infer from context
        # This specifically targets the second file format where some cells contain "[object Object]"
        try:
            # Check rows with the same debit and credit account structure
            # but look at a different part of the file (further away)
            for search_distance in [50, 100, 150]:  # Try looking further away
                for direction in [-1, 1]:  # Look both before and after
                    check_idx = current_row_idx + (search_distance * direction)
                    if check_idx < 0 or check_idx >= len(df):
                        continue
                        
                    # Check this distant row
                    distant_row = df.iloc[check_idx]
                    distant_debit = None
                    distant_credit = None
                    
                    # Get debit and credit accounts
                    if column_map.get('debit') is not None and column_map.get('debit') < len(distant_row):
                        distant_debit = self.clean_string(distant_row.iloc[column_map.get('debit')])
                    if column_map.get('credit') is not None and column_map.get('credit') < len(distant_row):
                        distant_credit = self.clean_string(distant_row.iloc[column_map.get('credit')])
                    
                    # Look for similar account structure (first part of account)
                    debit_match = False
                    credit_match = False
                    
                    if distant_debit and debit_account:
                        # Compare by account prefix (e.g., "601/1" matches "601/2")
                        distant_debit_parts = distant_debit.split('/')
                        debit_account_parts = debit_account.split('/')
                        if distant_debit_parts[0] == debit_account_parts[0]:
                            debit_match = True
                            
                    if distant_credit and credit_account:
                        # Compare by account prefix
                        distant_credit_parts = distant_credit.split('/')
                        credit_account_parts = credit_account.split('/')
                        if distant_credit_parts[0] == credit_account_parts[0]:
                            credit_match = True
                    
                    if debit_match and credit_match:
                        # Found similar account structure, check for amount
                        if amount_idx < len(distant_row):
                            amount_value = distant_row.iloc[amount_idx]
                            if isinstance(amount_value, (int, float)) and not pd.isna(amount_value):
                                recovered_amount = float(amount_value)
                                print(f"[DEBUG] AJUR parser - Recovered amount {recovered_amount} from distant row {check_idx} with similar account structure")
                                return recovered_amount
        except Exception as e:
            print(f"[DEBUG] AJUR parser - Error in distant row recovery: {e}")
        
        print(f"[DEBUG] AJUR parser - Could not recover amount for row {current_row_idx}")
        return None