import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime
from io import BytesIO

from app.services.parsers.base_parser import BaseExcelParser


class IncognitaParser(BaseExcelParser):
    """Parser for Incognita Excel format"""
    
    def parse(self, file_path: str, file_id: int, import_uuid: str = None) -> List[Dict[str, Any]]:
        """
        Parse the Incognita Excel file and extract accounting operations
        
        For Incognita format:
        - Headers typically in row 3 with data starting from row 4
        - Key columns: "ДТ Сметка", "КТ Сметка", "A/A", "Дт Сметка описание", 
          "Кт Сметка описание", "Дата", "Ст-Ст в лева", "Док. Номер", 
          "Предмет на доставка", "Контрагент"
        
        Args:
            file_path: Path to the Excel file
            file_id: ID of the uploaded file in the database
            import_uuid: UUID of the import batch this file belongs to
            
        Returns:
            List of dictionaries containing accounting operations data
        """
        try:
            # Read Excel file, skipping the first 2 rows as Incognita typically has headers in row 3
            df = pd.read_excel(file_path, skiprows=2)
            
            print(f"[DEBUG] Incognita parser - Original DataFrame columns: {list(df.columns)}")
            print(f"[DEBUG] Incognita parser - Original DataFrame shape: {df.shape}")
            print(f"[DEBUG] Incognita parser - First 3 rows sample (original):")
            for i in range(min(3, len(df))):
                print(f"[DEBUG] Original Row {i}: {list(df.iloc[i].values)}")
            
            # Detect columns based on Incognita headers
            column_map = self._detect_columns(df)
            
            # Ensure document number is always mapped to column L (index 11)
            column_map['doc_number'] = 11
            
            print(f"[DEBUG] Incognita parser - Detected column mapping: {column_map}")
            print(f"[DEBUG] Incognita parser - Force mapped document number to column L (index 11)")
            
            # Skip any additional header rows if necessary
            data_start_row = self._find_data_start_row(df, column_map)
            print(f"[DEBUG] Incognita parser - Data start row detected at: {data_start_row}")
            
            if data_start_row > 0:
                df = df.iloc[data_start_row:]
                df = df.reset_index(drop=True)
                print(f"[DEBUG] Incognita parser - After skipping headers, DataFrame shape: {df.shape}")
            
            # Print first few rows to see the structure after header removal
            print(f"[DEBUG] Incognita parser - First 3 rows sample after header removal:")
            for i in range(min(3, len(df))):
                row_values = list(df.iloc[i].values)
                print(f"[DEBUG] Row {i}: {row_values}")
                
                # Print key columns with their indices for easier debugging
                for col_name, col_idx in column_map.items():
                    if col_idx is not None and col_idx < len(row_values):
                        print(f"  {col_name} (col {col_idx}): {row_values[col_idx]}")
            
            operations = []
            
            # Process each row
            for idx, row in df.iterrows():
                if idx % 20 == 0:  # Reduce verbosity by only logging every 20th row
                    print(f"[DEBUG] Incognita parser - Processing row {idx} of {len(df)}")
                
                try:
                    # Get values using column map
                    amount_idx = column_map.get('amount')
                    debit_idx = column_map.get('debit')
                    credit_idx = column_map.get('credit')
                    seq_idx = column_map.get('sequence_number')
                    
                    # Check for required columns
                    if amount_idx is None or (debit_idx is None and credit_idx is None):
                        if idx == 0:  # Only show this error once
                            print(f"[DEBUG] Incognita parser - Critical columns not detected in the file structure")
                            print(f"[DEBUG] Incognita parser - Column map: {column_map}")
                            continue
                        else:
                            continue
                    
                    # Extract values with index safety checks
                    amount_value = row.iloc[amount_idx] if amount_idx is not None and amount_idx < len(row) else None
                    debit_value = row.iloc[debit_idx] if debit_idx is not None and debit_idx < len(row) else None
                    credit_value = row.iloc[credit_idx] if credit_idx is not None and credit_idx < len(row) else None
                    seq_value = row.iloc[seq_idx] if seq_idx is not None and seq_idx < len(row) else None
                    
                    # Log values for debugging
                    if idx < 5 or idx % 100 == 0:  # Log first 5 rows and then every 100th row
                        print(f"[DEBUG] Incognita parser - Row {idx} Raw values: sequence={seq_value}, amount={amount_value}, debit={debit_value}, credit={credit_value}")
                    
                    # Skip rows that don't have amount or both debit and credit accounts
                    if pd.isna(amount_value) or (pd.isna(debit_value) and pd.isna(credit_value)):
                        if idx < 5:  # More detailed logging for troubleshooting first few rows
                            print(f"[DEBUG] Incognita parser - Skipping row {idx} - missing required data")
                            if pd.isna(amount_value):
                                print(f"[DEBUG] Incognita parser - Amount is NaN/None")
                            if pd.isna(debit_value):
                                print(f"[DEBUG] Incognita parser - Debit is NaN/None")
                            if pd.isna(credit_value):
                                print(f"[DEBUG] Incognita parser - Credit is NaN/None")
                        continue
                    
                    # Extract other fields using the column map
                    date_idx = column_map.get('date')
                    doc_num_idx = column_map.get('doc_number')
                    analytical_debit_idx = column_map.get('analytical_debit')
                    analytical_credit_idx = column_map.get('analytical_credit')
                    desc_idx = column_map.get('description')
                    partner_idx = column_map.get('partner')
                    
                    # Extract and clean date
                    raw_date_value = row.iloc[date_idx] if date_idx is not None and date_idx < len(row) else None
                    operation_date = self.convert_to_date(raw_date_value)
                    
                    # Try to get document number from the doc_number column if available
                    doc_num_from_column = None
                    if doc_num_idx is not None and doc_num_idx < len(row):
                        raw_doc_num = row.iloc[doc_num_idx]
                        if not pd.isna(raw_doc_num):
                            # Convert to string and clean
                            doc_num_from_column = str(raw_doc_num).strip()
                            # Log the document number from the column
                            if idx < 5:
                                print(f"[DEBUG] Incognita parser - Document number from column: {doc_num_from_column}")
                    
                    # Always use the document number from dedicated column when available
                    document_number = doc_num_from_column
                    
                    # Process debit account and analytical info
                    debit_account_raw = self.clean_string(debit_value)
                    
                    # Use the full account code including sub-accounts
                    debit_account = debit_account_raw
                    
                    # Log the full account code being used
                    if idx < 5:  # Log for first few rows
                        print(f"[DEBUG] Incognita parser - Using full debit account code: {debit_account}")
                    
                    # Get analytical_debit and try to extract document number from it
                    analytical_debit_raw = self.clean_string(
                        row.iloc[analytical_debit_idx] if analytical_debit_idx is not None and analytical_debit_idx < len(row) else None
                    )
                    
                    # Extract document number from analytical_debit only if not found in the dedicated column
                    if document_number is None and analytical_debit_raw and '.' in analytical_debit_raw:
                        parts = analytical_debit_raw.split('.')
                        if parts and parts[0].strip():
                            document_number = parts[0].strip()
                    
                    # Store the full account code with sub-accounts in analytical_debit if it's empty
                    if not analytical_debit_raw and '-' in debit_account_raw:
                        analytical_debit = debit_account_raw
                    else:
                        analytical_debit = analytical_debit_raw
                    
                    # Process credit account and analytical info
                    credit_account_raw = self.clean_string(credit_value)
                    
                    # Use the full account code including sub-accounts
                    credit_account = credit_account_raw
                    
                    # Log the full account code being used
                    if idx < 5:  # Log for first few rows
                        print(f"[DEBUG] Incognita parser - Using full credit account code: {credit_account}")
                    
                    # Get analytical_credit and try to extract document number from it if still not found
                    analytical_credit_raw = self.clean_string(
                        row.iloc[analytical_credit_idx] if analytical_credit_idx is not None and analytical_credit_idx < len(row) else None
                    )
                    
                    # Extract document number from analytical_credit only if not found from other sources
                    if document_number is None and analytical_credit_raw and '.' in analytical_credit_raw:
                        parts = analytical_credit_raw.split('.')
                        if parts and parts[0].strip():
                            document_number = parts[0].strip()
                    
                    # Store the full account code with sub-accounts in analytical_credit if it's empty
                    if not analytical_credit_raw and '-' in credit_account_raw:
                        analytical_credit = credit_account_raw
                    else:
                        analytical_credit = analytical_credit_raw
                    
                    # Extract description and partner
                    description = self.clean_string(
                        row.iloc[desc_idx] if desc_idx is not None and desc_idx < len(row) else None
                    )
                    
                    partner_name = self.clean_string(
                        row.iloc[partner_idx] if partner_idx is not None and partner_idx < len(row) else None
                    )
                    
                    # Extract amount with special handling for different formats
                    amount = None
                    try:
                        # First try direct conversion if it's a number
                        if isinstance(amount_value, (int, float)) and not pd.isna(amount_value):
                            amount = float(amount_value)
                        # Then try string cleaning if it's a string
                        elif isinstance(amount_value, str) and amount_value.strip():
                            # Regular string cleaning for currency values
                            cleaned = amount_value.replace(' ', '').replace(',', '.').strip()
                            if cleaned and any(c.isdigit() for c in cleaned):
                                try:
                                    amount = float(cleaned)
                                except ValueError:
                                    pass
                        
                        # Fall back to the clean_numeric method
                        if amount is None:
                            amount = self.clean_numeric(amount_value)
                            
                    except Exception as e:
                        print(f"[DEBUG] Incognita parser - Error processing amount '{amount_value}': {str(e)}")
                        amount = None
                    
                    # Skip if we don't have necessary data
                    if not amount:
                        print(f"[DEBUG] Incognita parser - Skipping row {idx} - missing amount")
                        continue
                    
                    # Ensure we have at least one account
                    if not debit_account and not credit_account:
                        print(f"[DEBUG] Incognita parser - Skipping row {idx} - missing both debit and credit accounts")
                        continue
                    
                    # If operation date is missing but we have account info and amount,
                    # use current date as fallback
                    if not operation_date:
                        print(f"[DEBUG] Incognita parser - Row {idx} is missing date but has account info - using fallback date")
                        operation_date = datetime.now().date()
                        
                    # Extract sequence number
                    sequence_number = None
                    original_seq_value = None
                    # PostgreSQL integer max value is 2^31-1
                    PG_INTEGER_MAX = 2147483647
                    
                    if seq_value is not None and not pd.isna(seq_value):
                        try:
                            # Store original value for reference
                            original_seq_value = seq_value
                            
                            if isinstance(seq_value, (int, float)):
                                # Check if within PostgreSQL integer range
                                if seq_value <= PG_INTEGER_MAX:
                                    sequence_number = int(seq_value)
                                else:
                                    print(f"[DEBUG] Incognita parser - Sequence number {seq_value} exceeds PostgreSQL integer limit, setting to None")
                                    sequence_number = None
                            elif isinstance(seq_value, str) and seq_value.strip().isdigit():
                                # For string numeric values, check range before converting
                                if int(seq_value.strip()) <= PG_INTEGER_MAX:
                                    sequence_number = int(seq_value.strip())
                                else:
                                    print(f"[DEBUG] Incognita parser - Sequence number {seq_value} exceeds PostgreSQL integer limit, setting to None")
                                    sequence_number = None
                        except (ValueError, TypeError):
                            sequence_number = None
                    
                except Exception as extract_error:
                    print(f"[DEBUG] Incognita parser - Error extracting data from row {idx}: {str(extract_error)}")
                    continue
                
                # Create operation dictionary
                # First convert row to dictionary, then sanitize to handle NaN values
                row_dict = row.to_dict()
                sanitized_raw_data = self._sanitize_json_data(row_dict)
                
                # Store original sequence number in raw_data if it differs from what we're using
                if original_seq_value is not None and sequence_number is None:
                    if sanitized_raw_data is None:
                        sanitized_raw_data = {}
                    sanitized_raw_data['original_sequence_number'] = str(original_seq_value)
                
                # Create structured operation data
                # Before creating the operation, log the account info for debugging
                print(f"[DEBUG] Incognita parser - Account information for operation from row {idx}:")
                print(f"[DEBUG] Incognita parser - Debit account: {debit_account}")
                print(f"[DEBUG] Incognita parser - Analytical debit: {analytical_debit}")
                print(f"[DEBUG] Incognita parser - Credit account: {credit_account}")
                print(f"[DEBUG] Incognita parser - Analytical credit: {analytical_credit}")
                
                operation = {
                    "file_id": file_id,
                    "operation_date": operation_date,
                    "document_type": "N/A",  # Set document_type to "N/A" for Incognita format
                    "document_number": document_number,
                    "debit_account": debit_account,
                    "credit_account": credit_account,
                    "amount": amount,
                    "description": description,
                    "partner_name": partner_name,
                    "analytical_debit": analytical_debit,
                    "analytical_credit": analytical_credit,
                    "template_type": "incognita",
                    "raw_data": sanitized_raw_data,
                    "import_uuid": import_uuid,
                    "sequence_number": sequence_number
                }
                
                operations.append(operation)
                if idx < 5 or len(operations) % 20 == 0:
                    print(f"[DEBUG] Incognita parser - Successfully added operation from row {idx}")
                    print(f"[DEBUG] Incognita parser - Operation details: Date={operation_date}, Amount={amount}")
                    print(f"[DEBUG] Incognita parser - Accounts: Debit={debit_account}, Credit={credit_account}")
            
            print(f"[DEBUG] Incognita parser - Total operations extracted: {len(operations)}")
            return operations
            
        except Exception as e:
            print(f"Error parsing Incognita Excel file: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def parse_memory(self, file_obj: BytesIO, file_id: int, import_uuid: str = None) -> List[Dict[str, Any]]:
        """
        Parse the Incognita Excel file from memory and extract accounting operations
        
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
            
            # Read Excel file from memory, skipping the first 2 rows as Incognita headers are in row 3
            df = pd.read_excel(file_obj, skiprows=2)
            
            print(f"[DEBUG] Incognita parser (memory) - Original DataFrame columns: {list(df.columns)}")
            print(f"[DEBUG] Incognita parser (memory) - Original DataFrame shape: {df.shape}")
            print(f"[DEBUG] Incognita parser (memory) - First 3 rows sample (original):")
            for i in range(min(3, len(df))):
                print(f"[DEBUG] Original Row {i}: {list(df.iloc[i].values)}")
            
            # Detect columns based on Incognita headers
            column_map = self._detect_columns(df)
            
            # Ensure document number is always mapped to column L (index 11)
            column_map['doc_number'] = 11
            
            print(f"[DEBUG] Incognita parser (memory) - Detected column mapping: {column_map}")
            print(f"[DEBUG] Incognita parser (memory) - Force mapped document number to column L (index 11)")
            
            # Skip any additional header rows if necessary
            data_start_row = self._find_data_start_row(df, column_map)
            print(f"[DEBUG] Incognita parser (memory) - Data start row detected at: {data_start_row}")
            
            if data_start_row > 0:
                df = df.iloc[data_start_row:]
                df = df.reset_index(drop=True)
                print(f"[DEBUG] Incognita parser (memory) - After skipping headers, DataFrame shape: {df.shape}")
            
            # Print first few rows to see the structure after header removal
            print(f"[DEBUG] Incognita parser (memory) - First 3 rows sample after header removal:")
            for i in range(min(3, len(df))):
                row_values = list(df.iloc[i].values)
                print(f"[DEBUG] Row {i}: {row_values}")
                
                # Print key columns with their indices for easier debugging
                for col_name, col_idx in column_map.items():
                    if col_idx is not None and col_idx < len(row_values):
                        print(f"  {col_name} (col {col_idx}): {row_values[col_idx]}")
            
            operations = []
            
            # Process each row
            for idx, row in df.iterrows():
                if idx % 20 == 0:  # Reduce verbosity by only logging every 20th row
                    print(f"[DEBUG] Incognita parser (memory) - Processing row {idx} of {len(df)}")
                
                try:
                    # Get values using column map
                    amount_idx = column_map.get('amount')
                    debit_idx = column_map.get('debit')
                    credit_idx = column_map.get('credit')
                    seq_idx = column_map.get('sequence_number')
                    
                    # Check for required columns
                    if amount_idx is None or (debit_idx is None and credit_idx is None):
                        if idx == 0:  # Only show this error once
                            print(f"[DEBUG] Incognita parser (memory) - Critical columns not detected in the file structure")
                            print(f"[DEBUG] Incognita parser (memory) - Column map: {column_map}")
                            continue
                        else:
                            continue
                    
                    # Extract values with index safety checks
                    amount_value = row.iloc[amount_idx] if amount_idx is not None and amount_idx < len(row) else None
                    debit_value = row.iloc[debit_idx] if debit_idx is not None and debit_idx < len(row) else None
                    credit_value = row.iloc[credit_idx] if credit_idx is not None and credit_idx < len(row) else None
                    seq_value = row.iloc[seq_idx] if seq_idx is not None and seq_idx < len(row) else None
                    
                    # Log values for debugging
                    if idx < 5 or idx % 100 == 0:  # Log first 5 rows and then every 100th row
                        print(f"[DEBUG] Incognita parser (memory) - Row {idx} Raw values: sequence={seq_value}, amount={amount_value}, debit={debit_value}, credit={credit_value}")
                    
                    # Skip rows that don't have amount or both debit and credit accounts
                    if pd.isna(amount_value) or (pd.isna(debit_value) and pd.isna(credit_value)):
                        if idx < 5:  # More detailed logging for troubleshooting first few rows
                            print(f"[DEBUG] Incognita parser (memory) - Skipping row {idx} - missing required data")
                            if pd.isna(amount_value):
                                print(f"[DEBUG] Incognita parser (memory) - Amount is NaN/None")
                            if pd.isna(debit_value):
                                print(f"[DEBUG] Incognita parser (memory) - Debit is NaN/None")
                            if pd.isna(credit_value):
                                print(f"[DEBUG] Incognita parser (memory) - Credit is NaN/None")
                        continue
                    
                    # Extract other fields using the column map
                    date_idx = column_map.get('date')
                    doc_num_idx = column_map.get('doc_number')
                    analytical_debit_idx = column_map.get('analytical_debit')
                    analytical_credit_idx = column_map.get('analytical_credit')
                    desc_idx = column_map.get('description')
                    partner_idx = column_map.get('partner')
                    
                    # Extract and clean date
                    raw_date_value = row.iloc[date_idx] if date_idx is not None and date_idx < len(row) else None
                    operation_date = self.convert_to_date(raw_date_value)
                    
                    # Try to get document number from the doc_number column if available
                    doc_num_from_column = None
                    if doc_num_idx is not None and doc_num_idx < len(row):
                        raw_doc_num = row.iloc[doc_num_idx]
                        if not pd.isna(raw_doc_num):
                            # Convert to string and clean
                            doc_num_from_column = str(raw_doc_num).strip()
                            # Log the document number from the column
                            if idx < 5:
                                print(f"[DEBUG] Incognita parser (memory) - Document number from column: {doc_num_from_column}")
                    
                    # Always use the document number from dedicated column when available
                    document_number = doc_num_from_column
                    
                    # Process debit account and analytical info
                    debit_account_raw = self.clean_string(debit_value)
                    
                    # Use the full account code including sub-accounts
                    debit_account = debit_account_raw
                    
                    # Log the full account code being used
                    if idx < 5:  # Log for first few rows
                        print(f"[DEBUG] Incognita parser (memory) - Using full debit account code: {debit_account}")
                    
                    # Get analytical_debit and try to extract document number from it
                    analytical_debit_raw = self.clean_string(
                        row.iloc[analytical_debit_idx] if analytical_debit_idx is not None and analytical_debit_idx < len(row) else None
                    )
                    
                    # Extract document number from analytical_debit only if not found in the dedicated column
                    if document_number is None and analytical_debit_raw and '.' in analytical_debit_raw:
                        parts = analytical_debit_raw.split('.')
                        if parts and parts[0].strip():
                            document_number = parts[0].strip()
                    
                    # Store the full account code with sub-accounts in analytical_debit if it's empty
                    if not analytical_debit_raw and '-' in debit_account_raw:
                        analytical_debit = debit_account_raw
                    else:
                        analytical_debit = analytical_debit_raw
                    
                    # Process credit account and analytical info
                    credit_account_raw = self.clean_string(credit_value)
                    
                    # Use the full account code including sub-accounts
                    credit_account = credit_account_raw
                    
                    # Log the full account code being used
                    if idx < 5:  # Log for first few rows
                        print(f"[DEBUG] Incognita parser (memory) - Using full credit account code: {credit_account}")
                    
                    # Get analytical_credit and try to extract document number from it if still not found
                    analytical_credit_raw = self.clean_string(
                        row.iloc[analytical_credit_idx] if analytical_credit_idx is not None and analytical_credit_idx < len(row) else None
                    )
                    
                    # Extract document number from analytical_credit only if not found from other sources
                    if document_number is None and analytical_credit_raw and '.' in analytical_credit_raw:
                        parts = analytical_credit_raw.split('.')
                        if parts and parts[0].strip():
                            document_number = parts[0].strip()
                    
                    # Store the full account code with sub-accounts in analytical_credit if it's empty
                    if not analytical_credit_raw and '-' in credit_account_raw:
                        analytical_credit = credit_account_raw
                    else:
                        analytical_credit = analytical_credit_raw
                    
                    # Extract description and partner
                    description = self.clean_string(
                        row.iloc[desc_idx] if desc_idx is not None and desc_idx < len(row) else None
                    )
                    
                    partner_name = self.clean_string(
                        row.iloc[partner_idx] if partner_idx is not None and partner_idx < len(row) else None
                    )
                    
                    # Extract amount with special handling for different formats
                    amount = None
                    try:
                        # First try direct conversion if it's a number
                        if isinstance(amount_value, (int, float)) and not pd.isna(amount_value):
                            amount = float(amount_value)
                        # Then try string cleaning if it's a string
                        elif isinstance(amount_value, str) and amount_value.strip():
                            # Regular string cleaning for currency values
                            cleaned = amount_value.replace(' ', '').replace(',', '.').strip()
                            if cleaned and any(c.isdigit() for c in cleaned):
                                try:
                                    amount = float(cleaned)
                                except ValueError:
                                    pass
                        
                        # Fall back to the clean_numeric method
                        if amount is None:
                            amount = self.clean_numeric(amount_value)
                            
                    except Exception as e:
                        print(f"[DEBUG] Incognita parser (memory) - Error processing amount '{amount_value}': {str(e)}")
                        amount = None
                    
                    # Skip if we don't have necessary data
                    if not amount:
                        print(f"[DEBUG] Incognita parser (memory) - Skipping row {idx} - missing amount")
                        continue
                    
                    # Ensure we have at least one account
                    if not debit_account and not credit_account:
                        print(f"[DEBUG] Incognita parser (memory) - Skipping row {idx} - missing both debit and credit accounts")
                        continue
                    
                    # If operation date is missing but we have account info and amount,
                    # use current date as fallback
                    if not operation_date:
                        print(f"[DEBUG] Incognita parser (memory) - Row {idx} is missing date but has account info - using fallback date")
                        operation_date = datetime.now().date()
                        
                    # Extract sequence number
                    sequence_number = None
                    original_seq_value = None
                    # PostgreSQL integer max value is 2^31-1
                    PG_INTEGER_MAX = 2147483647
                    
                    if seq_value is not None and not pd.isna(seq_value):
                        try:
                            # Store original value for reference
                            original_seq_value = seq_value
                            
                            if isinstance(seq_value, (int, float)):
                                # Check if within PostgreSQL integer range
                                if seq_value <= PG_INTEGER_MAX:
                                    sequence_number = int(seq_value)
                                else:
                                    print(f"[DEBUG] Incognita parser (memory) - Sequence number {seq_value} exceeds PostgreSQL integer limit, setting to None")
                                    sequence_number = None
                            elif isinstance(seq_value, str) and seq_value.strip().isdigit():
                                # For string numeric values, check range before converting
                                if int(seq_value.strip()) <= PG_INTEGER_MAX:
                                    sequence_number = int(seq_value.strip())
                                else:
                                    print(f"[DEBUG] Incognita parser (memory) - Sequence number {seq_value} exceeds PostgreSQL integer limit, setting to None")
                                    sequence_number = None
                        except (ValueError, TypeError):
                            sequence_number = None
                    
                except Exception as extract_error:
                    print(f"[DEBUG] Incognita parser (memory) - Error extracting data from row {idx}: {str(extract_error)}")
                    continue
                
                # Create operation dictionary
                # First convert row to dictionary, then sanitize to handle NaN values
                row_dict = row.to_dict()
                sanitized_raw_data = self._sanitize_json_data(row_dict)
                
                # Store original sequence number in raw_data if it differs from what we're using
                if original_seq_value is not None and sequence_number is None:
                    if sanitized_raw_data is None:
                        sanitized_raw_data = {}
                    sanitized_raw_data['original_sequence_number'] = str(original_seq_value)
                
                # Create structured operation data
                # Before creating the operation, log the account info for debugging
                print(f"[DEBUG] Incognita parser (memory) - Account information for operation from row {idx}:")
                print(f"[DEBUG] Incognita parser (memory) - Debit account: {debit_account}")
                print(f"[DEBUG] Incognita parser (memory) - Analytical debit: {analytical_debit}")
                print(f"[DEBUG] Incognita parser (memory) - Credit account: {credit_account}")
                print(f"[DEBUG] Incognita parser (memory) - Analytical credit: {analytical_credit}")
                
                operation = {
                    "file_id": file_id,
                    "operation_date": operation_date,
                    "document_type": "N/A",  # Set document_type to "N/A" for Incognita format
                    "document_number": document_number,
                    "debit_account": debit_account,
                    "credit_account": credit_account,
                    "amount": amount,
                    "description": description,
                    "partner_name": partner_name,
                    "analytical_debit": analytical_debit,
                    "analytical_credit": analytical_credit,
                    "template_type": "incognita",
                    "raw_data": sanitized_raw_data,
                    "import_uuid": import_uuid,
                    "sequence_number": sequence_number
                }
                
                operations.append(operation)
                if idx < 5 or len(operations) % 20 == 0:
                    print(f"[DEBUG] Incognita parser (memory) - Successfully added operation from row {idx}")
                    print(f"[DEBUG] Incognita parser (memory) - Operation details: Date={operation_date}, Amount={amount}")
                    print(f"[DEBUG] Incognita parser (memory) - Accounts: Debit={debit_account}, Credit={credit_account}")
            
            print(f"[DEBUG] Incognita parser (memory) - Total operations extracted: {len(operations)}")
            return operations
            
        except Exception as e:
            print(f"Error parsing Incognita Excel file from memory: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def _detect_columns(self, df: pd.DataFrame) -> Dict[str, Optional[int]]:
        """
        Detect column indices for Incognita format based on column headers
        
        Args:
            df: DataFrame with the Excel content
            
        Returns:
            Dictionary mapping column types to their indices
        """
        # Initialize column map with all None values
        column_map = {
            'sequence_number': None,  # A/A
            'debit': None,           # ДТ Сметка
            'analytical_debit': None, # Дт Сметка описание
            'credit': None,          # КТ Сметка
            'analytical_credit': None, # Кт Сметка описание
            'date': None,            # Дата
            'amount': None,          # Ст-Ст в лева
            'doc_number': None,      # Док. Номер
            'description': None,     # Предмет на доставка
            'partner': None          # Контрагент
        }
        
        # Expected Incognita column headers (lowercase for case-insensitive matching)
        expected_headers = {
            'sequence_number': ["a/a", "а/а", "№", "no", "номер"],
            'debit': ["дт сметка", "dt сметка", "дт с-ка", "дебит", "debit", "дебитна сметка", "дт смет"],
            'analytical_debit': ["дт сметка описание", "дт аналитична", "дебит описание", "аналитична дебит", "дт смет описание", "дт сметка описани"],
            'credit': ["кт сметка", "kt сметка", "кт с-ка", "кредит", "credit", "кредитна сметка", "кт смет"],
            'analytical_credit': ["кт сметка описание", "кт аналитична", "кредит описание", "аналитична кредит", "кт смет описание", "кт сметка описани"],
            'date': ["дата", "date"],
            'amount': ["ст-ст в лева", "сума", "стойност", "сума в лева", "amount", "value", "сумма"],
            'doc_number': ["док. номер", "документ номер", "номер на документ", "doc number", "документ №"],
            'description': ["предмет на доставка", "описание", "обяснение", "description", "details", "основание", "предмет"],
            'partner': ["контрагент", "партньор", "клиент", "partner", "client"]
        }
        
        # First check column headers from the first row
        for idx, col_name in enumerate(df.columns):
            col_str = str(col_name).lower().strip()
            
            # More verbose logging for column detection
            print(f"[DEBUG] Incognita parser - Checking column '{col_name}' (lower: '{col_str}')")
            
            # Match exact column names for Incognita format
            if col_str == "дт сметка" or col_str == "dt сметка":
                column_map['debit'] = idx
                print(f"[DEBUG] Incognita parser - Confirmed debit account column: '{col_name}' at index {idx}")
            elif col_str == "дт сметка описание" or col_str == "dt сметка описание":
                column_map['analytical_debit'] = idx
                print(f"[DEBUG] Incognita parser - Confirmed analytical debit column: '{col_name}' at index {idx}")
            elif col_str == "кт сметка" or col_str == "kt сметка":
                column_map['credit'] = idx
                print(f"[DEBUG] Incognita parser - Confirmed credit account column: '{col_name}' at index {idx}")
            elif col_str == "кт сметка описание" or col_str == "kt сметка описание":
                column_map['analytical_credit'] = idx
                print(f"[DEBUG] Incognita parser - Confirmed analytical credit column: '{col_name}' at index {idx}")
            # Broader matching for other account columns
            elif col_str.startswith("дт") and any(header in col_str for header in expected_headers['debit']):
                if 'debit' not in column_map or column_map['debit'] is None:
                    column_map['debit'] = idx
                    print(f"[DEBUG] Incognita parser - Found debit column: '{col_name}' at index {idx}")
            elif col_str.startswith("дт") and any(header in col_str for header in expected_headers['analytical_debit']):
                if 'analytical_debit' not in column_map or column_map['analytical_debit'] is None:
                    column_map['analytical_debit'] = idx
                    print(f"[DEBUG] Incognita parser - Found analytical debit column: '{col_name}' at index {idx}")
            elif col_str.startswith("кт") and any(header in col_str for header in expected_headers['credit']):
                if 'credit' not in column_map or column_map['credit'] is None:
                    column_map['credit'] = idx
                    print(f"[DEBUG] Incognita parser - Found credit column: '{col_name}' at index {idx}")
            elif col_str.startswith("кт") and any(header in col_str for header in expected_headers['analytical_credit']):
                if 'analytical_credit' not in column_map or column_map['analytical_credit'] is None:
                    column_map['analytical_credit'] = idx
                    print(f"[DEBUG] Incognita parser - Found analytical credit column: '{col_name}' at index {idx}")
            else:
                # Check each expected header category for other fields
                for field, possible_headers in expected_headers.items():
                    if field not in ['debit', 'credit', 'analytical_debit', 'analytical_credit'] and any(header in col_str for header in possible_headers):
                        column_map[field] = idx
                        print(f"[DEBUG] Incognita parser - Found {field} column in headers: '{col_name}' at index {idx}")
                        break
        
        # If we didn't find critical columns in the headers, check the first few data rows
        # This handles cases where there might be additional header rows
        missing_critical = any(column_map[field] is None for field in ['debit', 'credit', 'amount'])
        
        if missing_critical and len(df) > 0:
            print("[DEBUG] Incognita parser - Checking first data row for column detection")
            
            # Check the first data row values
            first_row = df.iloc[0]
            for idx, cell_value in enumerate(first_row):
                if pd.isna(cell_value):
                    continue
                    
                cell_str = str(cell_value).lower().strip()
                
                # Check each expected header category
                for field, possible_headers in expected_headers.items():
                    if any(header in cell_str for header in possible_headers):
                        column_map[field] = idx
                        print(f"[DEBUG] Incognita parser - Found {field} column in first row: '{cell_value}' at index {idx}")
                        break
        
        # If still missing critical columns, try a few fallback strategies
        
        # 1. Try to identify amount column based on numeric values
        if column_map['amount'] is None:
            print("[DEBUG] Incognita parser - Looking for amount column based on numeric values")
            for idx in range(len(df.columns)):
                # Skip first column (often sequence numbers)
                if idx == 0:
                    continue
                    
                # Count numeric values in this column
                numeric_values = 0
                total_value = 0
                for i in range(min(10, len(df))):
                    try:
                        val = df.iloc[i, idx]
                        if isinstance(val, (int, float)) and not pd.isna(val):
                            numeric_values += 1
                            total_value += val
                    except:
                        continue
                
                # If most rows have numeric values and average is reasonably large, likely an amount column
                if numeric_values >= 5:
                    avg = total_value / numeric_values
                    if avg > 10:  # Reasonable threshold for accounting amounts
                        column_map['amount'] = idx
                        print(f"[DEBUG] Incognita parser - Identified probable amount column at index {idx} (avg value: {avg})")
                        break
        
        # 2. Try to identify account columns based on common patterns and column headers
        if column_map['debit'] is None or column_map['credit'] is None or column_map['analytical_debit'] is None or column_map['analytical_credit'] is None:
            print("[DEBUG] Incognita parser - Looking for account columns based on patterns and headers")
            
            # Manual override for known Incognita format if standard columns are found
            # Check if we have at least A/A and St-St v leva (sequence and amount)
            if 'sequence_number' in column_map and column_map['sequence_number'] is not None and 'amount' in column_map and column_map['amount'] is not None:
                # Check if we have the typical Incognita column structure
                if len(df.columns) >= 6:  # We need at least 6 columns for the standard Incognita format
                    # ZRB_Chronologiq_2023.xlsx specific format - force mapping based on positions
                    if column_map['debit'] is None:
                        column_map['debit'] = 1  # ДТ Сметка is at index 1
                        print(f"[DEBUG] Incognita parser - Force mapped debit account to column index 1 (ДТ Сметка)")
                    if column_map['analytical_debit'] is None:
                        column_map['analytical_debit'] = 2  # Дт Сметка описание is at index 2
                        print(f"[DEBUG] Incognita parser - Force mapped analytical debit to column index 2 (Дт Сметка описание)")
                    if column_map['credit'] is None:
                        column_map['credit'] = 4  # КТ Сметка is at index 4
                        print(f"[DEBUG] Incognita parser - Force mapped credit account to column index 4 (КТ Сметка)")
                    if column_map['analytical_credit'] is None:
                        column_map['analytical_credit'] = 5  # Кт Сметка описание is at index 5
                        print(f"[DEBUG] Incognita parser - Force mapped analytical credit to column index 5 (Кт Сметка описание)")
            
            # Always ensure document number column is mapped to index 11 for Incognita format
            # This is a critical fix for the document number parsing issue
            column_map['doc_number'] = 11  # Док. Номер is always at index 11
            print(f"[DEBUG] Incognita parser - Force mapped document number to column index 11 (Док. Номер)")
            
            # Fallback: Check first row values for column headers if not found
            if column_map['debit'] is None or column_map['credit'] is None or column_map['analytical_debit'] is None or column_map['analytical_credit'] is None:
                # First check column headers explicitly
                for idx, col_name in enumerate(df.columns):
                    # Skip already identified columns
                    if idx in column_map.values():
                        continue
                        
                    col_str = str(col_name).lower().strip()
                    
                    # Check for specific Incognita column patterns by exact matching
                    if "дт сметка" in col_str or "dt сметка" in col_str:
                        column_map['debit'] = idx
                        print(f"[DEBUG] Incognita parser - Identified debit account column (ДТ Сметка) at index {idx}")
                    elif "дт сметка описание" in col_str or "дт описание" in col_str:
                        column_map['analytical_debit'] = idx
                        print(f"[DEBUG] Incognita parser - Identified analytical debit column (Дт Сметка описание) at index {idx}")
                    elif "кт сметка" in col_str or "kt сметка" in col_str:
                        column_map['credit'] = idx
                        print(f"[DEBUG] Incognita parser - Identified credit account column (КТ Сметка) at index {idx}")
                    elif "кт сметка описание" in col_str or "кт описание" in col_str:
                        column_map['analytical_credit'] = idx
                        print(f"[DEBUG] Incognita parser - Identified analytical credit column (Кт Сметка описание) at index {idx}")
                        
                # Then try to identify account columns based on content patterns
                if column_map['debit'] is None or column_map['credit'] is None:
                    for idx in range(len(df.columns)):
                        # Skip already identified columns
                        if idx in column_map.values():
                            continue
                            
                        # Look for account number patterns in first few rows
                        account_patterns = 0
                        for i in range(min(10, len(df))):
                            try:
                                val = str(df.iloc[i, idx]) if not pd.isna(df.iloc[i, idx]) else ""
                                # Account numbers often contain digits with potential separators
                                if val and any(c.isdigit() for c in val) and len(val) <= 10 and "-" in val:
                                    account_patterns += 1
                            except:
                                continue
                        
                        # If multiple rows have account-like patterns
                        if account_patterns >= 3:
                            # Assign to first missing account column
                            if column_map['debit'] is None:
                                column_map['debit'] = idx
                                print(f"[DEBUG] Incognita parser - Identified probable debit account column at index {idx}")
                            elif column_map['credit'] is None:
                                column_map['credit'] = idx
                                print(f"[DEBUG] Incognita parser - Identified probable credit account column at index {idx}")
        
        # Print the final column mapping for debugging
        print(f"[DEBUG] Incognita parser - Final column mapping: {column_map}")
        return column_map
    
    def _find_data_start_row(self, df: pd.DataFrame, column_map: Dict[str, Optional[int]]) -> int:
        """
        Determine if there are additional header rows to skip
        
        Args:
            df: DataFrame with the Excel content
            column_map: Dictionary mapping column types to their indices
            
        Returns:
            Number of additional rows to skip (0 if no additional rows needed)
        """
        # In Incognita format, we've already skipped 2 rows, so headers should be in row 1
        # But check for additional header rows just in case
        
        # Check if first row contains header-like text instead of actual data
        if len(df) > 0:
            first_row = df.iloc[0]
            header_indicators = 0
            
            for field, idx in column_map.items():
                if idx is not None and idx < len(first_row):
                    val = first_row.iloc[idx]
                    if not pd.isna(val):
                        val_str = str(val).lower()
                        
                        # Check if value looks like a header rather than data
                        if any(keyword in val_str for keyword in ["сметка", "номер", "дата", "сума", "описание"]):
                            header_indicators += 1
            
            if header_indicators >= 2:  # If multiple columns contain header-like text
                print(f"[DEBUG] Incognita parser - First row looks like headers ({header_indicators} indicators), skipping it")
                return 1
        
        # No additional rows to skip
        return 0
    
    def _sanitize_json_data(self, data_dict):
        """
        Sanitize data dictionary to handle non-JSON-serializable values like NaN or dates
        
        Args:
            data_dict: Dictionary potentially containing NaN or other non-serializable values
            
        Returns:
            Sanitized dictionary safe for JSON serialization
        """
        import math
        import json
        import numpy as np
        
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
            # Handle datetime objects
            elif isinstance(value, (datetime)):
                result[key] = value.isoformat()
            # Recursively handle nested dictionaries
            elif isinstance(value, dict):
                result[key] = self._sanitize_json_data(value)
            # Handle lists by sanitizing each item
            elif isinstance(value, list):
                result[key] = [self._sanitize_json_data(item) if isinstance(item, dict) else
                              (None if (isinstance(item, float) and (math.isnan(item) or math.isinf(item))) else item)
                              for item in value]
            else:
                result[key] = value
                
        return result