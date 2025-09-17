import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional
from datetime import datetime
from io import BytesIO
from app.services.parsers.base_parser import BaseExcelParser
import json
from datetime import datetime



class MicroinvestParser(BaseExcelParser):
    """Parser for Microinvest Excel format"""
    
    def parse(self, file_path: str, file_id: int, import_uuid: str = None) -> List[Dict[str, Any]]:
        """
        Parse the Microinvest Excel file and extract accounting operations
        
        Args:
            file_path: Path to the Excel file
            file_id: ID of the uploaded file in the database
            import_uuid: UUID of the import batch this file belongs to
            
        Returns:
            List of dictionaries containing accounting operations data
        """
        try:
            # Read Excel file
            df = pd.read_excel(file_path, engine='xlrd')
            # print(f"[DEBUG] Successfully read Microinvest file with shape: {df.shape}")
            # print(f"[DEBUG] Columns: {list(df.columns)}")
            
            # print(f"[DEBUG] Calling _extract_operations for Microinvest file with {len(df)} rows")
            operations = self._extract_operations(df, file_id, import_uuid)
            # print(f"[DEBUG] Extracted {len(operations)} operations from Microinvest file")
            return operations
            
        except Exception as e:
            print(f"[ERROR] Error parsing Microinvest Excel file: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def parse_memory(self, file_obj: BytesIO, file_id: int, import_uuid: str = None) -> List[Dict[str, Any]]:
        """
        Parse the Microinvest Excel file from memory and extract accounting operations
        
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
            
            # Try different engines
            try:
                df = pd.read_excel(file_obj, engine='xlrd')
            except Exception as e:
                print(f"[WARNING] Failed to read with xlrd engine: {e}, trying openpyxl")
                file_obj.seek(0)
                df = pd.read_excel(file_obj, engine='openpyxl')
                
            # print(f"[DEBUG] Successfully read Microinvest file with shape: {df.shape}")
            # print(f"[DEBUG] Columns: {list(df.columns)}")
            
            # print(f"[DEBUG] Calling _extract_operations for Microinvest file with {len(df)} rows")
            operations = self._extract_operations(df, file_id, import_uuid)
            # print(f"[DEBUG] Extracted {len(operations)} operations from Microinvest file")
            return operations
            
        except Exception as e:
            print(f"[ERROR] Error parsing Microinvest Excel file from memory: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def _extract_operations(self, df: pd.DataFrame, file_id: int, import_uuid: str = None) -> List[Dict[str, Any]]:
        """
        Extract accounting operations from the DataFrame
        
        Args:
            df: DataFrame containing the Excel data
            file_id: ID of the uploaded file in the database
            import_uuid: UUID of the import batch this file belongs to
            
        Returns:
            List of dictionaries containing accounting operations data
        """
        operations = []
        
        # First, let's normalize column names to make it easier to work with them
        df.columns = [str(col).lower().strip() for col in df.columns]
        
        # Try to identify required columns (being flexible with possible names)
        debit_account_col = self._find_column(df, ['дебит сметка', 'дт сметка', 'дт с-ка', 'debit account'])
        credit_account_col = self._find_column(df, ['кредит сметка', 'кт сметка', 'кт с-ка', 'credit account'])
        date_col = self._find_column(df, ['дата', 'date'])
        amount_col = self._find_column(df, ['сума', 'сума дт', 'amount', 'value'])
        doc_type_col = self._find_column(df, ['док. вид', 'вид док', 'document type'])
        doc_number_col = self._find_column(df, ['документ №', 'номер', 'doc number'])
        description_col = self._find_column(df, ['основание', 'описание', 'description'])
        partner_col = self._find_column(df, ['партньор', 'partner'])
        
        # print(f"[DEBUG] Microinvest identified columns: debit={debit_account_col}, credit={credit_account_col}, date={date_col}, amount={amount_col}, document_type={doc_type_col}, document_number={doc_number_col}, description={description_col}, partner={partner_col}")
        
        # Process each row
        for idx, row in df.iterrows():
            # Log progress every 100 rows (commented out)
            # if idx % 100 == 0:
            #     print(f"[DEBUG] Processing row {idx} of {len(df)}")
                
            try:
                # Get data from the row, handling missing columns
                debit_account = self._get_value(row, debit_account_col) if debit_account_col else None
                credit_account = self._get_value(row, credit_account_col) if credit_account_col else None
                operation_date = self._get_date(row, date_col) if date_col else None
                
                # Skip rows without proper account information
                if not debit_account and not credit_account:
                    continue
                
                # Try to get amount - this might be in different columns or need calculation
                # print(f"[DEBUG] Getting amount for row {idx}")
                amount = self._get_amount(row, amount_col)
                if amount is None or amount == 0:
                    # If no amount found, skip this row
                    # print(f"[DEBUG] Skipping row {idx} - no valid amount found")
                    continue
                # print(f"[DEBUG] Found amount for row {idx}: {amount}")
                
                # Get other fields
                document_type = self._get_value(row, doc_type_col) if doc_type_col else None
                document_number = self._get_value(row, doc_number_col) if doc_number_col else None
                description = self._get_value(row, description_col) if description_col else None
                partner_name = self._get_value(row, partner_col) if partner_col else None
                
                # Create operation dictionary
                operation = {
                    "file_id": file_id,
                    "operation_date": operation_date or datetime.now().date(),  # Use current date if not found
                    "document_type": document_type,
                    "document_number": document_number,
                    "debit_account": debit_account,
                    "credit_account": credit_account,
                    "amount": amount,
                    "description": description,
                    "partner_name": partner_name,
                    "template_type": "MICROINVEST",
                    "raw_data": self._make_json_serializable(row),
                    "import_uuid": import_uuid
                }
                
                operations.append(operation)
                
            except Exception as row_error:
                print(f"[ERROR] Error processing row {idx} in Microinvest file: {row_error}")
                import traceback
                traceback.print_exc()
                continue
        
        print(f"[INFO] Extracted {len(operations)} operations from Microinvest file")
        return operations
    
    def _find_column(self, df: pd.DataFrame, possible_names: List[str]) -> Optional[str]:
        """
        Find a column by trying several possible names
        
        Args:
            df: DataFrame to search in
            possible_names: List of possible column names to try
            
        Returns:
            Actual column name if found, None otherwise
        """
        for name in possible_names:
            if name in df.columns:
                return name
                
        # Try partial matches
        for name in possible_names:
            for col in df.columns:
                if name in col:
                    return col
                    
        return None
    
    def _get_value(self, row: pd.Series, column: Optional[str]) -> Optional[str]:
        """
        Safely get a value from a row, handling missing columns
        
        Args:
            row: DataFrame row
            column: Column name
            
        Returns:
            Value as string if found and not NaN, None otherwise
        """
        if column is None:
            return None
            
        try:
            value = row[column]
            return self.clean_string(value)
        except:
            return None
    
    def _get_date(self, row: pd.Series, column: Optional[str]) -> Optional[datetime.date]:
        """
        Safely get a date from a row, handling missing columns and conversion errors
        
        Args:
            row: DataFrame row
            column: Column name
            
        Returns:
            Date if found and valid, None otherwise
        """
        if column is None:
            return None
            
        try:
            value = row[column]
            return self.convert_to_date(value)
        except:
            return None
    
    def _get_amount(self, row: pd.Series, amount_col: Optional[str]) -> Optional[float]:
        """
        Get the amount from a row, with fallbacks for different formats
        
        Args:
            row: DataFrame row
            amount_col: Primary amount column name
            
        Returns:
            Amount as float if found, None otherwise
        """
        # Try the primary amount column first
        if amount_col and not pd.isna(row.get(amount_col, None)):
            try:
                # Check if this column looks like an account number column
                val = row[amount_col]
                if isinstance(val, str) and ('/' in val or '-' in val or 'оборот' in val.lower()):
                    # Skip columns with account numbers or description texts
                    print(f"Skipping potential account or description column: {amount_col} with value: {val}")
                else:
                    amount = self.clean_numeric(val)
                    if amount is not None:
                        return amount
            except Exception as e:
                print(f"Error processing primary amount column {amount_col}: {e}")
        
        # Look for columns that might contain an amount - be more selective
        amount_keywords = ['сума', 'стойност', 'amount', 'value']
        
        # First pass: look for columns with exact keyword matches
        exact_amount_cols = []
        for col in row.index:
            col_str = str(col).lower()
            if any(col_str == keyword for keyword in amount_keywords):
                exact_amount_cols.append(col)
        
        # Try exact matches first
        for col in exact_amount_cols:
            try:
                val = row[col]
                # Skip if it looks like an account number or text description
                if isinstance(val, str) and ('/' in val or '-' in val or 'оборот' in val.lower()):
                    continue
                
                amount = self.clean_numeric(val)
                if amount is not None and amount > 0:
                    # print(f"Found amount in exact match column {col}: {amount}")
                    return amount
            except Exception as e:
                print(f"Error processing amount column {col}: {e}")
        
        # Second pass: look for columns containing amount keywords
        for col in row.index:
            col_str = str(col).lower()
            if any(keyword in col_str for keyword in amount_keywords) and col not in exact_amount_cols:
                try:
                    val = row[col]
                    # Skip if it looks like an account number or text description
                    if isinstance(val, str) and ('/' in val or '-' in val or 'оборот' in val.lower()):
                        continue
                    
                    amount = self.clean_numeric(val)
                    if amount is not None and amount > 0:
                        # print(f"Found amount in keyword match column {col}: {amount}")
                        return amount
                except Exception as e:
                    print(f"Error processing amount column {col}: {e}")
        
        # If no amount found, log which columns were checked and values
        # print(f"[DEBUG] No valid amount found in row. Columns and values: {[(col, row[col]) for col in row.index]}")
        # Return 0 to signal no amount found, which will cause the row to be skipped
        return 0
        
    def _make_json_serializable(self, row: pd.Series) -> Dict[str, Any]:
        """
        Convert a pandas Series to a JSON-serializable dictionary.
        
        Args:
            row: pandas Series containing the row data
            
        Returns:
            Dictionary with all values converted to JSON-serializable types
        """
        result = {}
        for key, value in row.items():
            try:
                # Handle strings directly - don't try to check if they're datetime dtypes
                # This prevents errors when strings like "430/3" or "1 - Вътрешен оборот"
                # are mistakenly interpreted as dtype specifications
                if isinstance(value, str):
                    result[key] = value
                    continue
                
                # Handle NaN, NaT, etc.
                if pd.isna(value):
                    result[key] = None
                    continue
                
                # Now it's safe to check for timestamp/datetime types
                if isinstance(value, pd.Timestamp):
                    # Convert to ISO format string
                    result[key] = value.isoformat()
                elif pd.api.types.is_datetime64_any_dtype(value):
                    # Convert to ISO format string
                    result[key] = pd.Timestamp(value).isoformat()
                # Handle numpy types
                elif isinstance(value, (np.integer, np.int64, np.int32)):
                    result[key] = int(value)
                elif isinstance(value, (np.floating, np.float64, np.float32)):
                    result[key] = float(value)
                elif isinstance(value, np.bool_):
                    result[key] = bool(value)
                else:
                    # Try to convert to a basic Python type
                    try:
                        # Test if it's JSON serializable
                        json.dumps({key: value})
                        result[key] = value
                    except (TypeError, OverflowError):
                        # If not serializable, convert to string
                        result[key] = str(value)
            except Exception as e:
                print(f"[ERROR] Error processing {key}={value} (type: {type(value).__name__}): {e}")
                # Fall back to string representation
                try:
                    result[key] = str(value)
                except:
                    result[key] = "ERROR: Unable to convert value to string"
        
        return result