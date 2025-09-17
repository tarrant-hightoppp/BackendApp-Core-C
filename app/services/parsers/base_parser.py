from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Union
import pandas as pd
from datetime import datetime
from io import BytesIO


class BaseExcelParser(ABC):
    """Base class for all Excel parsers"""
    
    @abstractmethod
    def parse(self, file_path: str, file_id: int, import_uuid: str = None) -> List[Dict[str, Any]]:
        """
        Parse the Excel file from disk and extract accounting operations
        
        Args:
            file_path: Path to the Excel file
            file_id: ID of the uploaded file in the database
            import_uuid: UUID of the import batch this file belongs to
            
        Returns:
            List of dictionaries containing accounting operations data
        """
        pass
    
    @abstractmethod
    def parse_memory(self, file_obj: BytesIO, file_id: int, import_uuid: str = None) -> List[Dict[str, Any]]:
        """
        Parse the Excel file from memory and extract accounting operations
        
        Args:
            file_obj: BytesIO object containing the Excel file
            file_id: ID of the uploaded file in the database
            import_uuid: UUID of the import batch this file belongs to
            
        Returns:
            List of dictionaries containing accounting operations data
        """
        pass
    
    def clean_column_name(self, name: str) -> str:
        """
        Clean column name for consistent processing
        
        Args:
            name: Raw column name
            
        Returns:
            Cleaned column name (lowercase, trimmed)
        """
        return str(name).lower().strip()
    
    def convert_to_date(self, date_value: Any) -> Optional[datetime.date]:
        """
        Convert various date formats to standard date
        
        Args:
            date_value: Date value in various formats (string, datetime, etc.)
            
        Returns:
            Standardized date object or None if conversion fails
        """
        if pd.isna(date_value):
            return None
            
        try:
            if isinstance(date_value, datetime):
                return date_value.date()
            elif isinstance(date_value, str):
                # Try different date formats
                for fmt in ["%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"]:
                    try:
                        return datetime.strptime(date_value, fmt).date()
                    except ValueError:
                        continue
                
                # If none of the formats match, try pandas to_datetime
                return pd.to_datetime(date_value).date()
            else:
                # Try using pandas to convert numeric or other types
                return pd.to_datetime(date_value).date()
        except Exception as e:
            print(f"Error converting date {date_value}: {e}")
            return None
    
    def clean_numeric(self, value: Any) -> Optional[float]:
        """
        Clean and convert numeric values
        
        Args:
            value: Value to convert to float
            
        Returns:
            Cleaned float value or None if conversion fails
        """
        if pd.isna(value):
            # print(f"[DEBUG] clean_numeric: Skipping NaN value")
            return None
            
        try:
            if isinstance(value, str):
                # print(f"[DEBUG] clean_numeric: Processing string value: '{value}'")
                # Check if the string has patterns that indicate it's not a number
                # Account numbers like "602/4" or descriptions like "1 - Вътрешен оборот"
                # should not be treated as numbers
                if '/' in value:
                    # print(f"[DEBUG] clean_numeric: Value contains slash: '{value}'")
                    # Check if this is really a numeric value with a slash
                    parts = value.split('/')
                    if len(parts) == 2 and all(p.strip().isdigit() for p in parts):
                        # This is likely a fraction, continue processing
                        # print(f"[DEBUG] clean_numeric: Value appears to be a fraction, continuing")
                        pass
                    else:
                        # This contains a slash and is not a simple fraction
                        # print(f"[DEBUG] clean_numeric: Value appears to be an account number, returning None")
                        return None
                
                if '-' in value:
                    # print(f"[DEBUG] clean_numeric: Value contains dash: '{value}'")
                    # Check if this is just a negative number or something else
                    if not value.replace('-', '').replace('.', '').replace(',', '').isdigit():
                        # This contains a dash and is not simply a negative number
                        # print(f"[DEBUG] clean_numeric: Value appears to be a description with dash, returning None")
                        return None
                
                if not any(c.isdigit() for c in value):
                    # print(f"[DEBUG] clean_numeric: Value contains no digits, returning None")
                    return None
                
                # Remove currency symbols and thousand separators
                # print(f"[DEBUG] clean_numeric: Cleaning string: '{value}'")
                cleaned = value.replace('$', '').replace('€', '').replace(' ', '')
                cleaned = cleaned.replace(',', '.').replace(' ', '')
                # print(f"[DEBUG] clean_numeric: After cleaning: '{cleaned}'")
                
                # Final check - if the result doesn't look like a number, return None
                if not cleaned.replace('.', '').replace('-', '').isdigit():
                    # Check if it's a valid float representation with one decimal point
                    parts = cleaned.split('.')
                    if len(parts) > 2 or not all(p.replace('-', '').isdigit() for p in parts):
                        # print(f"[DEBUG] clean_numeric: Cleaned value is not a valid number, returning None")
                        return None
                
                result = float(cleaned)
                # print(f"[DEBUG] clean_numeric: Successfully converted to float: {result}")
                return result
            else:
                # print(f"[DEBUG] clean_numeric: Processing non-string value: {type(value).__name__}: {value}")
                result = float(value)
                # print(f"[DEBUG] clean_numeric: Successfully converted to float: {result}")
                return result
        except Exception as e:
            print(f"[ERROR] Error converting numeric value '{value}' of type {type(value).__name__}: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def clean_string(self, value: Any) -> Optional[str]:
        """
        Clean string values
        
        Args:
            value: Value to convert to string
            
        Returns:
            Cleaned string value or None if empty or NaN
        """
        if pd.isna(value):
            return None
            
        try:
            result = str(value).strip()
            return result if result else None
        except Exception as e:
            print(f"Error converting string value {value}: {e}")
            return None