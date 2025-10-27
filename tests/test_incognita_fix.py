import os
import sys
import unittest
import pandas as pd
from io import BytesIO

# Add the parent directory to sys.path to import app modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.services.parsers.incognita_parser import IncognitaParser


class TestIncognitaColumnMapping(unittest.TestCase):
    """Test the fix for Incognita parser's column mapping issue"""

    def setUp(self):
        """Initialize parser and test file path"""
        self.parser = IncognitaParser()
        self.test_file_path = "files/ZRB_Chronologiq_2023.xlsx"
        
        # Make sure the test file exists
        self.assertTrue(os.path.exists(self.test_file_path), 
                       f"Test file not found: {self.test_file_path}")

    def test_column_detection(self):
        """Test that columns are detected correctly for the specific file"""
        # Read Excel file with pandas to test column detection separately
        df = pd.read_excel(self.test_file_path, skiprows=2)
        
        # Call the column detection method
        column_map = self.parser._detect_columns(df)
        
        # Verify that the column mapping is correct
        self.assertEqual(column_map['debit'], 1, "Debit account should be at index 1 (ДТ Сметка)")
        self.assertEqual(column_map['analytical_debit'], 2, "Analytical debit should be at index 2 (Дт Сметка описание)")
        self.assertEqual(column_map['credit'], 4, "Credit account should be at index 4 (КТ Сметка)")
        self.assertEqual(column_map['analytical_credit'], 5, "Analytical credit should be at index 5 (Кт Сметка описание)")

    def test_data_parsing(self):
        """Test that operations are created with correct account mappings"""
        # Parse the file
        operations = self.parser.parse(self.test_file_path, file_id=1, import_uuid="test_uuid")
        
        # Verify that we have operations
        self.assertTrue(len(operations) > 0, "No operations were extracted from the file")
        
        # Check a few operations for correct field mappings
        # For the first operation in the sample file
        first_op = operations[0]
        self.assertEqual(first_op['debit_account'], "602-99-11",
                         "First operation should have full debit account 602-99-11")
        self.assertEqual(first_op['analytical_debit'], "ДРУГИ РАЗХОДИ ЗА ВЪНШНИ УСЛУГИ /ПР-ВО ЕНДЖИТЕК",
                         "First operation should have correct analytical debit")
        self.assertEqual(first_op['credit_account'], "401-01",
                         "First operation should have full credit account 401-01")
        self.assertEqual(first_op['analytical_credit'], "ДОСТАВЧИЦИ ОТ СТРАНАТА",
                         "First operation should have correct analytical credit")
        
        # Check another operation from the file (e.g., row 7)
        # Indices are 0-based, so row 7 in Excel is index 6
        if len(operations) > 6:
            seventh_op = operations[6]
            self.assertEqual(seventh_op['debit_account'], "453-01",
                             "Seventh operation should have full debit account 453-01")
            self.assertEqual(seventh_op['credit_account'], "401-01",
                             "Seventh operation should have full credit account 401-01")
            
        # Check that account codes with dashes are preserved correctly
        for op in operations[:5]:  # Check first few operations
            debit = op['debit_account']
            credit = op['credit_account']
            
            # If original account codes contain dashes, make sure they're preserved
            if '-' in debit:
                self.assertGreaterEqual(len(debit.split('-')), 2,
                                       f"Debit account {debit} should preserve full code with sub-accounts")
            
            if '-' in credit:
                self.assertGreaterEqual(len(credit.split('-')), 2,
                                       f"Credit account {credit} should preserve full code with sub-accounts")


def test_document_number_matching(self):
    """Test that document numbers match the values from column L (Док. Номер)"""
    # Parse the file
    operations = self.parser.parse(self.test_file_path, file_id=1, import_uuid="test_uuid")
    
    # Verify that we have operations
    self.assertTrue(len(operations) > 0, "No operations were extracted from the file")
    
    # Manually read the Excel file to get the document numbers from column L
    df = pd.read_excel(self.test_file_path, skiprows=2)
    
    # Find the column index for "Док. Номер" - should be column L which is typically index 11
    doc_num_column = None
    for idx, col in enumerate(df.columns):
        if str(col).lower() == "док. номер" or "док" in str(col).lower() and "номер" in str(col).lower():
            doc_num_column = idx
            break
    
    self.assertIsNotNone(doc_num_column, "Could not find 'Док. Номер' column")
    
    # Get first few operations and compare document numbers
    for i in range(min(10, len(operations))):
        excel_doc_num = df.iloc[i, doc_num_column]
        if pd.notna(excel_doc_num):
            excel_doc_num = str(excel_doc_num)
        else:
            excel_doc_num = None
            
        parsed_doc_num = operations[i]['document_number']
        
        # Print for debugging
        print(f"Row {i+1}: Excel doc num: {excel_doc_num}, Parsed doc num: {parsed_doc_num}")
        
        # Test equality - both should be the same
        self.assertEqual(str(excel_doc_num), str(parsed_doc_num) if parsed_doc_num else "None",
                        f"Document number mismatch at row {i+1}")


if __name__ == '__main__':
    unittest.main()