import os
import sys
import unittest
import uuid
from io import BytesIO
import pandas as pd
from unittest.mock import patch, MagicMock

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.services.parsers.rival_parser import RivalParser
from app.services.parsers.ajur_parser import AjurParser
from app.services.file_processor import FileProcessor
from app.models.file import UploadedFile


class TestParsers(unittest.TestCase):
    """Test case for the parser implementations with import_uuid"""

    def setUp(self):
        """Set up test data"""
        self.import_uuid = str(uuid.uuid4())
        self.file_id = 1
        
        # Create test DataFrame for Rival parser
        self.rival_data = {
            0: ["Фактура", "INV001", "01.01.2023", "Test Partner", "101", "401", 1000, "Test operation"],
            1: ["Фактура", "INV002", "02.01.2023", "Test Partner", "102", "402", 2000, "Test operation 2"]
        }
        self.rival_df = pd.DataFrame.from_dict(self.rival_data, orient='index')
        
        # Create test DataFrame for AJUR parser
        self.ajur_data = {
            0: ["Фактура", "INV001", "01.01.2023", "101", "D101", "401", "K401", 1000, "Test operation"],
            1: ["Фактура", "INV002", "02.01.2023", "102", "D102", "402", "K402", 2000, "Test operation 2"]
        }
        self.ajur_df = pd.DataFrame.from_dict(self.ajur_data, orient='index')
    
    @patch('app.services.parsers.rival_parser.pd.read_excel')
    def test_rival_parser_with_import_uuid(self, mock_read_excel):
        """Test that the Rival parser correctly handles import_uuid"""
        # Setup mock
        mock_read_excel.return_value = self.rival_df
        
        # Create parser
        parser = RivalParser()
        
        # Test with import_uuid
        operations = parser.parse_memory(BytesIO(), self.file_id, self.import_uuid)
        
        # Check that operations have import_uuid
        for op in operations:
            self.assertEqual(op["import_uuid"], self.import_uuid)
        
        # Test without import_uuid
        operations = parser.parse_memory(BytesIO(), self.file_id)
        
        # Check that operations have None for import_uuid
        for op in operations:
            self.assertIsNone(op["import_uuid"])
    
    @patch('app.services.parsers.ajur_parser.pd.read_excel')
    def test_ajur_parser_with_import_uuid(self, mock_read_excel):
        """Test that the AJUR parser correctly handles import_uuid"""
        # Setup mock
        mock_read_excel.return_value = self.ajur_df
        
        # Create parser
        parser = AjurParser()
        
        # Test with import_uuid
        operations = parser.parse_memory(BytesIO(), self.file_id, self.import_uuid)
        
        # Check that operations have import_uuid
        for op in operations:
            self.assertEqual(op["import_uuid"], self.import_uuid)
        
        # Test without import_uuid
        operations = parser.parse_memory(BytesIO(), self.file_id)
        
        # Check that operations have None for import_uuid
        for op in operations:
            self.assertIsNone(op["import_uuid"])
    
    @patch('app.services.file_processor.S3Service')
    @patch('app.services.parsers.rival_parser.pd.read_excel')
    def test_file_processor_passes_import_uuid(self, mock_read_excel, mock_s3_service):
        """Test that the FileProcessor correctly passes import_uuid to parsers"""
        # Setup mocks
        mock_read_excel.return_value = self.rival_df
        mock_s3_instance = MagicMock()
        mock_s3_instance.download_file.return_value = b"file content"
        mock_s3_service.return_value = mock_s3_instance
        
        # Create a mock database session
        mock_db = MagicMock()
        
        # Create a mock file record
        file_record = UploadedFile(
            id=self.file_id,
            filename="test_file.xlsx",
            template_type="RIVAL",
            file_path="test/path/file.xlsx",
            processed=False,
            import_uuid=self.import_uuid
        )
        
        # Mock the query to return our file record
        mock_db.query.return_value.filter.return_value.first.return_value = file_record
        
        # Create file processor
        processor = FileProcessor(mock_db)
        
        # Call process_file
        processor.process_file(self.file_id)
        
        # Check that db.add was called for each operation with import_uuid
        for call in mock_db.add.call_args_list:
            # Get the AccountingOperation object from the call
            operation = call[0][0]
            # Check that it has the correct import_uuid
            self.assertEqual(operation.import_uuid, self.import_uuid)


if __name__ == "__main__":
    unittest.main()