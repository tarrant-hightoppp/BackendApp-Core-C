import os
import sys
import unittest
import uuid
import tempfile
import pandas as pd
from datetime import date
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from unittest.mock import patch, MagicMock

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.app import app
from app.db.base import Base
from app.models.file import UploadedFile
from app.models.operation import AccountingOperation
from app.services.accounting_operation_processor import AccountingOperationProcessor
from app.services.file_processor import FileProcessor
from app.api import deps

# Create a test database and session
SQLALCHEMY_DATABASE_URL = "sqlite:///./test_workflow.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Override the get_db dependency in tests
def override_get_db():
    try:
        db = TestingSessionLocal()
        yield db
    finally:
        db.close()

app.dependency_overrides[deps.get_db] = override_get_db

# Create a test client
client = TestClient(app)


class TestFileImportWorkflow(unittest.TestCase):
    """Test the complete file import workflow from upload to account processing"""

    def setUp(self):
        """Set up the test database"""
        # Create all tables
        Base.metadata.create_all(bind=engine)
        
        # Create a session
        self.db = TestingSessionLocal()
        
        # Create a temporary Excel file for testing
        self.temp_file = self._create_temp_excel_file()
        
    def tearDown(self):
        """Clean up after each test"""
        # Delete test data
        self.db.query(AccountingOperation).delete()
        self.db.query(UploadedFile).delete()
        self.db.commit()
        
        # Close the session
        self.db.close()
        
        # Drop all tables
        Base.metadata.drop_all(bind=engine)
        
        # Remove temporary file
        os.unlink(self.temp_file)
    
    def _create_temp_excel_file(self):
        """Create a temporary Excel file with test data"""
        # Create DataFrame with test operations
        data = {
            "Вид док": ["Фактура", "Фактура", "Фактура", "Фактура", "Фактура"],
            "Номер документ": ["INV001", "INV002", "INV003", "INV004", "INV005"],
            "Дата документ": [date(2023, 1, 1), date(2023, 1, 2), date(2023, 1, 3), date(2023, 1, 4), date(2023, 1, 5)],
            "Име": ["Partner 1", "Partner 2", "Partner 3", "Partner 4", "Partner 5"],
            "Сметка дебит": ["101", "101", "102", "201", "202"],
            "Сметка кредит": ["401", "402", "403", "501", "502"],
            "Стойност": [1000, 2000, 3000, 4000, 5000],
            "Обяснение на статия": ["Test 1", "Test 2", "Test 3", "Test 4", "Test 5"]
        }
        
        df = pd.DataFrame(data)
        
        # Create a temporary file
        fd, path = tempfile.mkstemp(suffix=".xlsx")
        os.close(fd)
        
        # Save DataFrame to Excel
        df.to_excel(path, index=False)
        
        return path

    @patch('app.services.s3.S3Service')
    def test_complete_workflow(self, mock_s3_service):
        """Test the complete workflow from file upload to account processing"""
        # Setup mock S3Service
        mock_s3_instance = MagicMock()
        mock_s3_instance.upload_file.return_value = (True, "File uploaded successfully")
        mock_s3_instance.download_file.return_value = open(self.temp_file, "rb").read()
        mock_s3_service.return_value = mock_s3_instance
        
        # Step 1: Upload file
        with open(self.temp_file, "rb") as f:
            response = client.post(
                "/api/files/upload",
                files={"file": ("test_file.xlsx", f, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
            )
        
        # Check response
        self.assertEqual(response.status_code, 200, f"File upload failed: {response.json()}")
        
        upload_result = response.json()
        file_id = upload_result["id"]
        import_uuid = upload_result["import_uuid"]
        
        # Step 2: Process the batch
        response = client.post(f"/api/files/batch/{import_uuid}/process")
        
        # Check response
        self.assertEqual(response.status_code, 200, f"Batch processing failed: {response.json()}")
        
        batch_result = response.json()
        self.assertTrue(batch_result["success"])
        
        # Verify that files were generated for both debit and credit accounts
        self.assertGreater(batch_result["debit_accounts_processed"], 0)
        self.assertGreater(batch_result["credit_accounts_processed"], 0)
        
        # Step 3: Check if operations were created in the database
        operations = self.db.query(AccountingOperation).filter(
            AccountingOperation.import_uuid == import_uuid
        ).all()
        
        # Verify operations
        self.assertGreater(len(operations), 0)
        
        # Step 4: Verify file naming pattern and S3 paths
        for file_info in batch_result["debit_files"]:
            account = file_info["account"]
            file_name = file_info["file_name"]
            
            # Verify file name follows pattern: DEBIT-account__import-uuid__timestamp.xlsx
            self.assertTrue(file_name.startswith(f"DEBIT-{account}__"))
            self.assertTrue(f"__{import_uuid}__" in file_name)
            self.assertTrue(file_name.endswith(".xlsx"))
            
            # Verify S3 key is correct
            expected_key = f"exports/{import_uuid}/DEBIT/{file_name}"
            self.assertEqual(file_info["s3_key"], expected_key)
        
        # Same checks for credit files
        for file_info in batch_result["credit_files"]:
            account = file_info["account"]
            file_name = file_info["file_name"]
            
            # Verify file name follows pattern: CREDIT-account__import-uuid__timestamp.xlsx
            self.assertTrue(file_name.startswith(f"CREDIT-{account}__"))
            self.assertTrue(f"__{import_uuid}__" in file_name)
            self.assertTrue(file_name.endswith(".xlsx"))
            
            # Verify S3 key is correct
            expected_key = f"exports/{import_uuid}/CREDIT/{file_name}"
            self.assertEqual(file_info["s3_key"], expected_key)


if __name__ == "__main__":
    unittest.main()