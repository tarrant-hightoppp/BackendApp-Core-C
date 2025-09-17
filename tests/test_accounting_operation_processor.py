import os
import sys
import unittest
import uuid
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
from app.api import deps

# Create a test database and session
SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"
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


class TestAccountingOperationProcessor(unittest.TestCase):
    """Test case for the AccountingOperationProcessor service"""

    def setUp(self):
        """Set up the test database and create test data"""
        # Create all tables
        Base.metadata.create_all(bind=engine)
        
        # Create a session
        self.db = TestingSessionLocal()
        
        # Create test data
        self.create_test_data()
    
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
    
    def create_test_data(self):
        """Create test data for testing"""
        # Create a test import
        self.import_uuid = str(uuid.uuid4())
        
        # Create test files
        file1 = UploadedFile(
            filename="test_file1.xlsx",
            template_type="rival",
            file_path="test/path/file1.xlsx",
            processed=True,
            import_uuid=self.import_uuid
        )
        
        file2 = UploadedFile(
            filename="test_file2.xlsx",
            template_type="ajur",
            file_path="test/path/file2.xlsx",
            processed=True,
            import_uuid=self.import_uuid
        )
        
        self.db.add(file1)
        self.db.add(file2)
        self.db.commit()
        
        # Get the file IDs
        self.file1_id = file1.id
        self.file2_id = file2.id
        
        # Create test operations
        # Create operations with different debit accounts
        operations = [
            # File 1 operations
            # Debit account 101
            AccountingOperation(
                file_id=self.file1_id,
                operation_date=date(2023, 1, 1),
                document_type="Invoice",
                document_number="INV001",
                debit_account="101",
                credit_account="401",
                amount=1000,
                description="Test operation 1",
                template_type="rival",
                import_uuid=self.import_uuid
            ),
            AccountingOperation(
                file_id=self.file1_id,
                operation_date=date(2023, 1, 2),
                document_type="Invoice",
                document_number="INV002",
                debit_account="101",
                credit_account="402",
                amount=2000,
                description="Test operation 2",
                template_type="rival",
                import_uuid=self.import_uuid
            ),
            
            # Debit account 102
            AccountingOperation(
                file_id=self.file1_id,
                operation_date=date(2023, 1, 3),
                document_type="Invoice",
                document_number="INV003",
                debit_account="102",
                credit_account="403",
                amount=3000,
                description="Test operation 3",
                template_type="rival",
                import_uuid=self.import_uuid
            ),
            
            # File 2 operations
            # Debit account 201
            AccountingOperation(
                file_id=self.file2_id,
                operation_date=date(2023, 1, 4),
                document_type="Invoice",
                document_number="INV004",
                debit_account="201",
                credit_account="501",
                amount=4000,
                description="Test operation 4",
                template_type="ajur",
                import_uuid=self.import_uuid
            ),
            
            # Debit account 202
            AccountingOperation(
                file_id=self.file2_id,
                operation_date=date(2023, 1, 5),
                document_type="Invoice",
                document_number="INV005",
                debit_account="202",
                credit_account="502",
                amount=5000,
                description="Test operation 5",
                template_type="ajur",
                import_uuid=self.import_uuid
            ),
        ]
        
        for op in operations:
            self.db.add(op)
        
        self.db.commit()
    
    @patch('app.services.accounting_operation_processor.S3Service')
    def test_process_import(self, mock_s3_service):
        """Test the process_import method"""
        # Setup mock S3Service with a way to capture uploaded content
        mock_s3_instance = MagicMock()
        mock_s3_instance.upload_file.return_value = (True, "File uploaded successfully")
        
        # Store the uploaded files for verification
        uploaded_files = {}
        
        def capture_upload(file_content, object_name):
            uploaded_files[object_name] = file_content
            return True, "File uploaded successfully"
            
        mock_s3_instance.upload_file.side_effect = capture_upload
        mock_s3_service.return_value = mock_s3_instance
        
        # Create an instance of the processor
        processor = AccountingOperationProcessor(self.db)
        
        # Process the import
        result = processor.process_import(self.import_uuid)
        
        # Check the result
        self.assertTrue(result["success"])
        self.assertEqual(result["debit_accounts_processed"], 4)  # 101, 102, 201, 202
        self.assertEqual(result["credit_accounts_processed"], 5)  # 401, 402, 403, 501, 502
        self.assertEqual(result["total_operations"], 5)
        
        # Check that the files were uploaded to S3
        self.assertEqual(mock_s3_instance.upload_file.call_count, 9)  # 4 debit files + 5 credit files
        
        # Verify file naming pattern in the results
        for file_info in result["debit_files"]:
            account = file_info["account"]
            file_name = file_info["file_name"]
            # Verify file name follows pattern: DEBIT-account__import-uuid__timestamp.xlsx
            self.assertTrue(file_name.startswith(f"DEBIT-{account}__"))
            self.assertTrue(f"__{self.import_uuid}__" in file_name)
            self.assertTrue(file_name.endswith(".xlsx"))
            
            # Verify S3 key is correct with new path structure
            expected_key = f"exports/{self.import_uuid}/DEBIT/{file_name}"
            self.assertEqual(file_info["s3_key"], expected_key)
            
        # Same checks for credit files
        for file_info in result["credit_files"]:
            account = file_info["account"]
            file_name = file_info["file_name"]
            # Verify file name follows pattern: CREDIT-account__import-uuid__timestamp.xlsx
            self.assertTrue(file_name.startswith(f"CREDIT-{account}__"))
            self.assertTrue(f"__{self.import_uuid}__" in file_name)
            self.assertTrue(file_name.endswith(".xlsx"))
            
            # Verify S3 key is correct with new path structure
            expected_key = f"exports/{self.import_uuid}/CREDIT/{file_name}"
            self.assertEqual(file_info["s3_key"], expected_key)
    
    @patch('app.services.accounting_operation_processor.S3Service')
    def test_group_by_account(self, mock_s3_service):
        """Test the _group_by_account method"""
        # Setup mock S3Service
        mock_s3_instance = MagicMock()
        mock_s3_service.return_value = mock_s3_instance
        
        # Create an instance of the processor
        processor = AccountingOperationProcessor(self.db)
        
        # Get all operations
        operations = processor._get_operations_by_import(self.import_uuid)
        
        # Group by debit account
        debit_groups = processor._group_by_account(operations, "debit")
        
        # Check the grouping
        self.assertEqual(len(debit_groups), 4)  # 101, 102, 201, 202
        self.assertEqual(len(debit_groups["101"]), 2)  # 2 operations for 101
        self.assertEqual(len(debit_groups["102"]), 1)  # 1 operation for 102
        self.assertEqual(len(debit_groups["201"]), 1)  # 1 operation for 201
        self.assertEqual(len(debit_groups["202"]), 1)  # 1 operation for 202
        
        # Group by credit account
        credit_groups = processor._group_by_account(operations, "credit")
        
        # Check the grouping
        self.assertEqual(len(credit_groups), 5)  # 401, 402, 403, 501, 502
    
    @patch('app.services.accounting_operation_processor.S3Service')
    def test_filter_operations_small(self, mock_s3_service):
        """Test the _filter_operations method with <= 30 operations"""
        # Setup mock S3Service
        mock_s3_instance = MagicMock()
        mock_s3_service.return_value = mock_s3_instance
        
        # Create an instance of the processor
        processor = AccountingOperationProcessor(self.db)
        
        # Get all operations
        operations = processor._get_operations_by_import(self.import_uuid)
        
        # Filter operations (should keep all since <= 30)
        filtered_operations = processor._filter_operations(operations)
        
        # Check the filtering
        self.assertEqual(len(filtered_operations), 5)  # All 5 operations should be kept
    
    @patch('app.services.accounting_operation_processor.S3Service')
    def test_filter_operations_large(self, mock_s3_service):
        """Test the _filter_operations method with > 30 operations (80% rule)"""
        # Setup mock S3Service
        mock_s3_instance = MagicMock()
        mock_s3_service.return_value = mock_s3_instance
        
        # Create an instance of the processor
        processor = AccountingOperationProcessor(self.db)
        
        # Create a large set of operations (>30) with varying amounts
        operations = []
        
        # Create one very large operation (40% of total)
        large_op = AccountingOperation(
            file_id=self.file1_id,
            operation_date=date(2023, 1, 1),
            document_type="Invoice",
            document_number="INV-LARGE",
            debit_account="999",
            credit_account="888",
            amount=4000,  # 40% of total (10,000)
            description="Large operation",
            template_type="test",
            import_uuid=self.import_uuid
        )
        operations.append(large_op)
        
        # Create 5 medium operations (8% each, 40% total)
        for i in range(5):
            medium_op = AccountingOperation(
                file_id=self.file1_id,
                operation_date=date(2023, 1, 2),
                document_type="Invoice",
                document_number=f"INV-MED-{i}",
                debit_account="999",
                credit_account="888",
                amount=800,  # 8% of total each
                description=f"Medium operation {i}",
                template_type="test",
                import_uuid=self.import_uuid
            )
            operations.append(medium_op)
        
        # Create 30 small operations (0.67% each, 20% total)
        for i in range(30):
            small_op = AccountingOperation(
                file_id=self.file1_id,
                operation_date=date(2023, 1, 3),
                document_type="Invoice",
                document_number=f"INV-SMALL-{i}",
                debit_account="999",
                credit_account="888",
                amount=67,  # ~0.67% of total each
                description=f"Small operation {i}",
                template_type="test",
                import_uuid=self.import_uuid
            )
            operations.append(small_op)
        
        # We should have 36 operations total (1 large + 5 medium + 30 small)
        self.assertEqual(len(operations), 36)
        
        # Verify total amount is approximately 10,000
        total_amount = sum(op.amount for op in operations)
        self.assertAlmostEqual(total_amount, 10000, delta=10)
        
        # Filter operations using the 80% rule
        filtered_operations = processor._filter_operations(operations)
        
        # We expect 6 operations to be included (the large one + 5 medium ones = 80%)
        self.assertEqual(len(filtered_operations), 6)
        
        # Verify that the operations are sorted by amount (descending)
        self.assertEqual(filtered_operations[0].amount, 4000)  # Large operation first
        
        # Verify that the total of filtered operations is >= 80% of total
        filtered_amount = sum(op.amount for op in filtered_operations)
        self.assertGreaterEqual(filtered_amount / total_amount, 0.8)
    
    @patch('app.services.accounting_operation_processor.S3Service')
    def test_api_endpoint(self, mock_s3_service):
        """Test the API endpoint for processing imports"""
        # Setup mock S3Service
        mock_s3_instance = MagicMock()
        mock_s3_instance.upload_file.return_value = (True, "File uploaded successfully")
        mock_s3_service.return_value = mock_s3_instance
        
        # Call the API endpoint
        response = client.post(f"/api/operations/process-import/{self.import_uuid}")
        
        # Check the response
        self.assertEqual(response.status_code, 200)
        
        result = response.json()
        self.assertTrue(result["success"])
        self.assertEqual(result["debit_accounts_processed"], 4)  # 101, 102, 201, 202
        self.assertEqual(result["credit_accounts_processed"], 5)  # 401, 402, 403, 501, 502
        self.assertEqual(result["total_operations"], 5)
    
    @patch('app.services.accounting_operation_processor.S3Service')
    def test_api_endpoint_invalid_import(self, mock_s3_service):
        """Test the API endpoint with an invalid import UUID"""
        # Setup mock S3Service
        mock_s3_instance = MagicMock()
        mock_s3_service.return_value = mock_s3_instance
        
        # Call the API endpoint with an invalid import UUID
        invalid_uuid = str(uuid.uuid4())
        response = client.post(f"/api/operations/process-import/{invalid_uuid}")
        
        # Check the response
        self.assertEqual(response.status_code, 404)


if __name__ == "__main__":
    unittest.main()