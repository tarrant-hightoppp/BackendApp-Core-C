"""
Test file for Incognita template integration.
This test verifies that the Incognita template detection, parsing, and processing works correctly.
"""
import os
import pytest
import pandas as pd
from datetime import datetime
from sqlalchemy.orm import Session
from typing import List, Dict, Any

from app.db.session import SessionLocal
from app.services.template_detector import TemplateDetector, TemplateType
from app.services.parsers.incognita_parser import IncognitaParser
from app.services.file_processor import FileProcessor
from app.models.operation import AccountingOperation
from app.models.file import UploadedFile


def get_test_db():
    """Return a test database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture
def db_session():
    """Database session fixture"""
    return next(get_test_db())


def test_incognita_template_detection():
    """Test that the template detector correctly identifies Incognita files"""
    # This test requires a sample Incognita file
    # Path to the sample file (replace with actual path when available)
    sample_file_path = "path/to/sample_incognita_file.xlsx"
    
    # Skip test if file doesn't exist
    if not os.path.exists(sample_file_path):
        pytest.skip(f"Sample file {sample_file_path} not found. Skipping test.")
    
    detector = TemplateDetector()
    template_type = detector.detect_template(sample_file_path)
    
    assert template_type is not None, "Template detection failed"
    assert template_type == TemplateType.INCOGNITA, f"Expected Incognita template, got {template_type}"


def test_incognita_parser(db_session: Session):
    """Test that the Incognita parser correctly extracts data from Incognita files"""
    # This test requires a sample Incognita file
    # Path to the sample file (replace with actual path when available)
    sample_file_path = "path/to/sample_incognita_file.xlsx"
    
    # Skip test if file doesn't exist
    if not os.path.exists(sample_file_path):
        pytest.skip(f"Sample file {sample_file_path} not found. Skipping test.")
    
    parser = IncognitaParser()
    operations = parser.parse(sample_file_path, file_id=1, import_uuid="test_import_uuid")
    
    assert operations is not None, "Parser returned None instead of operations"
    assert len(operations) > 0, "Parser didn't extract any operations"
    
    # Verify that operations have the required fields
    for op in operations:
        assert "debit_account" in op or "credit_account" in op, "Operation missing account information"
        assert "amount" in op, "Operation missing amount"
        assert "operation_date" in op, "Operation missing date"
        assert op["template_type"] == "incognita", f"Operation has wrong template_type: {op['template_type']}"


def test_incognita_end_to_end(db_session: Session):
    """Test the entire workflow for Incognita files - from detection to processing"""
    # This test requires a sample Incognita file
    # Path to the sample file (replace with actual path when available)
    sample_file_path = "path/to/sample_incognita_file.xlsx"
    
    # Skip test if file doesn't exist
    if not os.path.exists(sample_file_path):
        pytest.skip(f"Sample file {sample_file_path} not found. Skipping test.")
    
    # First, detect the template type
    detector = TemplateDetector()
    template_type = detector.detect_template(sample_file_path)
    
    assert template_type == TemplateType.INCOGNITA, f"Expected Incognita template, got {template_type}"
    
    # Create a file record in the database
    file_processor = FileProcessor(db_session)
    file_record = file_processor.create_file(
        filename=os.path.basename(sample_file_path),
        template_type=template_type.value,
        file_path=sample_file_path,  # For testing, use local path instead of S3 path
        import_uuid="test_import_uuid"
    )
    
    # Process the file
    # Note: For proper testing, you'd need to modify process_file to accept local files
    # This example assumes the method has been modified for testing purposes
    operations = file_processor.process_file(file_record.id)
    
    # Verify operations were created
    assert operations is not None, "File processing failed"
    assert len(operations) > 0, "No operations were created"
    
    # Verify operations are in the database
    db_operations = db_session.query(AccountingOperation).filter(
        AccountingOperation.file_id == file_record.id
    ).all()
    
    assert len(db_operations) > 0, "No operations were saved to the database"
    assert len(db_operations) == len(operations), "Not all operations were saved to the database"
    
    # Check a few operations in detail
    for op in db_operations[:3]:  # Check first 3 operations
        assert op.template_type == "incognita", f"Operation has wrong template_type: {op.template_type}"
        assert op.import_uuid == "test_import_uuid", "Operation has wrong import_uuid"
        assert op.file_id == file_record.id, "Operation has wrong file_id"


# Helper method for manual testing (not a pytest test)
def verify_incognita_sample_file(file_path: str, db_session: Session = None):
    """
    Utility method for manual verification of an Incognita file.
    This can be used in the Python REPL to check a file without running the full test suite.
    
    Args:
        file_path: Path to the Incognita file to verify
        db_session: Optional database session (if saving operations to database)
        
    Returns:
        Dictionary with verification results
    """
    results = {
        "file_exists": os.path.exists(file_path),
        "detection_result": None,
        "operations_count": 0,
        "sample_operations": [],
        "success": False
    }
    
    if not results["file_exists"]:
        print(f"File {file_path} not found")
        return results
    
    # Detect template type
    detector = TemplateDetector()
    try:
        template_type = detector.detect_template(file_path)
        results["detection_result"] = template_type.value if template_type else None
        
        if template_type != TemplateType.INCOGNITA:
            print(f"File detected as {template_type}, not as Incognita")
            return results
    except Exception as e:
        print(f"Error detecting template: {e}")
        results["detection_error"] = str(e)
        return results
    
    # Parse the file
    parser = IncognitaParser()
    try:
        operations = parser.parse(file_path, file_id=0, import_uuid="test_verify")
        results["operations_count"] = len(operations)
        results["sample_operations"] = operations[:3] if operations else []
        
        if not operations:
            print("No operations extracted from the file")
            return results
    except Exception as e:
        print(f"Error parsing file: {e}")
        results["parsing_error"] = str(e)
        return results
    
    # Print some statistics
    debit_accounts = set(op.get("debit_account") for op in operations if op.get("debit_account"))
    credit_accounts = set(op.get("credit_account") for op in operations if op.get("credit_account"))
    
    results["unique_debit_accounts"] = len(debit_accounts)
    results["unique_credit_accounts"] = len(credit_accounts)
    results["debit_accounts_sample"] = list(debit_accounts)[:5]
    results["credit_accounts_sample"] = list(credit_accounts)[:5]
    results["success"] = True
    
    print(f"Verification successful: {len(operations)} operations extracted")
    print(f"Unique debit accounts: {len(debit_accounts)}")
    print(f"Unique credit accounts: {len(credit_accounts)}")
    
    return results


if __name__ == "__main__":
    # This section can be used for manual testing
    import sys
    
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        print(f"Testing Incognita file: {file_path}")
        
        db = SessionLocal()
        try:
            results = verify_incognita_sample_file(file_path, db)
            print(f"Test results: {'SUCCESS' if results['success'] else 'FAILURE'}")
        finally:
            db.close()
    else:
        print("Please provide a file path as command-line argument")