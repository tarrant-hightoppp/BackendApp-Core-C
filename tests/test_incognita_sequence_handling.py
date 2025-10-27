"""
Test file for Incognita parser's handling of large sequence numbers.
This test verifies that the fix for handling sequence numbers exceeding PostgreSQL integer limits works correctly.
"""
import os
import io
import uuid
import pandas as pd
import pytest
from datetime import datetime

from app.services.parsers.incognita_parser import IncognitaParser
from app.models.operation import AccountingOperation
from app.db.session import SessionLocal


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


def test_large_sequence_number_handling():
    """Test that the Incognita parser correctly handles sequence numbers exceeding PostgreSQL integer limits"""
    # Create a DataFrame with a row containing a sequence number larger than PostgreSQL's integer limit
    # PostgreSQL integer limit is 2^31-1 = 2,147,483,647
    large_seq_num = 9999999999  # Larger than PostgreSQL integer limit
    alphanumeric_seq = "ABC123456789"
    
    # Create a simple DataFrame simulating Incognita format
    data = {
        "A/A": [large_seq_num, alphanumeric_seq, 12345],  # Sequence numbers
        "ДТ Сметка": ["1001", "1002", "1003"],  # Debit accounts
        "КТ Сметка": ["2001", "2002", "2003"],  # Credit accounts
        "Дата": [datetime.now(), datetime.now(), datetime.now()],  # Dates
        "Ст-Ст в лева": [100.0, 200.0, 300.0]  # Amounts
    }
    df = pd.DataFrame(data)
    
    # Convert DataFrame to Excel in memory
    excel_buffer = io.BytesIO()
    df.to_excel(excel_buffer, index=False)
    excel_buffer.seek(0)
    
    # Create parser and parse the in-memory Excel file
    parser = IncognitaParser()
    import_uuid = str(uuid.uuid4())
    operations = parser.parse_memory(excel_buffer, file_id=1, import_uuid=import_uuid)
    
    # Verify operations were extracted
    assert operations is not None, "Parser didn't extract operations"
    assert len(operations) == 3, f"Expected 3 operations, got {len(operations)}"
    
    # Verify handling of large sequence number
    large_seq_op = operations[0]
    assert large_seq_op["sequence_number"] is None, "Large sequence number should be set to None"
    assert "original_sequence_number" in large_seq_op["raw_data"], "Original sequence value not stored in raw_data"
    assert large_seq_op["raw_data"]["original_sequence_number"] == str(large_seq_num), "Original sequence value incorrect"
    
    # Verify handling of alphanumeric sequence
    alpha_seq_op = operations[1]
    assert alpha_seq_op["sequence_number"] is None, "Alphanumeric sequence should be set to None"
    assert "original_sequence_number" in alpha_seq_op["raw_data"], "Original sequence value not stored in raw_data"
    assert alpha_seq_op["raw_data"]["original_sequence_number"] == alphanumeric_seq, "Original sequence value incorrect"
    
    # Verify handling of normal sequence number
    normal_seq_op = operations[2]
    assert normal_seq_op["sequence_number"] == 12345, "Normal sequence number not preserved"
    assert "original_sequence_number" not in normal_seq_op["raw_data"], "Shouldn't have original_sequence_number for normal values"


def test_db_saving_with_large_sequence(db_session):
    """Test that operations with large sequence numbers can be saved to the database"""
    # Create a DataFrame with a row containing a sequence number larger than PostgreSQL's integer limit
    large_seq_num = 9999999999  # Larger than PostgreSQL integer limit
    
    # Create a simple DataFrame simulating Incognita format
    data = {
        "A/A": [large_seq_num],  # Sequence number exceeding PostgreSQL integer limit
        "ДТ Сметка": ["1001"],  # Debit account
        "КТ Сметка": ["2001"],  # Credit account
        "Дата": [datetime.now()],  # Date
        "Ст-Ст в лева": [100.0]  # Amount
    }
    df = pd.DataFrame(data)
    
    # Convert DataFrame to Excel in memory
    excel_buffer = io.BytesIO()
    df.to_excel(excel_buffer, index=False)
    excel_buffer.seek(0)
    
    # Create parser and parse the in-memory Excel file
    parser = IncognitaParser()
    import_uuid = str(uuid.uuid4())
    operations = parser.parse_memory(excel_buffer, file_id=1, import_uuid=import_uuid)
    
    # Verify operation was extracted and sequence_number is None
    assert operations is not None, "Parser didn't extract operations"
    assert len(operations) == 1, f"Expected 1 operation, got {len(operations)}"
    assert operations[0]["sequence_number"] is None, "Large sequence number should be set to None"
    
    # Try saving to database
    try:
        # Create AccountingOperation object
        operation_data = operations[0]
        db_operation = AccountingOperation(
            file_id=operation_data["file_id"],
            operation_date=operation_data["operation_date"],
            document_type=operation_data["document_type"],
            document_number=operation_data["document_number"],
            debit_account=operation_data["debit_account"],
            credit_account=operation_data["credit_account"],
            amount=operation_data["amount"],
            description=operation_data["description"],
            template_type=operation_data["template_type"],
            import_uuid=operation_data["import_uuid"],
            sequence_number=operation_data["sequence_number"],  # Should be None
            raw_data=operation_data["raw_data"]
        )
        
        # Save to database
        db_session.add(db_operation)
        db_session.commit()
        
        # Verify saved operation
        saved_op = db_session.query(AccountingOperation).filter(
            AccountingOperation.import_uuid == import_uuid
        ).first()
        
        assert saved_op is not None, "Operation not saved to database"
        assert saved_op.sequence_number is None, "Sequence number should be None in database"
        assert saved_op.raw_data["original_sequence_number"] == str(large_seq_num), "Original sequence value incorrect in database"
        
    finally:
        # Clean up
        db_session.query(AccountingOperation).filter(
            AccountingOperation.import_uuid == import_uuid
        ).delete()
        db_session.commit()


if __name__ == "__main__":
    # This can be used for manual testing
    test_large_sequence_number_handling()
    print("Test completed successfully")