"""
Test file for Incognita parser's handling of account codes with subaccount separators.
This test verifies that the parser correctly extracts main account codes from the full account string.
"""
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


def test_account_code_extraction():
    """Test that the Incognita parser correctly extracts main account codes from dashed account strings"""
    # Create test data with account codes containing subaccount separators
    data = {
        "A/A": [1, 2, 3],  # Sequence numbers
        "ДТ Сметка": ["602-99-11", "601-03-12", "453-01"],  # Debit accounts with subaccounts
        "Дт Сметка описание": ["ДРУГИ РАЗХОДИ ЗА ВЪНШНИ УСЛУГИ /ПР-ВО ЕНДЖИТЕК", "", ""],  # Debit descriptions
        "КТ Сметка": ["401-01", "405-01", "401-01"],  # Credit accounts with subaccounts
        "Кт Сметка описание": ["ДОСТАВЧИЦИ ОТ СТРАНАТА", "МОНБАТ АД", ""],  # Credit descriptions
        "Дата": [datetime.now(), datetime.now(), datetime.now()],  # Dates
        "Ст-Ст в лева": [189.3, 4232.52, 144.0]  # Amounts
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
    
    # Verify the first operation - should extract "602" as main account with the full code in analytical field
    assert operations[0]["debit_account"] == "602", f"Expected debit account 602, got {operations[0]['debit_account']}"
    assert operations[0]["analytical_debit"] == "ДРУГИ РАЗХОДИ ЗА ВЪНШНИ УСЛУГИ /ПР-ВО ЕНДЖИТЕК", "Debit analytical info not preserved"
    assert operations[0]["credit_account"] == "401", f"Expected credit account 401, got {operations[0]['credit_account']}"
    assert operations[0]["analytical_credit"] == "ДОСТАВЧИЦИ ОТ СТРАНАТА", "Credit analytical info not preserved"
    
    # Verify the second operation - should extract "601" as main account and store full code in analytical field
    assert operations[1]["debit_account"] == "601", f"Expected debit account 601, got {operations[1]['debit_account']}"
    assert operations[1]["analytical_debit"] == "601-03-12", "Full account code not stored in analytical field when description is empty"
    assert operations[1]["credit_account"] == "405", f"Expected credit account 405, got {operations[1]['credit_account']}"
    assert operations[1]["analytical_credit"] == "МОНБАТ АД", "Credit analytical info not preserved"
    
    # Verify the third operation - should extract "453" as main account
    assert operations[2]["debit_account"] == "453", f"Expected debit account 453, got {operations[2]['debit_account']}"
    assert operations[2]["analytical_debit"] == "453-01", "Full account code not stored in analytical field when description is empty"
    assert operations[2]["credit_account"] == "401", f"Expected credit account 401, got {operations[2]['credit_account']}"
    assert operations[2]["analytical_credit"] == "401-01", "Full account code not stored in analytical field when description is empty"


def test_db_saving_with_account_codes(db_session):
    """Test that operations with extracted account codes can be saved to the database"""
    # Create test data with account codes containing subaccount separators
    data = {
        "A/A": [1],  # Sequence number
        "ДТ Сметка": ["602-99-11"],  # Debit account with subaccounts
        "Дт Сметка описание": ["ДРУГИ РАЗХОДИ ЗА ВЪНШНИ УСЛУГИ /ПР-ВО ЕНДЖИТЕК"],  # Debit description
        "КТ Сметка": ["401-01"],  # Credit account with subaccounts
        "Кт Сметка описание": ["ДОСТАВЧИЦИ ОТ СТРАНАТА"],  # Credit description
        "Дата": [datetime.now()],  # Date
        "Ст-Ст в лева": [189.3]  # Amount
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
    
    # Verify operation was extracted and accounts were extracted correctly
    assert operations is not None, "Parser didn't extract operations"
    assert len(operations) == 1, f"Expected 1 operation, got {len(operations)}"
    assert operations[0]["debit_account"] == "602", f"Expected debit account 602, got {operations[0]['debit_account']}"
    assert operations[0]["credit_account"] == "401", f"Expected credit account 401, got {operations[0]['credit_account']}"
    
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
            sequence_number=operation_data["sequence_number"],
            analytical_debit=operation_data["analytical_debit"],
            analytical_credit=operation_data["analytical_credit"],
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
        assert saved_op.debit_account == "602", f"Expected debit account 602, got {saved_op.debit_account}"
        assert saved_op.credit_account == "401", f"Expected credit account 401, got {saved_op.credit_account}"
        assert saved_op.analytical_debit == "ДРУГИ РАЗХОДИ ЗА ВЪНШНИ УСЛУГИ /ПР-ВО ЕНДЖИТЕК", "Analytical debit info not preserved"
        assert saved_op.analytical_credit == "ДОСТАВЧИЦИ ОТ СТРАНАТА", "Analytical credit info not preserved"
        
    finally:
        # Clean up
        db_session.query(AccountingOperation).filter(
            AccountingOperation.import_uuid == import_uuid
        ).delete()
        db_session.commit()


if __name__ == "__main__":
    # This can be used for manual testing
    test_account_code_extraction()
    print("Account code extraction test completed successfully")
    
    # To run database test, uncomment the following:
    # db_session = next(get_test_db())
    # test_db_saving_with_account_codes(db_session)
    # print("Database saving test completed successfully")