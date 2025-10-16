import os
import uuid
import pytest
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.models.operation import AccountingOperation
from app.services.parsers.ajur_parser import AjurParser
from app.services.ajur_audit_processor import AjurAuditProcessor


@pytest.fixture
def db_session():
    """Get a database session for testing"""
    db = next(get_db())
    try:
        yield db
    finally:
        db.close()


@pytest.fixture
def sample_ajur_file_path():
    """Fixture to provide the path to a sample AJUR file"""
    return os.path.join("files", "ajur.xlsx")


@pytest.fixture
def ajur_operations(db_session, sample_ajur_file_path):
    """Create sample Ajur operations for testing the audit processor"""
    # Parse the sample file to get realistic operations
    parser = AjurParser()
    
    # Generate a unique import UUID for this test
    import_uuid = str(uuid.uuid4())
    file_id = -2  # Using a negative ID to avoid conflicts with real data
    
    operations_data = parser.parse(sample_ajur_file_path, file_id, import_uuid)
    
    # Store operations in the database
    db_operations = []
    for op_data in operations_data:
        # Create the operation in the DB
        op_data["file_id"] = file_id
        op_data["import_uuid"] = import_uuid
        
        operation = AccountingOperation(**op_data)
        db_session.add(operation)
        db_operations.append(operation)
    
    db_session.commit()
    
    # Refresh operations to get their IDs
    for op in db_operations:
        db_session.refresh(op)
    
    try:
        yield db_operations
    finally:
        # Clean up the operations after the test
        for op in db_operations:
            db_session.delete(op)
        db_session.commit()


def test_ajur_audit_processor(db_session, ajur_operations):
    """Test the AjurAuditProcessor functionality"""
    # Verify we have operations to work with
    assert len(ajur_operations) > 0, "No test operations available"
    
    # Get the import UUID from the first operation
    import_uuid = ajur_operations[0].import_uuid
    
    # Create the AjurAuditProcessor
    processor = AjurAuditProcessor(db_session)
    
    # Process the operations
    result = processor.process_audit(import_uuid, audit_approach="full")
    
    # Verify the result
    assert result["success"] is True, "Audit processing failed"
    assert result["total_operations"] == len(ajur_operations), "Not all operations were processed"
    
    # Verify that both debit and credit accounts were processed
    assert result["debit_accounts_processed"] > 0, "No debit accounts were processed"
    assert result["credit_accounts_processed"] > 0, "No credit accounts were processed"
    
    # Verify that files were generated and uploaded to S3
    assert len(result["debit_files"]) > 0, "No debit account files were generated"
    assert len(result["credit_files"]) > 0, "No credit account files were generated"
    
    # Print some information about the generated files
    print(f"\nGenerated {len(result['debit_files'])} debit account files:")
    for file_info in result["debit_files"]:
        print(f"  - {file_info['account']}: {file_info['file_name']} "
              f"({file_info['filtered_operations']} of {file_info['total_operations']} operations)")
    
    print(f"\nGenerated {len(result['credit_files'])} credit account files:")
    for file_info in result["credit_files"]:
        print(f"  - {file_info['account']}: {file_info['file_name']} "
              f"({file_info['filtered_operations']} of {file_info['total_operations']} operations)")
    
    # Verify that the account numbers are correct by checking a few examples
    # We expect to find accounts like 401, 602, etc.
    debit_accounts = [file_info["account"] for file_info in result["debit_files"]]
    credit_accounts = [file_info["account"] for file_info in result["credit_files"]]
    
    print(f"\nDebit accounts found: {debit_accounts[:5]}...")
    print(f"Credit accounts found: {credit_accounts[:5]}...")
    
    # Verify we have standard account numbers in the expected format
    assert any(account.startswith("4") for account in debit_accounts + credit_accounts), \
        "No accounts starting with 4 (e.g. 401) found"
    assert any(account.startswith("6") for account in debit_accounts + credit_accounts), \
        "No accounts starting with 6 (e.g. 602) found"


def test_ajur_account_grouping(db_session, ajur_operations):
    """Test that operations are correctly grouped by debit and credit accounts"""
    # Create the AjurAuditProcessor
    processor = AjurAuditProcessor(db_session)
    
    # Group operations by debit account
    debit_groups = processor._group_by_debit_account(ajur_operations)
    
    # Group operations by credit account
    credit_groups = processor._group_by_credit_account(ajur_operations)
    
    # Verify that we have groups for both debit and credit accounts
    assert len(debit_groups) > 0, "No debit account groups were created"
    assert len(credit_groups) > 0, "No credit account groups were created"
    
    # Print some information about the groups
    print(f"\nGrouped operations into {len(debit_groups)} debit account groups:")
    for account, ops in list(debit_groups.items())[:5]:  # Show first 5 groups
        print(f"  - {account}: {len(ops)} operations")
    
    print(f"\nGrouped operations into {len(credit_groups)} credit account groups:")
    for account, ops in list(credit_groups.items())[:5]:  # Show first 5 groups
        print(f"  - {account}: {len(ops)} operations")
    
    # Verify that the account numbers match those from the original operations
    for operation in ajur_operations[:10]:  # Check first 10 operations
        debit_account = operation.debit_account
        if debit_account and ';' in debit_account:
            debit_account = debit_account.split(';')[0]
        
        credit_account = operation.credit_account
        if credit_account and ';' in credit_account:
            credit_account = credit_account.split(';')[0]
        
        if debit_account:
            assert any(debit_account == account for account in debit_groups.keys()), \
                f"Debit account {debit_account} not found in grouped accounts"
        
        if credit_account:
            assert any(credit_account == account for account in credit_groups.keys()), \
                f"Credit account {credit_account} not found in grouped accounts"


if __name__ == "__main__":
    pytest.main(["-xvs", __file__])