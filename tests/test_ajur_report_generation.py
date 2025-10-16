import os
import pytest
import uuid
from datetime import datetime
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.models.operation import AccountingOperation
from app.services.excel_report.template_generator import ReportGenerator
from app.services.parsers.ajur_parser import AjurParser


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
    """Create sample Ajur operations for testing report generation"""
    # Parse the sample file to get realistic operations
    parser = AjurParser()
    
    # Generate a unique import UUID for this test
    import_uuid = str(uuid.uuid4())
    file_id = -1  # Using a negative ID to avoid conflicts with real data
    
    operations_data = parser.parse(sample_ajur_file_path, file_id, import_uuid)
    
    # Store a subset of operations in the database
    db_operations = []
    for i, op_data in enumerate(operations_data[:10]):  # Use first 10 operations
        # Create the operation in the DB
        op_data["file_id"] = file_id
        op_data["import_uuid"] = import_uuid
        
        # Ensure we have valid dates
        if not op_data.get("operation_date"):
            op_data["operation_date"] = datetime.now().date()
            
        # Create the operation
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


def test_ajur_report_generation(db_session, ajur_operations):
    """Test that Ajur operations are correctly included in generated reports"""
    # Verify we have operations to work with
    assert len(ajur_operations) > 0, "No test operations available"
    
    # Get the import UUID from the first operation
    import_uuid = ajur_operations[0].import_uuid
    
    # Create a report generator
    generator = ReportGenerator(db_session)
    
    # Generate a report
    report_data = generator.generate_report(import_uuid)
    
    # Verify report was generated
    assert report_data is not None, "Report generation failed"
    assert isinstance(report_data, bytes), "Report should be returned as bytes"
    assert len(report_data) > 0, "Report should not be empty"
    
    # Save the report to a file for manual inspection if needed
    report_path = os.path.join("files", f"test_ajur_report_{import_uuid}.xlsx")
    with open(report_path, "wb") as f:
        f.write(report_data)
    
    print(f"Report saved to {report_path} for inspection")
    
    # Try to read it back with pandas to verify it's a valid Excel file
    try:
        import pandas as pd
        df = pd.read_excel(report_path)
        assert not df.empty, "Report should contain data"
        print(f"Report contains {len(df)} rows")
    except Exception as e:
        pytest.fail(f"Failed to read generated report: {e}")
    
    # Clean up the report file
    try:
        os.remove(report_path)
    except:
        pass


def test_ajur_specific_report_content(db_session, ajur_operations):
    """Test that Ajur-specific data is correctly included in the report"""
    import_uuid = ajur_operations[0].import_uuid
    
    # Create a report generator
    generator = ReportGenerator(db_session)
    
    # Generate a report
    report_data = generator.generate_report(import_uuid)
    
    # Save the report temporarily
    report_path = os.path.join("files", f"test_ajur_content_{import_uuid}.xlsx")
    with open(report_path, "wb") as f:
        f.write(report_data)
    
    try:
        # Read back the report to verify specific content
        import pandas as pd
        df = pd.read_excel(report_path)
        
        # Verify account columns exist
        account_columns = [col for col in df.columns if 'account' in str(col).lower()]
        assert len(account_columns) >= 2, "Report should contain at least debit and credit account columns"
        
        # Check for analytical accounts (which are important for Ajur)
        analytical_columns = [col for col in df.columns if 'analytical' in str(col).lower()]
        assert len(analytical_columns) >= 1, "Report should contain analytical account columns for Ajur data"
        
        # Verify some operations from our test data are included
        # We'll check for account numbers with the slash format typical of Ajur
        accounts_with_slash = False
        for idx, row in df.iterrows():
            # Try to check all account columns
            for col in account_columns:
                val = row.get(col)
                if isinstance(val, str) and '/' in val:
                    accounts_with_slash = True
                    print(f"Found Ajur-style account format: {val}")
                    break
            if accounts_with_slash:
                break
        
        assert accounts_with_slash, "Report should contain Ajur-style account formats with slashes"
        
        print("Successfully verified Ajur-specific content in the report")
    
    finally:
        # Clean up
        try:
            os.remove(report_path)
        except:
            pass


if __name__ == "__main__":
    pytest.main(["-xvs", __file__])