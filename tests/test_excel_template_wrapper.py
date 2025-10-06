import io
import os
import pandas as pd
import pytest
import locale
from datetime import datetime, date

from app.services.excel_template_wrapper import ExcelTemplateWrapper
from app.services.accounting_operation_processor import AccountingOperationProcessor
from app.models.operation import AccountingOperation


def create_sample_operations(num_operations=5):
    """Create a list of sample AccountingOperation objects for testing"""
    operations = []
    
    for i in range(1, num_operations + 1):
        op = AccountingOperation(
            id=i,
            file_id=1,
            operation_date=date(2023, 1, i),
            document_type="Фактура",
            document_number=f"2023-{i:03d}",
            debit_account=f"411",
            credit_account=f"702",
            amount=i * 1000.0,
            description=f"Test operation {i}",
            partner_name="Test Partner",
            analytical_debit="Test Debit",
            analytical_credit="Test Credit",
            account_name="Test Account",
            sequence_number=i,
            verified_amount=i * 1000.0,
            deviation_amount=0.0,
            control_action="No issues found",
            deviation_note="",
            template_type="TEST",
            import_uuid="test-import-uuid"
        )
        operations.append(op)
    
    return operations


def test_create_template_workbook():
    """Test creating a template workbook from scratch"""
    wrapper = ExcelTemplateWrapper()
    wb = wrapper._create_template_workbook(
        company_name="Test Company",
        year="2023",
        auditor_name="Test Auditor"
    )
    
    # Check that the workbook was created
    assert wb is not None
    
    # Check that the sheet was created
    assert len(wb.sheetnames) == 1
    assert wb.sheetnames[0] == "Sheet1"
    
    # Check some key cells
    sheet = wb.active
    assert sheet["F2"].value == "Test Company"
    assert sheet["F5"].value == "2023"
    
    # Save the workbook to a BytesIO object for inspection
    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    
    # You can uncomment this to save the file for manual inspection
    with open("test_template.xlsx", "wb") as f:
        f.write(output.getvalue())
    
    print("Template workbook created successfully")


def test_wrap_excel_with_template():
    """Test wrapping an Excel file with the template"""
    # Create a sample DataFrame with multiple account groups to test subtotals
    df = pd.DataFrame({
        "№ по ред": [1, 2, 3, 4, 5, 6],
        "Документ №": ["DOC-001", "DOC-002", "DOC-003", "DOC-004", "DOC-005", "DOC-006"],
        "Дата": [date(2023, 1, 1), date(2023, 1, 2), date(2023, 1, 3),
                date(2023, 1, 4), date(2023, 1, 5), date(2023, 1, 6)],
        "Дт с/ка": ["411", "411", "411", "411", "411", "411"],
        "Аналитична сметка/Партньор (Дт)": ["Partner 1", "Partner 2", "Partner 3",
                                         "Partner 4", "Partner 5", "Partner 6"],
        "Кт с/ка": ["702", "702", "702", "705", "705", "709"],
        "Аналитична сметка/Партньор (Кт)": ["Credit 1", "Credit 2", "Credit 3",
                                         "Credit 4", "Credit 5", "Credit 6"],
        "Сума": [1000.0, 2000.0, 3000.0, 1500.0, 2500.0, 500.0],
        "Обяснение/Обоснование": ["Description 1", "Description 2", "Description 3",
                               "Description 4", "Description 5", "Description 6"],
        "Установена сума при одита": [1000.0, 2000.0, 3000.0, 1500.0, 2500.0, 500.0],
        "Отклонение": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
    })
    
    # Save the DataFrame to a BytesIO object
    excel_buffer = io.BytesIO()
    df.to_excel(excel_buffer, index=False)
    excel_buffer.seek(0)
    
    # Wrap the Excel file with the template
    wrapper = ExcelTemplateWrapper()
    wrapped_excel = wrapper.wrap_excel_with_template(
        excel_buffer,
        company_name="Форт България ЕООД",
        year="2023"
    )
    
    # Check that the wrapped Excel file was created
    assert wrapped_excel is not None
    
    # Save the file for inspection
    with open("test_wrapped.xlsx", "wb") as f:
        f.write(wrapped_excel.getvalue())
    
    print("Excel file wrapped with template successfully")


def test_wrap_and_upload_excel():
    """Test the wrap_and_upload_excel method with a mock S3 service"""
    # Create a mock S3Service
    class MockS3Service:
        def download_file(self, s3_key):
            # Create a sample DataFrame
            df = pd.DataFrame({
                "№ по ред": [1, 2, 3],
                "Документ №": ["DOC-001", "DOC-002", "DOC-003"],
                "Дата": [date(2023, 1, 1), date(2023, 1, 2), date(2023, 1, 3)],
                "Дт с/ка": ["411", "411", "411"],
                "Аналитична сметка/Партньор (Дт)": ["Partner 1", "Partner 2", "Partner 3"],
                "Кт с/ка": ["702", "702", "702"],
                "Аналитична сметка/Партньор (Кт)": ["Credit 1", "Credit 2", "Credit 3"],
                "Сума": [1000.0, 2000.0, 3000.0],
                "Обяснение/Обоснование": ["Description 1", "Description 2", "Description 3"],
                "Установена сума при одита": [1000.0, 2000.0, 3000.0],
                "Отклонение": [0.0, 0.0, 0.0]
            })
            
            # Save the DataFrame to a BytesIO object
            excel_buffer = io.BytesIO()
            df.to_excel(excel_buffer, index=False)
            excel_buffer.seek(0)
            
            return excel_buffer.getvalue()
            
        def upload_file(self, file_content, s3_key):
            # Just return success without actually uploading
            return True, f"Mock upload of {s3_key}"
    
    # Create an ExcelTemplateWrapper with the mock S3Service
    wrapper = ExcelTemplateWrapper()
    wrapper.s3_service = MockS3Service()
    
    # Test the wrap_and_upload_excel method
    s3_key = wrapper.wrap_and_upload_excel(
        s3_key="test/path/test_file.xlsx",
        company_name="Test Company",
        year="2023"
    )
    
    # Check that the method returned an S3 key
    assert s3_key is not None
    assert "test/path/test_file_wrapped.xlsx" == s3_key
    
    print("wrap_and_upload_excel method tested successfully")


def test_accounting_operation_processor_with_template():
    """Test the AccountingOperationProcessor with the template wrapper"""
    # Create a mock S3Service that doesn't actually upload files
    class MockS3Service:
        def upload_file(self, file_content, object_name):
            # Save the file for inspection
            with open("test_processor_output.xlsx", "wb") as f:
                f.write(file_content.getvalue())
            # Return success
            return True, f"Mock upload of {object_name}"
    
    # Create a mock DB session
    class MockSession:
        def query(self, *args, **kwargs):
            return self
            
        def filter(self, *args, **kwargs):
            return self
            
        def all(self):
            return []
    
    # Create sample operations
    operations = create_sample_operations(5)
    
    # Create an AccountingOperationProcessor with mocked dependencies
    processor = AccountingOperationProcessor(MockSession())
    
    # Replace the S3Service with our mock
    processor.s3_service = MockS3Service()
    
    # Test the _generate_and_upload_file method
    s3_key = processor._generate_and_upload_file(
        operations=operations,
        file_name="test_file.xlsx",
        account_type="debit",
        import_uuid="test-import-uuid"
    )
    
    # Check that the method returned an S3 key
    assert s3_key is not None
    assert "test-import-uuid" in s3_key
    assert "test_file.xlsx" in s3_key
    
    print("AccountingOperationProcessor tested successfully")


if __name__ == "__main__":
    # Run the tests
    print("Running tests for ExcelTemplateWrapper...")
    test_create_template_workbook()
    test_wrap_excel_with_template()
    test_wrap_and_upload_excel()
    test_accounting_operation_processor_with_template()
    
    print("\nAll tests passed!")