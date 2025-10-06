import os
import sys
import io
import pandas as pd
from datetime import datetime

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.services.excel_template_wrapper import ExcelTemplateWrapper
from app.services.accounting_operation_processor import AccountingOperationProcessor
from app.models.operation import AccountingOperation

# Mock DB session for testing
class MockSession:
    def query(self, *args, **kwargs):
        return self
    
    def filter(self, *args, **kwargs):
        return self
    
    def all(self):
        return []
    
    def expire_all(self):
        pass

def create_test_operations(account_type="debit"):
    """Create test operations with various account formats"""
    operations = []
    
    # Create operations with different account formats based on account_type
    if account_type == "debit":
        # Test with debit accounts
        test_accounts = [
            ("453/2", "503/1", 100.0),
            ("453/9", "503/1", 200.0),
            ("453/1", "503/1", 300.0),
        ]
    else:
        # Test with credit accounts
        test_accounts = [
            ("411", "702/1", 400.0),
            ("411", "702/2", 500.0),
            ("411", "702/3", 600.0),
        ]
    
    for i, (debit, credit, amount) in enumerate(test_accounts):
        op = AccountingOperation(
            id=i+1,
            import_uuid="test-import",
            sequence_number=i+1,
            document_type="Test",
            document_number=f"DOC-{i+1}",
            operation_date=datetime.now(),
            debit_account=debit,
            credit_account=credit,
            amount=amount,
            description=f"Test operation {i+1}"
        )
        operations.append(op)
    
    return operations

def test_conclusion_text_for_account_type():
    """Test that the conclusion text references the correct account based on account_type"""
    print("\n=== Testing Conclusion Text Generation for Different Account Types ===")
    
    # Create a template wrapper
    wrapper = ExcelTemplateWrapper()
    
    # Test with debit account
    print("\nTesting conclusion text for DEBIT account:")
    
    # Create a sample DataFrame with debit account operations
    df_debit = pd.DataFrame({
        "№ по ред": [1, 2, 3],
        "Документ №": ["DOC-001", "DOC-002", "DOC-003"],
        "Дата": [datetime.now(), datetime.now(), datetime.now()],
        "Дт с/ка": ["453", "453/1", "453/2"],
        "Аналитична сметка/Партньор (Дт)": ["Partner 1", "Partner 2", "Partner 3"],
        "Кт с/ка": ["503", "503", "503"],
        "Аналитична сметка/Партньор (Кт)": ["Credit 1", "Credit 2", "Credit 3"],
        "Сума": [1000.0, 2000.0, 3000.0],
        "Обяснение/Обоснование": ["Description 1", "Description 2", "Description 3"]
    })
    
    # Save the DataFrame to a BytesIO object
    excel_buffer_debit = io.BytesIO()
    df_debit.to_excel(excel_buffer_debit, index=False)
    excel_buffer_debit.seek(0)
    
    # Wrap the Excel file with the template for debit account
    wrapped_excel_debit = wrapper.wrap_excel_with_template(
        excel_buffer_debit,
        company_name="Test Company",
        year="2023",
        account_type="debit"
    )
    
    # Save the file for inspection
    with open("test_conclusion_debit.xlsx", "wb") as f:
        f.write(wrapped_excel_debit.getvalue())
    
    print("Created test_conclusion_debit.xlsx for inspection")
    
    # Test with credit account
    print("\nTesting conclusion text for CREDIT account:")
    
    # Create a sample DataFrame with credit account operations
    df_credit = pd.DataFrame({
        "№ по ред": [1, 2, 3],
        "Документ №": ["DOC-001", "DOC-002", "DOC-003"],
        "Дата": [datetime.now(), datetime.now(), datetime.now()],
        "Дт с/ка": ["411", "411", "411"],
        "Аналитична сметка/Партньор (Дт)": ["Partner 1", "Partner 2", "Partner 3"],
        "Кт с/ка": ["702", "702/1", "702/2"],
        "Аналитична сметка/Партньор (Кт)": ["Credit 1", "Credit 2", "Credit 3"],
        "Сума": [1000.0, 2000.0, 3000.0],
        "Обяснение/Обоснование": ["Description 1", "Description 2", "Description 3"]
    })
    
    # Save the DataFrame to a BytesIO object
    excel_buffer_credit = io.BytesIO()
    df_credit.to_excel(excel_buffer_credit, index=False)
    excel_buffer_credit.seek(0)
    
    # Wrap the Excel file with the template for credit account
    wrapped_excel_credit = wrapper.wrap_excel_with_template(
        excel_buffer_credit,
        company_name="Test Company",
        year="2023",
        account_type="credit"
    )
    
    # Save the file for inspection
    with open("test_conclusion_credit.xlsx", "wb") as f:
        f.write(wrapped_excel_credit.getvalue())
    
    print("Created test_conclusion_credit.xlsx for inspection")
    
    # Test with the AccountingOperationProcessor
    print("\nTesting conclusion text with AccountingOperationProcessor:")
    
    # Create processor with mock session
    processor = AccountingOperationProcessor(MockSession())
    
    # Replace the S3Service with a mock that doesn't actually upload files
    class MockS3Service:
        def upload_file(self, file_content, object_name):
            # Save the file for inspection
            with open(f"test_processor_{object_name.split('/')[-1]}", "wb") as f:
                f.write(file_content.getvalue())
            # Return success
            return True, f"Mock upload of {object_name}"
    
    # Replace the S3Service with our mock
    processor.s3_service = MockS3Service()
    
    # Test with debit operations
    debit_operations = create_test_operations(account_type="debit")
    debit_key = processor._generate_and_upload_file(
        operations=debit_operations,
        file_name="test_debit_account.xlsx",
        account_type="debit",
        import_uuid="test-import"
    )
    
    print(f"Generated debit account file: {debit_key}")
    
    # Test with credit operations
    credit_operations = create_test_operations(account_type="credit")
    credit_key = processor._generate_and_upload_file(
        operations=credit_operations,
        file_name="test_credit_account.xlsx",
        account_type="credit",
        import_uuid="test-import"
    )
    
    print(f"Generated credit account file: {credit_key}")
    
    print("\nTest completed. Please check the generated Excel files to verify the conclusion text.")
    print("The conclusion text should reference the specific account being analyzed.")

if __name__ == "__main__":
    test_conclusion_text_for_account_type()