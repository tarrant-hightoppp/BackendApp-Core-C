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
from decimal import Decimal

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

def create_test_operations(account_type="debit", count=40):
    """Create test operations with various account formats"""
    operations = []
    
    # Create operations with different account formats based on account_type
    if account_type == "debit":
        # Test with debit accounts
        base_accounts = [
            ("453/2", "503/1"),
            ("453/9", "503/1"),
            ("453/1", "503/1"),
        ]
    else:
        # Test with credit accounts
        base_accounts = [
            ("411", "702/1"),
            ("411", "702/2"),
            ("411", "702/3"),
        ]
    
    # Create a larger number of operations to test filtering
    test_accounts = []
    for i in range(count):
        # Cycle through the base accounts
        base_account = base_accounts[i % len(base_accounts)]
        # Create operations with decreasing amounts to test the 80/20 rule
        amount = 1000.0 / (i + 1)
        test_accounts.append((base_account[0], base_account[1], amount))
    
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
    
    # Test with different audit approaches
    audit_approaches = ["statistical", "full", "selected"]
    
    for audit_approach in audit_approaches:
        print(f"\nTesting with audit approach: {audit_approach}")
        
        # Test with debit account
        print(f"Testing conclusion text for DEBIT account with {audit_approach} approach:")
        
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
            account_type="debit",
            audit_approach=audit_approach
        )
        
        # Save the file for inspection
        with open(f"test_conclusion_debit_{audit_approach}.xlsx", "wb") as f:
            f.write(wrapped_excel_debit.getvalue())
        
        print(f"Created test_conclusion_debit_{audit_approach}.xlsx for inspection")
        
        # Test with credit account
        print(f"Testing conclusion text for CREDIT account with {audit_approach} approach:")
        
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
            account_type="credit",
            audit_approach=audit_approach
        )
        
        # Save the file for inspection
        with open(f"test_conclusion_credit_{audit_approach}.xlsx", "wb") as f:
            f.write(wrapped_excel_credit.getvalue())
        
        print(f"Created test_conclusion_credit_{audit_approach}.xlsx for inspection")
    
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
    
    # Test with different audit approaches
    audit_approaches = ["statistical", "full", "selected"]
    
    for audit_approach in audit_approaches:
        print(f"\nTesting with audit approach: {audit_approach}")
        
        # Create a larger set of operations to test filtering
        debit_operations = create_test_operations(account_type="debit", count=50)
        credit_operations = create_test_operations(account_type="credit", count=50)
        
        # Calculate total amount for verification
        debit_total = sum(op.amount for op in debit_operations)
        credit_total = sum(op.amount for op in credit_operations)
        
        print(f"Created {len(debit_operations)} debit operations with total amount: {debit_total:.2f}")
        print(f"Created {len(credit_operations)} credit operations with total amount: {credit_total:.2f}")
        
        # Test filtering logic directly
        if audit_approach == "statistical":
            filtered_debit = processor._filter_operations(debit_operations, audit_approach=audit_approach)
            filtered_credit = processor._filter_operations(credit_operations, audit_approach=audit_approach)
            
            filtered_debit_total = sum(op.amount for op in filtered_debit)
            filtered_credit_total = sum(op.amount for op in filtered_credit)
            
            debit_percentage = (filtered_debit_total / debit_total) * 100
            credit_percentage = (filtered_credit_total / credit_total) * 100
            
            print(f"Statistical filtering for debit: {len(filtered_debit)} of {len(debit_operations)} operations")
            print(f"Representing {debit_percentage:.2f}% of total amount")
            
            print(f"Statistical filtering for credit: {len(filtered_credit)} of {len(credit_operations)} operations")
            print(f"Representing {credit_percentage:.2f}% of total amount")
            
            # Verify that filtering is working correctly (should be around 80%)
            assert 75 <= debit_percentage <= 100, f"Debit filtering percentage {debit_percentage:.2f}% is outside expected range"
            assert 75 <= credit_percentage <= 100, f"Credit filtering percentage {credit_percentage:.2f}% is outside expected range"
            assert len(filtered_debit) < len(debit_operations), "Statistical filtering should reduce the number of operations"
        else:
            # For full and selected approaches, no filtering should be applied
            filtered_debit = processor._filter_operations(debit_operations, audit_approach=audit_approach)
            filtered_credit = processor._filter_operations(credit_operations, audit_approach=audit_approach)
            
            # Verify that no filtering is applied for full approach
            assert len(filtered_debit) == len(debit_operations), f"{audit_approach} approach should include all operations"
            assert len(filtered_credit) == len(credit_operations), f"{audit_approach} approach should include all operations"
            
            print(f"{audit_approach} approach correctly includes all {len(filtered_debit)} debit operations")
            print(f"{audit_approach} approach correctly includes all {len(filtered_credit)} credit operations")
        
        # Test with debit operations
        debit_key = processor._generate_and_upload_file(
            operations=debit_operations,
            file_name=f"test_debit_account_{audit_approach}.xlsx",
            account_type="debit",
            import_uuid="test-import",
            audit_approach=audit_approach
        )
        
        print(f"Generated debit account file with {audit_approach} approach: {debit_key}")
        
        # Test with credit operations
        credit_key = processor._generate_and_upload_file(
            operations=credit_operations,
            file_name=f"test_credit_account_{audit_approach}.xlsx",
            account_type="credit",
            import_uuid="test-import",
            audit_approach=audit_approach
        )
        
        print(f"Generated credit account file with {audit_approach} approach: {credit_key}")
    
    print("\nTest completed. Please check the generated Excel files to verify the conclusion text.")
    print("The conclusion text should reference the specific account being analyzed.")

if __name__ == "__main__":
    test_conclusion_text_for_account_type()