import os
import sys
from datetime import datetime

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.services.accounting_operation_processor import AccountingOperationProcessor
from app.services.excel_template_wrapper import ExcelTemplateWrapper
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

def create_test_operations():
    """Create test operations with various account formats"""
    operations = []
    
    # Create operations with different account formats
    # Format: debit_account, credit_account, amount
    test_accounts = [
        ("453/2", "503/1", 100.0),
        ("453/9", "503/1", 200.0),
        ("453/1", "503/1", 300.0),
        ("411", "503/1", 400.0),
        ("453/2", "411", 500.0),
        ("453/9", "411", 600.0),
        # Add nested subaccounts
        ("453/2/1", "503/1/2", 700.0),
        ("453/2/3", "503/1/2", 800.0),
        ("453", "503", 900.0),  # Main account summary
        ("453/2", "503/1", 1000.0),  # Duplicate to test grouping
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

def test_audit_approach_full():
    """Test the full audit approach (100% population check)"""
    print("\n=== Testing FULL Audit Approach (100% population check) ===")
    
    # Create processor with mock session
    processor = AccountingOperationProcessor(MockSession())
    
    # Create test operations
    operations = create_test_operations()
    
    # Test the _process_accounts method with full audit approach
    print("\nTesting DEBIT account processing with FULL audit approach:")
    debit_results = processor._process_accounts(operations, "debit", "test-import", "full")
    
    print(f"Generated {len(debit_results)} debit account files:")
    for result in debit_results:
        print(f"  Account '{result['account']}' with {result['total_operations']} operations")
        print(f"  Filtered operations: {result['filtered_operations']}")
        # Verify that all operations are included (no filtering)
        assert result['total_operations'] == result['filtered_operations'], "Full audit approach should include all operations"
    
    return True

def test_audit_approach_statistical():
    """Test the statistical audit approach (80/20 rule)"""
    print("\n=== Testing STATISTICAL Audit Approach (80/20 rule) ===")
    
    # Create processor with mock session
    processor = AccountingOperationProcessor(MockSession())
    
    # Create test operations - add more to trigger the 80/20 rule
    operations = create_test_operations()
    
    # Add more operations to exceed the minimum threshold
    for i in range(30):
        op = AccountingOperation(
            id=100+i,
            import_uuid="test-import",
            sequence_number=100+i,
            document_type="Test",
            document_number=f"DOC-{100+i}",
            operation_date=datetime.now(),
            debit_account="453",
            credit_account="503",
            amount=10.0,  # Small amount to ensure they're filtered out
            description=f"Small test operation {i+1}"
        )
        operations.append(op)
    
    # Test the _process_accounts method with statistical audit approach
    print("\nTesting DEBIT account processing with STATISTICAL audit approach:")
    debit_results = processor._process_accounts(operations, "debit", "test-import", "statistical")
    
    print(f"Generated {len(debit_results)} debit account files:")
    for result in debit_results:
        print(f"  Account '{result['account']}' with {result['total_operations']} operations")
        print(f"  Filtered operations: {result['filtered_operations']}")
        
        # For accounts with more than 30 operations, filtered should be less than total
        if result['total_operations'] > 30:
            assert result['filtered_operations'] < result['total_operations'], "Statistical approach should filter operations for accounts with >30 operations"
    
    return True

def test_audit_approach_selected():
    """Test the selected objects audit approach"""
    print("\n=== Testing SELECTED OBJECTS Audit Approach ===")
    
    # Create processor with mock session
    processor = AccountingOperationProcessor(MockSession())
    
    # Create test operations
    operations = create_test_operations()
    
    # Test the _process_accounts method with selected objects audit approach
    print("\nTesting DEBIT account processing with SELECTED OBJECTS audit approach:")
    debit_results = processor._process_accounts(operations, "debit", "test-import", "selected")
    
    print(f"Generated {len(debit_results)} debit account files:")
    for result in debit_results:
        print(f"  Account '{result['account']}' with {result['total_operations']} operations")
        print(f"  Filtered operations: {result['filtered_operations']}")
    
    return True

def test_template_with_audit_approach():
    """Test the template generation with different audit approaches"""
    print("\n=== Testing Template Generation with Different Audit Approaches ===")
    
    # Create template wrapper
    template_wrapper = ExcelTemplateWrapper()
    
    # Test template creation with different audit approaches
    for approach in ["full", "statistical", "selected"]:
        print(f"\nCreating template with {approach.upper()} audit approach:")
        wb = template_wrapper._create_template_workbook(
            company_name="Test Company",
            year="2025",
            audit_approach=approach
        )
        
        # Check the X mark position based on the audit approach
        ws = wb.active
        
        if approach == "full":
            print(f"  Cell A16 (100% population): {ws['A16'].value}")
            print(f"  Cell A17 (selected objects): {ws['A17'].value}")
            print(f"  Cell A18 (statistical): {ws['A18'].value}")
            assert ws['A16'].value == "X", "Full approach should have X in cell A16"
            assert ws['A17'].value == "", "Full approach should not have X in cell A17"
            assert ws['A18'].value == "", "Full approach should not have X in cell A18"
        elif approach == "selected":
            print(f"  Cell A16 (100% population): {ws['A16'].value}")
            print(f"  Cell A17 (selected objects): {ws['A17'].value}")
            print(f"  Cell A18 (statistical): {ws['A18'].value}")
            assert ws['A16'].value == "", "Selected approach should not have X in cell A16"
            assert ws['A17'].value == "X", "Selected approach should have X in cell A17"
            assert ws['A18'].value == "", "Selected approach should not have X in cell A18"
        else:  # statistical
            print(f"  Cell A16 (100% population): {ws['A16'].value}")
            print(f"  Cell A17 (selected objects): {ws['A17'].value}")
            print(f"  Cell A18 (statistical): {ws['A18'].value}")
            assert ws['A16'].value == "", "Statistical approach should not have X in cell A16"
            assert ws['A17'].value == "", "Statistical approach should not have X in cell A17"
            assert ws['A18'].value == "X", "Statistical approach should have X in cell A18"
    
    return True

if __name__ == "__main__":
    print("Testing audit approach implementation")
    
    # Run tests
    full_success = test_audit_approach_full()
    statistical_success = test_audit_approach_statistical()
    selected_success = test_audit_approach_selected()
    template_success = test_template_with_audit_approach()
    
    if full_success and statistical_success and selected_success and template_success:
        print("\n✅ All tests passed successfully!")
    else:
        print("\n❌ Some tests failed")