import os
import sys
from datetime import datetime

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

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

def test_main_account_grouping():
    """Test the main account grouping logic"""
    print("\n=== Testing Main Account Grouping Logic ===")
    
    # Create processor with mock session
    processor = AccountingOperationProcessor(MockSession())
    
    # Create test operations
    operations = create_test_operations()
    
    # Test the _process_accounts method directly
    print("\nTesting DEBIT account processing:")
    debit_results = processor._process_accounts(operations, "debit", "test-import")
    
    print(f"Generated {len(debit_results)} debit account files:")
    for result in debit_results:
        print(f"  Account '{result['account']}' with {result['total_operations']} operations")
    
    print("\nTesting CREDIT account processing:")
    credit_results = processor._process_accounts(operations, "credit", "test-import")
    
    print(f"Generated {len(credit_results)} credit account files:")
    for result in credit_results:
        print(f"  Account '{result['account']}' with {result['total_operations']} operations")
    
    # Verify that we have the correct number of main account groups
    # After our fix, we should have 2 debit main account groups (453, 411)
    # and 2 credit main account groups (503, 411)
    expected_debit_groups = 2
    expected_credit_groups = 2
    
    if len(debit_results) == expected_debit_groups:
        print(f"\n✅ Debit grouping CORRECT: Found {len(debit_results)} main account groups (expected {expected_debit_groups})")
    else:
        print(f"\n❌ Debit grouping ERROR: Found {len(debit_results)} main account groups (expected {expected_debit_groups})")
    
    if len(credit_results) == expected_credit_groups:
        print(f"✅ Credit grouping CORRECT: Found {len(credit_results)} main account groups (expected {expected_credit_groups})")
    else:
        print(f"❌ Credit grouping ERROR: Found {len(credit_results)} main account groups (expected {expected_credit_groups})")
    
    # Check specific accounts
    debit_accounts = [result['account'] for result in debit_results]
    credit_accounts = [result['account'] for result in credit_results]
    
    if "453" in debit_accounts:
        print("✅ Found main debit account '453' as expected")
    else:
        print("❌ Missing main debit account '453'")
    
    if "411" in debit_accounts:
        print("✅ Found main debit account '411' as expected")
    else:
        print("❌ Missing main debit account '411'")
    
    if "503" in credit_accounts:
        print("✅ Found main credit account '503' as expected")
    else:
        print("❌ Missing main credit account '503'")
    
    return len(debit_results) == expected_debit_groups and len(credit_results) == expected_credit_groups

if __name__ == "__main__":
    print("Testing account number processing after fix")
    success = test_main_account_grouping()
    
    if success:
        print("\n✅ All tests passed successfully!")
    else:
        print("\n❌ Some tests failed")