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

def test_account_grouping():
    """Test the account grouping logic"""
    print("\n=== Testing Account Grouping Logic ===")
    
    # Create processor with mock session
    processor = AccountingOperationProcessor(MockSession())
    
    # Create test operations
    operations = create_test_operations()
    
    # Test debit account grouping
    print("\nTesting DEBIT account grouping:")
    debit_groups = processor._group_by_account(operations, "debit")
    
    print(f"Found {len(debit_groups)} debit account groups:")
    for account, ops in debit_groups.items():
        print(f"  Account '{account}' has {len(ops)} operations")
    
    # Test credit account grouping
    print("\nTesting CREDIT account grouping:")
    credit_groups = processor._group_by_account(operations, "credit")
    
    print(f"Found {len(credit_groups)} credit account groups:")
    for account, ops in credit_groups.items():
        print(f"  Account '{account}' has {len(ops)} operations")
    
    # Verify that we have the correct number of groups
    # After our fix, we should have 4 debit groups (453/2, 453/9, 453/1, 411)
    # and 2 credit groups (503/1, 411)
    expected_debit_groups = 4
    expected_credit_groups = 2
    
    if len(debit_groups) == expected_debit_groups:
        print(f"\n✅ Debit grouping CORRECT: Found {len(debit_groups)} groups (expected {expected_debit_groups})")
    else:
        print(f"\n❌ Debit grouping ERROR: Found {len(debit_groups)} groups (expected {expected_debit_groups})")
    
    if len(credit_groups) == expected_credit_groups:
        print(f"✅ Credit grouping CORRECT: Found {len(credit_groups)} groups (expected {expected_credit_groups})")
    else:
        print(f"❌ Credit grouping ERROR: Found {len(credit_groups)} groups (expected {expected_credit_groups})")
    
    # Check specific accounts
    if "453/2" in debit_groups:
        print("✅ Found debit account '453/2' as expected")
    else:
        print("❌ Missing debit account '453/2'")
    
    if "453/9" in debit_groups:
        print("✅ Found debit account '453/9' as expected")
    else:
        print("❌ Missing debit account '453/9'")
    
    return len(debit_groups) == expected_debit_groups and len(credit_groups) == expected_credit_groups

if __name__ == "__main__":
    print("Testing account number processing after fix")
    success = test_account_grouping()
    
    if success:
        print("\n✅ All tests passed successfully!")
    else:
        print("\n❌ Some tests failed")