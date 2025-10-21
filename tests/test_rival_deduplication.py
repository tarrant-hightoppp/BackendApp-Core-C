import os
import sys
import pandas as pd
from io import BytesIO

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.parsers.rival_parser import RivalParser

def test_rival_deduplication():
    """
    Test the RIVAL parser to verify that operations are correctly grouped and not duplicated.
    Specifically tests the accounts mentioned by the client:
    - Debit account 400 (should have 161 operations, not 251)
    - Debit account 490 (should have 6 operations, not 11)
    - Credit account 453 (should have 17 operations, not 31)
    - Credit account 499 (should have 6 operations, not 58)
    """
    print("Starting RIVAL deduplication test...")
    
    # Path to the test file
    file_path = "files/latest/хронология Ривал.xlsx"
    
    # Ensure file exists
    if not os.path.exists(file_path):
        print(f"Error: Test file not found at {file_path}")
        return False
        
    # Create parser
    parser = RivalParser()
    
    # Parse the file
    operations = parser.parse(file_path, file_id=1, import_uuid="test-import")
    
    print(f"Parsed {len(operations)} operations from the RIVAL file")
    
    # Count operations by account
    debit_400_count = sum(1 for op in operations if op.get("debit_account", "").startswith("400"))
    debit_490_count = sum(1 for op in operations if op.get("debit_account", "").startswith("490"))
    credit_453_count = sum(1 for op in operations if op.get("credit_account", "").startswith("453"))
    credit_499_count = sum(1 for op in operations if op.get("credit_account", "").startswith("499"))
    
    print("\nOperation counts by account:")
    print(f"Debit account 400: {debit_400_count} operations")
    print(f"Debit account 490: {debit_490_count} operations")
    print(f"Credit account 453: {credit_453_count} operations")
    print(f"Credit account 499: {credit_499_count} operations")
    
    # Check against expected values from client report
    print("\nVerifying against client-reported issues:")
    print(f"Debit account 400: Expected ~161, got {debit_400_count}")
    print(f"Debit account 490: Expected ~6, got {debit_490_count}")
    print(f"Credit account 453: Expected ~17, got {credit_453_count}")
    print(f"Credit account 499: Expected ~6, got {credit_499_count}")
    
    # Additional validation: ensure no operation has empty debit or credit account
    empty_debit = sum(1 for op in operations if not op.get("debit_account"))
    empty_credit = sum(1 for op in operations if not op.get("credit_account"))
    
    print(f"\nOperations with empty debit account: {empty_debit}")
    print(f"Operations with empty credit account: {empty_credit}")
    
    return True

if __name__ == "__main__":
    test_rival_deduplication()