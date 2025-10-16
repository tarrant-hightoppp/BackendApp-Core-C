import pandas as pd
from app.services.parsers.ajur_parser import AjurParser

def test_ajur_parser_fix():
    """Test that AJUR parser correctly extracts account numbers"""
    
    parser = AjurParser()
    
    # Parse the AJUR file
    operations = parser.parse("files/ajur.xlsx", file_id=1, import_uuid="test-uuid")
    
    print(f"\nTotal operations parsed: {len(operations)}")
    
    # Check first few operations to verify correct parsing
    print("\nFirst 5 operations:")
    for i, op in enumerate(operations[:5]):
        print(f"\nOperation {i+1}:")
        print(f"  Date: {op['operation_date']}")
        print(f"  Debit Account: {op['debit_account']}")
        print(f"  Credit Account: {op['credit_account']}")
        print(f"  Analytical Debit: {op['analytical_debit']}")
        print(f"  Analytical Credit: {op['analytical_credit']}")
        print(f"  Amount: {op['amount']}")
        print(f"  Description: {op['description']}")
        
        # Verify credit account format
        if op['credit_account']:
            # Credit account should be in format XXX/YYY or XXX, not contain semicolons
            if ';' in op['credit_account']:
                print(f"  ❌ ERROR: Credit account contains semicolon: {op['credit_account']}")
            else:
                print(f"  ✓ Credit account format is correct")
        
        # Verify analytical credit is separate
        if op['analytical_credit'] and ';' in op['analytical_credit']:
            print(f"  ✓ Analytical credit contains detailed info: {op['analytical_credit']}")

    # Check specific known cases from the Excel file
    print("\n\nChecking specific test cases:")
    
    # First operation should have credit account "401/1" and analytical credit "20;ЕКОНТ Експрес ООД;1220700737;11.01.2023"
    if operations:
        op1 = operations[0]
        expected_credit = "401/1"
        expected_analytical = "20;ЕКОНТ Експрес ООД;1220700737;11.01.2023"
        
        print(f"\nOperation 1:")
        print(f"  Expected credit account: {expected_credit}")
        print(f"  Actual credit account: {op1['credit_account']}")
        print(f"  Match: {'✓' if op1['credit_account'] == expected_credit else '❌'}")
        
        print(f"\n  Expected analytical credit: {expected_analytical}")
        print(f"  Actual analytical credit: {op1['analytical_credit']}")
        print(f"  Match: {'✓' if op1['analytical_credit'] == expected_analytical else '❌'}")
        
    # Count operations with correct format
    correct_format_count = 0
    incorrect_format_count = 0
    
    for op in operations:
        if op['credit_account'] and ';' not in op['credit_account']:
            correct_format_count += 1
        elif op['credit_account'] and ';' in op['credit_account']:
            incorrect_format_count += 1
            
    print(f"\n\nSummary:")
    print(f"  Operations with correct credit account format: {correct_format_count}")
    print(f"  Operations with incorrect credit account format: {incorrect_format_count}")
    
    if incorrect_format_count == 0:
        print("\n✓ All credit accounts are in the correct format!")
    else:
        print(f"\n❌ {incorrect_format_count} operations have incorrect credit account format")

if __name__ == "__main__":
    test_ajur_parser_fix()