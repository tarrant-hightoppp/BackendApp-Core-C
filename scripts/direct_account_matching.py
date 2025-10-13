"""
Direct Account Matching Example

This script demonstrates how to use the AccountMatcher service directly in code
to match credit with debit accounts and vice versa.
"""

import os
import sys
import pandas as pd
from datetime import datetime

# Add project root to Python path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.services.account_matcher import AccountMatcher
from app.services.parsers.rival_parser import RivalParser


def load_operations_from_excel(file_path):
    """Load operations from an Excel file with accounting data"""
    
    # Read the Excel file
    df = pd.read_excel(file_path)
    
    # Find the data start row (row with column headers)
    data_start_row = None
    for i, row in df.iterrows():
        if '№ по ред' in str(row.iloc[0]):
            data_start_row = i
            break
    
    if data_start_row is None:
        raise ValueError("Could not find column headers in the file")
    
    # Get column headers
    headers = df.iloc[data_start_row]
    
    # Extract data rows
    data_rows = df.iloc[data_start_row + 1:].reset_index(drop=True)
    data_rows.columns = headers
    
    # Convert to operations format
    operations = []
    for _, row in data_rows.iterrows():
        if pd.isna(row.get('Сума', None)):
            continue
            
        try:
            date_value = row.get('Дата')
            if isinstance(date_value, str):
                try:
                    date_value = datetime.strptime(date_value, "%Y-%m-%d")
                except ValueError:
                    try:
                        date_value = datetime.strptime(date_value, "%d.%m.%Y")
                    except:
                        pass
            
            operations.append({
                "document_number": str(row.get('Документ №', '')),
                "operation_date": date_value,
                "debit_account": row.get('Дт с/ка', '') if not pd.isna(row.get('Дт с/ка', '')) else "",
                "credit_account": row.get('Кт с/ка', '') if not pd.isna(row.get('Кт с/ка', '')) else "",
                "amount": float(row.get('Сума', 0)),
                "description": row.get('Обяснение/Обоснование', '') if not pd.isna(row.get('Обяснение/Обоснование', '')) else ""
            })
        except Exception as e:
            print(f"Error processing row: {e}")
    
    return operations


def example_1_match_credit_with_debit():
    """Example 1: Match credit operations with missing debit accounts"""
    
    print("\n=== Example 1: Match credit operations with missing debit accounts ===")
    
    # Create sample operations with missing debit accounts
    credit_operations = [
        {
            "document_number": "0000000003",
            "operation_date": datetime(2024, 1, 31),
            "debit_account": "",  # Missing debit account
            "credit_account": "240001",
            "amount": 2056.80,
            "description": "ОСЧЕТОВОДЯВАНЕ НА АМОРТИЗАЦИИ ЗА МЕСЕЦ - Януари"
        },
        {
            "document_number": "0000000020",
            "operation_date": datetime(2024, 2, 29),
            "debit_account": "",  # Missing debit account
            "credit_account": "240001",
            "amount": 2056.80,
            "description": "ОСЧЕТОВОДЯВАНЕ НА АМОРТИЗАЦИИ ЗА МЕСЕЦ - Февруари"
        },
        {
            "document_number": "0000000031",
            "operation_date": datetime(2024, 3, 31),
            "debit_account": "602001",  # This one has the debit account - will be used as reference
            "credit_account": "240001",
            "amount": 2056.80,
            "description": "ОСЧЕТОВОДЯВАНЕ НА АМОРТИЗАЦИИ ЗА МЕСЕЦ - Март"
        }
    ]
    
    # Create matcher instance
    matcher = AccountMatcher()
    
    # Match credit operations with debit accounts
    enriched_operations = matcher.match_credit_with_debit(credit_operations)
    
    # Display results
    print(f"Before matching: {sum(1 for op in credit_operations if not op['debit_account'])} operations missing debit accounts")
    print(f"After matching: {sum(1 for op in enriched_operations if not op['debit_account'])} operations still missing debit accounts")
    
    for i, op in enumerate(enriched_operations):
        original_op = credit_operations[i]
        if not original_op['debit_account'] and op['debit_account']:
            print(f"\nOperation matched:")
            print(f"  Document: {op['document_number']} | Date: {op['operation_date']} | Amount: {op['amount']}")
            print(f"  Debit Account: [MATCHED] {op['debit_account']}")
            print(f"  Credit Account: {op['credit_account']}")


def example_2_match_debit_with_credit():
    """Example 2: Match debit operations with missing credit accounts"""
    
    print("\n=== Example 2: Match debit operations with missing credit accounts ===")
    
    # Create sample operations with missing credit accounts
    debit_operations = [
        {
            "document_number": "INV001",
            "operation_date": datetime(2024, 4, 15),
            "debit_account": "101001",
            "credit_account": "",  # Missing credit account
            "amount": 1000.00,
            "description": "Invoice payment"
        },
        {
            "document_number": "INV002",
            "operation_date": datetime(2024, 4, 20),
            "debit_account": "101002",
            "credit_account": "",  # Missing credit account
            "amount": 2500.00,
            "description": "Equipment purchase"
        },
        {
            "document_number": "INV002",
            "operation_date": datetime(2024, 4, 20),
            "debit_account": "101002",
            "credit_account": "200001",  # This one has the credit account - will be used as reference
            "amount": 2500.00,
            "description": "Equipment purchase"
        }
    ]
    
    # Create matcher instance
    matcher = AccountMatcher()
    
    # Match debit operations with credit accounts
    enriched_operations = matcher.match_debit_with_credit(debit_operations)
    
    # Display results
    print(f"Before matching: {sum(1 for op in debit_operations if not op['credit_account'])} operations missing credit accounts")
    print(f"After matching: {sum(1 for op in enriched_operations if not op['credit_account'])} operations still missing credit accounts")
    
    for i, op in enumerate(enriched_operations):
        original_op = debit_operations[i]
        if not original_op['credit_account'] and op['credit_account']:
            print(f"\nOperation matched:")
            print(f"  Document: {op['document_number']} | Date: {op['operation_date']} | Amount: {op['amount']}")
            print(f"  Debit Account: {op['debit_account']}")
            print(f"  Credit Account: [MATCHED] {op['credit_account']}")


def example_3_cross_match_files():
    """Example 3: Cross-match between two separate sets of operations from different files"""
    
    print("\n=== Example 3: Cross-match between two separate files ===")
    
    # Create matcher instance
    matcher = AccountMatcher()
    
    # Sample operations (in a real scenario, you would load these from files)
    credit_operations = [
        {
            "document_number": "DOC001",
            "operation_date": datetime(2024, 5, 10),
            "debit_account": "",  # Missing debit account
            "credit_account": "702001",
            "amount": 5000.00,
            "description": "Revenue recognition"
        }
    ]
    
    debit_operations = [
        {
            "document_number": "DOC001",
            "operation_date": datetime(2024, 5, 10),
            "debit_account": "411001",
            "credit_account": "",  # Missing credit account
            "amount": 5000.00,
            "description": "Client payment"
        }
    ]
    
    # Cross-match between the two sets of operations
    enriched_debit, enriched_credit = matcher.cross_match_accounts(
        debit_operations,
        credit_operations
    )
    
    # Display results for credit operations
    print("Credit operations:")
    for i, op in enumerate(enriched_credit):
        original_op = credit_operations[i]
        print(f"  Document: {op['document_number']} | Date: {op['operation_date']} | Amount: {op['amount']}")
        
        if not original_op['debit_account'] and op['debit_account']:
            print(f"  Debit Account: [MATCHED] {op['debit_account']}")
        else:
            print(f"  Debit Account: {op['debit_account']}")
            
        print(f"  Credit Account: {op['credit_account']}")
    
    # Display results for debit operations
    print("\nDebit operations:")
    for i, op in enumerate(enriched_debit):
        original_op = debit_operations[i]
        print(f"  Document: {op['document_number']} | Date: {op['operation_date']} | Amount: {op['amount']}")
        print(f"  Debit Account: {op['debit_account']}")
        
        if not original_op['credit_account'] and op['credit_account']:
            print(f"  Credit Account: [MATCHED] {op['credit_account']}")
        else:
            print(f"  Credit Account: {op['credit_account']}")


def example_4_match_with_file():
    """Example 4: Match operations from a real file (if available)"""
    
    # File paths
    credit_file = "files/a1fe03df-3be2-40e1-81cd-18a2f27d4c13-CREDIT-240__20251013093034.xlsx"
    
    if not os.path.exists(credit_file):
        print(f"\n=== Example 4: File not found: {credit_file} ===")
        return
        
    print(f"\n=== Example 4: Match operations from file {credit_file} ===")
    
    try:
        # Load operations from file
        operations = load_operations_from_excel(credit_file)
        
        if not operations:
            print("No operations found in the file")
            return
            
        # Count missing accounts
        missing_debit = sum(1 for op in operations if not op['debit_account'])
        missing_credit = sum(1 for op in operations if not op['credit_account'])
        
        print(f"Loaded {len(operations)} operations from file")
        print(f"Before matching: {missing_debit} operations missing debit accounts, {missing_credit} missing credit accounts")
        
        # Create matcher instance
        matcher = AccountMatcher()
        
        # Match operations
        enriched_operations = matcher.match_credit_with_debit(operations)
        
        # Count missing accounts after matching
        missing_debit_after = sum(1 for op in enriched_operations if not op['debit_account'])
        missing_credit_after = sum(1 for op in enriched_operations if not op['credit_account'])
        
        print(f"After matching: {missing_debit_after} operations missing debit accounts, {missing_credit_after} missing credit accounts")
        print(f"Filled: {missing_debit - missing_debit_after} debit accounts, {missing_credit - missing_credit_after} credit accounts")
        
        # Show a few examples of matched operations
        matched_count = 0
        for i, op in enumerate(enriched_operations):
            if matched_count >= 3:  # Show only the first 3 matched operations
                break
                
            original_op = operations[i]
            
            # Check if account was filled by matching
            if (not original_op['debit_account'] and op['debit_account']) or \
               (not original_op['credit_account'] and op['credit_account']):
                
                print(f"\nOperation {i+1}:")
                print(f"  Document: {op['document_number']} | Date: {op['operation_date']} | Amount: {op['amount']}")
                
                if not original_op['debit_account'] and op['debit_account']:
                    print(f"  Debit Account: [MATCHED] {op['debit_account']}")
                else:
                    print(f"  Debit Account: {op['debit_account']}")
                    
                if not original_op['credit_account'] and op['credit_account']:
                    print(f"  Credit Account: [MATCHED] {op['credit_account']}")
                else:
                    print(f"  Credit Account: {op['credit_account']}")
                    
                matched_count += 1
                
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    print("Direct Account Matching Examples")
    print("===============================")
    
    # Run all examples
    example_1_match_credit_with_debit()
    example_2_match_debit_with_credit()
    example_3_cross_match_files()
    example_4_match_with_file()