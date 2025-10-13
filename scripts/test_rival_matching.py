"""
Test script for account matching with Rival template files

This script tests the AccountMatcher service integration with the RivalParser
using the provided files.
"""

import os
import sys
import pandas as pd
from datetime import datetime

# Add the project root directory to Python path to make imports work
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.services.parsers.rival_parser import RivalParser
from app.services.account_matcher import AccountMatcher


def test_rival_matching(rival_file_path, credit_file_path):
    """
    Test account matching functionality with provided files
    
    Args:
        rival_file_path: Path to the Rival format Excel file
        credit_file_path: Path to the CREDIT format Excel file
    """
    print(f"Testing account matching with files:")
    print(f"- Rival file: {rival_file_path}")
    print(f"- Credit file: {credit_file_path}")
    print("-" * 60)
    
    # Initialize parser and account matcher
    rival_parser = RivalParser()
    account_matcher = AccountMatcher()
    
    # 1. Parse Rival file
    print("Parsing Rival file...")
    rival_operations = rival_parser.parse(rival_file_path, file_id=1)
    print(f"Extracted {len(rival_operations)} operations from Rival file")
    
    # 2. Read CREDIT file using pandas
    print("\nReading CREDIT file...")
    credit_df = pd.read_excel(credit_file_path)
    
    # Find the data start row (row with column headers)
    data_start_row = None
    for i, row in credit_df.iterrows():
        if '№ по ред' in str(row.iloc[0]):
            data_start_row = i
            break
    
    if data_start_row is None:
        print("ERROR: Could not find column headers in the CREDIT file")
        return
    
    # Get column headers and data
    headers = credit_df.iloc[data_start_row]
    data_rows = credit_df.iloc[data_start_row + 1:].reset_index(drop=True)
    data_rows.columns = headers
    
    # 3. Convert CREDIT data to operations format
    print("Converting CREDIT data to operations format...")
    credit_operations = []
    
    for _, row in data_rows.iterrows():
        if pd.isna(row['Сума']):
            continue
            
        try:
            date_value = row['Дата']
            if isinstance(date_value, str):
                try:
                    date_value = datetime.strptime(date_value, "%Y-%m-%d")
                except ValueError:
                    try:
                        date_value = datetime.strptime(date_value, "%d.%m.%Y")
                    except:
                        pass
            
            credit_operations.append({
                "document_number": str(row['Документ №']),
                "operation_date": date_value,
                "debit_account": row['Дт с/ка'] if not pd.isna(row['Дт с/ка']) else "",
                "credit_account": row['Кт с/ка'] if not pd.isna(row['Кт с/ка']) else "",
                "amount": float(row['Сума']),
                "description": row['Обяснение/Обоснование'] if not pd.isna(row['Обяснение/Обоснование']) else ""
            })
        except Exception as e:
            print(f"Error processing row: {e}")
    
    print(f"Extracted {len(credit_operations)} operations from CREDIT file")
    
    # 4. Count missing debit accounts in CREDIT operations
    missing_debit_before = sum(1 for op in credit_operations if not op['debit_account'])
    missing_credit_before = sum(1 for op in credit_operations if not op['credit_account'])
    
    print(f"\nBefore matching:")
    print(f"- Missing debit accounts: {missing_debit_before} of {len(credit_operations)}")
    print(f"- Missing credit accounts: {missing_credit_before} of {len(credit_operations)}")
    
    # 5. Apply account matching
    print("\nApplying account matching...")
    enriched_operations = account_matcher.match_rival_accounts(credit_operations, rival_operations)
    
    # 6. Count missing accounts after matching
    missing_debit_after = sum(1 for op in enriched_operations if not op['debit_account'])
    missing_credit_after = sum(1 for op in enriched_operations if not op['credit_account'])
    
    print(f"\nAfter matching:")
    print(f"- Missing debit accounts: {missing_debit_after} of {len(enriched_operations)}")
    print(f"- Missing credit accounts: {missing_credit_after} of {len(enriched_operations)}")
    
    # 7. Calculate improvement
    debit_improvement = missing_debit_before - missing_debit_after
    credit_improvement = missing_credit_before - missing_credit_after
    
    print(f"\nMatching results:")
    print(f"- Filled {debit_improvement} debit accounts ({debit_improvement/len(credit_operations)*100:.2f}%)")
    print(f"- Filled {credit_improvement} credit accounts ({credit_improvement/len(credit_operations)*100:.2f}%)")
    
    # 8. Display some samples of matched accounts
    print("\nSample matched operations:")
    samples_shown = 0
    
    for i, op in enumerate(enriched_operations):
        original_op = credit_operations[i]
        
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
                
            print(f"  Description: {op['description']}")
            
            samples_shown += 1
            if samples_shown >= 5:
                break
    
    print("\nTest completed successfully!")


if __name__ == "__main__":
    # Default file paths
    rival_file = "files/хронология Ривал.xlsx"
    credit_file = "files/1fb018fa-cc56-4584-b9dc-cc9bbd50b3dd-CREDIT-240__20251013082227.xlsx"
    
    # Check command-line arguments
    if len(sys.argv) > 1:
        rival_file = sys.argv[1]
    if len(sys.argv) > 2:
        credit_file = sys.argv[2]
    
    # Run the test
    test_rival_matching(rival_file, credit_file)