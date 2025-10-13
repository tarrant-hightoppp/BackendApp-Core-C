"""
Test the improved account matching algorithm with confidence scoring.

This script compares the original matching approach with the new confidence-based
approach using test data to show the accuracy improvements.
"""

import os
import sys
import pandas as pd
from datetime import datetime
import logging

# Add project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.services.account_matcher import AccountMatcher


# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def create_test_operations():
    """Create a set of test operations with some challenging matching scenarios"""
    
    # Operations with various challenging scenarios
    operations = [
        # Operation with slightly different document number formats (leading zeros)
        {
            "document_number": "0000123",
            "operation_date": datetime(2024, 5, 15),
            "debit_account": "101001",
            "credit_account": None,  # Missing credit account
            "amount": 1500.00,
            "description": "Test with leading zeros"
        },
        # Matching operation with different document format
        {
            "document_number": "123",  # Same as above but without leading zeros
            "operation_date": datetime(2024, 5, 15),
            "debit_account": None,  # Missing debit account
            "credit_account": "401001",
            "amount": 1500.00,
            "description": "Test without leading zeros"
        },
        
        # Operations with special characters in document numbers
        {
            "document_number": "INV-456",
            "operation_date": datetime(2024, 6, 20),
            "debit_account": "101002",
            "credit_account": None,  # Missing credit account
            "amount": 2500.00,
            "description": "Test with hyphen"
        },
        # Matching operation with different format
        {
            "document_number": "INV/456",  # Same but with slash instead of hyphen
            "operation_date": datetime(2024, 6, 20),
            "debit_account": None,  # Missing debit account
            "credit_account": "402001",
            "amount": 2500.00,
            "description": "Test with slash"
        },
        
        # Operations with date time vs date objects
        {
            "document_number": "DOC789",
            "operation_date": datetime(2024, 7, 10, 14, 30, 0),  # Has time component
            "debit_account": "101003",
            "credit_account": None,  # Missing credit account
            "amount": 3500.00,
            "description": "Test with datetime"
        },
        # Matching operation with date only
        {
            "document_number": "DOC789",
            "operation_date": datetime(2024, 7, 10).date(),  # Date object only
            "debit_account": None,  # Missing debit account
            "credit_account": "403001",
            "amount": 3500.00,
            "description": "Test with date"
        },
        
        # Operations with large amount - test adaptive tolerance
        {
            "document_number": "BIG001",
            "operation_date": datetime(2024, 8, 5),
            "debit_account": "101004",
            "credit_account": None,  # Missing credit account
            "amount": 1000000.50,  # Large amount
            "description": "Test with large amount"
        },
        # Matching operation with small difference in large amount
        {
            "document_number": "BIG001",
            "operation_date": datetime(2024, 8, 5),
            "debit_account": None,  # Missing debit account
            "credit_account": "404001",
            "amount": 1000000.75,  # Small difference (0.25) but > 0.01 tolerance
            "description": "Test with large amount slight difference"
        },
        
        # Operations with multiple potential matches - test confidence scoring
        {
            "document_number": "MULTI001",
            "operation_date": datetime(2024, 9, 15),
            "debit_account": "101005",
            "credit_account": None,  # Missing credit account
            "amount": 5000.00,
            "description": "Test with multiple potential matches - correct one"
        },
        {
            "document_number": "MULTI001",
            "operation_date": datetime(2024, 9, 15),
            "debit_account": "101006",  # Different debit account
            "credit_account": None,  # Missing credit account
            "amount": 5000.00,  # Same amount
            "description": "Test with multiple potential matches - incorrect one"
        },
        {
            "document_number": "MULTI001",
            "operation_date": datetime(2024, 9, 15),
            "debit_account": None,  # Missing debit account
            "credit_account": "405001",
            "amount": 5000.00,
            "description": "Test to match with the correct debit account"
        },
    ]
    
    return operations


def test_and_compare_matching():
    """Test and compare the improved confidence-based matching algorithm"""
    # Create test operations
    operations = create_test_operations()
    
    # Initialize matcher
    matcher = AccountMatcher()
    
    # Enable detailed logging
    matcher.enable_detailed_logging = True
    
    # Set confidence threshold for demonstration
    matcher.confidence_threshold = 70  # 0-100 scale
    
    print("\n=== Testing Account Matching with Improved Algorithm ===\n")
    print(f"Confidence threshold: {matcher.confidence_threshold}\n")
    
    print("Original Operations:")
    for i, op in enumerate(operations):
        print(f"Op #{i+1}: Doc: {op['document_number']}, Date: {op['operation_date']}, "
              f"Debit: {op['debit_account'] or 'MISSING'}, Credit: {op['credit_account'] or 'MISSING'}, "
              f"Amount: {op['amount']}")
    
    # Match operations with confidence-based approach
    print("\nRunning account matching...")
    enriched_operations = matcher.match_credit_with_debit(operations)
    enriched_operations = matcher.match_debit_with_credit(enriched_operations)
    
    # Print results with match quality
    print("\nMatched Operations:")
    for i, op in enumerate(enriched_operations):
        orig_op = operations[i]
        
        debit_matched = (orig_op['debit_account'] is None and op['debit_account'])
        credit_matched = (orig_op['credit_account'] is None and op['credit_account'])
        
        if debit_matched or credit_matched:
            match_quality = op.get('_match_quality', 'unknown')
            match_conf = op.get('_match_confidence', 0)
            
            print(f"Op #{i+1}: Doc: {op['document_number']}, Date: {op['operation_date']}")
            
            if debit_matched:
                print(f"  Debit Account: [MATCHED] {op['debit_account']} (quality: {match_quality}, confidence: {match_conf})")
            else:
                print(f"  Debit Account: {op['debit_account']}")
                
            if credit_matched:
                print(f"  Credit Account: [MATCHED] {op['credit_account']} (quality: {match_quality}, confidence: {match_conf})")
            else:
                print(f"  Credit Account: {op['credit_account']}")
                
            print(f"  Amount: {op['amount']}")
            print(f"  Description: {op['description']}")
            print()
    
    # Count matches
    debit_matches = sum(1 for i, op in enumerate(enriched_operations) 
                        if operations[i]['debit_account'] is None and op['debit_account'])
                        
    credit_matches = sum(1 for i, op in enumerate(enriched_operations) 
                        if operations[i]['credit_account'] is None and op['credit_account'])
                        
    # Calculate statistics  
    debit_missing = sum(1 for op in operations if op['debit_account'] is None)
    credit_missing = sum(1 for op in operations if op['credit_account'] is None)
    
    print("\nMatching Statistics:")
    print(f"Debit accounts: filled {debit_matches} of {debit_missing} missing ({debit_matches/debit_missing*100:.2f}%)")
    print(f"Credit accounts: filled {credit_matches} of {credit_missing} missing ({credit_matches/credit_missing*100:.2f}%)")
    

if __name__ == "__main__":
    test_and_compare_matching()