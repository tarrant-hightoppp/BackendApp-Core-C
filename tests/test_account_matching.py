"""
Test Account Matching functionality

This module tests the AccountMatcher service's ability to match and enrich
accounting operations with missing account information.
"""
import unittest
import pandas as pd
from datetime import datetime
from app.services.account_matcher import AccountMatcher


class TestAccountMatcher(unittest.TestCase):
    """Test cases for the AccountMatcher service"""

    def setUp(self):
        """Set up test data"""
        self.matcher = AccountMatcher()
        
        # Create sample operations with missing debit accounts
        self.operations = [
            {
                "document_number": "0000000003",
                "operation_date": datetime(2024, 1, 31),
                "debit_account": "",
                "credit_account": "240001",
                "amount": 2056.80,
                "description": "ОСЧЕТОВОДЯВАНЕ НА АМОРТИЗАЦИИ ЗА МЕСЕЦ - Януари"
            },
            {
                "document_number": "0000000020",
                "operation_date": datetime(2024, 2, 29),
                "debit_account": "",
                "credit_account": "240001",
                "amount": 2056.80,
                "description": "ОСЧЕТОВОДЯВАНЕ НА АМОРТИЗАЦИИ ЗА МЕСЕЦ - Февруари"
            },
            {
                "document_number": "0000000031",
                "operation_date": datetime(2024, 3, 31),
                "debit_account": "",
                "credit_account": "240001",
                "amount": 2056.80,
                "description": "ОСЧЕТОВОДЯВАНЕ НА АМОРТИЗАЦИИ ЗА МЕСЕЦ - Март"
            }
        ]
        
        # Create reference operations with complete account information
        self.reference_operations = [
            {
                "document_number": "0000000003",
                "operation_date": datetime(2024, 1, 31),
                "debit_account": "602001",
                "credit_account": "240001",
                "amount": 2056.80,
                "description": "ОСЧЕТОВОДЯВАНЕ НА АМОРТИЗАЦИИ ЗА МЕСЕЦ - Януари"
            },
            {
                "document_number": "0000000031",
                "operation_date": datetime(2024, 3, 31),
                "debit_account": "602001",
                "credit_account": "240001",
                "amount": 2056.80,
                "description": "ОСЧЕТОВОДЯВАНЕ НА АМОРТИЗАЦИИ ЗА МЕСЕЦ - Март"
            }
        ]

    def test_match_rival_accounts(self):
        """Test matching accounts from Rival format"""
        # Match operations using reference data
        enriched_ops = self.matcher.match_rival_accounts(
            self.operations,
            self.reference_operations
        )
        
        # Verify the results
        self.assertEqual(enriched_ops[0]["debit_account"], "602001", 
                         "First operation should have matched debit account")
        self.assertEqual(enriched_ops[1]["debit_account"], "", 
                         "Second operation should not have matched (no reference)")
        self.assertEqual(enriched_ops[2]["debit_account"], "602001", 
                         "Third operation should have matched debit account")
    
    def test_matching_with_different_amounts(self):
        """Test matching behavior with slightly different amounts (floating point tolerance)"""
        # Create test data with slight amount difference
        ops_with_diff_amount = [
            {
                "document_number": "0000000003",
                "operation_date": datetime(2024, 1, 31),
                "debit_account": "",
                "credit_account": "240001",
                "amount": 2056.81,  # Slight difference from reference (2056.80)
                "description": "ОСЧЕТОВОДЯВАНЕ НА АМОРТИЗАЦИИ ЗА МЕСЕЦ - Януари"
            }
        ]
        
        # Match operations
        enriched_ops = self.matcher.match_rival_accounts(
            ops_with_diff_amount,
            self.reference_operations
        )
        
        # Verify the results - should still match due to tolerance
        self.assertEqual(enriched_ops[0]["debit_account"], "602001", 
                         "Should match despite small amount difference")


if __name__ == '__main__':
    unittest.main()