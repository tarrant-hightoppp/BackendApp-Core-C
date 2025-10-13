"""
Test Direct Account Matching

This module tests the direct utility methods added to the AccountMatcher service
for matching credit with debit accounts and vice versa.
"""
import unittest
from datetime import datetime
from app.services.account_matcher import AccountMatcher


class TestDirectAccountMatching(unittest.TestCase):
    """Test cases for the direct account matching utility methods"""
    
    def setUp(self):
        """Set up test data"""
        self.matcher = AccountMatcher()
        
        # Create sample data for testing
        self.credit_operations = [
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
                "debit_account": "602001",  # Has debit account - will be used as reference
                "credit_account": "240001",
                "amount": 2056.80,
                "description": "ОСЧЕТОВОДЯВАНЕ НА АМОРТИЗАЦИИ ЗА МЕСЕЦ - Март"
            }
        ]
        
        self.debit_operations = [
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
                "credit_account": "200001",  # Has credit account - will be used as reference
                "amount": 2500.00,
                "description": "Equipment purchase"
            }
        ]
    
    def test_match_credit_with_debit(self):
        """Test matching credit operations with missing debit accounts"""
        # Apply the matching
        enriched_operations = self.matcher.match_credit_with_debit(self.credit_operations)
        
        # Verify the results
        self.assertEqual(len(enriched_operations), 3, "Should return the same number of operations")
        
        # First operation should have its debit account filled from the third operation
        self.assertEqual(enriched_operations[0]["debit_account"], "602001", 
                        "First operation should have matched debit account")
        
        # Second operation should also have its debit account filled
        self.assertEqual(enriched_operations[1]["debit_account"], "602001", 
                        "Second operation should have matched debit account")
        
        # Third operation should remain unchanged
        self.assertEqual(enriched_operations[2]["debit_account"], "602001", 
                        "Third operation should keep its original debit account")
    
    def test_match_debit_with_credit(self):
        """Test matching debit operations with missing credit accounts"""
        # Apply the matching
        enriched_operations = self.matcher.match_debit_with_credit(self.debit_operations)
        
        # Verify the results
        self.assertEqual(len(enriched_operations), 3, "Should return the same number of operations")
        
        # Operations with the same document number as the third operation should get its credit account
        # The second operation should have its credit account filled from the third operation
        self.assertEqual(enriched_operations[1]["credit_account"], "200001", 
                        "Second operation should have matched credit account")
        
        # Third operation should remain unchanged
        self.assertEqual(enriched_operations[2]["credit_account"], "200001", 
                        "Third operation should keep its original credit account")
    
    def test_cross_match_accounts(self):
        """Test cross-matching between two separate sets of operations"""
        # Create sample data for cross-matching
        debit_ops = [
            {
                "document_number": "DOC001",
                "operation_date": datetime(2024, 5, 10),
                "debit_account": "411001",
                "credit_account": "",  # Missing credit account
                "amount": 5000.00,
                "description": "Client payment"
            }
        ]
        
        credit_ops = [
            {
                "document_number": "DOC001",
                "operation_date": datetime(2024, 5, 10),
                "debit_account": "",  # Missing debit account
                "credit_account": "702001",
                "amount": 5000.00,
                "description": "Revenue recognition"
            }
        ]
        
        # Apply the cross-matching
        enriched_debit, enriched_credit = self.matcher.cross_match_accounts(debit_ops, credit_ops)
        
        # Verify the results
        self.assertEqual(enriched_debit[0]["credit_account"], "702001",
                        "Debit operation should have received the credit account")
        
        self.assertEqual(enriched_credit[0]["debit_account"], "411001",
                        "Credit operation should have received the debit account")
    
    def test_matching_with_multiple_references(self):
        """Test matching behavior when multiple reference operations are available"""
        # Create sample data with multiple possible matches
        operations = [
            {
                "document_number": "MULTI001",
                "operation_date": datetime(2024, 6, 15),
                "debit_account": "",  # Missing debit account
                "credit_account": "401001",
                "amount": 3000.00,
                "description": "Test multiple matches"
            },
            {
                "document_number": "MULTI001",
                "operation_date": datetime(2024, 6, 15),
                "debit_account": "101001",
                "credit_account": "401001",
                "amount": 3000.00,
                "description": "First reference"
            },
            {
                "document_number": "MULTI001",
                "operation_date": datetime(2024, 6, 15),
                "debit_account": "101002",
                "credit_account": "401001",
                "amount": 3000.00,
                "description": "Second reference"
            }
        ]
        
        # Apply the matching
        enriched_ops = self.matcher.match_credit_with_debit(operations)
        
        # Verify the results - should match with one of the reference operations
        self.assertIn(enriched_ops[0]["debit_account"], ["101001", "101002"],
                     "Should match with one of the available reference operations")


if __name__ == '__main__':
    unittest.main()