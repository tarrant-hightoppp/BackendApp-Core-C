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
        
        # Create operations for testing proportional matching
        self.proportional_operations = [
            # Group 1: Same document number and date, with complementary operations
            {
                "document_number": "INV001",
                "operation_date": datetime(2024, 4, 15),
                "debit_account": "101001",
                "credit_account": "",
                "amount": 1000.00,
                "description": "Invoice payment - first part"
            },
            {
                "document_number": "INV001",
                "operation_date": datetime(2024, 4, 15),
                "debit_account": "",
                "credit_account": "702001",
                "amount": 600.00,
                "description": "Invoice payment - service component"
            },
            {
                "document_number": "INV001",
                "operation_date": datetime(2024, 4, 15),
                "debit_account": "",
                "credit_account": "703001",
                "amount": 400.00,
                "description": "Invoice payment - product component"
            },
            
            # Group 2: Split credit operation matching a debit operation
            {
                "document_number": "INV002",
                "operation_date": datetime(2024, 4, 20),
                "debit_account": "101002",
                "credit_account": "",
                "amount": 2500.00,
                "description": "Equipment purchase"
            },
            {
                "document_number": "INV002",
                "operation_date": datetime(2024, 4, 20),
                "debit_account": "",
                "credit_account": "200001",
                "amount": 2500.00,
                "description": "Equipment purchase"
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
    
    def test_matching_with_null_values(self):
        """Test matching behavior with NULL values in accounts"""
        # Create operations with None/NULL values instead of empty strings
        operations_with_null = [
            {
                "document_number": "0000000003",
                "operation_date": datetime(2024, 1, 31),
                "debit_account": None,  # NULL value
                "credit_account": "240001",
                "amount": 2056.80,
                "description": "ОСЧЕТОВОДЯВАНЕ НА АМОРТИЗАЦИИ ЗА МЕСЕЦ - Януари"
            }
        ]
        
        # Match operations
        enriched_ops = self.matcher.match_rival_accounts(
            operations_with_null,
            self.reference_operations
        )
        
        # Verify NULL value was replaced with matched account
        self.assertEqual(enriched_ops[0]["debit_account"], "602001",
                         "Should handle NULL values in debit account")
    
    def test_relaxed_matching(self):
        """Test relaxed matching that ignores credit account mismatches"""
        # Create operation with matching doc number, date, amount but different credit account
        operation_diff_credit = [
            {
                "document_number": "0000000003",
                "operation_date": datetime(2024, 1, 31),
                "debit_account": "",
                "credit_account": "999999",  # Different from reference (240001)
                "amount": 2056.80,
                "description": "ОСЧЕТОВОДЯВАНЕ НА АМОРТИЗАЦИИ ЗА МЕСЕЦ - Януари"
            }
        ]
        
        # Match operations
        enriched_ops = self.matcher.match_rival_accounts(
            operation_diff_credit,
            self.reference_operations
        )
        
        # Verify relaxed matching still found the debit account
        self.assertEqual(enriched_ops[0]["debit_account"], "602001",
                         "Should match via relaxed criteria despite credit account mismatch")
    
    def test_date_format_handling(self):
        """Test matching with different datetime formats"""
        # Reference with date object
        reference_with_date = [
            {
                "document_number": "0000000003",
                "operation_date": datetime(2024, 1, 31).date(),  # Date object, not datetime
                "debit_account": "602001",
                "credit_account": "240001",
                "amount": 2056.80,
                "description": "ОСЧЕТОВОДЯВАНЕ НА АМОРТИЗАЦИИ ЗА МЕСЕЦ - Януари"
            }
        ]
        
        # Operation with datetime object
        operation_with_datetime = [
            {
                "document_number": "0000000003",
                "operation_date": datetime(2024, 1, 31, 12, 30, 0),  # Datetime with time component
                "debit_account": "",
                "credit_account": "240001",
                "amount": 2056.80,
                "description": "ОСЧЕТОВОДЯВАНЕ НА АМОРТИЗАЦИИ ЗА МЕСЕЦ - Януари"
            }
        ]
        
        # Match operations
        enriched_ops = self.matcher.match_rival_accounts(
            operation_with_datetime,
            reference_with_date
        )
        
        # Verify date normalization worked correctly
        self.assertEqual(enriched_ops[0]["debit_account"], "602001",
                         "Should match despite different date object types")


    def test_proportional_matching(self):
        """Test proportional matching for complementary operations"""
        # Test with operations that have matching document numbers and dates
        enriched_ops = self.matcher.match_rival_accounts(
            self.proportional_operations,
            []  # No reference operations, should match within the group
        )
        
        # Verify Group 1: The debit operation should have a credit account filled from the combined credit operations
        debit_op = next(op for op in enriched_ops if op['debit_account'] == '101001')
        self.assertIn(debit_op['credit_account'], ['702001', '703001'],
                     "Debit operation should have received a credit account from its group")
        
        # Verify Group 1: The credit operations should have received the debit account
        credit_op_1 = next(op for op in enriched_ops if op['credit_account'] == '702001')
        credit_op_2 = next(op for op in enriched_ops if op['credit_account'] == '703001')
        
        self.assertEqual(credit_op_1['debit_account'], '101001',
                         "First credit operation should have received the debit account")
        self.assertEqual(credit_op_2['debit_account'], '101001',
                         "Second credit operation should have received the debit account")
        
        # Verify Group 2: Operations should have exchanged accounts
        debit_op_2 = next(op for op in enriched_ops if op['debit_account'] == '101002')
        credit_op_3 = next(op for op in enriched_ops if op['credit_account'] == '200001')
        
        self.assertEqual(debit_op_2['credit_account'], '200001',
                         "Debit operation should have received the credit account")
        self.assertEqual(credit_op_3['debit_account'], '101002',
                         "Credit operation should have received the debit account")
    
    def test_unbalanced_proportional_matching(self):
        """Test proportional matching when debits and credits don't balance exactly"""
        unbalanced_operations = [
            # Group with uneven debit and credit amounts
            {
                "document_number": "UNBAL001",
                "operation_date": datetime(2024, 5, 1),
                "debit_account": "101003",
                "credit_account": "",
                "amount": 1500.00,
                "description": "Unbalanced debit"
            },
            {
                "document_number": "UNBAL001",
                "operation_date": datetime(2024, 5, 1),
                "debit_account": "",
                "credit_account": "200002",
                "amount": 1000.00,
                "description": "Unbalanced credit - partial"
            }
        ]
        
        # Add reference operations that have the complete information
        references = [
            {
                "document_number": "UNBAL001",
                "operation_date": datetime(2024, 5, 1),
                "debit_account": "101003",
                "credit_account": "200002",
                "amount": 1000.00,
                "description": "First part"
            },
            {
                "document_number": "UNBAL001",
                "operation_date": datetime(2024, 5, 1),
                "debit_account": "101003",
                "credit_account": "200003",
                "amount": 500.00,
                "description": "Second part"
            }
        ]
        
        enriched_ops = self.matcher.match_rival_accounts(
            unbalanced_operations,
            references
        )
        
        # Verify the operations were matched
        debit_op = next(op for op in enriched_ops if op['debit_account'] == '101003')
        credit_op = next(op for op in enriched_ops if op['credit_account'] == '200002')
        
        # Debit operation with missing credit account should still get one from reference
        self.assertIn(debit_op['credit_account'], ['200002', '200003'],
                     "Debit operation should have received a credit account from reference")
        
        # Credit operation should get the debit account
        self.assertEqual(credit_op['debit_account'], '101003',
                         "Credit operation should have received the debit account")


if __name__ == '__main__':
    unittest.main()