"""
AccountMatcher Service

This service provides functionality to match debit and credit accounts
in accounting data, specifically optimized for Rival template auditing.

It can be used as part of the pre-processing stage to enrich accounting
operations with missing account information.
"""

import pandas as pd
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import logging

from app.models.operation import AccountingOperation


class AccountMatcher:
    """Service for matching debit and credit accounts in accounting data"""

    def __init__(self):
        """Initialize the account matcher service"""
        self.logger = logging.getLogger(__name__)

    def match_rival_accounts(self, 
                           operations: List[Dict[str, Any]],
                           reference_operations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Match debit and credit accounts between operations from Rival template.
        
        This method analyzes operations parsed from a Rival template file,
        identifies operations with missing debit accounts, and attempts to fill
        them based on matching reference operations.
        
        Args:
            operations: List of accounting operations to enrich (target operations)
            reference_operations: List of reference accounting operations with complete account info
            
        Returns:
            List of enriched accounting operations with filled account information
        """
        enriched_operations = []
        matches_found = 0
        total_entries = len(operations)
        
        self.logger.info(f"Starting account matching for {total_entries} Rival operations")
        
        for operation in operations:
            # Make a copy of the operation to avoid modifying the original
            enriched_op = operation.copy()
            
            # Skip if operation already has both accounts
            if (operation.get('debit_account') and operation.get('credit_account') and
                str(operation['debit_account']) != 'nan' and str(operation['credit_account']) != 'nan'):
                enriched_operations.append(enriched_op)
                continue
            
            # Extract matching criteria
            doc_number = str(operation.get('document_number', ''))
            doc_date = operation.get('operation_date')
            amount = operation.get('amount', 0)
            
            # Skip if we don't have enough matching criteria
            if not doc_number or doc_date is None or amount is None:
                enriched_operations.append(enriched_op)
                continue
            
            # Look for matches in reference operations
            found_match = False
            
            for ref_op in reference_operations:
                ref_doc_number = str(ref_op.get('document_number', ''))
                ref_date = ref_op.get('operation_date')
                ref_amount = ref_op.get('amount', 0)
                
                # Check if dates are comparable
                date_match = False
                if isinstance(doc_date, datetime) and isinstance(ref_date, datetime):
                    date_match = doc_date.date() == ref_date.date()
                else:
                    date_match = doc_date == ref_date
                
                # Check for match by document number, date and amount
                if (ref_doc_number == doc_number and 
                    date_match and 
                    abs(float(ref_amount) - float(amount)) < 0.01):
                    
                    # If operation is missing debit account
                    if not operation.get('debit_account') or str(operation['debit_account']) == 'nan':
                        if ref_op.get('debit_account') and str(ref_op['debit_account']) != 'nan':
                            enriched_op['debit_account'] = ref_op['debit_account']
                            found_match = True
                    
                    # If operation is missing credit account
                    if not operation.get('credit_account') or str(operation['credit_account']) == 'nan':
                        if ref_op.get('credit_account') and str(ref_op['credit_account']) != 'nan':
                            enriched_op['credit_account'] = ref_op['credit_account']
                            found_match = True
                    
                    if found_match:
                        matches_found += 1
                        self.logger.debug(f"Match found for Doc #{doc_number}, Amount: {amount}")
                        break
            
            enriched_operations.append(enriched_op)
        
        self.logger.info(f"Account matching complete: {matches_found} of {total_entries} entries matched ({matches_found/total_entries*100:.2f}% match rate)")
        return enriched_operations

    def match_operations_from_db(self,
                               operations: List[Dict[str, Any]],
                               db_session) -> List[Dict[str, Any]]:
        """
        Match debit and credit accounts using historical operations from database.
        
        This method is particularly useful for enriching new operations with account
        information based on previously processed operations stored in the database.
        
        Args:
            operations: List of accounting operations to enrich
            db_session: SQLAlchemy database session
            
        Returns:
            List of enriched accounting operations
        """
        from sqlalchemy import and_, or_
        
        enriched_operations = []
        matches_found = 0
        total_entries = len(operations)
        
        self.logger.info(f"Starting account matching with DB records for {total_entries} operations")
        
        for operation in operations:
            # Make a copy of the operation to avoid modifying the original
            enriched_op = operation.copy()
            
            # Skip if operation already has both accounts
            if (operation.get('debit_account') and operation.get('credit_account') and
                str(operation['debit_account']) != 'nan' and str(operation['credit_account']) != 'nan'):
                enriched_operations.append(enriched_op)
                continue
                
            # Extract matching criteria
            doc_number = str(operation.get('document_number', ''))
            doc_date = operation.get('operation_date')
            amount = operation.get('amount', 0)
            
            # Skip if we don't have enough matching criteria
            if not doc_number or doc_date is None or amount is None:
                enriched_operations.append(enriched_op)
                continue
            
            # Query the database for matching operations
            try:
                # Build query to find similar operations
                query = db_session.query(AccountingOperation).filter(
                    and_(
                        AccountingOperation.document_number == doc_number,
                        AccountingOperation.amount == amount,
                        # Note: Date comparison may need adjustment based on DB engine
                        # This is a simplified version
                        AccountingOperation.operation_date == doc_date
                    )
                )
                
                # Execute query
                db_matches = query.all()
                
                if db_matches:
                    # Use the first match (could implement more sophisticated matching if needed)
                    db_match = db_matches[0]
                    
                    # If operation is missing debit account
                    if not operation.get('debit_account') or str(operation['debit_account']) == 'nan':
                        if db_match.debit_account:
                            enriched_op['debit_account'] = db_match.debit_account
                            matches_found += 1
                    
                    # If operation is missing credit account
                    if not operation.get('credit_account') or str(operation['credit_account']) == 'nan':
                        if db_match.credit_account:
                            enriched_op['credit_account'] = db_match.credit_account
                            matches_found += 1
            except Exception as e:
                self.logger.error(f"Error querying database for matches: {str(e)}")
            
            enriched_operations.append(enriched_op)
        
        self.logger.info(f"DB matching complete: {matches_found} matches found")
        return enriched_operations

    def match_operations(self,
                        operations: List[AccountingOperation],
                        reference_operations: Optional[List[AccountingOperation]] = None,
                        db_session=None) -> List[AccountingOperation]:
        """
        Match and enrich accounting operations from ORM models.
        
        This is a convenience method that works directly with SQLAlchemy ORM models.
        
        Args:
            operations: List of AccountingOperation objects to enrich
            reference_operations: Optional list of reference AccountingOperation objects
            db_session: Optional SQLAlchemy database session (for DB matching)
            
        Returns:
            List of enriched AccountingOperation objects
        """
        # Convert ORM models to dictionaries
        ops_dicts = [self._operation_to_dict(op) for op in operations]
        
        if reference_operations:
            # If reference operations provided, use them for matching
            ref_dicts = [self._operation_to_dict(op) for op in reference_operations]
            enriched_dicts = self.match_rival_accounts(ops_dicts, ref_dicts)
        elif db_session:
            # If DB session provided, use DB for matching
            enriched_dicts = self.match_operations_from_db(ops_dicts, db_session)
        else:
            # No reference data, return as is
            return operations
        
        # Update the original operation objects with enriched data
        for i, op in enumerate(operations):
            enriched = enriched_dicts[i]
            
            # Update accounts if they were enriched
            if 'debit_account' in enriched and str(enriched['debit_account']) != 'nan':
                op.debit_account = enriched['debit_account']
                
            if 'credit_account' in enriched and str(enriched['credit_account']) != 'nan':
                op.credit_account = enriched['credit_account']
        
        return operations
    
    def _operation_to_dict(self, operation: AccountingOperation) -> Dict[str, Any]:
        """Convert an AccountingOperation ORM model to a dictionary"""
        return {
            "document_number": operation.document_number,
            "operation_date": operation.operation_date,
            "amount": operation.amount,
            "debit_account": operation.debit_account,
            "credit_account": operation.credit_account,
            "description": operation.description
        }