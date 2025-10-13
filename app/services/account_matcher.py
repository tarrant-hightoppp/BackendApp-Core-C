"""
AccountMatcher Service

This service provides functionality to match debit and credit accounts
in accounting data, specifically optimized for Rival template auditing.

It can be used as part of the pre-processing stage to enrich accounting
operations with missing account information.
"""

import pandas as pd
import re
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, date
import logging

from app.models.operation import AccountingOperation


class AccountMatcher:
    """Service for matching debit and credit accounts in accounting data"""

    def __init__(self):
        """Initialize the account matcher service"""
        self.logger = logging.getLogger(__name__)
        # Configure detailed logging for diagnostics
        self.enable_detailed_logging = True
        # Configure the confidence threshold for accepting matches
        self.confidence_threshold = 70  # 0-100 scale

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
        
        # Group operations by document number and date for proportional matching
        operation_groups = {}
        for op in operations:
            doc_number = str(op.get('document_number', ''))
            doc_date = op.get('operation_date')
            
            # Normalize date for grouping
            if doc_date is not None:
                doc_date_normalized = doc_date.date() if isinstance(doc_date, datetime) else doc_date
            else:
                doc_date_normalized = None
                
            # Skip if we don't have valid identifiers
            if not doc_number or doc_date_normalized is None:
                enriched_operations.append(op.copy())
                continue
                
            key = (doc_number, doc_date_normalized)
            if key not in operation_groups:
                operation_groups[key] = []
            operation_groups[key].append(op)
        
        # Group reference operations for faster lookup
        ref_groups = {}
        for ref_op in reference_operations:
            ref_doc_number = str(ref_op.get('document_number', ''))
            ref_date = ref_op.get('operation_date')
            
            # Normalize date for grouping
            if ref_date is not None:
                ref_date_normalized = ref_date.date() if isinstance(ref_date, datetime) else ref_date
            else:
                ref_date_normalized = None
                
            # Skip if we don't have valid identifiers
            if not ref_doc_number or ref_date_normalized is None:
                continue
                
            key = (ref_doc_number, ref_date_normalized)
            if key not in ref_groups:
                ref_groups[key] = []
            ref_groups[key].append(ref_op)
                
        # Process each group of operations
        for key, ops in operation_groups.items():
            doc_number, doc_date = key
            
            # If we only have one operation in the group, use the old single-operation matching
            if len(ops) == 1:
                enriched_op = self._match_single_operation(ops[0], reference_operations)
                enriched_operations.append(enriched_op)
                if enriched_op.get('_matched', False):
                    matches_found += 1
                continue
                
            # For multiple operations, apply proportional matching
            self.logger.debug(f"Applying proportional matching for document {doc_number}, date {doc_date} with {len(ops)} operations")
            
            # Split into debit and credit operations
            debit_ops = [op for op in ops if self._has_debit_account(op)]
            credit_ops = [op for op in ops if self._has_credit_account(op)]
            missing_ops = [op for op in ops if not self._has_debit_account(op) and not self._has_credit_account(op)]
            
            # Calculate total amounts
            debit_total = sum(float(op['amount']) for op in debit_ops)
            credit_total = sum(float(op['amount']) for op in credit_ops)
            
            # Check if we have reference operations for this document/date
            ref_ops = ref_groups.get(key, [])
            
            # Apply proportional matching
            if len(debit_ops) > 0 and len(credit_ops) == 0:
                # We have only debit operations, need to find/create credit entries
                self._match_missing_credit_accounts(debit_ops, ref_ops)
                matches_found += sum(1 for op in debit_ops if op.get('_matched', False))
            elif len(debit_ops) == 0 and len(credit_ops) > 0:
                # We have only credit operations, need to find/create debit entries
                self._match_missing_debit_accounts(credit_ops, ref_ops)
                matches_found += sum(1 for op in credit_ops if op.get('_matched', False))
            elif len(debit_ops) > 0 and len(credit_ops) > 0:
                # We have both debit and credit operations, check if they balance
                if abs(debit_total - credit_total) < 0.01:
                    # Totals match, fill in any missing accounts
                    self._cross_fill_accounts(debit_ops, credit_ops)
                    matches_found += sum(1 for op in ops if op.get('_matched', False))
                else:
                    # Totals don't match, try to match using reference operations
                    self._match_with_references(debit_ops, credit_ops, ref_ops)
                    matches_found += sum(1 for op in ops if op.get('_matched', False))
            
            # For completely missing operations, try single operation matching
            for op in missing_ops:
                enriched_op = self._match_single_operation(op, reference_operations)
                if enriched_op.get('_matched', False):
                    matches_found += 1
            
            # Add all operations to the result
            for op in ops:
                # Remove the _matched flag before returning
                if '_matched' in op:
                    del op['_matched']
                enriched_operations.append(op)
        
        self.logger.info(f"Account matching complete: {matches_found} of {total_entries} entries matched ({matches_found/total_entries*100:.2f}% match rate)")
        return enriched_operations
    
    # Helper methods for improved matching
    
    def _normalize_document_number(self, doc_num):
        """
        Normalize document numbers for consistent matching
        
        - Converts to string
        - Removes non-alphanumeric characters
        - Trims leading zeros
        - Converts to uppercase
        
        Args:
            doc_num: Document number to normalize
            
        Returns:
            Normalized document number
        """
        if doc_num is None:
            return ""
            
        # Convert to string, remove non-alphanumeric chars, trim leading zeros, uppercase
        normalized = re.sub(r'[^a-zA-Z0-9]', '', str(doc_num)).lstrip('0').upper()
        
        if self.enable_detailed_logging and normalized != str(doc_num):
            self.logger.debug(f"Normalized document number: '{doc_num}' -> '{normalized}'")
            
        return normalized
    
    def _amounts_match(self, amount1, amount2):
        """
        Check if two amounts match using adaptive tolerance
        
        For small amounts (< 100), uses fixed tolerance of 0.01
        For larger amounts, uses percentage-based tolerance (0.1%)
        
        Args:
            amount1: First amount
            amount2: Second amount
            
        Returns:
            True if amounts match, False otherwise
        """
        try:
            amount1 = float(amount1) if amount1 is not None else 0
            amount2 = float(amount2) if amount2 is not None else 0
            
            # If either amount is zero, use fixed tolerance
            if amount1 == 0 or amount2 == 0:
                return abs(amount1 - amount2) < 0.01
                
            # For small amounts, use fixed tolerance
            if abs(amount1) < 100 and abs(amount2) < 100:
                match = abs(amount1 - amount2) < 0.01
            # For larger amounts, use percentage-based tolerance (0.1%)
            else:
                match = abs(amount1 - amount2) / max(abs(amount1), abs(amount2)) < 0.001
                
            if self.enable_detailed_logging:
                self.logger.debug(f"Amount match: {amount1} vs {amount2} = {match}")
                
            return match
        except Exception as e:
            self.logger.warning(f"Error comparing amounts {amount1} and {amount2}: {str(e)}")
            # Fall back to simple comparison
            return abs(float(amount1) - float(amount2)) < 0.01
    
    def _dates_match(self, date1, date2):
        """
        Check if two dates match after normalization
        
        Args:
            date1: First date
            date2: Second date
            
        Returns:
            True if dates match, False otherwise
        """
        if date1 is None or date2 is None:
            return False
            
        # Normalize dates by converting to date objects without time
        try:
            date1_norm = date1.date() if isinstance(date1, datetime) else date1
            date2_norm = date2.date() if isinstance(date2, datetime) else date2
            
            # Ensure we're working with date objects
            if not isinstance(date1_norm, date):
                date1_norm = date1
            if not isinstance(date2_norm, date):
                date2_norm = date2
                
            match = date1_norm == date2_norm
            
            if self.enable_detailed_logging:
                self.logger.debug(f"Date match: {date1} vs {date2} = {match}")
                
            return match
        except Exception as e:
            self.logger.warning(f"Error comparing dates {date1} and {date2}: {str(e)}")
            # Fall back to string comparison
            return str(date1) == str(date2)
    
    def _calculate_match_confidence(self, operation, reference_op):
        """
        Calculate confidence score for a potential match (0-100 scale)
        
        Args:
            operation: The operation to match
            reference_op: The potential matching reference operation
            
        Returns:
            Confidence score (0-100) where higher means more confident match
        """
        score = 0
        
        # Get key values for matching
        op_doc_num = str(operation.get('document_number', ''))
        ref_doc_num = str(reference_op.get('document_number', ''))
        op_date = operation.get('operation_date')
        ref_date = reference_op.get('operation_date')
        op_amount = operation.get('amount', 0)
        ref_amount = reference_op.get('amount', 0)
        
        # 1. Document number match (0-40 points)
        if op_doc_num and ref_doc_num:
            norm_op_doc = self._normalize_document_number(op_doc_num)
            norm_ref_doc = self._normalize_document_number(ref_doc_num)
            
            if norm_op_doc == norm_ref_doc:
                # Exact match on normalized document number
                score += 40
            elif norm_op_doc in norm_ref_doc or norm_ref_doc in norm_op_doc:
                # Partial match (substring)
                score += 20
        
        # 2. Date match (0-30 points)
        if self._dates_match(op_date, ref_date):
            score += 30
        
        # 3. Amount match (0-30 points)
        if self._amounts_match(op_amount, ref_amount):
            score += 30
        
        if self.enable_detailed_logging:
            self.logger.debug(f"Match confidence score: {score} for operation {op_doc_num}/{op_date}/{op_amount} vs {ref_doc_num}/{ref_date}/{ref_amount}")
            
        return score
    
    def _has_debit_account(self, operation):
        """Check if operation has a valid debit account"""
        return (operation.get('debit_account') is not None and
                operation.get('debit_account') and
                str(operation.get('debit_account')) != 'nan')
    
    def _has_credit_account(self, operation):
        """Check if operation has a valid credit account"""
        return (operation.get('credit_account') is not None and
                operation.get('credit_account') and
                str(operation.get('credit_account')) != 'nan')
                
    def _match_single_operation(self, operation, reference_operations):
        """
        Match a single operation using confidence scoring system
        
        This improved version:
        1. Uses confidence scoring instead of binary matching
        2. Evaluates all potential matches and selects the best one
        3. Uses normalized document numbers and adaptive amount matching
        4. Provides detailed logging for diagnostics
        """
        enriched_op = operation.copy()
        doc_number = str(operation.get('document_number', ''))
        doc_date = operation.get('operation_date')
        amount = operation.get('amount', 0)
        
        # Skip if we don't have enough matching criteria
        if not doc_number or doc_date is None or amount is None:
            return enriched_op
        
        if self.enable_detailed_logging:
            self.logger.debug(f"Matching operation: Doc #{doc_number}, Date: {doc_date}, Amount: {amount}")
            self.logger.debug(f"  Current debit account: {operation.get('debit_account', 'MISSING')}")
            self.logger.debug(f"  Current credit account: {operation.get('credit_account', 'MISSING')}")
        
        # Score and sort all potential matches
        potential_matches = []
        
        for ref_op in reference_operations:
            # Skip reference operations that don't have accounts we need
            needs_debit = not self._has_debit_account(operation)
            needs_credit = not self._has_credit_account(operation)
            
            has_debit_to_provide = needs_debit and self._has_debit_account(ref_op)
            has_credit_to_provide = needs_credit and self._has_credit_account(ref_op)
            
            # Skip if this reference can't provide any accounts we need
            if not (has_debit_to_provide or has_credit_to_provide):
                continue
            
            # Calculate confidence score for this match
            confidence = self._calculate_match_confidence(operation, ref_op)
            
            # Only consider matches above the confidence threshold
            if confidence >= self.confidence_threshold:
                potential_matches.append((ref_op, confidence))
        
        # Sort by confidence score (highest first)
        potential_matches.sort(key=lambda x: x[1], reverse=True)
        
        if self.enable_detailed_logging:
            self.logger.debug(f"Found {len(potential_matches)} potential matches above threshold")
            for i, (ref_op, score) in enumerate(potential_matches[:3]):  # Show top 3 matches
                self.logger.debug(f"  Match {i+1}: Doc #{ref_op.get('document_number')}, "
                                f"Date: {ref_op.get('operation_date')}, "
                                f"Amount: {ref_op.get('amount')}, "
                                f"Score: {score}")
        
        # Use the highest confidence match if available
        if potential_matches:
            best_match, confidence = potential_matches[0]
            
            # Fill in missing accounts from the best match
            if not self._has_debit_account(operation) and self._has_debit_account(best_match):
                enriched_op['debit_account'] = best_match['debit_account']
                enriched_op['_matched'] = True
                
                if self.enable_detailed_logging:
                    self.logger.debug(f"Filled debit account: {best_match['debit_account']} (confidence: {confidence})")
            
            if not self._has_credit_account(operation) and self._has_credit_account(best_match):
                enriched_op['credit_account'] = best_match['credit_account']
                enriched_op['_matched'] = True
                
                if self.enable_detailed_logging:
                    self.logger.debug(f"Filled credit account: {best_match['credit_account']} (confidence: {confidence})")
        
        # Final outcome
        if enriched_op.get('_matched', False):
            self.logger.debug(f"Successfully matched: Doc #{doc_number}, Date: {doc_date}, Amount: {amount}")
            # Track match quality for analysis
            match_quality = "high" if confidence >= 90 else "medium" if confidence >= 80 else "low"
            enriched_op['_match_quality'] = match_quality
            enriched_op['_match_confidence'] = confidence
        else:
            self.logger.debug(f"No match found: Doc #{doc_number}, Date: {doc_date}, Amount: {amount}")
        
        return enriched_op
    
    def _match_missing_credit_accounts(self, debit_ops, ref_ops):
        """
        Fill in missing credit accounts for a set of debit operations
        Using confidence-based matching with normalized documents and adaptive amount tolerance
        """
        for debit_op in debit_ops:
            # Skip if the debit operation already has a credit account
            if self._has_credit_account(debit_op):
                continue
            
            # Score and sort all potential matches
            potential_matches = []
            
            for ref_op in ref_ops:
                # Skip if this reference doesn't have a credit account to provide
                if not self._has_credit_account(ref_op):
                    continue
                
                # Calculate confidence score for this match
                confidence = self._calculate_match_confidence(debit_op, ref_op)
                
                # Only consider matches above the confidence threshold
                if confidence >= self.confidence_threshold:
                    potential_matches.append((ref_op, confidence))
            
            # Sort by confidence score (highest first)
            potential_matches.sort(key=lambda x: x[1], reverse=True)
            
            # Use the highest confidence match if available
            if potential_matches:
                best_match, confidence = potential_matches[0]
                debit_op['credit_account'] = best_match['credit_account']
                debit_op['_matched'] = True
                debit_op['_match_quality'] = "high" if confidence >= 90 else "medium" if confidence >= 80 else "low"
                debit_op['_match_confidence'] = confidence
                
                self.logger.debug(f"Filled missing credit account for debit op with amount {debit_op.get('amount')}, "
                                f"Doc #{debit_op.get('document_number')}, confidence: {confidence}")
    
    def _match_missing_debit_accounts(self, credit_ops, ref_ops):
        """
        Fill in missing debit accounts for a set of credit operations
        Using confidence-based matching with normalized documents and adaptive amount tolerance
        """
        for credit_op in credit_ops:
            # Skip if the credit operation already has a debit account
            if self._has_debit_account(credit_op):
                continue
            
            # Score and sort all potential matches
            potential_matches = []
            
            for ref_op in ref_ops:
                # Skip if this reference doesn't have a debit account to provide
                if not self._has_debit_account(ref_op):
                    continue
                
                # Calculate confidence score for this match
                confidence = self._calculate_match_confidence(credit_op, ref_op)
                
                # Only consider matches above the confidence threshold
                if confidence >= self.confidence_threshold:
                    potential_matches.append((ref_op, confidence))
            
            # Sort by confidence score (highest first)
            potential_matches.sort(key=lambda x: x[1], reverse=True)
            
            # Use the highest confidence match if available
            if potential_matches:
                best_match, confidence = potential_matches[0]
                credit_op['debit_account'] = best_match['debit_account']
                credit_op['_matched'] = True
                credit_op['_match_quality'] = "high" if confidence >= 90 else "medium" if confidence >= 80 else "low"
                credit_op['_match_confidence'] = confidence
                
                self.logger.debug(f"Filled missing debit account for credit op with amount {credit_op.get('amount')}, "
                                f"Doc #{credit_op.get('document_number')}, confidence: {confidence}")
    
    def _cross_fill_accounts(self, debit_ops, credit_ops):
        """
        Fill missing accounts by cross-referencing between debit and credit operations
        Using confidence-based matching with normalized documents and adaptive amount tolerance
        """
        # For each debit operation with missing credit account
        for debit_op in debit_ops:
            if self._has_credit_account(debit_op):
                continue
                
            # Score and sort all potential matches from credit operations
            potential_matches = []
            
            for credit_op in credit_ops:
                # Skip if this credit operation doesn't have a credit account
                if not self._has_credit_account(credit_op):
                    continue
                
                # Calculate confidence score - check document number, date and amount
                confidence = self._calculate_match_confidence(debit_op, credit_op)
                
                # Only consider matches above the confidence threshold
                if confidence >= self.confidence_threshold:
                    potential_matches.append((credit_op, confidence))
            
            # Sort by confidence score (highest first)
            potential_matches.sort(key=lambda x: x[1], reverse=True)
            
            # Use the highest confidence match if available
            if potential_matches:
                best_match, confidence = potential_matches[0]
                debit_op['credit_account'] = best_match['credit_account']
                debit_op['_matched'] = True
                debit_op['_match_quality'] = "high" if confidence >= 90 else "medium" if confidence >= 80 else "low"
                debit_op['_match_confidence'] = confidence
                
                self.logger.debug(f"Cross-filled credit account for debit op with amount {debit_op.get('amount')}, "
                               f"Doc #{debit_op.get('document_number')}, confidence: {confidence}")
        
        # For each credit operation with missing debit account
        for credit_op in credit_ops:
            if self._has_debit_account(credit_op):
                continue
                
            # Score and sort all potential matches from debit operations
            potential_matches = []
            
            for debit_op in debit_ops:
                # Skip if this debit operation doesn't have a debit account
                if not self._has_debit_account(debit_op):
                    continue
                
                # Calculate confidence score
                confidence = self._calculate_match_confidence(credit_op, debit_op)
                
                # Only consider matches above the confidence threshold
                if confidence >= self.confidence_threshold:
                    potential_matches.append((debit_op, confidence))
            
            # Sort by confidence score (highest first)
            potential_matches.sort(key=lambda x: x[1], reverse=True)
            
            # Use the highest confidence match if available
            if potential_matches:
                best_match, confidence = potential_matches[0]
                credit_op['debit_account'] = best_match['debit_account']
                credit_op['_matched'] = True
                credit_op['_match_quality'] = "high" if confidence >= 90 else "medium" if confidence >= 80 else "low"
                credit_op['_match_confidence'] = confidence
                
                self.logger.debug(f"Cross-filled debit account for credit op with amount {credit_op.get('amount')}, "
                               f"Doc #{credit_op.get('document_number')}, confidence: {confidence}")
    
    def _match_with_references(self, debit_ops, credit_ops, ref_ops):
        """
        Match using reference operations when debit and credit totals don't balance
        Using confidence-based matching with normalized documents and adaptive amount tolerance
        """
        # For each debit operation with missing credit account
        for debit_op in debit_ops:
            if self._has_credit_account(debit_op):
                continue
                
            # Score and sort all potential reference matches
            potential_matches = []
            
            for ref_op in ref_ops:
                # Skip if this reference doesn't have a credit account to provide
                if not self._has_credit_account(ref_op):
                    continue
                
                # Calculate confidence score
                confidence = self._calculate_match_confidence(debit_op, ref_op)
                
                # Only consider matches above the confidence threshold
                if confidence >= self.confidence_threshold:
                    potential_matches.append((ref_op, confidence))
            
            # Sort by confidence score (highest first)
            potential_matches.sort(key=lambda x: x[1], reverse=True)
            
            # Use the highest confidence match if available
            if potential_matches:
                best_match, confidence = potential_matches[0]
                debit_op['credit_account'] = best_match['credit_account']
                debit_op['_matched'] = True
                debit_op['_match_quality'] = "high" if confidence >= 90 else "medium" if confidence >= 80 else "low"
                debit_op['_match_confidence'] = confidence
                
                self.logger.debug(f"Filled credit account from reference for debit op with amount {debit_op.get('amount')}, "
                               f"Doc #{debit_op.get('document_number')}, confidence: {confidence}")
        
        # For each credit operation with missing debit account
        for credit_op in credit_ops:
            if self._has_debit_account(credit_op):
                continue
                
            # Score and sort all potential reference matches
            potential_matches = []
            
            for ref_op in ref_ops:
                # Skip if this reference doesn't have a debit account to provide
                if not self._has_debit_account(ref_op):
                    continue
                
                # Calculate confidence score
                confidence = self._calculate_match_confidence(credit_op, ref_op)
                
                # Only consider matches above the confidence threshold
                if confidence >= self.confidence_threshold:
                    potential_matches.append((ref_op, confidence))
            
            # Sort by confidence score (highest first)
            potential_matches.sort(key=lambda x: x[1], reverse=True)
            
            # Use the highest confidence match if available
            if potential_matches:
                best_match, confidence = potential_matches[0]
                credit_op['debit_account'] = best_match['debit_account']
                credit_op['_matched'] = True
                credit_op['_match_quality'] = "high" if confidence >= 90 else "medium" if confidence >= 80 else "low"
                credit_op['_match_confidence'] = confidence
                
                self.logger.debug(f"Filled debit account from reference for credit op with amount {credit_op.get('amount')}, "
                               f"Doc #{credit_op.get('document_number')}, confidence: {confidence}")
        

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
                # Use normalized document number and adaptive amount matching
                normalized_doc_num = self._normalize_document_number(doc_number)
                
                # Get all operations from database for potential matching
                db_operations = db_session.query(AccountingOperation).all()
                
                # Score and evaluate all potential matches
                potential_matches = []
                
                for db_op in db_operations:
                    # Convert DB model to dict for scoring
                    db_op_dict = {
                        "document_number": db_op.document_number,
                        "operation_date": db_op.operation_date,
                        "amount": db_op.amount,
                        "debit_account": db_op.debit_account,
                        "credit_account": db_op.credit_account,
                        "description": db_op.description
                    }
                    
                    # Calculate confidence score
                    confidence = self._calculate_match_confidence(operation, db_op_dict)
                    
                    # Only consider matches above threshold that have accounts we need
                    needs_debit = not self._has_debit_account(operation)
                    needs_credit = not self._has_credit_account(operation)
                    
                    has_debit = db_op.debit_account is not None and db_op.debit_account
                    has_credit = db_op.credit_account is not None and db_op.credit_account
                    
                    if confidence >= self.confidence_threshold and ((needs_debit and has_debit) or (needs_credit and has_credit)):
                        potential_matches.append((db_op, confidence))
                
                # Sort by confidence score (highest first)
                potential_matches.sort(key=lambda x: x[1], reverse=True)
                
                # Log potential matches for debugging
                if self.enable_detailed_logging and potential_matches:
                    self.logger.debug(f"Found {len(potential_matches)} potential DB matches above threshold for Doc #{doc_number}")
                    for i, (match, score) in enumerate(potential_matches[:3]):  # Show top 3
                        self.logger.debug(f"  Match {i+1}: Doc #{match.document_number}, "
                                       f"Date: {match.operation_date}, "
                                       f"Amount: {match.amount}, "
                                       f"Score: {score}")
                
                # Use the highest confidence match if available
                if potential_matches:
                    best_match, confidence = potential_matches[0]
                    
                    # If operation is missing debit account
                    if not self._has_debit_account(operation):
                        if best_match.debit_account:
                            enriched_op['debit_account'] = best_match.debit_account
                            enriched_op['_match_quality'] = "high" if confidence >= 90 else "medium" if confidence >= 80 else "low"
                            enriched_op['_match_confidence'] = confidence
                            matches_found += 1
                            self.logger.info(f"DB match found for debit account: Doc #{doc_number}, Score: {confidence}")
                    
                    # If operation is missing credit account
                    if not self._has_credit_account(operation):
                        if best_match.credit_account:
                            enriched_op['credit_account'] = best_match.credit_account
                            enriched_op['_match_quality'] = "high" if confidence >= 90 else "medium" if confidence >= 80 else "low"
                            enriched_op['_match_confidence'] = confidence
                            matches_found += 1
                            self.logger.info(f"DB match found for credit account: Doc #{doc_number}, Score: {confidence}")
            except Exception as e:
                self.logger.error(f"Error during database matching: {str(e)}")
                import traceback
                traceback.print_exc()
            
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
            if 'debit_account' in enriched and enriched['debit_account'] is not None and str(enriched['debit_account']) != 'nan':
                op.debit_account = str(enriched['debit_account'])
                
            if 'credit_account' in enriched and enriched['credit_account'] is not None and str(enriched['credit_account']) != 'nan':
                op.credit_account = str(enriched['credit_account'])
        
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
        
    # Direct utility methods for easy matching
    
    def match_credit_with_debit(self, operations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Directly match credit operations with their corresponding debit accounts.
        
        Use this method when you have operations with credit accounts but missing debit accounts.
        It will attempt to fill in the missing debit accounts using the other operations as reference.
        
        Args:
            operations: List of accounting operations (some with missing debit accounts)
            
        Returns:
            List of enriched operations with filled debit accounts where possible
        """
        # Use the same operations as both target and reference
        return self.match_rival_accounts(operations, operations)
        
    def match_debit_with_credit(self, operations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Directly match debit operations with their corresponding credit accounts.
        
        Use this method when you have operations with debit accounts but missing credit accounts.
        It will attempt to fill in the missing credit accounts using the other operations as reference.
        
        Args:
            operations: List of accounting operations (some with missing credit accounts)
            
        Returns:
            List of enriched operations with filled credit accounts where possible
        """
        # Use the same operations as both target and reference
        return self.match_rival_accounts(operations, operations)
        
    def cross_match_accounts(self, debit_operations: List[Dict[str, Any]],
                            credit_operations: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Cross-match between two sets of operations (debit and credit).
        
        Use this method when you have two separate sets of operations:
        - One set with debit accounts but missing credit accounts
        - Another set with credit accounts but missing debit accounts
        
        Args:
            debit_operations: Operations with debit accounts (may be missing credit accounts)
            credit_operations: Operations with credit accounts (may be missing debit accounts)
            
        Returns:
            Tuple containing:
            - Enriched debit operations (with credit accounts filled where possible)
            - Enriched credit operations (with debit accounts filled where possible)
        """
        # Fill missing credit accounts in debit operations using credit operations as reference
        enriched_debit = self.match_rival_accounts(debit_operations, credit_operations)
        
        # Fill missing debit accounts in credit operations using debit operations as reference
        enriched_credit = self.match_rival_accounts(credit_operations, debit_operations)
        
        return enriched_debit, enriched_credit