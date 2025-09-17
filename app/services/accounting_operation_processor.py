import io
import time
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.models.operation import AccountingOperation
from app.models.file import UploadedFile
from app.services.s3 import S3Service


class AccountingOperationProcessor:
    """Service for processing accounting operations and generating account-specific files"""
    
    def __init__(self, db: Session):
        self.db = db
        self.s3_service = S3Service()
    
    def process_import(self, import_uuid: str) -> Dict[str, Any]:
        """
        Process all operations for a specific import and generate account-specific files
        
        Args:
            import_uuid: UUID of the import to process
            
        Returns:
            Dictionary with processing results and statistics
        """
        # Get all operations for this import
        operations = self._get_operations_by_import(import_uuid)
        
        # print(f"[DEBUG] Found {len(operations)} operations for import {import_uuid}")
        
        if not operations:
            print(f"[ERROR] No operations found for import {import_uuid}")
            return {"success": False, "message": f"No operations found for import {import_uuid}"}
        
        # Process debit accounts
        debit_results = self._process_accounts(operations, "debit", import_uuid)
        
        # Process credit accounts
        credit_results = self._process_accounts(operations, "credit", import_uuid)
        
        return {
            "success": True,
            "debit_accounts_processed": len(debit_results),
            "credit_accounts_processed": len(credit_results),
            "debit_files": debit_results,
            "credit_files": credit_results,
            "import_uuid": import_uuid,
            "total_operations": len(operations)
        }
    
    def _get_operations_by_import(self, import_uuid: str) -> List[AccountingOperation]:
        """
        Get all operations related to a specific import
        
        Args:
            import_uuid: UUID of the import
            
        Returns:
            List of AccountingOperation objects
        """
        # Try multiple times to get operations, with a short delay between attempts
        # This helps handle potential race conditions where operations are still being committed
        max_attempts = 3
        attempt = 0
        operations = []
        
        while attempt < max_attempts and not operations:
            # Query operations using their import_uuid field
            operations = self.db.query(AccountingOperation).filter(
                AccountingOperation.import_uuid == import_uuid
            ).all()
            
            if operations:
                print(f"[INFO] Found {len(operations)} operations for import {import_uuid} on attempt {attempt+1}")
                break
                
            attempt += 1
            if attempt < max_attempts:
                print(f"[INFO] No operations found for import {import_uuid} on attempt {attempt}. Retrying in 1 second...")
                time.sleep(1)  # Wait 1 second before retrying
                # Refresh the session to ensure we get the latest data
                self.db.expire_all()
        
        return operations
    
    def _process_accounts(self, operations: List[AccountingOperation], 
                          account_type: str, import_uuid: str) -> List[Dict[str, Any]]:
        """
        Process operations for all accounts of a specific type (debit/credit)
        
        Args:
            operations: List of operations to process
            account_type: "debit" or "credit" to determine which accounts to process
            import_uuid: UUID of the import
            
        Returns:
            List of dictionaries with results for each account
        """
        # Group operations by account
        account_groups = self._group_by_account(operations, account_type)
        
        # print(f"[DEBUG] Grouped {account_type} operations into {len(account_groups)} accounts")
        # for account, ops in account_groups.items():
        #     print(f"[DEBUG] Account {account} has {len(ops)} operations")
            
        results = []
        
        # Process each account
        for account, account_operations in account_groups.items():
            # Apply filtering logic
            filtered_operations = self._filter_operations(account_operations)
            
            # Generate and upload file
            # Create a timestamp for the filename
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            
            # Format: DEBIT/CREDIT-account__importUUID__timestamp.xlsx
            # Replace any forward slashes in account numbers with underscores to avoid creating nested directories in S3
            safe_account = account.replace('/', '_')
            file_name = f"{account_type.upper()}-{safe_account}__{import_uuid}__{timestamp}.xlsx"
            
            # Generate and upload the account-specific Excel file
            # print(f"[DEBUG] Generating Excel file for {account_type} account {account} with {len(filtered_operations)} operations")
            s3_key = self._generate_and_upload_file(filtered_operations, file_name, account_type, import_uuid)
            
            if s3_key:
                print(f"[INFO] Successfully uploaded file to S3: {s3_key}")
                results.append({
                    "account": account,
                    "total_operations": len(account_operations),
                    "filtered_operations": len(filtered_operations),
                    "s3_key": s3_key,
                    "file_name": file_name
                })
        
        return results
    
    def _group_by_account(self, operations: List[AccountingOperation], 
                          account_type: str) -> Dict[str, List[AccountingOperation]]:
        """
        Group operations by account number
        
        Args:
            operations: List of operations to group
            account_type: "debit" or "credit" to determine which account field to use
            
        Returns:
            Dictionary with account numbers as keys and lists of operations as values
        """
        account_groups = {}
        
        for operation in operations:
            account = operation.debit_account if account_type == "debit" else operation.credit_account
            
            if not account:
                continue
                
            if account not in account_groups:
                account_groups[account] = []
                
            account_groups[account].append(operation)
            
        return account_groups
    
    def _filter_operations(self, operations: List[AccountingOperation]) -> List[AccountingOperation]:
        """
        Apply filtering logic to operations based on the 80% rule:
        - If <= 30 operations, include all operations (100%)
        - If > 30 operations, include operations that make up 80% of total amount
          (sorted by amount in descending order - largest transactions first)
        
        This implements the business rule that for accounts with many transactions,
        we focus on the most significant ones that represent 80% of the financial value.
        
        Args:
            operations: List of operations to filter
            
        Returns:
            Filtered list of operations
        """
        # If we have 30 or fewer operations, include all of them
        if len(operations) <= 30:
            return operations
            
        # Sort operations by amount (descending) to prioritize largest transactions
        sorted_operations = sorted(operations, key=lambda x: x.amount, reverse=True)
        
        # Calculate total amount across all operations
        total_amount = sum(op.amount for op in operations)
        
        # Calculate the 80% threshold amount - handle Decimal vs float
        try:
            # Import Decimal to handle decimal arithmetic properly
            from decimal import Decimal
            
            # Convert the threshold percentage to a Decimal for consistent handling
            threshold_percentage = Decimal('0.8')
            
            # Check if total_amount is already a Decimal, if not convert it
            if not isinstance(total_amount, Decimal):
                total_amount = Decimal(str(total_amount))
                
            # Calculate threshold using Decimal arithmetic
            threshold = total_amount * threshold_percentage
            
            # print(f"[DEBUG] Total amount: {total_amount} (type: {type(total_amount).__name__})")
            # print(f"[DEBUG] Threshold (80%): {threshold} (type: {type(threshold).__name__})")
            
        except Exception as e:
            # Fallback to float if there's an issue with Decimal
            print(f"[WARNING] Error handling Decimal arithmetic: {e}, falling back to float")
            # Convert everything to float to ensure compatibility
            total_amount = float(total_amount)
            threshold = total_amount * 0.8
        
        # Select operations until reaching the 80% threshold
        filtered_operations = []
        cumulative_amount = 0
        
        for operation in sorted_operations:
            filtered_operations.append(operation)
            
            # Handle Decimal arithmetic consistently
            op_amount = operation.amount
            
            # Make sure we're adding compatible types
            if isinstance(cumulative_amount, Decimal) and not isinstance(op_amount, Decimal):
                op_amount = Decimal(str(op_amount))
            elif not isinstance(cumulative_amount, Decimal) and isinstance(op_amount, Decimal):
                cumulative_amount = Decimal(str(cumulative_amount))
                
            cumulative_amount += op_amount
            
            # Once we reach or exceed 80% of the total value, we can stop
            if cumulative_amount >= threshold:
                break
        
        # For percentage calculation, make sure we're using compatible types
        try:
            if isinstance(cumulative_amount, Decimal) and isinstance(total_amount, Decimal):
                percentage = (cumulative_amount / total_amount) * 100
                percentage_str = f"{float(percentage):.2f}%"
            else:
                percentage = (float(cumulative_amount) / float(total_amount)) * 100
                percentage_str = f"{percentage:.2f}%"
        except Exception as e:
            print(f"[WARNING] Error calculating percentage: {e}")
            percentage_str = "unknown%"
            
        print(f"Filtered operations: {len(filtered_operations)} of {len(operations)} "
              f"representing {percentage_str} of total amount")
                
        return filtered_operations
    
    def _generate_and_upload_file(self, operations: List[AccountingOperation],
                                  file_name: str, account_type: str, import_uuid: str) -> Optional[str]:
        """
        Generate Excel file with operations and upload to S3
        
        Args:
            operations: List of operations to include in the file
            file_name: Name of the file to generate
            account_type: "debit" or "credit" to include in file metadata
            import_uuid: UUID of the import batch
            
        Returns:
            S3 key if successful, None otherwise
        """
        try:
            # Convert operations to dataframe
            operation_data = []
            
            for op in operations:
                operation_data.append({
                    "operation_date": op.operation_date,
                    "document_type": op.document_type,
                    "document_number": op.document_number,
                    "debit_account": op.debit_account,
                    "credit_account": op.credit_account,
                    "amount": float(op.amount),
                    "description": op.description,
                    "partner_name": op.partner_name,
                    "analytical_debit": op.analytical_debit,
                    "analytical_credit": op.analytical_credit,
                    "account_name": op.account_name,
                    "import_uuid": op.import_uuid
                })
                
            # Create DataFrame and sort it appropriately
            df = pd.DataFrame(operation_data)
            
            # For account-specific reports, we want to sort by amount (descending)
            # to show the most significant operations first
            df = df.sort_values(by="amount", ascending=False)
            
            # Create Excel file in memory
            excel_buffer = io.BytesIO()
            df.to_excel(excel_buffer, index=False)
            excel_buffer.seek(0)
            
            # Upload to S3 with the standard path structure: /exports/Debit or /exports/Credits
            # This ensures files go to the correct directories with proper capitalization
            directory = "Debit" if account_type.lower() == "debit" else "Credits"
            s3_key = f"exports/{directory}/{file_name}"
            # print(f"[DEBUG] Uploading Excel file to S3: {s3_key}")
            success, message = self.s3_service.upload_file(excel_buffer, s3_key)
            # print(f"[DEBUG] S3 upload result: success={success}, message={message}")
            
            if success:
                return s3_key
            else:
                print(f"Error uploading file to S3: {message}")
                return None
                
        except Exception as e:
            error_msg = f"Error generating Excel file for {account_type} account {file_name}: {str(e)}"
            print(error_msg)
            # Log the full exception details for debugging
            import traceback
            traceback.print_exc()
            return None