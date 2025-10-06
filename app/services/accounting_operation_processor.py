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
from app.services.excel_template_wrapper import ExcelTemplateWrapper


class AccountingOperationProcessor:
    """Service for processing accounting operations and generating account-specific files"""
    
    # Column name constants to ensure consistency
    COL_SEQ_NUM = "№ по ред"
    COL_DOC_TYPE = "Вид документ"
    COL_DOC_NUM = "Документ №"
    COL_DATE = "Дата"
    COL_DEBIT_ACC = "Дт с/ка"
    COL_DEBIT_ANALYTICAL = "Аналитична сметка/Партньор (Дт)"
    COL_CREDIT_ACC = "Кт с/ка"
    COL_CREDIT_ANALYTICAL = "Аналитична сметка/Партньор (Кт)"
    COL_AMOUNT = "Сума"
    COL_DESCRIPTION = "Обяснение/Обоснование"
    COL_VERIFIED_AMOUNT = "Установена сума при одита"
    COL_DEVIATION = "Отклонение"
    COL_CONTROL_ACTION = "Установено контролно действие при одита"
    COL_DEVIATION_NOTE = "Отклонение (забележка)"
    
    def __init__(self, db: Session):
        self.db = db
        self.s3_service = S3Service()
        self.template_wrapper = ExcelTemplateWrapper()
    
    def process_import(self, import_uuid: str, audit_approach: str = "statistical") -> Dict[str, Any]:
        """
        Process all operations for a specific import and generate account-specific files
        
        Args:
            import_uuid: UUID of the import to process
            audit_approach: The audit approach to use:
                - "full" for 100% population check
                - "statistical" for statistical audit sampling (80/20 rule)
                - "selected" for check of selected population objects
            
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
        debit_results = self._process_accounts(operations, "debit", import_uuid, audit_approach)
        
        # Process credit accounts
        credit_results = self._process_accounts(operations, "credit", import_uuid, audit_approach)
        
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
                          account_type: str, import_uuid: str,
                          audit_approach: str = "statistical") -> List[Dict[str, Any]]:
        """
        Process operations for all accounts of a specific type (debit/credit)
        
        Args:
            operations: List of operations to process
            account_type: "debit" or "credit" to determine which accounts to process
            import_uuid: UUID of the import
            audit_approach: The audit approach to use:
                - "full" for 100% population check
                - "statistical" for statistical audit sampling (80/20 rule)
                - "selected" for check of selected population objects
            
        Returns:
            List of dictionaries with results for each account
        """
        # Group operations by main account (first 3 digits)
        main_account_groups = {}
        
        for operation in operations:
            account = operation.debit_account if account_type == "debit" else operation.credit_account
            
            if not account:
                continue
            
            # Extract the main account number (first 3 digits)
            # For accounts like "453/2", "453/9", the main account is "453"
            # For accounts with nested subaccounts like "453/2/1", the main account is still "453"
            parts = account.split('/')
            main_account = parts[0] if parts else account
            
            # Only use the first 3 digits if it's a numeric account
            if main_account.isdigit() and len(main_account) > 3:
                main_account = main_account[:3]
                
            if main_account not in main_account_groups:
                main_account_groups[main_account] = []
                
            main_account_groups[main_account].append(operation)
        
        print(f"[DEBUG] Grouped {account_type} operations into {len(main_account_groups)} main accounts")
        for account, ops in main_account_groups.items():
            print(f"[DEBUG] Main account {account} has {len(ops)} operations")
            
        results = []
        
        # Process each main account
        for main_account, main_account_operations in main_account_groups.items():
            # Create a timestamp for the filename
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            
            # Using import_uuid at the beginning of the filename for easy identification
            file_name = f"{import_uuid}-{account_type.upper()}-{main_account}__{timestamp}.xlsx"
            
            # Don't filter operations - include all subaccounts
            # This ensures we include all operations for the main account and its subaccounts
            
            # Apply filtering based on audit approach
            filtered_operations = main_account_operations
            
            # Only apply filtering for statistical approach
            # For "full" approach, we include all operations (100%)
            if audit_approach == "statistical":
                filtered_operations = self._filter_operations(main_account_operations, audit_approach=audit_approach)
                print(f"[DEBUG] Filtered {account_type} main account {main_account} operations: {len(filtered_operations)} of {len(main_account_operations)}")
            else:
                # For "full" approach, ensure we're using ALL operations
                filtered_operations = main_account_operations
                print(f"[DEBUG] Using all {len(main_account_operations)} operations for {account_type} main account {main_account} (audit approach: {audit_approach})")
            
            # Generate and upload the account-specific Excel file
            print(f"[DEBUG] Generating Excel file for {account_type} main account {main_account} with {len(filtered_operations)} operations")
            s3_key = self._generate_and_upload_file(filtered_operations, file_name, account_type, import_uuid, audit_approach)
            
            if s3_key:
                print(f"[INFO] Successfully uploaded file to S3: {s3_key}")
                results.append({
                    "account": main_account,
                    "total_operations": len(main_account_operations),
                    "filtered_operations": len(filtered_operations),  # Use the actual filtered count
                    "s3_key": s3_key,
                    "file_name": file_name
                })
        
        return results
    
    def _group_by_account(self, operations: List[AccountingOperation],
                          account_type: str) -> Dict[str, List[AccountingOperation]]:
        """
        Group operations by account number
        
        For accounts with format like "453/2", "453/9", we preserve the full account number
        including the subaccount part, as the subaccount is significant for accounting purposes.
        
        Args:
            operations: List of operations to group
            account_type: "debit" or "credit" to determine which account field to use
            
        Returns:
            Dictionary with full account numbers as keys and lists of operations as values
        """
        account_groups = {}
        
        for operation in operations:
            account = operation.debit_account if account_type == "debit" else operation.credit_account
            
            if not account:
                continue
            
            # Use the full account number including the subaccount part
            if account not in account_groups:
                account_groups[account] = []
                
            account_groups[account].append(operation)
            
        return account_groups
    
    def _filter_operations(self, operations: List[AccountingOperation],
                          threshold_percentage: float = 0.8,
                          min_operations: int = 30,
                          audit_approach: str = "statistical") -> List[AccountingOperation]:
        """
        Apply filtering logic to operations based on the specified threshold and audit approach:
        - For "full" audit approach, include all operations (100%)
        - For "statistical" audit approach:
          - If <= min_operations operations, include all operations (100%)
          - If > min_operations operations, include operations that make up the specified threshold
            of total amount (sorted by amount in descending order - largest transactions first)
        
        This implements the business rule that for accounts with many transactions,
        we focus on the most significant ones that represent a specified percentage of the financial value.
        
        Args:
            operations: List of operations to filter
            threshold_percentage: The percentage threshold for filtering (default: 0.8 for 80%)
            min_operations: The minimum number of operations to apply filtering (default: 30)
            audit_approach: The audit approach to use (default: "statistical")
            
        Returns:
            Filtered list of operations
        """
        # For "full" audit approach, include all operations (100%)
        if audit_approach == "full":
            print(f"[INFO] Using full audit approach - including all {len(operations)} operations")
            return operations
            
        # For other approaches, apply filtering logic
        # If we have min_operations or fewer operations, include all of them
        if len(operations) <= min_operations:
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
            threshold_percentage_decimal = Decimal(str(threshold_percentage))
            
            # Check if total_amount is already a Decimal, if not convert it
            if not isinstance(total_amount, Decimal):
                total_amount = Decimal(str(total_amount))
                
            # Calculate threshold using Decimal arithmetic
            threshold = total_amount * threshold_percentage_decimal
            
            # print(f"[DEBUG] Total amount: {total_amount} (type: {type(total_amount).__name__})")
            # print(f"[DEBUG] Threshold (80%): {threshold} (type: {type(threshold).__name__})")
            
        except Exception as e:
            # Fallback to float if there's an issue with Decimal
            print(f"[WARNING] Error handling Decimal arithmetic: {e}, falling back to float")
            # Convert everything to float to ensure compatibility
            total_amount = float(total_amount)
            threshold = total_amount * threshold_percentage
        
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
                                  file_name: str, account_type: str, import_uuid: str,
                                  audit_approach: str = "statistical") -> Optional[str]:
        """
        Generate Excel file with operations and upload to S3
        
        Args:
            operations: List of operations to include in the file
            file_name: Name of the file to generate
            account_type: "debit" or "credit" to include in file metadata
            import_uuid: UUID of the import batch
            audit_approach: The audit approach to use (default: "statistical")
            
        Returns:
            S3 key if successful, None otherwise
        """
        try:
            # Convert operations to dataframe
            operation_data = []
            
            # Counter for sequence number if not already assigned
            seq_count = 1
            
            for op in operations:
                # Use existing sequence_number or assign a new one
                seq_number = op.sequence_number if op.sequence_number is not None else seq_count
                
                operation_data.append({
                    self.COL_SEQ_NUM: seq_number,
                    self.COL_DOC_TYPE: op.document_type,
                    self.COL_DOC_NUM: op.document_number,
                    self.COL_DATE: op.operation_date,
                    self.COL_DEBIT_ACC: op.debit_account,
                    self.COL_DEBIT_ANALYTICAL: op.analytical_debit,
                    self.COL_CREDIT_ACC: op.credit_account,
                    self.COL_CREDIT_ANALYTICAL: op.analytical_credit,
                    self.COL_AMOUNT: float(op.amount),
                    self.COL_DESCRIPTION: op.description,
                    # Always use the same amount from the database for the verified amount
                    self.COL_VERIFIED_AMOUNT: float(op.amount),
                    # Set deviation to 0.0 as requested
                    self.COL_DEVIATION: 0.0,
                    self.COL_CONTROL_ACTION: op.control_action,
                    self.COL_DEVIATION_NOTE: op.deviation_note,
                    # Keep original fields for reference/compatibility
                    "partner_name": op.partner_name,
                    "account_name": op.account_name,
                    "import_uuid": op.import_uuid
                })
                
                if op.sequence_number is None:
                    seq_count += 1
                
            # Create DataFrame
            df = pd.DataFrame(operation_data)
            
            # Add robust sorting with fallback to prevent KeyErrors when columns don't exist
            try:
                # For account-specific reports, we want to sort first by account number
                # (to group subaccounts together), then by amount (descending)
                if account_type == "debit":
                    # Sort by debit account to group subaccounts together
                    df = df.sort_values(by=[self.COL_DEBIT_ACC, self.COL_AMOUNT], ascending=[True, False])
                    
                    # Remove any summary rows that might be in the middle of the report
                    # These are rows where the debit account doesn't have a subaccount part (no slash)
                    # and there are other rows with the same main account but with subaccounts
                    main_accounts = set()
                    has_subaccounts = set()
                    
                    # First pass: identify accounts with subaccounts (including nested subaccounts)
                    for acc in df[self.COL_DEBIT_ACC]:
                        if acc and '/' in acc:
                            # Handle nested subaccounts like "453/2/1"
                            parts = acc.split('/')
                            main_part = parts[0]
                            has_subaccounts.add(main_part)
                            main_accounts.add(main_part)
                            
                            # Also handle intermediate subaccounts
                            if len(parts) > 2:
                                for i in range(1, len(parts)):
                                    intermediate = '/'.join(parts[:i])
                                    has_subaccounts.add(intermediate)
                        elif acc:
                            main_accounts.add(acc)
                    
                    # Second pass: filter out summary rows for accounts that have subaccounts
                    if has_subaccounts:
                        df = df[~df[self.COL_DEBIT_ACC].apply(
                            lambda x: x and (x in has_subaccounts) and not any(
                                x + '/' in acc for acc in df[self.COL_DEBIT_ACC]
                            )
                        )]
                else:  # credit
                    # Sort by credit account to group subaccounts together
                    df = df.sort_values(by=[self.COL_CREDIT_ACC, self.COL_AMOUNT], ascending=[True, False])
                    
                    # Similar logic for credit accounts
                    main_accounts = set()
                    has_subaccounts = set()
                    
                    # First pass: identify accounts with subaccounts (including nested subaccounts)
                    for acc in df[self.COL_CREDIT_ACC]:
                        if acc and '/' in acc:
                            # Handle nested subaccounts like "453/2/1"
                            parts = acc.split('/')
                            main_part = parts[0]
                            has_subaccounts.add(main_part)
                            main_accounts.add(main_part)
                            
                            # Also handle intermediate subaccounts
                            if len(parts) > 2:
                                for i in range(1, len(parts)):
                                    intermediate = '/'.join(parts[:i])
                                    has_subaccounts.add(intermediate)
                        elif acc:
                            main_accounts.add(acc)
                    
                    # Second pass: filter out summary rows for accounts that have subaccounts
                    if has_subaccounts:
                        df = df[~df[self.COL_CREDIT_ACC].apply(
                            lambda x: x and (x in has_subaccounts) and not any(
                                x + '/' in acc for acc in df[self.COL_CREDIT_ACC]
                            )
                        )]
            except KeyError as e:
                print(f"[WARNING] Sorting error: Column {e} not found. Falling back to basic sorting.")
                try:
                    # Try sorting by just the amount as fallback
                    df = df.sort_values(by=self.COL_AMOUNT, ascending=False)
                except KeyError:
                    # If even that fails, log it but continue without sorting
                    print("[WARNING] Fallback sorting failed as well. Continuing without sorting.")
            
            # Create Excel file in memory
            excel_buffer = io.BytesIO()
            df.to_excel(excel_buffer, index=False)
            excel_buffer.seek(0)
            
            # Wrap the Excel file with the template
            # Get company name from the first operation if available
            company_name = "Форт България ЕООД"  # Default
            year = None
            
            # Try to extract year from operations
            if operations and len(operations) > 0:
                # Get the year from the first operation's date
                if operations[0].operation_date:
                    year = str(operations[0].operation_date.year)
            
            # Wrap the Excel file with the template
            wrapped_excel = self.template_wrapper.wrap_excel_with_template(
                excel_buffer,
                company_name=company_name,
                year=year,
                account_type=account_type,
                audit_approach=audit_approach
            )
            
            # Upload to S3 with the import-specific folder structure:
            # Each import creates its own directory structure
            directory = "sorted_by_debit" if account_type.lower() == "debit" else "sorted_by_credit"
            s3_key = f"{import_uuid}/{directory}/{file_name}"
            # print(f"[DEBUG] Uploading Excel file to S3: {s3_key}")
            success, message = self.s3_service.upload_file(wrapped_excel, s3_key)
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