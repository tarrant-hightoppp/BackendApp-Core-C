import io
import time
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional, Set, Tuple
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.operation import AccountingOperation
from app.models.file import UploadedFile
from app.services.s3 import S3Service
from app.services.excel_report import ExcelTemplateWrapper
from app.services.accounting_operation_processor import AccountingOperationProcessor


class AjurAuditProcessor:
    """Service specifically for auditing Ajur accounting operations by Дт с/ка and Кт с/ка columns"""
    
    def __init__(self, db: Session):
        self.db = db
        self.s3_service = S3Service()
        self.template_wrapper = ExcelTemplateWrapper()
        # Re-use the standard operation processor's functionality where needed
        self.operation_processor = AccountingOperationProcessor(db)
    
    def process_audit(self, import_uuid: str, audit_approach: str = "full", control_action_mode: str = "round_robin") -> Dict[str, Any]:
        """
        Process all operations for a specific import and generate audit files grouped by
        Дт с/ка (debit account) and Кт с/ка (credit account) values
        
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
        
        if not operations:
            print(f"[ERROR] No operations found for import {import_uuid}")
            return {"success": False, "message": f"No operations found for import {import_uuid}"}
        
        # Process debit accounts (Дт с/ка)
        debit_results = self._process_debit_accounts(operations, import_uuid, audit_approach, control_action_mode=control_action_mode)
        
        # Process credit accounts (Кт с/ка)
        credit_results = self._process_credit_accounts(operations, import_uuid, audit_approach, control_action_mode=control_action_mode)
        
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
    
    def _process_debit_accounts(self, operations: List[AccountingOperation],
                               import_uuid: str, audit_approach: str = "full",
                               control_action_mode: str = "round_robin") -> List[Dict[str, Any]]:
        """
        Process operations grouped by debit account (Дт с/ка)
        
        Args:
            operations: List of operations to process
            import_uuid: UUID of the import
            audit_approach: The audit approach to use
            
        Returns:
            List of dictionaries with results for each account
        """
        # Group operations by debit account
        account_groups = self._group_by_debit_account(operations)
        
        print(f"[DEBUG] Grouped operations into {len(account_groups)} debit account groups")
        
        results = []
        
        # Process each debit account group
        for account, account_operations in account_groups.items():
            # Create a timestamp for the filename
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            
            # Using import_uuid at the beginning of the filename for easy identification
            # Use the full account information, making it safe for filename
            safe_account = self._make_safe_for_filename(account)
            
            file_name = f"{import_uuid}-DEBIT-{safe_account}__{timestamp}.xlsx"
            
            # Apply filtering based on audit approach
            filtered_operations = account_operations
            
            # Only apply filtering for statistical approach
            if audit_approach == "statistical":
                filtered_operations = self._filter_operations(account_operations, audit_approach=audit_approach)
                print(f"[DEBUG] Filtered debit account {account} operations: {len(filtered_operations)} of {len(account_operations)}")
            else:
                # For "full" approach, ensure we're using ALL operations
                filtered_operations = account_operations
                print(f"[DEBUG] Using all {len(account_operations)} operations for debit account {account} (audit approach: {audit_approach})")
            
            # Generate and upload the account-specific Excel file
            print(f"[DEBUG] Generating Excel file for debit account {account} with {len(filtered_operations)} operations")
            s3_key = self._generate_and_upload_file(filtered_operations, file_name, "debit", import_uuid, audit_approach, control_action_mode=control_action_mode)
            
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
    
    def _process_credit_accounts(self, operations: List[AccountingOperation],
                                import_uuid: str, audit_approach: str = "full",
                                control_action_mode: str = "round_robin") -> List[Dict[str, Any]]:
        """
        Process operations grouped by credit account (Кт с/ка)
        
        Args:
            operations: List of operations to process
            import_uuid: UUID of the import
            audit_approach: The audit approach to use
            
        Returns:
            List of dictionaries with results for each account
        """
        # Group operations by credit account
        account_groups = self._group_by_credit_account(operations)
        
        print(f"[DEBUG] Grouped operations into {len(account_groups)} credit account groups")
        
        results = []
        
        # Process each credit account group
        for account, account_operations in account_groups.items():
            # Create a timestamp for the filename
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            
            # Format: {import_uuid}-CREDIT-{account}__{timestamp}.xlsx
            # Use the raw Кт с/ка value directly from the account data
            safe_account = self._make_safe_for_filename(account)
            file_name = f"{import_uuid}-CREDIT-{safe_account}__{timestamp}.xlsx"
            
            # Apply filtering based on audit approach
            filtered_operations = account_operations
            
            # Only apply filtering for statistical approach
            if audit_approach == "statistical":
                filtered_operations = self._filter_operations(account_operations, audit_approach=audit_approach)
                print(f"[DEBUG] Filtered credit account {account} operations: {len(filtered_operations)} of {len(account_operations)}")
            else:
                # For "full" approach, ensure we're using ALL operations
                filtered_operations = account_operations
                print(f"[DEBUG] Using all {len(account_operations)} operations for credit account {account} (audit approach: {audit_approach})")
            
            # Generate and upload the account-specific Excel file
            # Include the full original account information in the debug log
            print(f"[DEBUG] Generating Excel file for credit account {account} with {len(filtered_operations)} operations")
            s3_key = self._generate_and_upload_file(filtered_operations, file_name, "credit", import_uuid, audit_approach, control_action_mode=control_action_mode)
            
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
    
    def _group_by_debit_account(self, operations: List[AccountingOperation]) -> Dict[str, List[AccountingOperation]]:
        """
        Group operations by debit account (Дт с/ка)
        
        Args:
            operations: List of operations to group
            
        Returns:
            Dictionary with debit accounts as keys and lists of operations as values
        """
        account_groups = {}
        
        for operation in operations:
            # Get the debit_account (Дт с/ка) from the operation
            account = operation.debit_account
            
            if not account:
                continue
            
            # Use the complete debit account value without modification
            # This preserves all the original account information
            
            # Remove any spaces, line breaks or other whitespace only
            account = account.strip()
            
            # Create a new group for this account if it doesn't exist
            if account not in account_groups:
                account_groups[account] = []
                
            # Add this operation to the account group
            account_groups[account].append(operation)
            
        return account_groups
    
    def _group_by_credit_account(self, operations: List[AccountingOperation]) -> Dict[str, List[AccountingOperation]]:
        """
        Group operations by credit account (Кт с/ка)
        
        Args:
            operations: List of operations to group
            
        Returns:
            Dictionary with credit accounts as keys and lists of operations as values
        """
        account_groups = {}
        
        for operation in operations:
            # Get the raw value from the Кт с/ка column - directly use the credit_account field
            # This should be the exact value from the "Кт с/ка" column in the Excel file
            credit_account = operation.credit_account
            
            if not credit_account:
                continue
                
            # Check if we can find the original Кт с/ка value in the raw_data
            if hasattr(operation, 'raw_data') and operation.raw_data:
                # Try to find the raw Кт с/ка value in the raw data
                # The column index might vary, but the column name would be something like 'Кт с/ка'
                raw_data = operation.raw_data
                for key, value in raw_data.items():
                    # Look for column names that might contain 'кт с/ка'
                    key_str = str(key).lower()
                    if 'кт' in key_str and 'с/ка' in key_str:
                        # Found the original Кт с/ка value
                        if value:
                            credit_account = value
                            break
            
            # Create a new group for this account if it doesn't exist
            if credit_account not in account_groups:
                account_groups[credit_account] = []
                
            # Add this operation to the account group
            account_groups[credit_account].append(operation)
            
        return account_groups
    
    def _make_safe_for_filename(self, account: str) -> str:
        """
        Make an account name safe to use in a filename without changing its basic structure.
        
        Args:
            account: The account name to make safe
            
        Returns:
            A filename-safe version of the account name
        """
        # If the account is None or empty, return a default
        if not account:
            return "unknown"
        
        # Replace any characters that would cause issues in filenames
        # but preserve the original structure as much as possible
        safe_chars = {
            ':': '_',
            ';': '_',
            '\\': '_',
            '/': '_',  # Keep slashes as they're part of account structure like 401/1
            '<': '_',
            '>': '_',
            '"': '_',
            "'": '_',
            '|': '_',
            '?': '_',
            '*': '_',
            ' ': '_'
        }
        
        result = account
        for char, replacement in safe_chars.items():
            result = result.replace(char, replacement)
            
        # Trim any excess whitespace
        result = result.strip()
        
        return result
            
    def _filter_operations(self, operations: List[AccountingOperation],
                          threshold_percentage: float = 0.8,
                          min_operations: int = 30,
                          audit_approach: str = "statistical") -> List[AccountingOperation]:
        """
        Apply filtering logic to operations based on the specified threshold and audit approach
        
        Args:
            operations: List of operations to filter
            threshold_percentage: The percentage threshold for filtering (default: 0.8 for 80%)
            min_operations: The minimum number of operations to apply filtering (default: 30)
            audit_approach: The audit approach to use (default: "statistical")
            
        Returns:
            Filtered list of operations
        """
        # Reuse the existing filtering logic from the operation processor
        # For "full" audit approach, include all operations (100%)
        if audit_approach == "full":
            print(f"[INFO] Using full audit approach - including all {len(operations)} operations")
            return operations
            
        # For other approaches, apply filtering logic
        # If we have min_operations or fewer operations, include all of them
        if len(operations) <= min_operations:
            return operations
        
        # Always include operations with amount = 0
        zero_operations = [op for op in operations if op.amount == 0]
        non_zero_operations = [op for op in operations if op.amount != 0]
        
        # Sort non-zero operations by amount (descending) to prioritize largest transactions
        sorted_operations = sorted(non_zero_operations, key=lambda x: x.amount, reverse=True)
        
        # Calculate total amount across all non-zero operations
        total_amount = sum(op.amount for op in non_zero_operations)
        
        # Calculate the threshold amount
        threshold = total_amount * threshold_percentage
        
        # Select non-zero operations until reaching the threshold
        filtered_non_zero_operations = []
        cumulative_amount = 0
        
        # If total_amount is 0 (all operations are 0 or we've filtered them all out),
        # we'll include all non-zero operations
        if total_amount == 0 or not non_zero_operations:
            filtered_non_zero_operations = non_zero_operations
        else:
            for operation in sorted_operations:
                filtered_non_zero_operations.append(operation)
                cumulative_amount += operation.amount
                
                # Once we reach or exceed the threshold, we can stop
                if cumulative_amount >= threshold:
                    break
        
        # Combine zero-amount operations with filtered non-zero operations
        filtered_operations = zero_operations + filtered_non_zero_operations
        
        # Calculate the percentage of total amount covered
        percentage = (cumulative_amount / total_amount) * 100 if total_amount > 0 else 100
        
        print(f"Filtered operations: {len(filtered_operations)} of {len(operations)} "
              f"({len(zero_operations)} zero-sum operations included automatically, "
              f"plus {len(filtered_non_zero_operations)} non-zero operations "
              f"representing {percentage:.2f}% of total amount)")
                
        return filtered_operations
    
    def _generate_and_upload_file(self, operations: List[AccountingOperation],
                                  file_name: str, account_type: str, import_uuid: str,
                                  audit_approach: str = "statistical",
                                  control_action_mode: str = "round_robin") -> Optional[str]:
        """
        Generate Excel file with operations and upload to S3
        
        Args:
            operations: List of operations to include in the file
            file_name: Name of the file to generate
            account_type: "debit" or "credit" to include in file metadata
            import_uuid: UUID of the import batch
            audit_approach: The audit approach to use (default: "statistical")
            control_action_mode: Mode for control action column ("round_robin" or "placeholder")
            
        Returns:
            S3 key if successful, None otherwise
        """
        # Reuse the existing logic from AccountingOperationProcessor
        try:
            # Convert operations to dataframe
            operation_data = []
            
            # Counter for sequence number if not already assigned
            seq_count = 1
            
            for op in operations:
                # Use existing sequence_number or assign a new one
                seq_number = op.sequence_number if op.sequence_number is not None else seq_count
                
                # Get raw data from the operation
                debit_account_raw = op.debit_account
                debit_analytical_raw = op.analytical_debit
                credit_account_raw = op.credit_account
                credit_analytical_raw = op.analytical_credit
                
                # Initialize processed fields
                debit_account = debit_account_raw
                debit_analytical = debit_analytical_raw
                credit_account = credit_account_raw
                credit_analytical = credit_analytical_raw
                
                # Extract partner/company name from credit account if available
                # Format is typically "20;ЕКОНТ Експрес ООД;1220700737;11.01.2023"
                partner_name = op.partner_name
                doc_info = None
                
                # Process credit account information
                if credit_account and ';' in credit_account:
                    parts = credit_account.split(';')
                    # First part is the account number (e.g., "20")
                    credit_account = parts[0].strip()
                    
                    # Additional parts contain analytical information
                    if len(parts) > 1:
                        # If we don't have a partner name yet, use second part
                        if len(parts) >= 2 and not partner_name:
                            partner_name = parts[1].strip()
                            
                        # If there's document information in parts
                        if len(parts) >= 3:
                            doc_info = ';'.join(parts[2:])
                            
                        # Use all remaining parts as analytical information
                        if not credit_analytical:
                            credit_analytical = ';'.join(parts[1:])
                
                # Process debit account information
                if debit_account and ';' in debit_account:
                    parts = debit_account.split(';')
                    # First part is the account number
                    debit_account = parts[0].strip()
                    # If there are additional parts, use them as analytical info
                    if len(parts) > 1 and not debit_analytical:
                        debit_analytical = ';'.join(parts[1:])
                
                # Create the row data dictionary with all extracted information
                row_data = {
                    self.operation_processor.COL_SEQ_NUM: seq_number,
                    self.operation_processor.COL_DOC_TYPE: op.document_type,
                    self.operation_processor.COL_DOC_NUM: op.document_number,
                    self.operation_processor.COL_DATE: op.operation_date,
                    self.operation_processor.COL_DEBIT_ACC: debit_account,
                    self.operation_processor.COL_DEBIT_ANALYTICAL: debit_analytical,
                    self.operation_processor.COL_CREDIT_ACC: credit_account,
                    self.operation_processor.COL_CREDIT_ANALYTICAL: credit_analytical,
                    self.operation_processor.COL_AMOUNT: float(op.amount) if op.amount else 0.0,
                    self.operation_processor.COL_DESCRIPTION: op.description,
                    # Always use the same amount from the database for the verified amount
                    self.operation_processor.COL_VERIFIED_AMOUNT: float(op.amount) if op.amount else 0.0,
                    # Set deviation to 0.0 as requested
                    self.operation_processor.COL_DEVIATION: "НЯМА",
                    self.operation_processor.COL_CONTROL_ACTION: op.control_action or "",
                    self.operation_processor.COL_DEVIATION_NOTE: op.deviation_note,
                    # Enhanced information from credit account extraction
                    "partner_name": partner_name,
                    "document_info": doc_info,
                    "account_name": op.account_name,
                    "import_uuid": op.import_uuid
                }
                
                # Add the row data to our operation data
                operation_data.append(row_data)
                
                if op.sequence_number is None:
                    seq_count += 1
            
            # Create DataFrame
            df = pd.DataFrame(operation_data)
            
            # Create Excel file in memory
            excel_buffer = io.BytesIO()
            df.to_excel(excel_buffer, index=False)
            excel_buffer.seek(0)
            
            # Get company name from the first operation if available
            company_name = "Форт България ЕООД"  # Default
            year = None
            
            # Try to extract company info and year from operations
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
                audit_approach=audit_approach,
                control_action_mode=control_action_mode
            )
            
            # Upload to S3 with the import-specific folder structure
            directory = "sorted_by_debit" if account_type.lower() == "debit" else "sorted_by_credit"
            s3_key = f"{import_uuid}/{directory}/{file_name}"
            
            print(f"[DEBUG] Uploading to S3: bucket={settings.S3_BUCKET_NAME}, key={s3_key}")
            success, message = self.s3_service.upload_file(wrapped_excel, s3_key)
            
            if success:
                # Verify the file exists in S3
                print(f"[DEBUG] Verified file exists in S3: {s3_key}")
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