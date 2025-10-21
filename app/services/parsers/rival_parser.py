import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime
from io import BytesIO

from app.services.parsers.base_parser import BaseExcelParser
from app.services.account_matcher import AccountMatcher


class RivalParser(BaseExcelParser):
    """Parser for Rival Excel format"""
    
    def __init__(self):
        """Initialize the Rival parser with account matcher service"""
        super().__init__()
        self.account_matcher = AccountMatcher()
    
    def _group_related_operations(self, rows: List[pd.Series], company_info: Dict[str, Any], file_id: int, import_uuid: str = None) -> List[Dict[str, Any]]:
        """
        Group related debit and credit entries in Rival format and create complete accounting operations.
        
        In Rival Excel files, each accounting transaction is typically split across multiple rows:
        - One row for the debit account
        - One or more rows for the credit accounts
        
        This method groups related entries by document number, date, and transaction group,
        then matches debit and credit accounts to create complete accounting operations.
        
        Args:
            rows: List of DataFrame rows containing operation data
            company_info: Dictionary with company information extracted from the header
            file_id: ID of the uploaded file in the database
            import_uuid: UUID of the import batch this file belongs to
            
        Returns:
            List of dictionaries containing complete accounting operations
        """
        # Group rows by document identifiers
        grouped_rows = {}
        
        for row in rows:
            # Extract key identifiers for grouping
            doc_number = self.clean_string(row.iloc[7])  # Document number
            doc_date = self.convert_to_date(row.iloc[9])  # Date
            doc_type = self.clean_string(row.iloc[4])  # Document type
            transaction_group = self.clean_string(row.iloc[3])  # Transaction group/статия
            
            if not doc_number or not doc_date:
                continue
                
            # Use a more comprehensive grouping key including document type
            key = (doc_number, doc_date, doc_type, transaction_group)
            if key not in grouped_rows:
                grouped_rows[key] = []
            grouped_rows[key].append(row)
        
        operations = []
        
        # Process each group
        for key, group_rows in grouped_rows.items():
            doc_number, doc_date, doc_type, transaction_group = key
            
            # Separate into debit and credit entries
            debit_entries = [r for r in group_rows if not pd.isna(r.iloc[12])]  # Has debit account
            credit_entries = [r for r in group_rows if not pd.isna(r.iloc[13])]  # Has credit account
            
            # Skip groups with no debit or credit entries
            if not debit_entries or not credit_entries:
                continue
            
            # Extract common fields from the first row in the group
            document_type = doc_type
            partner_name = self.clean_string(group_rows[0].iloc[0])  # Using "Папка" as partner
            description = self.clean_string(group_rows[0].iloc[25])
            
            # Try to get sequence number if available
            sequence_number = None
            if len(group_rows[0]) > 0 and isinstance(group_rows[0].iloc[0], (int, float)) and not pd.isna(group_rows[0].iloc[0]):
                try:
                    sequence_number = int(group_rows[0].iloc[0])
                except (ValueError, TypeError):
                    pass
            
            # Create a mapping of debits and credits by amount
            debit_amounts = {}
            credit_amounts = {}
            
            # First, calculate total debits and credits to verify balanced operations
            total_debit = 0
            total_credit = 0
            
            # Map entries by amount for easy matching
            for debit in debit_entries:
                amount = self.clean_numeric(debit.iloc[14])
                if amount is None:
                    continue
                
                total_debit += amount
                if amount not in debit_amounts:
                    debit_amounts[amount] = []
                debit_amounts[amount].append(debit)
                
            for credit in credit_entries:
                amount = self.clean_numeric(credit.iloc[14])
                if amount is None:
                    continue
                
                total_credit += amount
                if amount not in credit_amounts:
                    credit_amounts[amount] = []
                credit_amounts[amount].append(credit)
            
            # Verify the operation is balanced within a small margin of error
            if abs(total_debit - total_credit) > 0.1:
                print(f"[WARNING] Unbalanced operation detected: doc_number={doc_number}, doc_date={doc_date}. "
                      f"Total debit: {total_debit}, total credit: {total_credit}")
                # Continue processing anyway but log the warning
            
            # New approach: create direct 1:1 operations where possible, then handle complex cases
            operations_for_group = []
            used_debits = set()
            used_credits = set()
            
            # Step 1: Direct amount matches first (1:1 mapping)
            for amount, debits in debit_amounts.items():
                if amount in credit_amounts:
                    for i, debit in enumerate(debits):
                        debit_id = id(debit)
                        if debit_id in used_debits:
                            continue
                            
                        # Find a matching credit
                        for credit in credit_amounts[amount]:
                            credit_id = id(credit)
                            if credit_id in used_credits:
                                continue
                                
                            # Create a 1:1 operation
                            operation = {
                                "file_id": file_id,
                                "operation_date": doc_date,
                                "document_type": document_type,
                                "document_number": doc_number,
                                "debit_account": self.clean_string(debit.iloc[12]),
                                "credit_account": self.clean_string(credit.iloc[13]),
                                "amount": amount,
                                "description": description,
                                "partner_name": partner_name,
                                "template_type": "RIVAL",
                                "raw_data": {
                                    "debit_entry": self._clean_dict_for_json(debit.to_dict()),
                                    "credit_entry": self._clean_dict_for_json(credit.to_dict()),
                                    "company_info": company_info
                                },
                                "import_uuid": import_uuid,
                                "sequence_number": sequence_number,
                                "verified_amount": None,
                                "deviation_amount": None,
                                "control_action": None,
                                "deviation_note": None
                            }
                            operations_for_group.append(operation)
                            used_debits.add(debit_id)
                            used_credits.add(credit_id)
                            break
            
            # Step 2: Process remaining entries with many-to-one mappings (multiple credits to one debit)
            remaining_debits = [d for d in debit_entries if id(d) not in used_debits]
            remaining_credits = [c for c in credit_entries if id(c) not in used_credits]
            
            # Process each remaining debit entry
            for debit in remaining_debits:
                debit_amount = self.clean_numeric(debit.iloc[14])
                if debit_amount is None or id(debit) in used_debits:
                    continue
                    
                # Find matching credits that sum up to this debit
                matching_credits = []
                credits_total = 0
                remaining_credits_copy = sorted(
                    [c for c in remaining_credits if id(c) not in used_credits],
                    key=lambda x: self.clean_numeric(x.iloc[14]) or 0, reverse=True
                )
                
                for credit in remaining_credits_copy:
                    credit_amount = self.clean_numeric(credit.iloc[14])
                    if credit_amount is None:
                        continue
                        
                    if credits_total + credit_amount <= debit_amount + 0.01:
                        matching_credits.append(credit)
                        credits_total += credit_amount
                        
                        # If we've matched exactly, create the operation
                        if abs(credits_total - debit_amount) < 0.01:
                            credit_accounts = [self.clean_string(c.iloc[13]) for c in matching_credits]
                            credit_account = " + ".join(credit_accounts)
                            
                            operation = {
                                "file_id": file_id,
                                "operation_date": doc_date,
                                "document_type": document_type,
                                "document_number": doc_number,
                                "debit_account": self.clean_string(debit.iloc[12]),
                                "credit_account": credit_account,
                                "amount": debit_amount,
                                "description": description,
                                "partner_name": partner_name,
                                "template_type": "RIVAL",
                                "raw_data": {
                                    "debit_entry": self._clean_dict_for_json(debit.to_dict()),
                                    "credit_entries": [self._clean_dict_for_json(c.to_dict()) for c in matching_credits],
                                    "company_info": company_info
                                },
                                "import_uuid": import_uuid,
                                "sequence_number": sequence_number,
                                "verified_amount": None,
                                "deviation_amount": None,
                                "control_action": None,
                                "deviation_note": None
                            }
                            operations_for_group.append(operation)
                            
                            # Mark entries as used
                            used_debits.add(id(debit))
                            for c in matching_credits:
                                used_credits.add(id(c))
                            break
            
            # Step 3: Process remaining entries with one-to-many mappings (one credit to multiple debits)
            remaining_debits = [d for d in debit_entries if id(d) not in used_debits]
            remaining_credits = [c for c in credit_entries if id(c) not in used_credits]
            
            # Process each remaining credit entry
            for credit in remaining_credits:
                credit_amount = self.clean_numeric(credit.iloc[14])
                if credit_amount is None or id(credit) in used_credits:
                    continue
                    
                # Find matching debits that sum up to this credit
                matching_debits = []
                debits_total = 0
                remaining_debits_copy = sorted(
                    [d for d in remaining_debits if id(d) not in used_debits],
                    key=lambda x: self.clean_numeric(x.iloc[14]) or 0, reverse=True
                )
                
                for debit in remaining_debits_copy:
                    debit_amount = self.clean_numeric(debit.iloc[14])
                    if debit_amount is None:
                        continue
                        
                    if debits_total + debit_amount <= credit_amount + 0.01:
                        matching_debits.append(debit)
                        debits_total += debit_amount
                        
                        # If we've matched exactly, create the operation
                        if abs(debits_total - credit_amount) < 0.01:
                            debit_accounts = [self.clean_string(d.iloc[12]) for d in matching_debits]
                            debit_account = " + ".join(debit_accounts)
                            
                            operation = {
                                "file_id": file_id,
                                "operation_date": doc_date,
                                "document_type": document_type,
                                "document_number": doc_number,
                                "debit_account": debit_account,
                                "credit_account": self.clean_string(credit.iloc[13]),
                                "amount": credit_amount,
                                "description": description,
                                "partner_name": partner_name,
                                "template_type": "RIVAL",
                                "raw_data": {
                                    "debit_entries": [self._clean_dict_for_json(d.to_dict()) for d in matching_debits],
                                    "credit_entry": self._clean_dict_for_json(credit.to_dict()),
                                    "company_info": company_info
                                },
                                "import_uuid": import_uuid,
                                "sequence_number": sequence_number,
                                "verified_amount": None,
                                "deviation_amount": None,
                                "control_action": None,
                                "deviation_note": None
                            }
                            operations_for_group.append(operation)
                            
                            # Mark entries as used
                            used_credits.add(id(credit))
                            for d in matching_debits:
                                used_debits.add(id(d))
                            break
            
            # Step 4: Handle any remaining entries that couldn't be matched
            # These entries might be problematic or might require more complex logic
            remaining_debits = [d for d in debit_entries if id(d) not in used_debits]
            remaining_credits = [c for c in credit_entries if id(c) not in used_credits]
            
            if remaining_debits or remaining_credits:
                # Log warning about unmatched entries
                print(f"[WARNING] Unmatched entries for document {doc_number}, date {doc_date}: "
                      f"{len(remaining_debits)} debits and {len(remaining_credits)} credits")
                
                # Create separate operations for remaining entries if we can't match them
                for debit in remaining_debits:
                    debit_amount = self.clean_numeric(debit.iloc[14])
                    if debit_amount is None:
                        continue
                        
                    # If we have no credits left, create an operation with empty credit
                    operation = {
                        "file_id": file_id,
                        "operation_date": doc_date,
                        "document_type": document_type,
                        "document_number": doc_number,
                        "debit_account": self.clean_string(debit.iloc[12]),
                        "credit_account": "",  # Empty credit account
                        "amount": debit_amount,
                        "description": description + " (UNMATCHED DEBIT)",
                        "partner_name": partner_name,
                        "template_type": "RIVAL",
                        "raw_data": {
                            "debit_entry": self._clean_dict_for_json(debit.to_dict()),
                            "company_info": company_info
                        },
                        "import_uuid": import_uuid,
                        "sequence_number": sequence_number,
                        "verified_amount": None,
                        "deviation_amount": None,
                        "control_action": None,
                        "deviation_note": None
                    }
                    operations_for_group.append(operation)
                
                for credit in remaining_credits:
                    credit_amount = self.clean_numeric(credit.iloc[14])
                    if credit_amount is None:
                        continue
                        
                    # If we have no debits left, create an operation with empty debit
                    operation = {
                        "file_id": file_id,
                        "operation_date": doc_date,
                        "document_type": document_type,
                        "document_number": doc_number,
                        "debit_account": "",  # Empty debit account
                        "credit_account": self.clean_string(credit.iloc[13]),
                        "amount": credit_amount,
                        "description": description + " (UNMATCHED CREDIT)",
                        "partner_name": partner_name,
                        "template_type": "RIVAL",
                        "raw_data": {
                            "credit_entry": self._clean_dict_for_json(credit.to_dict()),
                            "company_info": company_info
                        },
                        "import_uuid": import_uuid,
                        "sequence_number": sequence_number,
                        "verified_amount": None,
                        "deviation_amount": None,
                        "control_action": None,
                        "deviation_note": None
                    }
                    operations_for_group.append(operation)
            
            # Add operations from this group to the main list
            operations.extend(operations_for_group)
        
        # Add a deduplication step to ensure no duplicate operations
        return self._deduplicate_operations(operations)
    
    def parse(self, file_path: str, file_id: int, import_uuid: str = None) -> List[Dict[str, Any]]:
        """
        Parse the Rival Excel file and extract accounting operations
        
        For Rival format:
        - The file has merged cells in rows 1, 2, 4, 5, 6 from column A to K (header section) containing important company information:
          - Row 1: Company name (e.g., "ФОРСТА ЕООД")
          - Row 2: Company address (e.g., "гр.София, бул. "България" № 69, Инфинити Тауър, ет. 14")
          - Row 3: "ХРОНОЛОГИЧЕН ОПИС НА ПАПКА" (Chronological list of folder)
          - Row 4: Period information (e.g., "за периода Януари - Юни, 2024г.")
          - Row 5: User information (e.g., "Всички потребители")
        - Rows 8 and 9 are merged and form the header of the data table
        - Actual data starts at row 10 with the following columns:
          - Column 1: Вид документ (Document type)
          - Column 2: Номер на документ (Document number)
          - Column 3: Дата (Date)
          - Column 4: Име (Name/Partner)
          - Column 5: Дебит (Debit account)
          - Column 6: Кредит (Credit account)
          - Column 7: Сума (Amount)
          - Column 8: Обяснение (Description)
          
        Important note: When analyzing the Rival template data, columns P to Y (indices 15-24)
        should be skipped as they contain internal or irrelevant data.
        
        Args:
            file_path: Path to the Excel file
            file_id: ID of the uploaded file in the database
            import_uuid: UUID of the import batch this file belongs to
            
        Returns:
            List of dictionaries containing accounting operations data
        """
        try:
            # Read Excel file
            df = pd.read_excel(file_path)
            
            # First extract company information from the header section (rows 0-5)
            company_info = self._extract_company_info(df)
            
            # Find the row where actual data starts after the merged cell header structure
            # Specific to Rival format with merged cells in rows 1, 2, 4, 5, 6 and header in rows 8-9
            data_start_row = self._find_data_start_row(df)
            
            # Log the detected start row for debugging
            print(f"[INFO] Rival parser detected data start at row {data_start_row} in file")
            print(f"[INFO] Company info extracted: {company_info}")
            
            # Prepare data for processing
            if data_start_row > 0:
                df = df.iloc[data_start_row:]
                df = df.reset_index(drop=True)
            
            # Collect all valid rows for processing
            valid_rows = []
            for _, row in df.iterrows():
                # Include rows that have either a debit or credit account and an amount
                if not pd.isna(row.iloc[14]) and (not pd.isna(row.iloc[12]) or not pd.isna(row.iloc[13])):
                    valid_rows.append(row)
            
            # Group related rows and create complete operations
            # First pass: Group related operations
            operations = self._group_related_operations(valid_rows, company_info, file_id, import_uuid)
            
            # Pre-processing: Match and fill missing accounts using the AccountMatcher service
            # We use the same operations as both source and reference, as some operations will have
            # complete account information while others might be missing accounts
            try:
                missing_debit_before = sum(1 for op in operations if not op.get('debit_account') or op.get('debit_account') is None or str(op.get('debit_account', '')) == 'nan')
                missing_credit_before = sum(1 for op in operations if not op.get('credit_account') or op.get('credit_account') is None or str(op.get('credit_account', '')) == 'nan')
                
                print(f"[INFO] Before matching: {missing_debit_before} operations missing debit accounts, {missing_credit_before} missing credit accounts")
                
                operations = self.account_matcher.match_rival_accounts(operations, operations)
                
                missing_debit_after = sum(1 for op in operations if not op.get('debit_account') or op.get('debit_account') is None or str(op.get('debit_account', '')) == 'nan')
                missing_credit_after = sum(1 for op in operations if not op.get('credit_account') or op.get('credit_account') is None or str(op.get('credit_account', '')) == 'nan')
                
                print(f"[INFO] Account matching applied to {len(operations)} operations")
                print(f"[INFO] After matching: {missing_debit_after} operations missing debit accounts, {missing_credit_after} missing credit accounts")
                print(f"[INFO] Filled: {missing_debit_before - missing_debit_after} debit accounts, {missing_credit_before - missing_credit_after} credit accounts")
            except Exception as e:
                print(f"[WARNING] Error during account matching: {str(e)}")
                import traceback
                traceback.print_exc()
            
            # If grouping didn't produce any operations, fall back to the old method
            if not operations:
                print("[WARNING] Grouping related operations didn't produce any results, falling back to individual row processing")
                operations = []
                
                # Process each row individually (legacy approach)
                for _, row in df.iterrows():
                    # Skip rows that don't have amount or both debit and credit accounts
                    if pd.isna(row.iloc[14]) or (pd.isna(row.iloc[12]) and pd.isna(row.iloc[13])):
                        continue
                    
                    # Extract and clean data
                    operation_date = self.convert_to_date(row.iloc[9])
                    document_type = self.clean_string(row.iloc[4])
                    document_number = self.clean_string(row.iloc[7])
                    partner_name = self.clean_string(row.iloc[0])  # Using "Папка" as partner
                    debit_account = self.clean_string(row.iloc[12])
                    credit_account = self.clean_string(row.iloc[13])
                    amount = self.clean_numeric(row.iloc[14])
                    description = self.clean_string(row.iloc[25])
                    
                    # Skip if we don't have a valid date or amount is None (but keep zero amounts)
                    if not operation_date or amount is None:
                        continue
                    
                    # Try to get sequence number if available
                    sequence_number = None
                    if len(row) > 0 and isinstance(row.iloc[0], (int, float)) and not pd.isna(row.iloc[0]):
                        try:
                            sequence_number = int(row.iloc[0])
                        except (ValueError, TypeError):
                            pass
                    
                    # Create operation dictionary
                    operation = {
                        "file_id": file_id,
                        "operation_date": operation_date,
                        "document_type": document_type,
                        "document_number": document_number,
                        "debit_account": debit_account,
                        "credit_account": credit_account,
                        "amount": amount,
                        "description": description,
                        "partner_name": partner_name,
                        "template_type": "RIVAL",
                        "raw_data": {**self._clean_dict_for_json(row.to_dict()), "company_info": company_info},  # Include company_info in raw_data
                        "import_uuid": import_uuid,
                        # New audit fields with default values
                        "sequence_number": sequence_number,
                        "verified_amount": None,
                        "deviation_amount": None,
                        "control_action": None,
                        "deviation_note": None
                    }
                    
                    operations.append(operation)
            
            # Deduplicate operations before returning
            deduplicated_operations = self._deduplicate_operations(operations)
            print(f"[INFO] Deduplication removed {len(operations) - len(deduplicated_operations)} duplicate operations")
            
            return deduplicated_operations
            
        except Exception as e:
            print(f"Error parsing Rival Excel file: {e}")
            return []
    
    def parse_memory(self, file_obj: BytesIO, file_id: int, import_uuid: str = None) -> List[Dict[str, Any]]:
        """
        Parse the Rival Excel file from memory and extract accounting operations
        
        The Rival Excel structure includes:
        - Merged cells in rows 1, 2, 4, 5, 6 from column A to K (header information)
        - Rows 8 and 9 merged to form the header of the data table
        - Actual data starts at row 10
        
        Args:
            file_obj: BytesIO object containing the Excel file
            file_id: ID of the uploaded file in the database
            import_uuid: UUID of the import batch this file belongs to
            
        Returns:
            List of dictionaries containing accounting operations data
        """
        try:
            # Reset file pointer to beginning
            file_obj.seek(0)
            
            # Read Excel file from memory
            df = pd.read_excel(file_obj)
            
            # First extract company information from the header section (rows 0-5)
            company_info = self._extract_company_info(df)
            
            # Find the row where actual data starts after the merged cell header structure
            # Specific to Rival format with merged cells in rows 1, 2, 4, 5, 6 and header in rows 8-9
            data_start_row = self._find_data_start_row(df)
            
            # Log the detected start row for debugging
            print(f"[INFO] Rival parser detected data start at row {data_start_row+1} (index {data_start_row})")
            
            if data_start_row > 0:
                df = df.iloc[data_start_row:]
                df = df.reset_index(drop=True)
            
            # Collect all valid rows for processing
            valid_rows = []
            for _, row in df.iterrows():
                # Include rows that have either a debit or credit account and an amount
                if not pd.isna(row.iloc[14]) and (not pd.isna(row.iloc[12]) or not pd.isna(row.iloc[13])):
                    valid_rows.append(row)
            
            # First pass: Group related operations
            operations = self._group_related_operations(valid_rows, company_info, file_id, import_uuid)
            
            # Pre-processing: Match and fill missing accounts using the AccountMatcher service
            # We use the same operations as both source and reference, as some operations will have
            # complete account information while others might be missing accounts
            try:
                missing_debit_before = sum(1 for op in operations if not op.get('debit_account') or op.get('debit_account') is None or str(op.get('debit_account', '')) == 'nan')
                missing_credit_before = sum(1 for op in operations if not op.get('credit_account') or op.get('credit_account') is None or str(op.get('credit_account', '')) == 'nan')
                
                print(f"[INFO] Before matching: {missing_debit_before} operations missing debit accounts, {missing_credit_before} missing credit accounts")
                
                operations = self.account_matcher.match_rival_accounts(operations, operations)
                
                missing_debit_after = sum(1 for op in operations if not op.get('debit_account') or op.get('debit_account') is None or str(op.get('debit_account', '')) == 'nan')
                missing_credit_after = sum(1 for op in operations if not op.get('credit_account') or op.get('credit_account') is None or str(op.get('credit_account', '')) == 'nan')
                
                print(f"[INFO] Account matching applied to {len(operations)} operations")
                print(f"[INFO] After matching: {missing_debit_after} operations missing debit accounts, {missing_credit_after} missing credit accounts")
                print(f"[INFO] Filled: {missing_debit_before - missing_debit_after} debit accounts, {missing_credit_before - missing_credit_after} credit accounts")
            except Exception as e:
                print(f"[WARNING] Error during account matching: {str(e)}")
                import traceback
                traceback.print_exc()
            
            # If grouping didn't produce any operations, fall back to the old method
            if not operations:
                print("[WARNING] Grouping related operations didn't produce any results, falling back to individual row processing")
                operations = []
                
                # Process each row individually (legacy approach)
                for _, row in df.iterrows():
                    # Skip rows that don't have amount or both debit and credit accounts
                    if pd.isna(row.iloc[14]) or (pd.isna(row.iloc[12]) and pd.isna(row.iloc[13])):
                        continue
                    
                    # Extract and clean data
                    operation_date = self.convert_to_date(row.iloc[9])
                    document_type = self.clean_string(row.iloc[4])
                    document_number = self.clean_string(row.iloc[7])
                    partner_name = self.clean_string(row.iloc[0])  # Using "Папка" as partner
                    debit_account = self.clean_string(row.iloc[12])
                    credit_account = self.clean_string(row.iloc[13])
                    amount = self.clean_numeric(row.iloc[14])
                    description = self.clean_string(row.iloc[25])
                    
                    # Skip if we don't have a valid date or amount is None (but keep zero amounts)
                    if not operation_date or amount is None:
                        continue
                    
                    # Try to get sequence number if available
                    sequence_number = None
                    if len(row) > 0 and isinstance(row.iloc[0], (int, float)) and not pd.isna(row.iloc[0]):
                        try:
                            sequence_number = int(row.iloc[0])
                        except (ValueError, TypeError):
                            pass
                    
                    # Create operation dictionary
                    operation = {
                        "file_id": file_id,
                        "operation_date": operation_date,
                        "document_type": document_type,
                        "document_number": document_number,
                        "debit_account": debit_account,
                        "credit_account": credit_account,
                        "amount": amount,
                        "description": description,
                        "partner_name": partner_name,
                        "template_type": "RIVAL",
                        "raw_data": {**self._clean_dict_for_json(row.to_dict()), "company_info": company_info},  # Include company_info in raw_data
                        "import_uuid": import_uuid,
                        # New audit fields with default values
                        "sequence_number": sequence_number,
                        "verified_amount": None,
                        "deviation_amount": None,
                        "control_action": None,
                        "deviation_note": None
                    }
                    
                    operations.append(operation)
            # Deduplicate operations before returning
            deduplicated_operations = self._deduplicate_operations(operations)
            print(f"[INFO] Deduplication removed {len(operations) - len(deduplicated_operations)} duplicate operations")
            
            return deduplicated_operations
            
            
        except Exception as e:
            print(f"Error parsing Rival Excel file from memory: {e}")
            return []
    
    def _find_data_start_row(self, df: pd.DataFrame) -> int:
        """
        Find the row where actual data starts
        
        In Rival Excel files, the structure is typically:
        - Rows 1, 2, 4, 5, 6 have merged cells from column A to K (header rows)
        - Rows 8 and 9 are merged and form the header of the data table
        - Actual data starts at row 10 (index 9 in 0-based indexing)
        
        Args:
            df: DataFrame with the Excel content
            
        Returns:
            Row index where data starts (0-based)
        """
        # For Rival format, we know data always starts at row 10 (index 9)
        if len(df) >= 10:
            return 9  # Return index 9 (row 10) as the start row
        
        # If file is too short, return a safe default
        if len(df) < 10:
            print("[WARNING] Rival file is shorter than expected (< 10 rows), using first row as data")
            return 0
            
        # For Rival format, data always starts at row 10 (index 9)
        return 9  # Return index 9 (row 10) as the start row
        
    def _extract_company_info(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Extract company information from the header rows of Rival Excel files
        
        Rival Excel files contain important company information in rows 1-6:
        - Row 1: Company name
        - Row 2: Company address
        - Row 3: Document type ("ХРОНОЛОГИЧЕН ОПИС НА ПАПКА")
        - Row 4: Period information
        - Row 5: User information
        
        Args:
            df: DataFrame with the Excel content
            
        Returns:
            Dictionary containing extracted company information
        """
        company_info = {
            "company_name": None,
            "address": None,
            "document_type": None,
            "period": None,
            "users": None
        }
        
        # Check if the DataFrame has enough rows
        if len(df) < 6:
            print("[WARNING] Rival file doesn't have enough header rows for company information")
            return company_info
            
        try:
            # Extract information from specific rows
            # Company name is typically in row 1 (index 0)
            if not pd.isna(df.iloc[0, 0]):
                company_info["company_name"] = str(df.iloc[0, 0])
                
            # Company address is typically in row 2 (index 1)
            if not pd.isna(df.iloc[1, 0]):
                company_info["address"] = str(df.iloc[1, 0])
                
            # Document type is typically in row 3 (index 2)
            if not pd.isna(df.iloc[2, 0]):
                company_info["document_type"] = str(df.iloc[2, 0])
                
            # Period information is typically in row 4 (index 3)
            if not pd.isna(df.iloc[3, 0]):
                company_info["period"] = str(df.iloc[3, 0])
                
            # User information is typically in row 5 (index 4)
            if not pd.isna(df.iloc[4, 0]):
                company_info["users"] = str(df.iloc[4, 0])
                
        except Exception as e:
            print(f"[WARNING] Error extracting company information: {e}")
            
        return company_info
        
    def _clean_dict_for_json(self, d: dict) -> dict:
        """
        Clean a dictionary to make it JSON-serializable by replacing NaN values
        
        Args:
            d: Dictionary to clean
            
        Returns:
            Cleaned dictionary that can be safely serialized to JSON
        """
        import math
        import numpy as np
        
        result = {}
        for k, v in d.items():
            # Handle NaN, None and other non-serializable values
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                result[k] = None
            elif isinstance(v, np.ndarray):
                result[k] = v.tolist()
            elif isinstance(v, (np.int8, np.int16, np.int32, np.int64, np.intc, np.intp,
                           np.uint8, np.uint16, np.uint32, np.uint64)):
                result[k] = int(v)
            elif isinstance(v, (np.float16, np.float32, np.float64)):
                if math.isnan(v) or math.isinf(v):
                    result[k] = None
                else:
                    result[k] = float(v)
            elif isinstance(v, dict):
                result[k] = self._clean_dict_for_json(v)
            else:
                result[k] = v
        return result
        
    def _deduplicate_operations(self, operations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Remove duplicate operations using a unique key based on key operation fields.
        
        Args:
            operations: List of operation dictionaries
            
        Returns:
            Deduplicated list of operations
        """
        seen_operations = set()
        deduplicated_operations = []
        
        for op in operations:
            # Create a tuple of key fields to use as a unique identifier
            # Using all critical fields that make an operation unique
            op_key = (
                op.get('document_number', ''),
                str(op.get('operation_date', '')),
                op.get('debit_account', ''),
                op.get('credit_account', ''),
                op.get('amount', 0)
            )
            
            if op_key not in seen_operations:
                seen_operations.add(op_key)
                deduplicated_operations.append(op)
        
        return deduplicated_operations