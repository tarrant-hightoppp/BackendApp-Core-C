# Accounting Operation Processor Design

## Overview

The `AccountingOperationProcessor` service will be responsible for:

1. Processing operations from a specific import (identified by import_uuid)
2. Generating account-specific Excel files based on the required filtering logic
3. Storing the generated files in S3

This document outlines the design and implementation details for this new functionality.

## Current Process Flow

Currently, the system:
1. Uploads Excel files to S3
2. Detects the template type (Rival, AJUR, etc.)
3. Processes the files to extract accounting operations
4. Stores the operations in the database

## New Functionality

The new functionality will add the following steps:
1. Group uploaded files by import_uuid
2. Process operations from all files in an import
3. Group operations by account number (both debit and credit)
4. Apply filtering logic to each account's operations
5. Generate account-specific Excel files
6. Store these files in S3

## Implementation Details

### 1. UploadedFile and AccountingOperation Models

The `UploadedFile` model has an `import_uuid` field which is used to track files belonging to the same import batch. The file upload process:

- Generates a new UUID for each import (whether it's a single file or multiple files)
- Sets this UUID for all files in the import
- Ensures the `import_uuid` field is properly populated and tracked

The `AccountingOperation` model also includes an `import_uuid` field to directly track which import batch each operation belongs to. This allows:

- Direct querying of operations by import_uuid without joining to the UploadedFile table
- Grouping operations by import batch for processing
- Maintaining import context even if the original file is deleted

### 2. AccountingOperationProcessor Service

This new service will have the following key methods:

#### `process_import(import_uuid: str) -> Dict[str, Any]`

- Main entry point for processing an import
- Gets all operations for the specified import_uuid
- Processes both debit and credit accounts
- Returns statistics and results

#### `_get_operations_by_import(import_uuid: str) -> List[AccountingOperation]`

- Retrieves all operations related to a specific import
- Directly queries the AccountingOperation table using the import_uuid field

#### `_process_accounts(operations: List[AccountingOperation], account_type: str, import_uuid: str) -> List[Dict[str, Any]]`

- Processes operations for all accounts of a specific type (debit/credit)
- Groups operations by account
- Applies filtering logic to each account's operations
- Generates and uploads Excel files
- Returns results for each account

#### `_group_by_account(operations: List[AccountingOperation], account_type: str) -> Dict[str, List[AccountingOperation]]`

- Groups operations by account number
- Uses debit_account or credit_account based on account_type

#### `_filter_operations(operations: List[AccountingOperation]) -> List[AccountingOperation]`

Applies the specified filtering logic:
- If ≤ 30 operations, include all
- If > 30 operations, include operations that make up 80% of total amount (sorted by amount)

#### `_generate_and_upload_file(operations: List[AccountingOperation], file_name: str, account_type: str) -> Optional[str]`

- Converts operations to a DataFrame
- Creates an Excel file in memory
- Uploads the file to S3
- Returns the S3 key if successful

### 3. S3 File Naming Strategy

Generated files will follow this naming pattern:
```
{account_number}_{import_uuid}_{timestamp}.xlsx
```

And will be stored in S3 with this path structure:
```
account_reports/{account_type}/{file_name}
```

Where:
- `account_type` is either "debit" or "credit"
- `file_name` is the generated file name

### 4. API Endpoint

A new API endpoint will be added:

```
POST /api/operations/process-import/{import_uuid}
```

This endpoint will:
- Call the `AccountingOperationProcessor.process_import()` method
- Return the processing results and statistics

### 5. Filtering Logic

For each account (both debit and credit):

1. If the account has ≤ 30 operations, include all operations
2. If the account has > 30 operations:
   - Sort operations by amount (descending)
   - Calculate total amount for the account
   - Select operations until reaching 80% of the total amount
   - This ensures that the most significant operations are included

### 6. Output Excel File Structure

The generated Excel files will maintain the original structure with these columns:
- operation_date
- document_type
- document_number
- debit_account
- credit_account
- amount
- description
- partner_name
- analytical_debit
- analytical_credit
- import_uuid (for tracking purposes)

## Implementation Plan

1. Update the file upload process to properly set import_uuid
2. Implement the AccountingOperationProcessor service
3. Add the new API endpoint
4. Add tests for the new functionality
5. Update documentation

## Example Usage

```python
# Example API endpoint implementation
@router.post("/process-import/{import_uuid}", response_model=Dict[str, Any])
def process_import(
    *,
    db: Session = Depends(deps.get_db),
    import_uuid: str
) -> Any:
    """
    Process all operations for a specific import and generate account-specific files.
    """
    processor = AccountingOperationProcessor(db)
    result = processor.process_import(import_uuid)
    
    if not result["success"]:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=result["message"]
        )
    
    return result