from typing import Any, List, Optional
from datetime import date
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from app.app import models, schemas
from app.api import deps
from app.services.accounting_operation_processor import AccountingOperationProcessor
from typing import Dict

router = APIRouter(tags=["operations"])


@router.get("/", response_model=schemas.operation.OperationList,
          summary="List operations",
          description="""
          Retrieve accounting operations with comprehensive filtering options
          
          ## Filtering Options
          
          * **Date Range**: Filter by start_date and end_date
          * **Account Numbers**: Filter by debit_account or credit_account
          * **Document Details**: Filter by document_type
          * **Amount Range**: Filter by min_amount and max_amount
          * **Description**: Search within descriptions using description_contains
          * **Template Type**: Filter by source accounting system (template_type)
          
          ## Audit Fields
          
          * **sequence_number**: Filter by specific sequence number
          * **has_verified_amount**: Show only operations that have/don't have verified amounts
          * **has_deviation**: Show only operations that have/don't have deviations
          * **has_control_action**: Show only operations that have/don't have control actions
          """)
def get_operations(
    db: Session = Depends(deps.get_db),
    skip: int = 0,
    limit: int = 100,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    document_type: Optional[str] = None,
    debit_account: Optional[str] = None,
    credit_account: Optional[str] = None,
    min_amount: Optional[float] = None,
    max_amount: Optional[float] = None,
    description_contains: Optional[str] = None,
    template_type: Optional[str] = None,
    file_id: Optional[int] = None,
    # New audit-related parameters
    sequence_number: Optional[int] = None,
    has_verified_amount: Optional[bool] = None,
    has_deviation: Optional[bool] = None,
    has_control_action: Optional[bool] = None,
) -> Any:
    """
    Retrieve accounting operations with filtering and pagination.
    
    This endpoint supports comprehensive filtering options for accounting operations,
    including the new audit-related fields. Use the filters to narrow down results
    based on accounting criteria or audit status.
    """
    # Start with a base query for all operations
    query = db.query(models.AccountingOperation).join(
        models.UploadedFile,
        models.AccountingOperation.file_id == models.UploadedFile.id
    )
    
    # Apply filters if provided
    filters = []
    
    if start_date:
        filters.append(models.AccountingOperation.operation_date >= start_date)
    
    if end_date:
        filters.append(models.AccountingOperation.operation_date <= end_date)
    
    if document_type:
        filters.append(models.AccountingOperation.document_type.ilike(f"%{document_type}%"))
    
    if debit_account:
        filters.append(models.AccountingOperation.debit_account.ilike(f"%{debit_account}%"))
    
    if credit_account:
        filters.append(models.AccountingOperation.credit_account.ilike(f"%{credit_account}%"))
    
    if min_amount is not None:
        filters.append(models.AccountingOperation.amount >= min_amount)
    
    if max_amount is not None:
        filters.append(models.AccountingOperation.amount <= max_amount)
    
    if description_contains:
        filters.append(models.AccountingOperation.description.ilike(f"%{description_contains}%"))
    
    if template_type:
        filters.append(models.AccountingOperation.template_type == template_type)
    
    if file_id:
        filters.append(models.AccountingOperation.file_id == file_id)
        
    # Add filters for audit fields
    if sequence_number is not None:
        filters.append(models.AccountingOperation.sequence_number == sequence_number)
    
    if has_verified_amount is not None:
        if has_verified_amount:
            filters.append(models.AccountingOperation.verified_amount.is_not(None))
        else:
            filters.append(models.AccountingOperation.verified_amount.is_(None))
    
    if has_deviation is not None:
        if has_deviation:
            filters.append(models.AccountingOperation.deviation_amount.is_not(None))
        else:
            filters.append(models.AccountingOperation.deviation_amount.is_(None))
    
    if has_control_action is not None:
        if has_control_action:
            filters.append(models.AccountingOperation.control_action.is_not(None))
        else:
            filters.append(models.AccountingOperation.control_action.is_(None))
    
    if filters:
        query = query.filter(and_(*filters))
    
    # Get total count for pagination
    total = query.count()
    
    # Apply pagination and ordering
    operations = query.order_by(models.AccountingOperation.operation_date.desc()).offset(skip).limit(limit).all()
    
    return {"items": operations, "total": total}


@router.get("/{operation_id}", response_model=schemas.operation.Operation,
           summary="Get operation details",
           description="""
           Get detailed information about a specific accounting operation
           
           Returns comprehensive information about an accounting operation, including:
           
           * Core accounting data (date, accounts, amount)
           * Document details (type, number)
           * Analytical information
           * Audit information (verified amount, deviations, control actions)
           """)
def get_operation(
    *,
    db: Session = Depends(deps.get_db),
    operation_id: int
) -> Any:
    """
    Get a specific accounting operation by ID.
    """
    operation = db.query(models.AccountingOperation).filter(
        models.AccountingOperation.id == operation_id
    ).first()
    
    if not operation:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Operation not found"
        )
    
    return operation


@router.get("/statistics/summary", response_model=dict,
            summary="Get operation statistics",
            description="Get summary statistics about accounting operations including counts and totals")
def get_operations_summary(
    db: Session = Depends(deps.get_db),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> Any:
    """
    Get summary statistics about accounting operations.
    """
    # Base query for all operations
    query = db.query(models.AccountingOperation)
    
    # Apply date filters if provided
    if start_date:
        query = query.filter(models.AccountingOperation.operation_date >= start_date)
    
    if end_date:
        query = query.filter(models.AccountingOperation.operation_date <= end_date)
    
    # Get total count
    total_operations = query.count()
    
    # Get sum of amounts
    total_amount = db.query(
        db.func.sum(models.AccountingOperation.amount)
    ).select_from(
        models.AccountingOperation
    )
    
    if start_date:
        total_amount = total_amount.filter(models.AccountingOperation.operation_date >= start_date)
    
    if end_date:
        total_amount = total_amount.filter(models.AccountingOperation.operation_date <= end_date)
    
    total_amount = total_amount.scalar() or 0
    
    # Get counts by template type
    template_counts = db.query(
        models.AccountingOperation.template_type,
        db.func.count(models.AccountingOperation.id)
    )
    
    if start_date:
        template_counts = template_counts.filter(models.AccountingOperation.operation_date >= start_date)
    
    if end_date:
        template_counts = template_counts.filter(models.AccountingOperation.operation_date <= end_date)
    
    template_counts = template_counts.group_by(
        models.AccountingOperation.template_type
    ).all()
    
    template_counts_dict = {template: count for template, count in template_counts}
    
    return {
        "total_operations": total_operations,
        "total_amount": float(total_amount),
        "template_counts": template_counts_dict
    }


@router.get("/export", response_model=dict,
           summary="Export operations",
           description="Export filtered accounting operations (placeholder for future implementation)")
def export_operations(
    db: Session = Depends(deps.get_db),
    # Include the same filters as get_operations
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    document_type: Optional[str] = None,
    debit_account: Optional[str] = None,
    credit_account: Optional[str] = None,
    min_amount: Optional[float] = None,
    max_amount: Optional[float] = None,
    description_contains: Optional[str] = None,
    template_type: Optional[str] = None,
    file_id: Optional[int] = None,
    # New audit-related parameters
    sequence_number: Optional[int] = None,
    has_verified_amount: Optional[bool] = None,
    has_deviation: Optional[bool] = None,
    has_control_action: Optional[bool] = None,
) -> Any:
    """
    Export accounting operations to CSV/Excel.
    
    This is a placeholder for the export functionality.
    In a real implementation, this would generate and return a file.
    """
    # In a real implementation, this would generate and return a file
    # For now, we'll just return a message
    return {
        "message": "Export functionality will be implemented in a future version.",
        "filter_params": {
            "start_date": start_date,
            "end_date": end_date,
            "document_type": document_type,
            "debit_account": debit_account,
            "credit_account": credit_account,
            "min_amount": min_amount,
            "max_amount": max_amount,
            "description_contains": description_contains,
            "template_type": template_type,
            "file_id": file_id,
            "sequence_number": sequence_number,
            "has_verified_amount": has_verified_amount,
            "has_deviation": has_deviation,
            "has_control_action": has_control_action
        }
    }


@router.post("/process-import/{import_uuid}", response_model=Dict[str, Any],
           summary="Process import operations and generate account reports",
           description="""
           Process all operations for a specific import and generate account-specific Excel files.
           
           ## Account Reporting Feature
           
           This endpoint implements the account reporting feature that:
           
           1. Groups operations by account number (separate processing for debit and credit accounts)
           2. Applies filtering based on the selected audit approach:
              - **Full (100%)**: Includes ALL operations regardless of count
              - **Statistical (80/20 rule)**:
                 - If an account has ≤ 30 operations: includes ALL operations
                 - If an account has > 30 operations: includes operations that constitute 80% of the total amount
                   (sorted by amount in descending order)
              - **Selected Objects**: Custom selection logic for specific objects
           3. Generates XLSX files for each account with naming pattern: `{account}_{import_uuid}_{timestamp}.xlsx`
           4. Uploads files to S3 storage for retrieval
           
           ## Response Format
           
           The response includes detailed statistics about the processed accounts and the generated files,
           including file names, S3 storage paths, and counts of operations processed.
           """)
def process_import(
    *,
    db: Session = Depends(deps.get_db),
    import_uuid: str,
    audit_approach: str = Query("statistical",
                               description="Audit approach to use: 'full' (100% population), 'statistical' (80/20 rule), or 'selected' (selected objects)")
) -> Any:
    """
    Process all operations for a specific import and generate account-specific Excel files.
    
    ## Workflow
    
    This endpoint:
    1. Finds all operations related to the specified import UUID
    2. Groups operations by account (separately for debit and credit)
    3. Applies filtering based on the selected audit approach:
       - **Full (100%)**: Includes ALL operations regardless of count
       - **Statistical (80/20 rule)**:
          - For accounts with ≤30 operations: includes ALL operations
          - For accounts with >30 operations: includes operations constituting 80% of total amount
            (sorted by largest amount first)
       - **Selected Objects**: Custom selection logic for specific objects
    4. Generates account-specific Excel files with all relevant operation details
    5. Uploads files to S3 with naming pattern: {account}_{import_uuid}_{timestamp}.xlsx
    
    ## Example Response
    
    ```json
    {
        "success": true,
        "debit_accounts_processed": 5,
        "credit_accounts_processed": 7,
        "debit_files": [
            {
                "account": "122",
                "total_operations": 45,
                "filtered_operations": 28,
                "s3_key": "abc123/sorted_by_debit/122_abc123_20250917041532.xlsx",
                "file_name": "122_abc123_20250917041532.xlsx"
            },
            ...
        ],
        "credit_files": [
            {
                "account": "401",
                "total_operations": 22,
                "filtered_operations": 22,
                "s3_key": "abc123/sorted_by_credit/401_abc123_20250917041532.xlsx",
                "file_name": "401_abc123_20250917041532.xlsx"
            },
            ...
        ],
        "import_uuid": "abc123",
        "total_operations": 156
    }
    ```
    
    Returns detailed statistics about the processed accounts and generated files.
    """
    # Check if import exists
    import_exists = db.query(models.UploadedFile).filter(
        models.UploadedFile.import_uuid == import_uuid
    ).first()
    
    if not import_exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Import with UUID {import_uuid} not found"
        )
    
    # Process the import with the specified audit approach
    processor = AccountingOperationProcessor(db)
    result = processor.process_import(import_uuid, audit_approach)
    
    if not result["success"]:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=result["message"]
        )
    
    return result