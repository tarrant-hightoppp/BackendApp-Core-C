import io
import os
import uuid
from typing import Any, List, Dict
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, status, Query
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from sqlalchemy import and_

from app.app import models, schemas
from app.api import deps
from app.core.config import settings
from app.services.file_processor import FileProcessor
from app.services.template_detector import TemplateDetector
from app.services.s3 import S3Service
from app.services.accounting_operation_processor import AccountingOperationProcessor

router = APIRouter(tags=["files"])


@router.post("/upload",
            response_model=schemas.file.File,
            summary="Upload and process a new file",
            description="Upload an Excel file to S3 storage, detect its template type, automatically process it and generate account-specific reports")
async def upload_file(
    *,
    db: Session = Depends(deps.get_db),
    file: UploadFile = File(..., description="Excel file to upload (.xls or .xlsx)"),
    import_uuid: str = None, description="Optional. If provided, the file will be part of this import batch. If omitted, a new import_uuid will be generated.",
    audit_approach: str = Query("statistical",
                               description="Audit approach to use: 'full' (100% population), 'statistical' (80/20 rule), or 'selected' (selected objects)")
) -> Any:
    """
    Upload an Excel file for processing accounting operations.
    
    Completely automatic process:
    1. The file is uploaded to S3 storage
    2. Template type is detected automatically
    3. Operations are extracted from the file
    4. Account-specific Excel files are generated for both debit and credit accounts based on the selected audit approach:
       - **Full (100%)**: Includes ALL operations regardless of count
       - **Statistical (80/20 rule)**:
          - If account has ≤30 operations: includes ALL operations
          - If account has >30 operations: includes operations that constitute 80% of total amount
            (sorted by largest transactions first)
       - **Selected Objects**: Custom selection logic for specific objects
    5. Account files are uploaded to S3 in exports/{import_uuid}/{DEBIT|CREDIT}/ directories
    
    For each new import (single file or multiple files), a new import_uuid is generated.
    All files within the same import will share the same import_uuid.
    
    To upload multiple files as part of the same batch, provide the same import_uuid for each file.
    """
    # Validate file extension
    if not file.filename.endswith(('.xls', '.xlsx')):
        print(f"[ERROR] Invalid file format: {file.filename}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid file format. Only Excel files (.xls, .xlsx) are supported."
        )
    
    print(f"[DEBUG] Processing file upload: {file.filename}")
    
    # Read file content
    contents = await file.read()
    print(f"[DEBUG] File content read, size: {len(contents)} bytes")
    
    # Create a BytesIO object for in-memory file processing
    file_obj = io.BytesIO(contents)
    
    # Detect template type
    print(f"[DEBUG] Starting template detection for file: {file.filename}")
    template_detector = TemplateDetector()
    template_type = template_detector.detect_template_from_bytes(file_obj)
    
    print(f"[DEBUG] Template detection result: {template_type}")
    
    if not template_type:
        print(f"[ERROR] Could not recognize template format for file: {file.filename}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not recognize Excel template format."
        )
    
    # Generate a unique S3 object key with import_uuid at the start of the filename
    # and organize files in uploaded_files/ directory
    s3_key = f"uploaded_files/{import_uuid}-{file.filename}"
    
    # Upload to S3
    s3_service = S3Service()
    success, message = s3_service.upload_file(contents, s3_key)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to upload file to storage: {message}"
        )
    
    # Create file record in database
    file_processor = FileProcessor(db)
    
    # Generate a new import_uuid if not provided
    if import_uuid is None:
        import_uuid = str(uuid.uuid4())
        
    print(f"[DEBUG] Using import_uuid: {import_uuid}")
    
    db_file = file_processor.create_file(
        filename=file.filename,
        template_type=template_type.value,
        file_path=s3_key,  # Store S3 key instead of local path
        import_uuid=import_uuid
    )
    
    # Process the file to extract operations
    try:
        print(f"[INFO] Processing file {db_file.id} to extract operations")
        operations = file_processor.process_file(db_file.id)
        
        # Double check operations from database to ensure they were saved
        db_operations = db.query(models.AccountingOperation).filter(
            models.AccountingOperation.file_id == db_file.id
        ).all()
        
        if db_operations and len(db_operations) > 0:
            print(f"[INFO] Successfully saved {len(db_operations)} operations from file {db_file.id} to database")
            
            # Force commit to ensure operations are persisted
            db.commit()
            
            # Create a new database session to ensure all previous operations are committed
            new_db = deps.get_db_session()
            try:
                # Add a small delay to ensure database transactions are complete
                import time
                time.sleep(1)
                
                # Trigger account processing for this import with the new session
                print(f"[INFO] Triggering account processing for import {import_uuid}")
                processor = AccountingOperationProcessor(new_db)
                result = processor.process_import(import_uuid, audit_approach)
                
                if result["success"]:
                    print(f"[INFO] Account processing successful. Generated {result['debit_accounts_processed']} debit files and {result['credit_accounts_processed']} credit files.")
                else:
                    print(f"[WARNING] Account processing failed: {result['message']}")
            except Exception as e:
                print(f"[ERROR] Exception during account processing: {str(e)}")
                import traceback
                traceback.print_exc()
            finally:
                new_db.close()
        else:
            print(f"[WARNING] No operations were saved to database from file {db_file.id}")
            
    except Exception as e:
        print(f"[WARNING] File processing failed: {str(e)}")
        import traceback
        traceback.print_exc()
        # Don't raise the exception - the file upload was still successful
    
    return db_file


@router.post("/{file_id}/process", response_model=schemas.operation.OperationList,
            summary="Process a file manually", description="Manually extract accounting operations from an uploaded file (typically not needed as files are processed automatically)")
def process_file(
    *,
    db: Session = Depends(deps.get_db),
    file_id: int
) -> Any:
    """
    Manually process an uploaded file to extract accounting operations.
    
    NOTE: This endpoint is typically not needed as files are automatically processed
    during upload. Use this only if automatic processing failed for some reason.
    """
    # Check if file exists
    file = db.query(models.UploadedFile).filter(
        models.UploadedFile.id == file_id
    ).first()
    
    if not file:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found"
        )
    
    # Check if file has already been processed
    if file.processed:
        # Get existing operations
        operations = db.query(models.AccountingOperation).filter(
            models.AccountingOperation.file_id == file_id
        ).all()
        
        return {"items": operations, "total": len(operations)}
    
    # Process the file
    file_processor = FileProcessor(db)
    operations = file_processor.process_file(file_id)
    
    # Force commit to ensure operations are persisted
    db.commit()
    
    # Get saved operations from database (to ensure we have the IDs)
    db_operations = db.query(models.AccountingOperation).filter(
        models.AccountingOperation.file_id == file_id
    ).all()
    
    if not db_operations or len(db_operations) == 0:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to process file - no operations were extracted"
        )
    
    # Check if this is the last file to process in this import batch
    # If so, trigger the account processing
    import_uuid = file.import_uuid
    
    # Get all files in this import
    all_files = db.query(models.UploadedFile).filter(
        models.UploadedFile.import_uuid == import_uuid
    ).all()
    
    # Check if all files are processed
    all_processed = all(f.processed for f in all_files)
    
    # Print debugging information
    print(f"[DEBUG] Import {import_uuid} has {len(all_files)} files, all processed: {all_processed}")
    for f in all_files:
        print(f"[DEBUG] File {f.id}: {f.filename}, processed: {f.processed}")
    
    # Check if all files are processed, then trigger account processing
    if all_processed:
        print(f"[INFO] All files in import {import_uuid} have been processed. Triggering account processing.")
        try:
            # Ensure all operations are committed
            db.commit()
            
            # Add a small delay to ensure database transactions are complete
            import time
            time.sleep(1)
            
            # Create a new database session to ensure all previous operations are committed
            new_db = deps.get_db_session()
            try:
                # Verify operations exist before processing
                operation_count = new_db.query(models.AccountingOperation).filter(
                    models.AccountingOperation.import_uuid == import_uuid
                ).count()
                
                if operation_count == 0:
                    print(f"[WARNING] No operations found for import {import_uuid} before processing")
                else:
                    print(f"[INFO] Found {operation_count} operations for import {import_uuid}")
                
                processor = AccountingOperationProcessor(new_db)
                result = processor.process_import(import_uuid)
                
                if result["success"]:
                    print(f"[INFO] Account processing successful. Generated {result['debit_accounts_processed']} debit files and {result['credit_accounts_processed']} credit files.")
                else:
                    print(f"[WARNING] Account processing failed: {result['message']}")
            except Exception as e:
                print(f"[ERROR] Exception during account processing: {str(e)}")
                import traceback
                traceback.print_exc()
            finally:
                new_db.close()
        except Exception as e:
            print(f"[ERROR] Exception during account processing: {str(e)}")
            import traceback
            traceback.print_exc()
    
    return {"items": db_operations, "total": len(db_operations)}


@router.get("/", response_model=schemas.file.FileList,
           summary="List files", description="Get a list of all uploaded files with filtering options")
def get_files(
    db: Session = Depends(deps.get_db),
    skip: int = 0,
    limit: int = 100,
    template_type: str = None,
    processed: bool = None,
    import_uuid: str = None
) -> Any:
    """
    Retrieve files with filtering and pagination.
    
    Optional filters:
    - template_type: Filter by detected template type
    - processed: Filter by processing status
    - import_uuid: Filter by import batch UUID to see all files from the same batch
    """
    query = db.query(models.UploadedFile)
    
    # Apply filters if provided
    if template_type:
        query = query.filter(models.UploadedFile.template_type == template_type)
    
    if processed is not None:
        query = query.filter(models.UploadedFile.processed == processed)
        
    if import_uuid:
        query = query.filter(models.UploadedFile.import_uuid == import_uuid)
    
    # Get total count for pagination
    total = query.count()
    
    # Apply pagination
    files = query.order_by(models.UploadedFile.upload_date.desc()).offset(skip).limit(limit).all()
    
    return {"items": files, "total": total}


@router.get("/{file_id}", response_model=schemas.file.FileWithOperationCount,
           summary="Get file details", description="Get detailed information about a specific file")
def get_file(
    *,
    db: Session = Depends(deps.get_db),
    file_id: int
) -> Any:
    """
    Get detailed information about a specific file.
    """
    # Get file with operation count
    file = db.query(
        models.UploadedFile,
        func.count(models.AccountingOperation.id).label("operation_count")
    ).join(
        models.AccountingOperation,
        models.UploadedFile.id == models.AccountingOperation.file_id,
        isouter=True
    ).filter(
        models.UploadedFile.id == file_id
    ).group_by(models.UploadedFile.id).first()
    
    if not file:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found"
        )
    
    result = file[0].__dict__
    result["operation_count"] = file[1]
    
    return result


@router.delete("/{file_id}", status_code=status.HTTP_204_NO_CONTENT, response_model=None,
              summary="Delete file", description="Delete a file from S3 storage and database")
def delete_file(
    *,
    db: Session = Depends(deps.get_db),
    file_id: int
) -> None:
    """
    Delete a file and its associated operations.
    """
    file = db.query(models.UploadedFile).filter(
        models.UploadedFile.id == file_id
    ).first()
    
    if not file:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found"
        )
    
    # Delete the file from S3 if it exists
    if file.file_path:
        try:
            s3_service = S3Service()
            s3_service.delete_file(file.file_path)
        except Exception as e:
            print(f"Error deleting file {file.file_path} from S3: {e}")
    
    # Delete the database record (cascade will delete operations)
    db.delete(file)
    db.commit()
    
    
@router.post("/batch/{import_uuid}/process",
             response_model=Dict[str, Any],
             summary="Process all files in a batch",
             description="Process all files with the same import_uuid and generate account-specific reports")
def process_batch(
    *,
    db: Session = Depends(deps.get_db),
    import_uuid: str,
    audit_approach: str = Query("statistical",
                               description="Audit approach to use: 'full' (100% population), 'statistical' (80/20 rule), or 'selected' (selected objects)")
) -> Any:
    """
    Process all files that belong to the same import batch (same import_uuid).
    
    This endpoint allows processing multiple files as a single batch. It will:
    1. Check if all files in the batch have been processed
    2. If any files are not processed, it will process them
    3. Generate account-specific Excel files for both debit and credit accounts
       - Based on the selected audit approach:
          - **Full (100%)**: Includes ALL operations regardless of count
          - **Statistical (80/20 rule)**:
             - If account has ≤30 operations: includes ALL operations
             - If account has >30 operations: includes operations that constitute 80% of total amount
               (sorted by largest transactions first)
          - **Selected Objects**: Custom selection logic for specific objects
    4. Upload generated files to S3 in exports/{import_uuid}/{DEBIT|CREDIT}/ directories
    
    Returns details about the processed batch including counts of generated files.
    """
    # Get all files in this import batch
    files = db.query(models.UploadedFile).filter(
        models.UploadedFile.import_uuid == import_uuid
    ).all()
    
    if not files:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No files found with import_uuid: {import_uuid}"
        )
    
    print(f"[INFO] Found {len(files)} files in batch {import_uuid}")
    
    # Check if any files need processing
    unprocessed_files = [f for f in files if not f.processed]
    
    # Process any unprocessed files
    file_processor = FileProcessor(db)
    for file in unprocessed_files:
        print(f"[INFO] Processing file {file.id}: {file.filename}")
        operations = file_processor.process_file(file.id)
        
        # Check if operations were extracted and saved
        db_operations = db.query(models.AccountingOperation).filter(
            models.AccountingOperation.file_id == file.id
        ).all()
        
        if db_operations and len(db_operations) > 0:
            print(f"[INFO] Successfully saved {len(db_operations)} operations from file {file.id}")
        else:
            print(f"[WARNING] No operations were saved from file {file.id}: {file.filename}")
    
    # Force commit to ensure all operations are persisted
    db.commit()
    
    # Add a small delay to ensure database transactions are complete
    import time
    time.sleep(1)
    
    # Generate account-specific reports with a new database session
    try:
        # Create a new database session to ensure all previous operations are committed
        new_db = deps.get_db_session()
        try:
            # Verify operations exist before processing
            operation_count = new_db.query(models.AccountingOperation).filter(
                models.AccountingOperation.import_uuid == import_uuid
            ).count()
            
            if operation_count == 0:
                print(f"[WARNING] No operations found for import {import_uuid} before batch processing")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"No operations found for import {import_uuid}"
                )
            else:
                print(f"[INFO] Found {operation_count} operations for import {import_uuid} before batch processing")
            
            processor = AccountingOperationProcessor(new_db)
            result = processor.process_import(import_uuid, audit_approach)
            
            if not result["success"]:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to process batch: {result.get('message', 'Unknown error')}"
                )
        finally:
            new_db.close()
        
        return {
            "import_uuid": import_uuid,
            "total_files": len(files),
            "files_processed": len(unprocessed_files),
            "total_operations": result["total_operations"],
            "debit_accounts_processed": result["debit_accounts_processed"],
            "credit_accounts_processed": result["credit_accounts_processed"],
            "debit_files": result["debit_files"],
            "credit_files": result["credit_files"]
        }
    except Exception as e:
        print(f"[ERROR] Exception during batch processing: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing batch: {str(e)}"
        )
        
        
@router.post("/upload/multiple",
            response_model=List[schemas.file.File],
            summary="Upload and process multiple files",
            description="Upload multiple Excel files to S3 storage, detect their template types, automatically process them and generate account-specific reports")
async def upload_multiple_files(
    *,
    db: Session = Depends(deps.get_db),
    files: List[UploadFile] = File(..., description="Multiple files to upload (all will share the same import_uuid)"),
    import_uuid: str = None, description="Optional. If provided, all files will be part of this import batch. If omitted, a new import_uuid will be generated for all these files.",
    audit_approach: str = Query("statistical",
                               description="Audit approach to use: 'full' (100% population), 'statistical' (80/20 rule), or 'selected' (selected objects)")
) -> Any:
    """
    Upload multiple Excel files for processing accounting operations.
    
    Completely automatic process for each file:
    1. The file is uploaded to S3 storage
    2. Template type is detected automatically
    3. Operations are extracted from the file
    4. Account-specific Excel files are generated for both debit and credit accounts
       - Based on the selected audit approach:
          - **Full (100%)**: Includes ALL operations regardless of count
          - **Statistical (80/20 rule)**:
             - If account has ≤30 operations: includes ALL operations
             - If account has >30 operations: includes operations that constitute 80% of total amount
               (sorted by largest transactions first)
          - **Selected Objects**: Custom selection logic for specific objects
    5. Account files are uploaded to S3 in exports/{import_uuid}/{DEBIT|CREDIT}/ directories
    
    All files uploaded in this request will share the same import_uuid.
    """
    # Generate a common import_uuid for all files if not provided
    if import_uuid is None:
        import_uuid = str(uuid.uuid4())
        
    print(f"[DEBUG] Using import_uuid: {import_uuid} for multiple file upload")
    
    processed_files = []
    
    for file in files:
        # Validate file extension
        if not file.filename.endswith(('.xls', '.xlsx')):
            print(f"[ERROR] Invalid file format: {file.filename}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid file format for {file.filename}. Only Excel files (.xls, .xlsx) are supported."
            )
        
        print(f"[DEBUG] Processing file upload: {file.filename}")
        
        # Read file content
        contents = await file.read()
        print(f"[DEBUG] File content read, size: {len(contents)} bytes")
        
        # Create a BytesIO object for in-memory file processing
        file_obj = io.BytesIO(contents)
        
        # Detect template type
        print(f"[DEBUG] Starting template detection for file: {file.filename}")
        template_detector = TemplateDetector()
        template_type = template_detector.detect_template_from_bytes(file_obj)
        
        print(f"[DEBUG] Template detection result: {template_type}")
        
        if not template_type:
            print(f"[ERROR] Could not recognize template format for file: {file.filename}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Could not recognize Excel template format for {file.filename}."
            )
        
        # Generate a unique S3 object key with import_uuid at the start of the filename
        # and organize files in uploaded_files/ directory
        s3_key = f"uploaded_files/{import_uuid}-{file.filename}"
        
        # Upload to S3
        s3_service = S3Service()
        success, message = s3_service.upload_file(contents, s3_key)
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to upload file {file.filename} to storage: {message}"
            )
        
        # Create file record in database
        file_processor = FileProcessor(db)
        
        db_file = file_processor.create_file(
            filename=file.filename,
            template_type=template_type.value,
            file_path=s3_key,  # Store S3 key instead of local path
            import_uuid=import_uuid
        )
        
        processed_files.append(db_file)
        
        # Process the file to extract operations
        try:
            print(f"[INFO] Processing file {db_file.id} to extract operations")
            operations = file_processor.process_file(db_file.id)
            
            # Double check operations from database to ensure they were saved
            db_operations = db.query(models.AccountingOperation).filter(
                models.AccountingOperation.file_id == db_file.id
            ).all()
            
            if db_operations and len(db_operations) > 0:
                print(f"[INFO] Successfully saved {len(db_operations)} operations from file {db_file.id} to database")
            else:
                print(f"[WARNING] No operations were saved to database from file {db_file.id}")
                
        except Exception as e:
            print(f"[WARNING] File processing failed: {str(e)}")
            import traceback
            traceback.print_exc()
            # Don't raise the exception - the file upload was still successful
    
    # Force commit to ensure operations are persisted
    db.commit()
    
    # Create a new database session to ensure all previous operations are committed
    new_db = deps.get_db_session()
    try:
        # Add a small delay to ensure database transactions are complete
        import time
        time.sleep(1)
        
        # Trigger account processing for this import with the new session
        print(f"[INFO] Triggering account processing for import {import_uuid}")
        processor = AccountingOperationProcessor(new_db)
        result = processor.process_import(import_uuid, audit_approach)
        
        if result["success"]:
            print(f"[INFO] Account processing successful. Generated {result['debit_accounts_processed']} debit files and {result['credit_accounts_processed']} credit files.")
        else:
            print(f"[WARNING] Account processing failed: {result['message']}")
    except Exception as e:
        print(f"[ERROR] Exception during account processing: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        new_db.close()
    
    return processed_files