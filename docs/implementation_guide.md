# Implementation Guide

This document provides step-by-step instructions for implementing the account processing functionality.

## 1. Update File Upload Process

First, we need to modify the file upload process to properly track import_uuid.

### Update `app/api/routes/files.py`

```python
@router.post("/upload", 
           response_model=schemas.file.File,
           summary="Upload a new file",
           description="Upload an Excel file to S3 storage and detect its template type")
async def upload_file(
    *,
    db: Session = Depends(deps.get_db),
    file: UploadFile = File(...),
    import_uuid: str = None  # Optional parameter for client to provide import_uuid
) -> Any:
    """
    Upload an Excel file for processing accounting operations.
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
    
    # Generate a unique S3 object key
    s3_key = f"{uuid.uuid4()}-{file.filename}"
    
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
    if not import_uuid:
        import_uuid = str(uuid.uuid4())
    
    db_file = file_processor.create_file(
        filename=file.filename,
        template_type=template_type.value,
        file_path=s3_key,  # Store S3 key instead of local path
        import_uuid=import_uuid  # Add import_uuid parameter
    )
    
    return db_file
```

### Update `app/services/file_processor.py`

```python
def create_file(self, filename: str, template_type: str, file_path: str, import_uuid: str) -> UploadedFile:
    """
    Create a record for an uploaded file
    
    Args:
        filename: Original filename
        template_type: Detected template type
        file_path: Path where the file is stored
        import_uuid: UUID for grouping files in an import
        
    Returns:
        Created UploadedFile record
    """
    db_file = UploadedFile(
        filename=filename,
        template_type=template_type,
        file_path=file_path,
        processed=False,
        import_uuid=import_uuid
    )
    
    self.db.add(db_file)
    self.db.commit()
    self.db.refresh(db_file)
    return db_file
```

### Update `app/schemas/file.py`

Add import_uuid to the schema:

```python
# Properties shared by models stored in DB
class FileInDBBase(FileBase):
    id: int
    upload_date: datetime
    processed: bool
    file_path: str
    import_uuid: str
    
    class Config:
        orm_mode = True
```

## 2. Create AccountingOperationProcessor Service

Create a new file `app/services/accounting_operation_processor.py` with the implementation described in the design document.

## 3. Add API Endpoint

Add a new endpoint in `app/api/routes/operations.py`:

```python
@router.post("/process-import/{import_uuid}", response_model=Dict[str, Any],
           summary="Process import operations",
           description="Process all operations for a specific import and generate account-specific files")
def process_import(
    *,
    db: Session = Depends(deps.get_db),
    import_uuid: str
) -> Any:
    """
    Process all operations for a specific import and generate account-specific files.
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
    
    # Process the import
    processor = AccountingOperationProcessor(db)
    result = processor.process_import(import_uuid)
    
    if not result["success"]:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=result["message"]
        )
    
    return result
```

## 4. Implementation Notes

### Performance Considerations

- When dealing with large datasets, consider processing the operations in batches to avoid memory issues.
- Use database indexes on `import_uuid` and `file_id` fields to optimize queries.
- Consider adding a progress tracking mechanism for long-running imports.

### Error Handling

Implement robust error handling:

1. Handle S3 upload/download failures
2. Handle Excel file generation errors
3. Handle database query errors
4. Implement proper logging for all operations

### Deployment Considerations

1. Ensure S3 bucket permissions are properly configured
2. Update database schema if necessary (using Alembic migrations)
3. Increase API timeout limits for processing large imports

## 5. Implementation Sequence

For smooth implementation, follow this sequence:

1. Update the database model and schemas
2. Update the file upload process
3. Create the AccountingOperationProcessor service
4. Add the API endpoint
5. Add tests
6. Deploy and test in a staging environment

## 6. Monitoring

Add monitoring for the new functionality:

1. Log processing times
2. Track number of files generated
3. Monitor S3 storage usage
4. Set up alerts for processing failures