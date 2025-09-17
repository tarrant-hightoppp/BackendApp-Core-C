# Testing Strategy for Account Processing Functionality

This document outlines the testing strategy for the new accounting operation processing functionality.

## Test Areas

### 1. File Upload with import_uuid Tracking

- Test that each new import gets a unique import_uuid
- Test that multiple files uploaded in a single import share the same import_uuid
- Test that the import_uuid is properly stored in the database

### 2. AccountingOperationProcessor Service

#### Unit Tests

- Test `_group_by_account` method to ensure operations are correctly grouped by account
- Test `_filter_operations` method to verify:
  - All operations are included when count ≤ 30
  - Only operations making up 80% of the total amount are included when count > 30
- Test `_generate_and_upload_file` method to verify Excel files are correctly generated
- Test error handling in all methods

#### Integration Tests

- Test `process_import` method with a known set of operations
- Verify that the correct number of files are generated
- Verify that files are correctly uploaded to S3

### 3. API Endpoint

- Test the process-import endpoint with valid import_uuid
- Test handling of non-existent import_uuid
- Test processing an import with no operations

## Test Data

We should create test fixtures with:

1. Sample imports with varying numbers of files
2. Operations with different account distributions:
   - Some accounts with < 30 operations
   - Some accounts with > 30 operations
   - Mix of debit and credit accounts

## Mock Strategy

- Mock S3Service for file upload tests to avoid actual S3 interactions
- Create a test database with predefined data for integration tests

## Example Test Cases

### Test File Upload with import_uuid

```python
def test_file_upload_with_import_uuid():
    # Setup test client and test files
    client = TestClient(app)
    file1 = ("test_file1.xlsx", b"file content 1")
    file2 = ("test_file2.xlsx", b"file content 2")
    
    # Upload first file
    response1 = client.post("/api/files/upload", files={"file": file1})
    assert response1.status_code == 200
    import_uuid1 = response1.json()["import_uuid"]
    
    # Upload second file in a separate import
    response2 = client.post("/api/files/upload", files={"file": file2})
    assert response2.status_code == 200
    import_uuid2 = response2.json()["import_uuid"]
    
    # Verify different import_uuid for separate uploads
    assert import_uuid1 != import_uuid2
```

### Test AccountingOperationProcessor

```python
def test_filter_operations():
    # Create a processor instance
    processor = AccountingOperationProcessor(db_session)
    
    # Test case 1: <= 30 operations (all should be included)
    operations_small = [create_test_operation(amount=100) for _ in range(20)]
    filtered_small = processor._filter_operations(operations_small)
    assert len(filtered_small) == 20
    
    # Test case 2: > 30 operations (should include operations up to 80% of total)
    operations_large = [create_test_operation(amount=10) for _ in range(50)]
    # Add some large operations
    operations_large.extend([create_test_operation(amount=1000) for _ in range(5)])
    
    filtered_large = processor._filter_operations(operations_large)
    
    # Verify that we get fewer than the original operations
    assert len(filtered_large) < len(operations_large)
    
    # Verify that the total amount is at least 80% of the original
    original_total = sum(op.amount for op in operations_large)
    filtered_total = sum(op.amount for op in filtered_large)
    assert filtered_total >= 0.8 * original_total
```

### Test API Endpoint

```python
def test_process_import_endpoint():
    # Setup test client and database with test data
    client = TestClient(app)
    import_uuid = create_test_import_with_operations()
    
    # Call the process-import endpoint
    response = client.post(f"/api/operations/process-import/{import_uuid}")
    assert response.status_code == 200
    
    result = response.json()
    assert result["success"] is True
    assert "debit_accounts_processed" in result
    assert "credit_accounts_processed" in result
```

## Performance Testing

Since this functionality may process large datasets, we should also include performance tests:

1. Test with a large number of operations (10,000+)
2. Test with many different accounts (100+)
3. Measure processing time and memory usage
4. Verify that the system can handle the expected load

## Continuous Integration

These tests should be integrated into the CI/CD pipeline to ensure that any changes to the codebase don't break the functionality.