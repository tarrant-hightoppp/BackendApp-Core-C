# Test Cases for Account Processing Functionality

This document provides detailed test cases for the new account processing functionality.

## Unit Tests

### 1. File Upload with import_uuid Tests

#### Test 1.1: Upload Single File

**Description:** Verify that a single file upload generates a valid import_uuid.

**Steps:**
1. Upload a single Excel file
2. Verify that the response contains a valid import_uuid
3. Check the database to ensure the file record has the correct import_uuid

**Expected Result:** File is uploaded with a valid import_uuid.

#### Test 1.2: Upload Multiple Files with Same import_uuid

**Description:** Verify that multiple files can be uploaded with the same import_uuid.

**Steps:**
1. Generate a new import_uuid
2. Upload first file with the generated import_uuid
3. Upload second file with the same import_uuid
4. Check the database to ensure both files have the same import_uuid

**Expected Result:** Both files are associated with the same import_uuid.

### 2. AccountingOperationProcessor Tests

#### Test 2.1: Group Operations by Account

**Description:** Verify that operations are correctly grouped by account.

**Steps:**
1. Create a list of test operations with different debit accounts
2. Call the `_group_by_account` method
3. Verify that operations are correctly grouped by account number

**Expected Result:** Operations are correctly grouped by account.

#### Test 2.2: Filter Operations (≤ 30 Operations)

**Description:** Verify that all operations are included when count is ≤ 30.

**Steps:**
1. Create a list of 30 test operations
2. Call the `_filter_operations` method
3. Verify that all 30 operations are included in the result

**Expected Result:** All operations are included.

#### Test 2.3: Filter Operations (> 30 Operations)

**Description:** Verify that operations making up 80% of the total amount are included when count is > 30.

**Steps:**
1. Create a list of 50 test operations with varying amounts
2. Call the `_filter_operations` method
3. Verify that the filtered operations make up at least 80% of the total amount
4. Verify that operations are sorted by amount (descending)

**Expected Result:** Operations making up 80% of the total amount are included.

#### Test 2.4: Generate and Upload File

**Description:** Verify that Excel files are correctly generated and uploaded to S3.

**Steps:**
1. Create a list of test operations
2. Mock the S3Service
3. Call the `_generate_and_upload_file` method
4. Verify that the S3Service was called with the correct parameters
5. Verify that the method returns the correct S3 key

**Expected Result:** Excel file is correctly generated and uploaded to S3.

#### Test 2.5: Process Import

**Description:** Verify that the `process_import` method correctly processes all operations for an import.

**Steps:**
1. Create test files and operations with a known import_uuid
2. Mock the S3Service
3. Call the `process_import` method
4. Verify that the correct number of files are generated
5. Verify that the method returns the correct statistics

**Expected Result:** All operations are processed and files are generated.

### 3. API Endpoint Tests

#### Test 3.1: Process Import Endpoint (Valid import_uuid)

**Description:** Verify that the process-import endpoint correctly processes an import with a valid import_uuid.

**Steps:**
1. Create test files and operations with a known import_uuid
2. Mock the S3Service
3. Call the process-import endpoint
4. Verify that the response contains the correct statistics

**Expected Result:** Import is processed and correct statistics are returned.

#### Test 3.2: Process Import Endpoint (Invalid import_uuid)

**Description:** Verify that the process-import endpoint correctly handles an invalid import_uuid.

**Steps:**
1. Call the process-import endpoint with a non-existent import_uuid
2. Verify that the response contains an appropriate error message

**Expected Result:** Error message is returned.

#### Test 3.3: Process Import Endpoint (No Operations)

**Description:** Verify that the process-import endpoint correctly handles an import with no operations.

**Steps:**
1. Create a test file with a known import_uuid but no operations
2. Call the process-import endpoint
3. Verify that the response indicates no operations were processed

**Expected Result:** Response indicates no operations were processed.

## Integration Tests

### 4. End-to-End Tests

#### Test 4.1: Full Processing Flow

**Description:** Verify the full processing flow from file upload to account-specific file generation.

**Steps:**
1. Upload a test Excel file
2. Get the import_uuid from the response
3. Call the process-file endpoint to extract operations
4. Call the process-import endpoint to generate account-specific files
5. Verify that the correct number of files are generated
6. Download and verify the content of the generated files

**Expected Result:** File is uploaded, operations are extracted, and account-specific files are generated.

#### Test 4.2: Multiple Files in Single Import

**Description:** Verify processing of multiple files in a single import.

**Steps:**
1. Generate a new import_uuid
2. Upload multiple test Excel files with the same import_uuid
3. Call the process-file endpoint for each file
4. Call the process-import endpoint
5. Verify that operations from all files are processed
6. Verify that the correct number of files are generated

**Expected Result:** All files in the import are processed and account-specific files are generated.

## Performance Tests

### 5. Performance Testing

#### Test 5.1: Large Number of Operations

**Description:** Verify that the system can handle a large number of operations.

**Steps:**
1. Create a test Excel file with 10,000+ operations
2. Upload the file and process it
3. Call the process-import endpoint
4. Measure the processing time
5. Verify that all operations are processed

**Expected Result:** System handles large number of operations within acceptable time.

#### Test 5.2: Many Different Accounts

**Description:** Verify that the system can handle operations spread across many different accounts.

**Steps:**
1. Create a test Excel file with operations spread across 100+ different accounts
2. Upload the file and process it
3. Call the process-import endpoint
4. Verify that the correct number of account-specific files are generated

**Expected Result:** System correctly generates files for all accounts.

## Test Data Preparation

### Sample Excel Files

For testing, we need to prepare sample Excel files that match the expected formats:

1. **Rival Format Sample** - Excel file in Rival format with multiple operations
2. **AJUR Format Sample** - Excel file in AJUR format with multiple operations
3. **Large Sample** - Excel file with 10,000+ operations
4. **Multi-Account Sample** - Excel file with operations spread across many different accounts

### Database Fixtures

We also need to prepare database fixtures for testing:

1. **Single Import Fixture** - Single import with one file and operations
2. **Multi-File Import Fixture** - Single import with multiple files and operations
3. **Empty Import Fixture** - Import with no operations

## Test Implementation Guidelines

1. Use pytest for all tests
2. Mock external dependencies (S3Service, etc.)
3. Use fixtures for common test data
4. Ensure all tests are isolated and don't depend on each other
5. Include cleanup code to remove test data after tests

## CI/CD Integration

These tests should be integrated into the CI/CD pipeline to run automatically on:

1. Pull requests
2. Merges to main branch
3. Release deployments

This ensures that the functionality remains working as the codebase evolves.