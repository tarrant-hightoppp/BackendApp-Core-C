# Ajur Parser Documentation

## Overview
This document describes the integration of Ajur format Excel files into the accounting operations processing system. The Ajur parser extracts accounting operations from Excel files produced by Ajur accounting software and converts them into a standardized format for further processing and analysis.

> **IMPORTANT**: For auditing purposes, we specifically focus on the main account columns "Дт с/ка" and "Кт с/ка" (not the analytical accounts). These main accounts are the primary subject of audit operations.

## Ajur Format Specification

### File Structure
Ajur Excel files typically contain accounting operations with the following column structure:

- **Потр.** - Operator/User identifier
- **Опер. No** - Operation number
- **Дата рег.** - Registration date
- **Вид док.** - Document type
- **Документ No / дата** - Document number and date
- **Рег. No** - Registration number
- **Дт с/ка** - Debit account
- **Аналитична сметка** - Analytical account for debit
- **Количество 1 по Дт** - Debit quantity 1
- **Мярка 1 по Дт** - Debit unit 1
- **Количество 2 по Дт** - Debit quantity 2
- **Мярка 2 по Дт** - Debit unit 2
- **Вал. кол. по Дт** - Debit currency amount
- **Вид валута по Дт** - Debit currency type
- **No доставка по Дт** - Debit delivery number
- **Кт с/ка** - Credit account
- **Аналитична сметка** - Analytical account for credit
- **Количество 1 по Кт** - Credit quantity 1
- **Мярка 1 по Кт** - Credit unit 1
- **Количество 2 по Кт** - Credit quantity 2
- **Мярка 2 по Кт** - Credit unit 2
- **Вал. кол. по Кт** - Credit currency amount
- **Вид валута по Кт** - Credit currency type
- **No доставка по Кт** - Credit delivery number
- **Сума** - Amount
- **Обяснителен текст** - Description/explanation
- **Обяснителен текст на друг език** - Description in another language
- Additional system fields (Код1, Код2, etc.)

### Account Format
Accounts in Ajur format typically use a numbering system with slashes, such as:
- `401/1` - Main account 401, subaccount 1
- `602` - Main account 602
- `611` - Main account 611

### Main Accounts vs. Analytical Accounts

#### Main Accounts (Primary Audit Focus)
The main accounts are found in the columns:
- **Дт с/ка** - Main debit account column (column 6 in sample file)
- **Кт с/ка** - Main credit account column (column 15 in sample file)

These main accounts are the **primary focus for auditing** and are stored in the `debit_account` and `credit_account` fields in the database.

#### Analytical Accounts (Secondary Information)
Analytical accounts in Ajur format provide additional classification details. They are often formatted as semicolon-separated values, for example:
```
1;Общи разходи;13;Куриерски услуги
```
This would represent a hierarchical classification for the account. While these are stored, they are **not the primary subject of auditing**.

## Parser Implementation

### Audit Focus
The parser is specifically designed to extract and focus on the main accounts ("Дт с/ка" and "Кт с/ка") for auditing purposes. The analytical accounts are stored for reference but are not the primary audit focus.

### Template Detection
The system uses a template detection mechanism to identify Ajur format files based on:
1. Presence of distinctive column headers like "Потр.", "Опер. No", "Дата рег.", etc.
2. Characteristic structure of data, especially the account formats with slashes
3. Special keywords in headers or early rows

### Column Mapping
The parser dynamically detects column positions based on header names and content patterns. If automatic detection fails, default column positions are used based on the sample file structure:

#### Primary Audit Columns (Main Focus)
- **Debit account** - Column 6 (`Дт с/ка`) - **MAIN ACCOUNT FOR AUDIT**
- **Credit account** - Column 15 (`Кт с/ка`) - **MAIN ACCOUNT FOR AUDIT**
- **Amount** - Column 24 (`Сума`)

#### Secondary Columns
- **Date** - Column 2 (`Дата рег.`)
- **Analytical debit** - Column 7 (not primary audit focus)
- **Analytical credit** - Column 16 (not primary audit focus)
- **Document type** - Column 3 (`Вид док.`)
- **Document number** - Column 4 (`Документ No / дата`)
- **Description** - Column 25 (`Обяснителен текст`)

### Data Extraction Process
1. Load the Excel file
2. Identify column positions using headers or default mapping
3. Determine the row where actual data starts
4. Process each row to extract accounting operation details
5. Handle special cases for account numbers, analytical accounts, and amounts
6. Store operations in a standardized format for database storage

### Credit Account Handling
Special care is taken when handling credit accounts to ensure proper extraction:

1. Direct extraction from the detected credit account column
2. If the value is a string, it's cleaned and used directly
3. If the value is numeric, it's converted to a string
4. If the value is missing, alternative columns are searched for account-like patterns
5. Final fallbacks include searching for strings with account-like format (containing "/")

## Integration with Existing System

### Workflow
1. User uploads an Excel file
2. Template detector identifies it as an Ajur file
3. Ajur parser extracts operations
4. Operations are stored in the database
5. The system matches accounts according to defined rules
6. Reports are generated based on the processed data

### Database Storage
Extracted operations are stored in the `accounting_operation` table with the template_type set to "ajur".

## Ajur Audit Processor

### Purpose
The `AjurAuditProcessor` is specifically designed to process Ajur accounting operations and generate audit reports based on the main account columns "Дт с/ка" and "Кт с/ка". This specialized processor focuses on auditing individual account operations rather than the traditional main account grouping.

### Key Features

#### Account Grouping
- **Debit Account Grouping**: Operations are grouped by their full debit account number (from the "Дт с/ка" column)
- **Credit Account Grouping**: Operations are grouped by their full credit account number (from the "Кт с/ка" column)
- **Special Handling for Analytical Formats**: For accounts in the format "1;Общи разходи;13;Куриерски услуги", the processor extracts just the numeric account identifier (e.g., "1")

#### Report Generation
For each unique account (from both debit and credit columns), the processor generates a separate Excel file containing:

1. All operations related to that specific account
2. Properly formatted columns including:
   - Document type
   - Document number
   - Date
   - Debit and credit accounts
   - Analytical accounts
   - Amount
   - Description
   - Audit fields (Verified amount, Deviation, Control action)
3. Subtotals and summary information
4. Wrapped in the standard audit template format

#### Audit Approaches
The processor supports the following audit approaches:

- **Full Audit** (`"full"`): Includes 100% of operations for each account
- **Statistical Audit** (`"statistical"`): Uses the 80/20 rule to focus on the most significant transactions (by amount) that make up 80% of the total value
- **Selected Audit** (`"selected"`): For checking specific selected objects in the population

#### File Naming
Files are named using a consistent pattern:
```
{import_uuid}-{DEBIT|CREDIT}-{account}__{timestamp}.xlsx
```

Examples:
- `2f3f7c0f-42b3-45e8-af74-790cebdccfac-DEBIT-602__20251016022629.xlsx`
- `2f3f7c0f-42b3-45e8-af74-790cebdccfac-CREDIT-401/1__20251016022629.xlsx`

Files are stored in the appropriate S3 directory:
- Debit account files: `{import_uuid}/sorted_by_debit/`
- Credit account files: `{import_uuid}/sorted_by_credit/`

### Usage
To process a specific import with the Ajur Audit Processor:

```python
from app.services.ajur_audit_processor import AjurAuditProcessor

# Create the processor with a database session
processor = AjurAuditProcessor(db_session)

# Process an import with the full audit approach
result = processor.process_audit(import_uuid, audit_approach="full")

# The result contains information about the processed accounts and generated files
print(f"Processed {result['debit_accounts_processed']} debit accounts")
print(f"Processed {result['credit_accounts_processed']} credit accounts")
```

## Testing and Validation
The implementation includes dedicated tests in:
1. `tests/test_ajur_parser.py` - For testing the basic Ajur parser
2. `tests/test_ajur_report_generation.py` - For testing report generation
3. `tests/test_ajur_workflow.py` - For testing the complete workflow
4. `tests/test_ajur_audit_processor.py` - For testing the specific audit processor

These tests verify:
1. Correct template detection
2. Proper extraction of operations from file
3. Memory-based parsing for API uploads
4. Data quality and completeness checks
5. Account format validation
6. Account grouping functionality
7. Report generation for individual accounts

## Troubleshooting

### Common Issues
- **Missing Column Detection**: If column detection fails, check if the Excel file structure matches the expected format.
- **Empty Operations List**: Verify that the data rows start after the header row and contain valid values.
- **Account Format Problems**: Ensure account numbers follow the expected format with slashes.
- **Missing Credit Accounts**: Credit accounts should be in column 15 (`Кт с/ка`). If missing, check for merged cells or alternative formats.
- **Confusion Between Main and Analytical Accounts**: Make sure the parser is correctly identifying the main account columns (`Дт с/ка` and `Кт с/ка`) and not using analytical account columns instead.

### Debugging
The parser includes extensive debug logging prefixed with `[DEBUG] AJUR parser`. These logs can help identify issues with specific rows or columns.

## References
- Sample file: `files/ajur.xlsx`
- Parser implementation: `app/services/parsers/ajur_parser.py`
- Audit processor: `app/services/ajur_audit_processor.py`
- Template detection: `app/services/template_detector.py`
- Tests: `tests/test_ajur_parser.py`, `tests/test_ajur_audit_processor.py`
