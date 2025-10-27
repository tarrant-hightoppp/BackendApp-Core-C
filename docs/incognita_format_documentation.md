# Incognita Format Documentation

This document provides comprehensive information about the Incognita accounting format, how it's detected, parsed, and processed within the auditing system.

## Overview

The Incognita format is an accounting software export format used by some accounting systems. It has a specific structure that differs from other supported formats like RIVAL, AJUR, and MICROINVEST.

## File Structure

Typical characteristics of Incognita files:

- **Headers**: Located in row 3 (1-indexed) of the Excel file
- **Data Start**: Data typically begins from row 4
- **Key Columns**: 
  - "A/A" - Sequence number
  - "ДТ Сметка" - Debit account
  - "Дт Сметка описание" - Debit account description/analytical info
  - "КТ Сметка" - Credit account
  - "Кт Сметка описание" - Credit account description/analytical info
  - "Дата" - Operation date
  - "Ст-Ст в лева" - Amount in local currency
  - "Док. Номер" - Document number
  - "Предмет на доставка" - Description/purpose of operation
  - "Контрагент" - Partner/counterparty

## Detection Algorithm

The system detects Incognita files through the `_check_incognita_pattern` method in the `TemplateDetector` class. The detection is based on:

1. Checking the first row's headers for Incognita-specific column names
2. If not found in the first row, examining row 3 (index 2) where Incognita typically places headers
3. Looking for distinctive patterns in the file content such as:
   - Presence of "ДТ Сметка" and "КТ Сметка" columns
   - The "A/A" sequence number column
   - "Предмет на доставка" description column

The detection requires at least 3 matches from the expected keywords to classify a file as Incognita format.

## Column Mapping

During parsing, the following mapping is used to extract accounting operations:

| Incognita Column        | System Field          | Description                         |
|-------------------------|-----------------------|-------------------------------------|
| A/A                     | sequence_number       | Operation sequence number           |
| ДТ Сметка               | debit_account         | Debit account number                |
| Дт Сметка описание      | analytical_debit      | Analytical info for debit account   |
| КТ Сметка               | credit_account        | Credit account number               |
| Кт Сметка описание      | analytical_credit     | Analytical info for credit account  |
| Дата                    | operation_date        | Date of the operation               |
| Ст-Ст в лева            | amount                | Operation amount in local currency  |
| Док. Номер              | document_number       | Reference document number           |
| Предмет на доставка     | description           | Operation description/purpose       |
| Контрагент              | partner_name          | Partner/counterparty name           |

## Parsing Process

The `IncognitaParser` class handles parsing Incognita files:

1. The file is read with `pd.read_excel()`, skipping the first 2 rows to start from the header row
2. Column detection is performed to identify the location of key fields
3. Any additional header rows are skipped based on content analysis
4. Each data row is converted to an `AccountingOperation` object
5. Data cleaning and validation is performed, including:
   - Date format standardization
   - Amount parsing with support for different number formats
   - Analytical information extraction
   - Sequence number extraction

## Integration with Processing Workflow

The `FileProcessor` integrates the Incognita parser into the overall workflow:

1. The file is detected as Incognita format using the `TemplateDetector`
2. The appropriate `IncognitaParser` is selected from the parsers dictionary
3. Operations are extracted and saved to the database
4. The accounting operation processor groups these operations by account for auditing
5. Standardized audit reports are generated following the same process as other formats

## Testing and Verification

A dedicated test file (`tests/test_incognita_integration.py`) provides:

1. Unit tests for the Incognita detection algorithm
2. Unit tests for the Incognita parser
3. End-to-end workflow tests
4. A helper method for manual verification of sample files

To test with a specific Incognita file, use the `verify_incognita_sample_file` function:

```python
from tests.test_incognita_integration import verify_incognita_sample_file

results = verify_incognita_sample_file("path/to/sample_incognita_file.xlsx")
```

## Troubleshooting

Common issues with Incognita files:

1. **Header Detection Failure**: If headers are not in the expected row 3
   - Solution: Examine file structure and adjust the skiprows parameter in the parser

2. **Column Mapping Issues**: If columns are not being correctly identified
   - Solution: Enable debug logging and check the output of `_detect_columns` method
   - Adjust the expected headers list in the parser if needed

3. **Date Format Problems**: If operation dates are not being correctly parsed
   - Solution: Check the date format in the file and ensure it's supported by the parser
   - Add additional date format patterns to the `convert_to_date` method if needed

4. **Amount Parsing Errors**: If amounts are not correctly extracted
   - Solution: Examine the number format in the file (decimal separator, thousands separator)
   - Update the amount extraction logic in the parser if needed

## Sample File

A sample Incognita file structure might look like:

```
Row 1: [Company information or report title]
Row 2: [Additional metadata]
Row 3: [A/A] [ДТ Сметка] [Дт Сметка описание] [КТ Сметка] [Кт Сметка описание] [Дата] [Ст-Ст в лева] [Док. Номер] [Предмет на доставка] [Контрагент]
Row 4: [1] [411] [Клиенти] [701] [Приходи от продажби] [01.01.2023] [1200.00] [0000123] [Продажба на услуги] [ООД Пример]
...
```

## Implementation Details

The Incognita support is implemented across several files:

1. `template_detector.py` - Added Incognita to the enum and detection method
2. `parsers/incognita_parser.py` - Created parser class for the format
3. `file_processor.py` - Added parser to the processor list
4. `tests/test_incognita_integration.py` - Added tests for the integration