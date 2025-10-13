# Account Matching Documentation

This document explains how to match debit and credit accounts directly in your code using the `AccountMatcher` class.

## Overview

The `AccountMatcher` service provides functionality to match debit and credit accounts in accounting operations. This is particularly useful when:

- You have credit operations missing their corresponding debit accounts
- You have debit operations missing their corresponding credit accounts
- You need to cross-match between two sets of operations from different sources

## Direct Usage in Code

The `AccountMatcher` class now provides three direct utility methods for easy matching:

### 1. Match Credit Operations with Missing Debit Accounts

```python
from app.services.account_matcher import AccountMatcher

# Initialize the matcher
matcher = AccountMatcher()

# Match credit operations with missing debit accounts
enriched_operations = matcher.match_credit_with_debit(credit_operations)

# Now enriched_operations will have debit accounts filled where matches were found
```

### 2. Match Debit Operations with Missing Credit Accounts

```python
from app.services.account_matcher import AccountMatcher

# Initialize the matcher
matcher = AccountMatcher()

# Match debit operations with missing credit accounts
enriched_operations = matcher.match_debit_with_credit(debit_operations)

# Now enriched_operations will have credit accounts filled where matches were found
```

### 3. Cross-Match Between Two Sets of Operations

```python
from app.services.account_matcher import AccountMatcher

# Initialize the matcher
matcher = AccountMatcher()

# Cross-match between the two sets of operations
enriched_debit, enriched_credit = matcher.cross_match_accounts(
    debit_operations,
    credit_operations
)

# Now both sets will have their missing accounts filled where matches were found
```

## Matching Criteria

The matcher uses the following criteria to identify corresponding operations:

- **Document number**: Operations must have the same document number
- **Date**: Operations must have the same date (normalized to handle both datetime and date objects)
- **Amount**: Operations must have matching amounts (with a tolerance of 0.01 to account for rounding errors)

## Complete Example

We've included a demonstration script that shows how to use these methods in different scenarios:

```bash
python scripts/direct_account_matching.py
```

The script includes examples of:
- Matching credit operations with missing debit accounts
- Matching debit operations with missing credit accounts
- Cross-matching between two sets of operations
- Processing operations from real Excel files

## Integration with RivalParser

The `RivalParser` already integrates account matching during parsing:

```python
from app.services.parsers.rival_parser import RivalParser

# Initialize the parser
parser = RivalParser()

# Parse operations from file (matching will be done automatically)
operations = parser.parse(
    file_path="path/to/rival_file.xlsx",
    file_id=1
)
```

## Advanced Usage with Database

For applications that need to match against database records:

```python
from app.services.account_matcher import AccountMatcher

# Initialize the matcher
matcher = AccountMatcher()

# Match using database records
enriched_operations = matcher.match_operations_from_db(
    operations_to_enrich,
    db_session  # Your SQLAlchemy session
)
```

This will attempt to find matching operations in the database and use them to fill missing account information.