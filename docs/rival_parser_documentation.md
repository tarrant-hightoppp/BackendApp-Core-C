# Rival Parser Algorithm Documentation

## Overview

This document provides detailed technical documentation for the Rival Excel file parser's data processing algorithm. The Rival accounting format presents unique challenges that require specialized processing to reconstruct complete accounting operations from their fragmented representation in the source file.

## Rival Format Structure

The Rival Excel format has a distinctive structure:

- **Header Section**: Rows 1-6 contain company information:
  - Row 1: Company name (e.g., "ФОРСТА ЕООД")
  - Row 2: Company address
  - Row 3: "ХРОНОЛОГИЧЕН ОПИС НА ПАПКА" (Chronological list of folder)
  - Row 4: Period information
  - Row 5: User information
- **Column Headers**: Rows 8-9 contain merged cells forming the data table headers
- **Data Rows**: Begin at row 10 with a unique accounting format where:
  - Each accounting entry is split across multiple rows
  - Debit entries appear in separate rows from credit entries 
  - Rows are implicitly related by document number, date, and transaction group

## Data Processing Challenge

Unlike standard accounting Excel formats where each row represents a complete debit-credit pair, Rival format presents:

1. **Fragmented Operations**: Accounting operations are split into separate debit and credit rows
2. **Implicit Relationships**: No explicit linkage between related debit and credit entries
3. **Complex Matching**: Potential for one-to-many and many-to-one relationships (one debit to multiple credits or vice versa)

## Reconstruction Algorithm

The core of the Rival parser is the `_group_related_operations` method, which implements a three-pass algorithm to reconstruct complete accounting operations.

### 1. Initial Grouping

Rows are first grouped by a composite key of:
- Document number (`doc_number`)
- Document date (`doc_date`)
- Transaction group/статия (`transaction_group`)

```python
key = (doc_number, doc_date, transaction_group)
if key not in grouped_rows:
    grouped_rows[key] = []
grouped_rows[key].append(row)
```

This groups together rows that likely belong to the same accounting transaction.

### 2. Separating Debit and Credit Entries

Within each group, rows are separated into debit and credit entries:

```python
debit_entries = [r for r in group_rows if not pd.isna(r.iloc[12])]  # Has debit account
credit_entries = [r for r in group_rows if not pd.isna(r.iloc[13])]  # Has credit account
```

### 3. Three-Pass Matching Algorithm

The algorithm then uses a sophisticated three-pass approach to match entries:

#### Pass 1: Simple One-to-One Matching

This handles the simplest case: a single debit matched with a single credit with equal amounts.

```python
if len(debit_entries) == 1 and len(credit_entries) == 1:
    debit = debit_entries[0]
    credit = credit_entries[0]
    
    debit_amount = self.clean_numeric(debit.iloc[14])
    credit_amount = self.clean_numeric(credit.iloc[14])
    
    if debit_amount and credit_amount and abs(debit_amount - credit_amount) < 0.01:
        # Create operation with both accounts
        operation = {
            # operation fields...
        }
        operations.append(operation)
```

#### Pass 2: One Debit to Multiple Credits

If the simple case fails, the algorithm tries to match a single debit with multiple credits:

1. For each remaining debit entry, calculate its amount
2. Sort remaining credit entries by amount (largest first)
3. Add credits to a matching set until their sum equals the debit amount (within a small tolerance)
4. When a match is found, create a composite operation and remove matched entries from remaining sets

```python
for debit in remaining_debits.copy():
    debit_amount = self.clean_numeric(debit.iloc[14])
    
    matching_credits = []
    credits_total = 0
    
    for credit in sorted(remaining_credits, key=lambda x: self.clean_numeric(x.iloc[14]) or 0, reverse=True):
        credit_amount = self.clean_numeric(credit.iloc[14])
        
        if credits_total + credit_amount <= debit_amount + 0.01:
            matching_credits.append(credit)
            credits_total += credit_amount
            
            if abs(credits_total - debit_amount) < 0.01:
                # Found a match - create a combined operation
                credit_accounts = [self.clean_string(c.iloc[13]) for c in matching_credits]
                credit_account = " + ".join(credit_accounts)
                
                operation = {
                    # operation fields with combined credit accounts
                }
                operations.append(operation)
                
                # Remove matched entries from remaining sets
                # ...
                break
```

#### Pass 3: Multiple Debits to One Credit

The third pass handles the reverse case:

1. For each remaining credit entry, calculate its amount
2. Sort remaining debit entries by amount (largest first)
3. Add debits to a matching set until their sum equals the credit amount (within a small tolerance)
4. When a match is found, create a composite operation and remove matched entries from remaining sets

```python
for credit in remaining_credits.copy():
    credit_amount = self.clean_numeric(credit.iloc[14])
    
    matching_debits = []
    debits_total = 0
    
    for debit in sorted(remaining_debits, key=lambda x: self.clean_numeric(x.iloc[14]) or 0, reverse=True):
        debit_amount = self.clean_numeric(debit.iloc[14])
        
        if debits_total + debit_amount <= credit_amount + 0.01:
            matching_debits.append(debit)
            debits_total += debit_amount
            
            if abs(debits_total - credit_amount) < 0.01:
                # Found a match - create a combined operation
                debit_accounts = [self.clean_string(d.iloc[12]) for d in matching_debits]
                debit_account = " + ".join(debit_accounts)
                
                operation = {
                    # operation fields with combined debit accounts
                }
                operations.append(operation)
                
                # Remove matched entries from remaining sets
                # ...
                break
```

## Fallback Processing

If the grouping algorithm fails to produce any operations (indicating a non-standard format or other issues), the parser falls back to a simpler approach:

```python
if not operations:
    print("[WARNING] Grouping related operations didn't produce any results, falling back to individual row processing")
    operations = []
    
    # Process each row individually (legacy approach)
    for _, row in df.iterrows():
        # Skip rows that don't have amount or both debit and credit accounts
        if pd.isna(row.iloc[14]) or (pd.isna(row.iloc[12]) and pd.isna(row.iloc[13])):
            continue
        
        # Create operation from individual row
        # ...
```

This ensures that at least some data can be extracted even if the complex matching process fails.

## Data Output Structure

Each reconstructed operation contains:

1. Standard operation fields (`file_id`, `operation_date`, etc.)
2. Account information (`debit_account`, `credit_account`)
3. Amount and description
4. Raw data including:
   - For simple matches: both debit and credit entries
   - For complex matches: all matched entries (multiple debits or credits)
   - Company information from the header

The `template_type` is set to "RIVAL" to identify the source format.

## Handling Edge Cases

The algorithm includes several mechanisms for handling edge cases:

1. **Floating Point Tolerance**: Uses a small tolerance (0.01) when comparing amounts to account for potential rounding differences
2. **Proper Cleanup**: Ensures matched entries are removed from consideration to prevent double-counting
3. **Fallback Processing**: If the sophisticated grouping fails, falls back to simpler row-by-row processing
4. **Null Value Handling**: Skips rows with missing critical data (like amount or both account fields)

## Conclusion

The Rival parser's reconstruction algorithm demonstrates a sophisticated approach to handling accounting data that doesn't follow the conventional one-row-per-transaction format. The three-pass matching system effectively rebuilds the logical relationships between debits and credits, even in complex scenarios involving multiple entries on either side.