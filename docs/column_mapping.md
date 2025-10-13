# Column Mapping for Different Accounting Software Formats

This document provides a comprehensive mapping of column information across different accounting software formats used in the system.

## Table from Image

The table in the image shows the following column mappings:

| Field | Ажур | Бизнес навигатор | Микроинвест | Ривал | Универсум |
|-------|------|-----------------|------------|-------|----------|
| № по ред | Колона A | - | - | - | - |
| Вид документ | Колона C | Колона K | Колона H | Колона F & G | Колона C |
| Документ № | Колона D | Колона L | Колона J | Колона H & I | Колона E |
| Дата | Колона B | Колона M | Колона B | Колона J | Колона B |
| Дт с/ка | Колона F | Колона Y / A | Колона C | Колона M | Колона I |
| Аналитична сметка/Партньор | Колона G | Колона Z | Колона D | Колона K & L | Колона J |
| Кт с/ка | Колона I | Колона Y / A | Колона E | Колона N | Колона K |
| Аналитична сметка/Партньор | Колона J | Колона Z | Колона F | Колона K & L | Колона L |
| Сума | Колона M | Колона V | Колона G | Колона O | Колона M |
| Обяснение/Обоснование | Колона Z | Колона N | Колона N | Колона Z | Колона C |
| Установена сума при одита | Колона O | - | - | - | - |
| Отклонение | Колона P | - | - | - | - |
| Установено контролно действие при одита | Колона Q | - | - | - | - |
| Отклонение | Колона S | - | - | - | - |

## Implementation in Code

Based on the parsers in the codebase, here's how the different formats are currently implemented:

### Ajur Parser

```python
# Column mapping for Ajur format
{
    'doc_type': 0,      # Вид документ
    'doc_number': 1,    # Документ №
    'date': 2,          # Дата
    'debit': 3,         # Дт с/ка
    'analytical_debit': 4,  # Аналитична сметка/Партньор (Дт)
    'credit': 5,        # Кт с/ка
    'analytical_credit': 6, # Аналитична сметка/Партньор (Кт)
    'amount': 7,        # Сума
    'description': 8    # Обяснение/Обоснование
}
```

The parser also attempts to dynamically detect columns based on keywords:
- "вид", "тип", "type", "документ" for document type
- "номер", "no.", "number" for document number
- "дата", "date" for date
- "дебит", "дт", "dt", "debit" for debit account
- "кредит", "кт", "kt", "credit" for credit account
- "сума", "amount", "value", "стойност" for amount
- "аналитична", "analytics", "analytic" for analytical accounts
- "обяснение", "описание", "description", "details", "основание" for description

### Microinvest Parser

The Microinvest parser searches for columns with these keywords:
- 'дебит сметка', 'дт сметка', 'дт с-ка', 'debit account' for debit account
- 'кредит сметка', 'кт сметка', 'кт с-ка', 'credit account' for credit account
- 'дата', 'date' for date
- 'сума', 'сума дт', 'amount', 'value' for amount
- 'док. вид', 'вид док', 'document type' for document type
- 'документ №', 'номер', 'doc number' for document number
- 'основание', 'описание', 'description' for description
- 'партньор', 'partner' for partner

### Rival Parser

For Rival format, the parser uses fixed column indices:
- Column 0: Вид документ (Document type)
- Column 1: Номер на документ (Document number)
- Column 2: Дата (Date)
- Column 3: Име (Name/Partner)
- Column 4: Дебит (Debit account)
- Column 5: Кредит (Credit account)
- Column 6: Сума (Amount)
- Column 7: Обяснение (Description)

## Recommended Unified Approach

Based on the table in the image and the current implementation, we should update the parsers to use the following column mappings:

### Ajur Format
- № по ред: Column A
- Вид документ: Column C
- Документ №: Column D
- Дата: Column B
- Дт с/ка: Column F
- Аналитична сметка/Партньор (Дт): Column G
- Кт с/ка: Column I
- Аналитична сметка/Партньор (Кт): Column J
- Сума: Column M
- Обяснение/Обоснование: Column Z
- Установена сума при одита: Column O
- Отклонение: Column P
- Установено контролно действие при одита: Column Q
- Отклонение: Column S

### Бизнес навигатор Format
- Вид документ: Column K
- Документ №: Column L
- Дата: Column M
- Дт с/ка: Column Y / A
- Аналитична сметка/Партньор (Дт): Column Z
- Кт с/ка: Column Y / A
- Аналитична сметка/Партньор (Кт): Column Z
- Сума: Column V
- Обяснение/Обоснование: Column N

### Микроинвест Format
- Вид документ: Column H
- Документ №: Column J
- Дата: Column B
- Дт с/ка: Column C
- Аналитична сметка/Партньор (Дт): Column D
- Кт с/ка: Column E
- Аналитична сметка/Партньор (Кт): Column F
- Сума: Column G
- Обяснение/Обоснование: Column N

### Ривал Format
- Вид документ: Column F & G (merged cells in header rows 1, 2, 4, 5, 6)
- Документ №: Column H & I (merged cells in header rows 1, 2, 4, 5, 6)
- Дата: Column J
- Дт с/ка: Column M
- Аналитична сметка/Партньор (Дт): Column K & L (merged cells in header rows 1, 2, 4, 5, 6)
- Кт с/ка: Column N
- Аналитична сметка/Партньор (Кт): Column K & L (merged cells in header rows 1, 2, 4, 5, 6)
- Сума: Column O
- Обяснение/Обоснование: Column Z

Note: In the Rival format, header rows 1, 2, 4, 5, 6 (0-based indexing: 0, 1, 3, 4, 5) are merged from column A to column K. This means that the content of these rows is only present in the first cell (column A) of each row. When parsing these rows, we need to extract the information from the first cell only and ignore the rest of the cells in the row.

### Универсум Format
- Вид документ: Column C
- Документ №: Column E
- Дата: Column B
- Дт с/ка: Column I
- Аналитична сметка/Партньор (Дт): Column J
- Кт с/ка: Column K
- Аналитична сметка/Партньор (Кт): Column L
- Сума: Column M
- Обяснение/Обоснование: Column C