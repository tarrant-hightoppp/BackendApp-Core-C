import pandas as pd

# Read the Excel file
df = pd.read_excel("files/ajur.xlsx")

# Test the column name detection logic
for i, col_name in enumerate(df.columns):
    col_str = str(col_name).lower().strip()
    
    # Check for credit column
    if 'кт' in col_str and 'с/ка' in col_str and 'аналитична' not in col_str:
        print(f"Found MAIN credit column at index {i}: {col_name}")
        print(f"  col_str: '{col_str}'")
        print(f"  Contains 'кт': {'кт' in col_str}")
        print(f"  Contains 'с/ка': {'с/ка' in col_str}")
        print(f"  Contains 'аналитична': {'аналитична' in col_str}")
        
# Let's also check what's happening with the _detect_columns logic
print("\n\nTesting _detect_columns logic:")
for i in range(min(30, len(df))):
    row_values = [str(val).lower() if not pd.isna(val) else "" for val in df.iloc[i].values]
    
    # Look for column headers by keywords
    for col_idx, val in enumerate(row_values):
        if not val:  # Skip empty values
            continue
            
        # Check for credit
        if ("кредит" in val or "кт" in val or "kt" in val or "credit" in val) and "аналитична" not in val:
            print(f"Row {i}, Col {col_idx}: Found potential credit column with value '{val}'")