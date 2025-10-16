import pandas as pd

# Read the Excel file to check column names
df = pd.read_excel("files/ajur.xlsx")

print("Column names and indices:")
for i, col in enumerate(df.columns):
    print(f"  {i}: {col}")
    
print("\n\nChecking column 15 and 16:")
print(f"Column 15: {df.columns[15]}")
print(f"Column 16: {df.columns[16]}")

print("\n\nFirst row data for columns 15 and 16:")
print(f"Column 15 value: {df.iloc[0, 15]}")
print(f"Column 16 value: {df.iloc[0, 16]}")