"""
Account Matcher Script

This script matches debit and credit accounts between different Excel accounting files.
It specifically reads a CREDIT file (with missing debit accounts) and a Rival file (complete entries),
matches corresponding entries, and fills in missing account information.
"""

import pandas as pd
import os
from datetime import datetime
import argparse
import re

def read_rival_file(file_path):
    """
    Read and parse the Rival Excel file.
    
    Args:
        file_path (str): Path to the Rival Excel file
    
    Returns:
        pd.DataFrame: Processed DataFrame with standardized columns
    """
    # Read the Excel file, skipping the header rows
    df = pd.read_excel(file_path)
    
    # Find the data start row (where the actual data begins after headers)
    data_start_row = 9  # Based on analysis of Rival format
    
    # Skip header rows and reset index
    df = df.iloc[data_start_row:].reset_index(drop=True)
    
    # Extract relevant columns
    # Column indices are specific to Rival format:
    # - Doc type: column 5 (Вид документ)
    # - Doc number: column 8 (Номер документ)
    # - Date: column 9 (Дата документ)
    # - Debit account: column 12 (Сметка дебит)
    # - Credit account: column 13 (Сметка кредит)
    # - Amount: column 14 (Стойност)
    # - Description: column 25 (Обяснение на статия)
    
    # Create a standardized DataFrame
    result = pd.DataFrame({
        'doc_type': df.iloc[:, 5],
        'doc_number': df.iloc[:, 7], 
        'date': df.iloc[:, 9],
        'debit_account': df.iloc[:, 12],
        'credit_account': df.iloc[:, 13],
        'amount': df.iloc[:, 14],
        'description': df.iloc[:, 25]
    })
    
    # Clean up data
    result = result.fillna('')
    
    # Convert date strings to datetime objects
    result['date'] = pd.to_datetime(result['date'], errors='coerce')
    
    # Convert amounts to float
    result['amount'] = pd.to_numeric(result['amount'], errors='coerce')
    
    return result

def read_credit_file(file_path):
    """
    Read and parse the CREDIT Excel file.
    
    Args:
        file_path (str): Path to the CREDIT Excel file
    
    Returns:
        pd.DataFrame: Processed DataFrame with standardized columns
    """
    # Read the Excel file
    df = pd.read_excel(file_path)
    
    # Find the data start row (row with column headers)
    data_start_row = None
    for i, row in df.iterrows():
        if '№ по ред' in str(row.iloc[0]):
            data_start_row = i
            break
    
    if data_start_row is None:
        raise ValueError("Could not find column headers in the CREDIT file")
    
    # Get column headers
    headers = df.iloc[data_start_row]
    
    # Extract data rows
    data_rows = df.iloc[data_start_row + 1:].reset_index(drop=True)
    data_rows.columns = headers
    
    # Clean up data
    result = data_rows.copy()
    result = result.fillna('')
    
    # Convert date strings to datetime objects
    if 'Дата' in result.columns:
        result['Дата'] = pd.to_datetime(result['Дата'], errors='coerce')
    
    # Convert amounts to float
    if 'Сума' in result.columns:
        result['Сума'] = pd.to_numeric(result['Сума'], errors='coerce')
    
    return result

def match_accounts(credit_df, rival_df):
    """
    Match entries between CREDIT and Rival files and fill in missing debit accounts.
    
    Args:
        credit_df (pd.DataFrame): DataFrame from the CREDIT file
        rival_df (pd.DataFrame): DataFrame from the Rival file
    
    Returns:
        pd.DataFrame: Updated CREDIT DataFrame with filled debit accounts
    """
    updated_credit_df = credit_df.copy()
    
    # Ensure there's a Debit account column
    if 'Дт с/ка' not in updated_credit_df.columns:
        updated_credit_df['Дт с/ка'] = ''
    
    # Track statistics
    matches_found = 0
    total_entries = len(credit_df)
    
    # Process each row in the CREDIT file
    for i, credit_row in updated_credit_df.iterrows():
        # Skip rows that already have a debit account
        if credit_row['Дт с/ка'] and str(credit_row['Дт с/ка']) != 'nan':
            continue
        
        # Extract matching criteria from CREDIT row
        credit_doc_num = str(credit_row.get('Документ №', ''))
        credit_date = credit_row.get('Дата')
        credit_amount = credit_row.get('Сума', 0)
        credit_acct = str(credit_row.get('Кт с/ка', ''))
        
        # Skip if we don't have enough matching criteria
        if not credit_doc_num or pd.isna(credit_date) or pd.isna(credit_amount):
            continue
        
        # Convert date to string for printing
        credit_date_str = credit_date.strftime('%Y-%m-%d') if isinstance(credit_date, pd.Timestamp) else str(credit_date)
        
        # Look for matches in the Rival file
        matches = rival_df[
            (rival_df['doc_number'] == credit_doc_num) & 
            (rival_df['date'].dt.date == credit_date.date()) & 
            (abs(rival_df['amount'] - credit_amount) < 0.01) &
            (rival_df['credit_account'] == credit_acct)
        ]
        
        if not matches.empty:
            # Get the matching debit account(s)
            matching_debit_accounts = matches['debit_account'].unique()
            
            if len(matching_debit_accounts) == 1:
                # Single match
                updated_credit_df.at[i, 'Дт с/ка'] = matching_debit_accounts[0]
                matches_found += 1
                print(f"Match found for Doc #{credit_doc_num}, Date: {credit_date_str}, Amount: {credit_amount}: {matching_debit_accounts[0]}")
            elif len(matching_debit_accounts) > 1:
                # Multiple matches - combine accounts
                updated_credit_df.at[i, 'Дт с/ка'] = " + ".join(matching_debit_accounts)
                matches_found += 1
                print(f"Multiple matches found for Doc #{credit_doc_num}, Date: {credit_date_str}, Amount: {credit_amount}: {' + '.join(matching_debit_accounts)}")
        else:
            # Try a more relaxed search without the credit account constraint
            relaxed_matches = rival_df[
                (rival_df['doc_number'] == credit_doc_num) & 
                (rival_df['date'].dt.date == credit_date.date()) & 
                (abs(rival_df['amount'] - credit_amount) < 0.01)
            ]
            
            if not relaxed_matches.empty:
                # Get the matching debit accounts from relaxed search
                matching_debit_accounts = relaxed_matches['debit_account'].unique()
                
                if len(matching_debit_accounts) >= 1:
                    # Use the first matching debit account
                    updated_credit_df.at[i, 'Дт с/ка'] = matching_debit_accounts[0]
                    matches_found += 1
                    print(f"Relaxed match found for Doc #{credit_doc_num}, Date: {credit_date_str}, Amount: {credit_amount}: {matching_debit_accounts[0]}")
    
    print(f"\nMatching complete: {matches_found} of {total_entries} entries matched ({matches_found/total_entries*100:.2f}%)")
    return updated_credit_df

def save_matched_file(credit_df, output_path):
    """
    Save the updated CREDIT DataFrame to an Excel file.
    
    Args:
        credit_df (pd.DataFrame): Updated CREDIT DataFrame
        output_path (str): Path to save the output Excel file
    """
    # Generate output filename if not provided
    if not output_path:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        output_path = f"matched_accounts_{timestamp}.xlsx"
    
    # Save to Excel
    credit_df.to_excel(output_path, index=False)
    print(f"Output saved to {output_path}")

def main():
    """
    Main function to run the account matching process.
    """
    parser = argparse.ArgumentParser(description='Match debit-credit accounts between Excel files')
    parser.add_argument('--credit', required=True, help='Path to the CREDIT Excel file')
    parser.add_argument('--rival', required=True, help='Path to the Rival Excel file')
    parser.add_argument('--output', help='Path to save the output Excel file (optional)')
    
    args = parser.parse_args()
    
    # Read input files
    print(f"Reading CREDIT file: {args.credit}")
    credit_df = read_credit_file(args.credit)
    
    print(f"Reading Rival file: {args.rival}")
    rival_df = read_rival_file(args.rival)
    
    # Match accounts
    print("Matching accounts between files...")
    updated_credit_df = match_accounts(credit_df, rival_df)
    
    # Save output file
    save_matched_file(updated_credit_df, args.output)

if __name__ == "__main__":
    main()