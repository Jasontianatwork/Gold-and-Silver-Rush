"""
Script to combine commodity trading data from multiple Excel files into a single panel dataset.

The input files follow the naming convention: COMMODITY_MM_YYYY.xlsx
Output: A CSV file organized by Commodity, Month, Year, Broker, and data variables.
"""

import pandas as pd
import os
import glob
import re

def parse_filename(filename):
    """
    Parse the filename to extract commodity name, month, and year.
    Expected format: COMMODITY_MM_YYYY.xlsx
    """
    basename = os.path.basename(filename)
    name_without_ext = os.path.splitext(basename)[0]

    # Split by underscore - last two parts are month and year
    parts = name_without_ext.split('_')

    if len(parts) >= 3:
        year = parts[-1]
        month = parts[-2]
        commodity = '_'.join(parts[:-2])  # In case commodity name has underscores
    else:
        raise ValueError(f"Unexpected filename format: {filename}")

    return commodity, int(month), int(year)


def read_and_process_file(filepath):
    """
    Read an Excel file and process it to extract broker trading data.
    """
    commodity, month, year = parse_filename(filepath)

    # Read the entire Excel file first to get the header row
    df_raw = pd.read_excel(filepath, header=None)

    # Row 5 (0-indexed) contains the actual column headers
    headers = df_raw.iloc[5].tolist()

    # Clean up header names - fix double spaces and strip whitespace
    headers = [str(h).strip().replace('  ', ' ') if pd.notna(h) else f'Col_{i}' for i, h in enumerate(headers)]

    # Rename first two columns for consistency
    headers[0] = 'Broker_Code'
    headers[1] = 'Broker_Name'

    # Read data starting from row 6
    df = pd.read_excel(filepath, header=None, skiprows=6)
    df.columns = headers

    # Drop rows where Broker_Code is NaN (empty rows)
    df = df.dropna(subset=['Broker_Code'])

    # Add commodity, month, and year columns
    df.insert(0, 'Year', year)
    df.insert(0, 'Month', month)
    df.insert(0, 'Commodity', commodity)

    return df


def combine_all_files(data_dir):
    """
    Combine all Excel files in the data directory into a single DataFrame.
    """
    # Find all Excel files
    excel_files = glob.glob(os.path.join(data_dir, '*.xlsx'))

    if not excel_files:
        raise ValueError(f"No Excel files found in {data_dir}")

    print(f"Found {len(excel_files)} Excel files to process...")

    all_data = []

    for filepath in sorted(excel_files):
        try:
            df = read_and_process_file(filepath)
            all_data.append(df)
            print(f"  Processed: {os.path.basename(filepath)} - {len(df)} brokers")
        except Exception as e:
            print(f"  Error processing {filepath}: {e}")

    # Combine all DataFrames
    combined_df = pd.concat(all_data, ignore_index=True)

    # Sort by Commodity, Year, Month, Broker_Code
    combined_df = combined_df.sort_values(
        by=['Commodity', 'Year', 'Month', 'Broker_Code'],
        ascending=[True, True, True, True]
    ).reset_index(drop=True)

    return combined_df


def main():
    # Set paths
    data_dir = '/home/user/Gold-and-Silver-Rush/Data'
    output_file = '/home/user/Gold-and-Silver-Rush/combined_commodity_data.csv'

    print("Combining commodity data files into panel dataset...")
    print("=" * 60)

    # Combine all files
    combined_df = combine_all_files(data_dir)

    print("=" * 60)
    print(f"\nCombined dataset summary:")
    print(f"  Total rows: {len(combined_df)}")
    print(f"  Commodities: {combined_df['Commodity'].unique().tolist()}")
    print(f"  Date range: {combined_df['Month'].min()}/{combined_df['Year'].min()} to {combined_df['Month'].max()}/{combined_df['Year'].max()}")
    print(f"  Unique brokers: {combined_df['Broker_Code'].nunique()}")

    # Display columns
    print(f"\nColumns in output:")
    for i, col in enumerate(combined_df.columns):
        print(f"  {i+1}. {col}")

    # Save to CSV
    combined_df.to_csv(output_file, index=False)
    print(f"\nOutput saved to: {output_file}")

    # Show sample of the data
    print("\nSample of combined data (first 10 rows):")
    print(combined_df.head(10).to_string())

    return combined_df


if __name__ == '__main__':
    main()
