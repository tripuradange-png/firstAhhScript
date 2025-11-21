import pandas as pd

# Read the CSV file
input_file = r"C:\Users\Tripura\Downloads\merged_1705_with_phones (1).csv"
output_file = r"C:\Users\Tripura\Downloads\filtered_20-11-2025.csv"

# Read the CSV with low_memory=False to avoid warnings
df = pd.read_csv(input_file, low_memory=False)

# Filter for the specific date (20-11-2025)
# The date format in the column is "DD-MM-YYYY HH:MM"
# We need to match dates that start with "20-11-2025"
filtered_df = df[df['loan_disbursed_date'].astype(str).str.startswith('20-11-2025')]

# Save to new CSV file with all columns intact
filtered_df.to_csv(output_file, index=False)

print(f"Total rows in original file: {len(df)}")
print(f"Rows matching date 20-11-2025: {len(filtered_df)}")
print(f"\nFiltered data saved to: {output_file}")

if len(filtered_df) > 0:
    print(f"\nAll {len(df.columns)} columns have been preserved in the output file.")
    print(f"\nFirst few rows preview:")
    print(filtered_df.head())
else:
    print("\nNo records found for date 20-11-2025 in the loan_disbursed_date column.")
    print("The file may not contain data for this date yet.")
