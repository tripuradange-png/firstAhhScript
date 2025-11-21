import pandas as pd

# Read the CSV file
input_file = r"C:\Users\Tripura\Downloads\merged_1705_with_phones (1).csv"

# Read the CSV with low_memory=False to avoid warnings
df = pd.read_csv(input_file, low_memory=False)

# Get unique date formats in loan_disbursed_date column (first 50 non-null values)
loan_dates = df['loan_disbursed_date'].dropna().head(100)

print("Sample loan_disbursed_date values:")
print(loan_dates.tolist())

# Check if there are any dates containing "2025-11-20" or "20-11-2025"
print("\n" + "="*50)
print("Checking for dates with 2025-11-20 or 20-11-2025:")

# Try different date patterns
count1 = len(df[df['loan_disbursed_date'].astype(str).str.contains('2025-11-20', na=False)])
count2 = len(df[df['loan_disbursed_date'].astype(str).str.contains('20-11-2025', na=False)])

print(f"Rows containing '2025-11-20': {count1}")
print(f"Rows containing '20-11-2025': {count2}")
