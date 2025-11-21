import requests
import json
from datetime import datetime, timedelta
from clickhouse_driver import Client
from sshtunnel import SSHTunnelForwarder
from paramiko import RSAKey
import time

# ClickHouse Database Constants (via SSH Tunnel)
CLICKHOUSE_HOST = "127.0.0.1"
CLICKHOUSE_PORT = 9000
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "aSh49aVjfy8P"
CLICKHOUSE_DATABASE = "default"

# SSH Tunnel Settings
SSH_HOST = "3.7.169.181"
SSH_PORT = 22
SSH_USERNAME = "ubuntu"
SSH_KEY_PATH = r"D:\ClickHouse\SML_Castlecraft.pem"

# API Constants
AUTH_URL = "https://dashboard.easebuzz.in/auth/v1/token"
BASE_URL = "https://dashboard.easebuzz.in/transaction/v2/retrieve/date"
KEY = "3POWAUBPC"
MERCHANT_EMAIL = "ashwani@rapidmoney.in"
AUTH_EMAIL = "ashwani@rapidmoney.in"
AUTH_PASSWORD = "Ash19771$"
FALLBACK_TOKEN = "Z1t6Riq8OYD62pT0PJEe_oJz2P2KOjZALZM5k7N3Rw4="

# Request headers
headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'Cookie': 'Path=/'
}

def validate_date(date_str):
    """Validate date format DD-MM-YYYY"""
    try:
        datetime.strptime(date_str, "%d-%m-%Y")
        return True
    except ValueError:
        return False

def get_date_input():
    """Get and validate date range from user"""
    print("\n" + "="*50)
    print("DATE RANGE INPUT")
    print("="*50)
    print("Enter dates in DD-MM-YYYY format (e.g., 15-11-2025)")
    print()

    while True:
        start_date = input("Enter START DATE (DD-MM-YYYY): ").strip()
        if validate_date(start_date):
            break
        else:
            print("Invalid date format! Please use DD-MM-YYYY format.")

    while True:
        end_date = input("Enter END DATE (DD-MM-YYYY): ").strip()
        if validate_date(end_date):
            break
        else:
            print("Invalid date format! Please use DD-MM-YYYY format.")

    return start_date, end_date

def generate_date_list(start_date_str, end_date_str):
    """Generate list of dates between start and end date"""
    start = datetime.strptime(start_date_str, "%d-%m-%Y")
    end = datetime.strptime(end_date_str, "%d-%m-%Y")

    date_list = []
    current = start
    while current <= end:
        date_list.append(current.strftime("%d-%m-%Y"))
        current += timedelta(days=1)

    return date_list

def generate_time_intervals(date_str, interval_minutes=15):
    """Generate 15-minute time intervals for a given date"""
    intervals = []

    # Parse the date
    date_obj = datetime.strptime(date_str, "%d-%m-%Y")

    # Generate intervals from 00:00 to 23:59
    current_time = date_obj.replace(hour=0, minute=0, second=0)
    end_of_day = date_obj.replace(hour=23, minute=59, second=59)

    while current_time < end_of_day:
        start_time = current_time
        end_time = current_time + timedelta(minutes=interval_minutes) - timedelta(seconds=1)

        if end_time > end_of_day:
            end_time = end_of_day

        intervals.append({
            'start': start_time.strftime("%d-%m-%Y %H:%M:%S"),
            'end': end_time.strftime("%d-%m-%Y %H:%M:%S")
        })

        current_time += timedelta(minutes=interval_minutes)

    return intervals

# Get date range from user
start_date, end_date = get_date_input()

print(f"\nFetching transactions from {start_date} to {end_date}...")
print(f"Note: Each day will be split into 15-minute intervals to fetch all transactions.\n")

# Generate list of dates
date_list = generate_date_list(start_date, end_date)
print(f"Processing {len(date_list)} day(s)...\n")

# Start SSH Tunnel
tunnel = None
try:
    print("Starting SSH tunnel...")
    pkey = RSAKey.from_private_key_file(SSH_KEY_PATH)
    tunnel = SSHTunnelForwarder(
        (SSH_HOST, SSH_PORT),
        ssh_username=SSH_USERNAME,
        ssh_pkey=pkey,
        remote_bind_address=('127.0.0.1', 9000),
        local_bind_address=('127.0.0.1', 9000)
    )
    tunnel.start()
    print(f"[OK] SSH Tunnel established: {tunnel.local_bind_address}\n")
except Exception as e:
    print(f"[ERROR] SSH Tunnel failed: {e}\n")
    tunnel = None

# Connect to ClickHouse
client = None
try:
    print("Connecting to ClickHouse database...")
    client = Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE
    )

    # Test connection
    result = client.execute('SELECT version()')
    print(f"Connected to ClickHouse! Version: {result[0][0]}\n")

    # Create table if not exists
    create_table_query = """
    CREATE TABLE IF NOT EXISTS payment_transactions (
        status String,
        total_debit_amount Decimal(18, 2),
        net_debit_amount Decimal(18, 2),
        easepayid String,
        firstname String,
        phone String,
        udf1 String,
        udf2 String,
        udf3 String,
        udf4 String,
        udf5 String,
        txnid String,
        email String,
        created_at DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY (created_at, txnid)
    """

    client.execute(create_table_query)
    print("Table 'payment_transactions' created or already exists.\n")

except Exception as e:
    print(f"Error connecting to ClickHouse: {e}\n")
    client = None

# Fetch authentication token
TOKEN = None
print("="*80)
print("TOKEN INFORMATION")
print("="*80)
print(f"OLD TOKEN (Fallback): {FALLBACK_TOKEN}\n")

try:
    print("Fetching authentication token...")

    auth_headers = {
        'Authorization': 'd13d4c1b99204b66af9cf102e7c354c9',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    auth_body = {
        "email": AUTH_EMAIL,
        "password": AUTH_PASSWORD
    }

    auth_response = requests.post(AUTH_URL, headers=auth_headers, json=auth_body)

    if auth_response.status_code == 200:
        auth_data = auth_response.json()
        TOKEN = auth_data.get('token')
        print(f"[OK] NEW TOKEN (Fetched): {TOKEN}\n")
        print(f"Token comparison:")
        print(f"  - Old token matches new: {FALLBACK_TOKEN == TOKEN}")
        print(f"  - Using: NEW TOKEN\n")
    else:
        print(f"[WARNING] Failed to fetch token. Status: {auth_response.status_code}")
        print("[INFO] Using fallback token...\n")

except Exception as e:
    print(f"Error fetching token: {e}\n")

# Use fallback token if auth API failed
if not TOKEN:
    print("[INFO] Using OLD TOKEN (Fallback)...")
    TOKEN = FALLBACK_TOKEN
    print(f"Active TOKEN: {TOKEN}\n")

print("="*80)


# Fetch transactions for each date and time interval
all_transactions = []
total_api_calls = 0

print("="*80)
print("FETCHING TRANSACTIONS BY TIME INTERVALS")
print("="*80)

for date in date_list:
    print(f"\nProcessing date: {date}")
    print("-" * 80)

    # Generate 15-minute intervals for this date
    intervals = generate_time_intervals(date, interval_minutes=15)

    for idx, interval in enumerate(intervals, 1):
        try:
            # Build request body with time range
            body = {
                "key": KEY,
                "merchant_email": MERCHANT_EMAIL,
                "date_range": {
                    "start_date": interval['start'],
                    "end_date": interval['end']
                },
                "token": TOKEN
            }

            response = requests.post(BASE_URL, headers=headers, json=body)
            total_api_calls += 1

            if response.status_code == 200:
                response_data = response.json()

                if response_data.get('status') and response_data.get('data'):
                    transactions = response_data['data']

                    if len(transactions) > 0:
                        all_transactions.extend(transactions)
                        print(f"  [{idx:3d}/96] {interval['start']} - {interval['end']}: {len(transactions)} transactions")

                        # Warn if we got exactly 20 (might be truncated)
                        if len(transactions) == 20:
                            print(f"         WARNING: Got exactly 20 transactions - may be truncated!")
                else:
                    # No transactions in this interval (normal)
                    pass
            else:
                print(f"  [{idx:3d}/96] {interval['start']} - ERROR: {response.status_code}")

            # Small delay to avoid rate limiting (50ms)
            time.sleep(0.05)

        except Exception as e:
            print(f"  [{idx:3d}/96] Error: {e}")

print("\n" + "="*80)
print(f"[OK] Total transactions fetched: {len(all_transactions)}")
print(f"[INFO] Total API calls made: {total_api_calls}")
print("="*80)

# Remove duplicates based on txnid
if len(all_transactions) > 0:
    unique_transactions = {}
    for txn in all_transactions:
        txnid = txn.get('txnid', '')
        if txnid and txnid not in unique_transactions:
            unique_transactions[txnid] = txn

    transactions = list(unique_transactions.values())

    if len(all_transactions) != len(transactions):
        print(f"\n[INFO] Removed {len(all_transactions) - len(transactions)} duplicate transactions")
        print(f"[OK] Unique transactions: {len(transactions)}\n")
    else:
        print(f"\n[INFO] No duplicate transactions found\n")

    # Display preview
    print("=" * 150)
    print("TRANSACTION PREVIEW (First 50)")
    print("=" * 150)

    # Print header
    print(f"{'Status':<15} {'Total Amt':<12} {'Net Amt':<12} {'EasepayID':<18} {'Name':<25} {'Phone':<15} {'Email':<30} {'TxnID':<30}")
    print("-" * 150)

    # Show first 50 transactions
    for txn in transactions[:50]:
        status = txn.get('status', '')
        total_amt = float(txn.get('total_debit_amount', 0))
        net_amt = float(txn.get('net_debit_amount', 0))
        easepayid = txn.get('easepayid', '')
        firstname = txn.get('firstname', '')
        phone = txn.get('phone', '')
        email = txn.get('email', '')
        txnid = txn.get('txnid', '')

        print(f"{status:<15} {total_amt:<12.2f} {net_amt:<12.2f} {easepayid:<18} {firstname:<25} {phone:<15} {email:<30} {txnid:<30}")

    print("=" * 150)

    if len(transactions) > 50:
        print(f"\n... and {len(transactions) - 50} more transactions")

    # Ask for user confirmation
    if client:
        user_input = input("\nDo you want to insert these transactions into the database? (yes/no): ").strip().lower()

        if user_input in ['yes', 'y']:
            # Prepare data for insertion
            insert_query = """
            INSERT INTO payment_transactions
            (status, total_debit_amount, net_debit_amount, easepayid, firstname,
             phone, udf1, udf2, udf3, udf4, udf5, txnid, email)
            VALUES
            """

            # Prepare rows for batch insert
            rows = []
            for txn in transactions:
                rows.append((
                    txn.get('status', ''),
                    float(txn.get('total_debit_amount', 0)),
                    float(txn.get('net_debit_amount', 0)),
                    txn.get('easepayid', ''),
                    txn.get('firstname', ''),
                    txn.get('phone', ''),
                    txn.get('udf1', ''),
                    txn.get('udf2', ''),
                    txn.get('udf3', ''),
                    txn.get('udf4', ''),
                    txn.get('udf5', ''),
                    txn.get('txnid', ''),
                    txn.get('email', '')
                ))

            # Execute batch insert
            client.execute(insert_query, rows)
            print(f"\n[OK] Successfully inserted {len(rows)} transactions into ClickHouse")

            # Query to verify insertion
            count_query = "SELECT COUNT(*) FROM payment_transactions"
            total_count = client.execute(count_query)[0][0]
            print(f"[OK] Total records in table: {total_count}\n")

            # Display the inserted data
            print("=" * 150)
            print("RECENT TRANSACTIONS IN DATABASE")
            print("=" * 150)

            # Query recent transactions
            query = """
            SELECT status, total_debit_amount, net_debit_amount, easepayid,
                   firstname, phone, email, txnid, created_at
            FROM payment_transactions
            ORDER BY created_at DESC
            LIMIT 50
            """

            results = client.execute(query)

            # Print header
            print(f"{'Status':<15} {'Total Amt':<12} {'Net Amt':<12} {'EasepayID':<18} {'Name':<25} {'Phone':<15} {'Email':<30} {'TxnID':<30}")
            print("-" * 150)

            # Print rows
            for row in results:
                status, total_amt, net_amt, easepayid, firstname, phone, email, txnid, created_at = row
                print(f"{status:<15} {total_amt:<12.2f} {net_amt:<12.2f} {easepayid:<18} {firstname:<25} {phone:<15} {email:<30} {txnid:<30}")

            print("=" * 150)
        else:
            print("\n[INFO] Data insertion skipped by user.")
    else:
        print("\n[WARNING] ClickHouse client not connected. Cannot insert data.")
else:
    print("\n[INFO] No transactions found for the specified date range.")

# Close ClickHouse connection
if client:
    client.disconnect()
    print("\nClickHouse connection closed.")

# Close SSH tunnel
if tunnel:
    tunnel.stop()
    print("SSH tunnel closed.")
