import requests
import json
from datetime import datetime
from clickhouse_driver import Client
from sshtunnel import SSHTunnelForwarder
from paramiko import RSAKey

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

# Get date range from user
start_date, end_date = get_date_input()

print(f"\nFetching transactions from {start_date} to {end_date}...")

# Start SSH Tunnel
tunnel = None
try:
    print("\nStarting SSH tunnel...")
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
        print(f"[OK] Token fetched successfully: {TOKEN[:20]}...\n")
    else:
        print(f"[WARNING] Failed to fetch token. Status: {auth_response.status_code}")
        print(f"Response: {auth_response.text}")
        print("[INFO] Using fallback token...\n")

except Exception as e:
    print(f"Error fetching token: {e}\n")

# Make POST request for transactions with pagination using offset/limit
try:
    # Use fallback token if auth API failed
    if not TOKEN:
        print("[INFO] Using fallback token...")
        TOKEN = FALLBACK_TOKEN

    print(f"Fetching transactions for date range: {start_date} to {end_date}\n")

    # Try pagination with offset and limit
    all_transactions = []
    offset = 0
    limit = 100  # Try requesting more records per page

    while True:
        # Build request body with offset/limit pagination
        body = {
            "key": KEY,
            "merchant_email": MERCHANT_EMAIL,
            "date_range": {
                "start_date": start_date,
                "end_date": end_date
            },
            "offset": offset,
            "limit": limit,
            "token": TOKEN
        }

        if offset == 0:
            print(f"Fetching transactions (offset: {offset})...")
        else:
            print(f"Fetching more transactions (offset: {offset})...")

        response = requests.post(BASE_URL, headers=headers, json=body)

        # Print status code on first request
        if offset == 0:
            print(f"API Status Code: {response.status_code}")

        # Print error response if not 200
        if response.status_code != 200:
            if offset == 0:
                print(f"Error Response: {response.text}")
            else:
                # If offset/limit not supported, try without them
                print(f"\n[INFO] Pagination may not be supported. Retrying without offset/limit...")
                body = {
                    "key": KEY,
                    "merchant_email": MERCHANT_EMAIL,
                    "date_range": {
                        "start_date": start_date,
                        "end_date": end_date
                    },
                    "token": TOKEN
                }
                response = requests.post(BASE_URL, headers=headers, json=body)
                if response.status_code == 200:
                    response_data = response.json()
                    if response_data.get('status') and response_data.get('data'):
                        all_transactions = response_data['data']
                        print(f"[WARNING] API returned only {len(all_transactions)} transactions (may be limited)")
                break
            break

        if response.status_code == 200:
            response_data = response.json()

            # Check if we got transactions
            if response_data.get('status') and response_data.get('data'):
                transactions = response_data['data']

                if len(transactions) == 0:
                    # No more transactions
                    break

                all_transactions.extend(transactions)
                print(f"  - Fetched {len(transactions)} transactions")

                # If we got less than the limit, we're done
                if len(transactions) < limit:
                    break

                offset += limit
            else:
                break
        else:
            break

    transactions = all_transactions
    if len(transactions) > 0:
        print(f"\n[OK] Total transactions fetched from API: {len(transactions)}\n")

        # Display the API data preview
        print("=" * 150)
        print("API RESPONSE - PREVIEW OF TRANSACTIONS")
        print("=" * 150)

        # Print header
        print(f"{'Status':<15} {'Total Amt':<12} {'Net Amt':<12} {'EasepayID':<18} {'Name':<25} {'Phone':<15} {'Email':<30} {'TxnID':<30}")
        print("-" * 150)

        # Show all transactions from API
        for txn in transactions:
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
        print(f"\nTotal transactions fetched from API: {len(transactions)}")

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

                # Query all recently inserted transactions
                query = """
                SELECT status, total_debit_amount, net_debit_amount, easepayid,
                       firstname, phone, email, txnid, created_at
                FROM payment_transactions
                ORDER BY created_at DESC
                LIMIT 1000
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

except requests.exceptions.RequestException as e:
    print(f"Error making request: {e}")
except json.JSONDecodeError:
    print("Response is not valid JSON")
    print(f"Raw response: {response.text}")
except Exception as e:
    print(f"Error processing data: {e}")

# Close ClickHouse connection
if client:
    client.disconnect()
    print("\nClickHouse connection closed.")

# Close SSH tunnel
if tunnel:
    tunnel.stop()
    print("SSH tunnel closed.")
