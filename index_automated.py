import requests
import json
from clickhouse_driver import Client
from sshtunnel import SSHTunnelForwarder
from paramiko import RSAKey
from datetime import datetime, timedelta
from tabulate import tabulate

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

# Automatically get today's date range (or customize as needed)
today = datetime.now()
start_date = today.strftime("%d-%m-%Y")
end_date = today.strftime("%d-%m-%Y")

print("=" * 60)
print(f"AUTOMATED RUN - {datetime.now()}")
print("=" * 60)
print(f"Date range: {start_date} to {end_date}")
print("=" * 60)
print()

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
    exit(1)

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
    if tunnel:
        tunnel.stop()
    exit(1)

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
        print(f"Using fallback token...\n")

except Exception as e:
    print(f"Error fetching token: {e}\n")

# Make POST request for transactions
try:
    if not TOKEN:
        TOKEN = FALLBACK_TOKEN

    body = {
        "key": KEY,
        "merchant_email": MERCHANT_EMAIL,
        "date_range": {
            "start_date": start_date,
            "end_date": end_date
        },
        "token": TOKEN
    }

    print(f"Fetching transactions for date range: {start_date} to {end_date}\n")

    response = requests.post(BASE_URL, headers=headers, json=body)

    print(f"API Status Code: {response.status_code}")

    if response.status_code != 200:
        print(f"Error Response: {response.text}")

    if response.status_code == 200:
        response_data = response.json()

        print(f"Found {response_data.get('count', 0)} transactions from API\n")

        if response_data.get('status') and response_data.get('data'):
            transactions = response_data['data']

            if len(transactions) > 0:
                # Display ALL API Response Data
                print("=" * 150)
                print("API RESPONSE - ALL TRANSACTIONS RECEIVED")
                print("=" * 150)

                api_table_data = []
                for txn in transactions:
                    api_table_data.append([
                        txn.get('txnid', '')[:20],
                        txn.get('easepayid', '')[:15],
                        txn.get('status', ''),
                        f"{float(txn.get('total_debit_amount', 0)):.2f}",
                        f"{float(txn.get('net_debit_amount', 0)):.2f}",
                        txn.get('firstname', '')[:20],
                        txn.get('phone', ''),
                        txn.get('email', '')[:30]
                    ])

                headers = ['TxnID', 'EasepayID', 'Status', 'Total Amt', 'Net Amt', 'Name', 'Phone', 'Email']
                print(tabulate(api_table_data, headers=headers, tablefmt='grid'))
                print(f"Total from API: {len(transactions)}")
                print("=" * 150)
                print()

                print(f"Processing {len(transactions)} transactions...\n")

                # Get existing transaction IDs from database
                print("Checking for existing transactions in database...")
                existing_txnids_query = "SELECT DISTINCT txnid FROM payment_transactions"
                existing_txnids_result = client.execute(existing_txnids_query)
                existing_txnids = set(row[0] for row in existing_txnids_result)
                print(f"Found {len(existing_txnids)} existing transaction IDs in database\n")

                # Filter out duplicates and prepare new/old transactions
                new_transactions = []
                old_transactions = []

                for txn in transactions:
                    txnid = txn.get('txnid', '')
                    if txnid not in existing_txnids:
                        new_transactions.append(txn)
                    else:
                        old_transactions.append(txn)

                print(f"[INFO] Total from API: {len(transactions)}")
                print(f"[INFO] Duplicates (already in DB): {len(old_transactions)}")
                print(f"[INFO] New transactions to insert: {len(new_transactions)}\n")

                # Display OLD (Duplicate) Transactions
                if len(old_transactions) > 0:
                    print("=" * 150)
                    print("OLD TRANSACTIONS (ALREADY IN DATABASE - SKIPPED)")
                    print("=" * 150)

                    old_table_data = []
                    for txn in old_transactions:
                        old_table_data.append([
                            txn.get('txnid', '')[:20],
                            txn.get('easepayid', '')[:15],
                            txn.get('status', ''),
                            f"{float(txn.get('total_debit_amount', 0)):.2f}",
                            f"{float(txn.get('net_debit_amount', 0)):.2f}",
                            txn.get('firstname', '')[:20],
                            txn.get('phone', ''),
                            txn.get('email', '')[:30]
                        ])

                    print(tabulate(old_table_data, headers=headers, tablefmt='grid'))
                    print(f"Total duplicates skipped: {len(old_transactions)}")
                    print("=" * 150)
                    print()

                # Display NEW Transactions
                if len(new_transactions) > 0:
                    print("=" * 150)
                    print("NEW TRANSACTIONS (TO BE INSERTED)")
                    print("=" * 150)

                    new_table_data = []
                    for txn in new_transactions:
                        new_table_data.append([
                            txn.get('txnid', '')[:20],
                            txn.get('easepayid', '')[:15],
                            txn.get('status', ''),
                            f"{float(txn.get('total_debit_amount', 0)):.2f}",
                            f"{float(txn.get('net_debit_amount', 0)):.2f}",
                            txn.get('firstname', '')[:20],
                            txn.get('phone', ''),
                            txn.get('email', '')[:30]
                        ])

                    print(tabulate(new_table_data, headers=headers, tablefmt='grid'))
                    print(f"Total new transactions: {len(new_transactions)}")
                    print("=" * 150)
                    print()

                    # Prepare data for insertion
                    insert_query = """
                    INSERT INTO payment_transactions
                    (status, total_debit_amount, net_debit_amount, easepayid, firstname,
                     phone, udf1, udf2, udf3, udf4, udf5, txnid, email)
                    VALUES
                    """

                    # Prepare rows for batch insert
                    rows = []
                    for txn in new_transactions:
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
                    print(f"[OK] Successfully inserted {len(rows)} NEW transactions into ClickHouse")

                    # Query to verify insertion
                    count_query = "SELECT COUNT(*) FROM payment_transactions"
                    total_count = client.execute(count_query)[0][0]
                    print(f"[OK] Total records in database: {total_count}\n")
                else:
                    print("[INFO] No new transactions to insert. All transactions already exist in database.\n")
            else:
                print("[INFO] No transactions found for this date range.\n")
        else:
            print("[INFO] No transaction data in response.\n")

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

print(f"\n[COMPLETED] Run finished at {datetime.now()}")
