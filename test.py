import requests
import json
from datetime import datetime
from clickhouse_driver import Client
from tabulate import tabulate

# ClickHouse Database Constants
CLICKHOUSE_HOST = "100.127.174.36"
CLICKHOUSE_PORT = 9000  # Native TCP protocol port (changed from 8123 HTTP port)
CLICKHOUSE_USER = "cibil"
CLICKHOUSE_PASSWORD = "root"
CLICKHOUSE_DATABASE = "cibil"

# API Constants
AUTH_URL = "https://dashboard.easebuzz.in/auth/v1/token"
BASE_URL = "https://dashboard.easebuzz.in/transaction/v2/retrieve/date"
KEY = "3POWAUBPC"
MERCHANT_EMAIL = "ashwani@rapidmoney.in"
AUTH_EMAIL = "ashwani@rapidmoney.in"
AUTH_PASSWORD = "Ash19771$"
FALLBACK_TOKEN = "Z1t6Riq8OYD62pT0PJEe_oJz2P2KOjZALZM5k7N3Rw4="  # Fallback token if auth API fails

# Request headers
headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'Cookie': 'Path=/'
}

# Get today's date in DD-MM-YYYY format
today_date = datetime.now().strftime("%d-%m-%Y")

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
    CREATE TABLE IF NOT EXISTS easebuzz_transactions (
        status String,
        total_debit_amount Float64,
        net_debit_amount Float64,
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
    print("Table 'easebuzz_transactions' created or already exists.\n")

except Exception as e:
    print(f"Error connecting to ClickHouse: {e}\n")
    client = None

# Fetch authentication token
TOKEN = None
try:
    print("Fetching authentication token...")

    # Auth headers with Authorization key
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
        print("[INFO] The auth API might require additional headers or parameters.")
        print("[INFO] You can manually update the token when it expires.\n")

except Exception as e:
    print(f"Error fetching token: {e}\n")

# Make POST request for transactions
try:
    # Use fallback token if auth API failed
    if not TOKEN:
        print("[INFO] Using fallback token...")
        TOKEN = FALLBACK_TOKEN

    # Build request body with fetched token
    body = {
        "key": KEY,
        "merchant_email": MERCHANT_EMAIL,
        "date_range": {
            "start_date": today_date,
            "end_date": today_date
        },
        "token": TOKEN
    }

    print(f"Fetching transactions for date: {today_date}\n")

    response = requests.post(BASE_URL, headers=headers, json=body)

    # Print status code
    print(f"API Status Code: {response.status_code}")

    # Print error response if not 200
    if response.status_code != 200:
        print(f"Error Response: {response.text}")

    if response.status_code == 200:
        response_data = response.json()

        # Print response summary
        print(f"Found {response_data.get('count', 0)} transactions from API\n")

        # Display API results first
        if response_data.get('status') and response_data.get('data'):
            transactions = response_data['data']

            if len(transactions) > 0:
                # Prepare table data for API preview
                table_data = []
                for idx, txn in enumerate(transactions[:20], 1):
                    table_data.append([
                        idx,
                        txn.get('status', '')[:12],
                        f"₹{float(txn.get('total_debit_amount', 0)):.2f}",
                        f"₹{float(txn.get('net_debit_amount', 0)):.2f}",
                        txn.get('easepayid', '')[:15],
                        txn.get('firstname', '')[:20],
                        txn.get('phone', '')[:12],
                        txn.get('email', '')[:25],
                        txn.get('txnid', '')[:25]
                    ])

                # Display API results in table format
                print("\n" + "=" * 160)
                print("API RESPONSE - PREVIEW OF TRANSACTIONS")
                print("=" * 160)

                headers = ['#', 'Status', 'Total Amt', 'Net Amt', 'EasepayID', 'Name', 'Phone', 'Email', 'TxnID']
                print(tabulate(table_data, headers=headers, tablefmt='grid'))

                print("=" * 160)
                print(f"\nTotal transactions fetched from API: {len(transactions)}")

                # Ask for user confirmation
                if client:
                    user_input = input("\nDo you want to insert these transactions into the database? (yes/no): ").strip().lower()

                    if user_input in ['yes', 'y']:
                        # Prepare data for insertion
                        insert_query = """
                        INSERT INTO easebuzz_transactions
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
                        count_query = "SELECT COUNT(*) FROM easebuzz_transactions"
                        total_count = client.execute(count_query)[0][0]
                        print(f"[OK] Total records in table: {total_count}\n")

                        # Display the inserted data
                        print("\n" + "=" * 160)
                        print("RECENT TRANSACTIONS IN DATABASE")
                        print("=" * 160)

                        # Query recent transactions
                        query = """
                        SELECT status, total_debit_amount, net_debit_amount, easepayid,
                               firstname, phone, email, txnid, created_at
                        FROM easebuzz_transactions
                        ORDER BY created_at DESC
                        LIMIT 20
                        """

                        results = client.execute(query)

                        # Prepare table data for database records
                        db_table_data = []
                        for idx, row in enumerate(results, 1):
                            status, total_amt, net_amt, easepayid, firstname, phone, email, txnid, created_at = row
                            db_table_data.append([
                                idx,
                                status[:12],
                                f"₹{total_amt:.2f}",
                                f"₹{net_amt:.2f}",
                                easepayid[:15],
                                firstname[:20],
                                phone[:12],
                                email[:25],
                                txnid[:25],
                                created_at.strftime('%Y-%m-%d %H:%M:%S')
                            ])

                        db_headers = ['#', 'Status', 'Total Amt', 'Net Amt', 'EasepayID', 'Name', 'Phone', 'Email', 'TxnID', 'Created At']
                        print(tabulate(db_table_data, headers=db_headers, tablefmt='grid'))
                        print("=" * 160)
                    else:
                        print("\n[INFO] Data insertion skipped by user.")
                else:
                    print("\n[WARNING] ClickHouse client not connected. Cannot insert data.")

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
