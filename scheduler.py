import schedule
import time
import subprocess
import os
from datetime import datetime

# Path to the script to run
SCRIPT_PATH = r"d:\test\index_automated.py"
PYTHON_EXECUTABLE = "python"  # or use full path like r"C:\Python39\python.exe"

def run_script():
    """Run the index_automated.py script"""
    print(f"\n{'='*80}")
    print(f"[{datetime.now()}] Starting scheduled execution...")
    print(f"{'='*80}")

    try:
        # Run the script and capture output
        result = subprocess.run(
            [PYTHON_EXECUTABLE, SCRIPT_PATH],
            capture_output=True,
            text=True,
            cwd=os.path.dirname(SCRIPT_PATH)
        )

        # Print the output
        if result.stdout:
            print(result.stdout)

        if result.stderr:
            print("STDERR:", result.stderr)

        if result.returncode == 0:
            print(f"[{datetime.now()}] Execution completed successfully!")
        else:
            print(f"[{datetime.now()}] Execution failed with return code: {result.returncode}")

    except Exception as e:
        print(f"[{datetime.now()}] Error running script: {e}")

    print(f"{'='*80}\n")

# Schedule the job to run every 20 seconds
schedule.every(20).seconds.do(run_script)

print("=" * 80)
print("SCHEDULER STARTED")
print("=" * 80)
print(f"Script to run: {SCRIPT_PATH}")
print(f"Frequency: Every 20 seconds")
print(f"Started at: {datetime.now()}")
print(f"Next run will be in 20 seconds...")
print("=" * 80)
print("\nPress Ctrl+C to stop the scheduler\n")

# Run immediately on startup (optional - comment out if you don't want this)
print("Running initial execution...")
run_script()

# Keep the scheduler running
try:
    while True:
        schedule.run_pending()
        time.sleep(1)
except KeyboardInterrupt:
    print("\n\nScheduler stopped by user.")
    print(f"Stopped at: {datetime.now()}")
