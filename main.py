import sys
import time
import datetime

from dotenv import load_dotenv

from utils.modbus_utils import ekk_modbus_read_and_store

load_dotenv()


if __name__ == "__main__":
    if len(sys.argv) < 2 or not sys.argv[1].strip():
        print("Warning: site_id parameter is required. Please provide it as the first argument.")
        sys.exit(1)

    site_id = int(sys.argv[1].strip())
    run_once = "--once" in sys.argv[2:]
    print(f"Using site_id: {site_id}")

    while True:
        ekk_modbus_read_and_store(site_id)
        if run_once:
            print("Finished single poll.")
            break
        print("Waiting for 5 minutes before next poll...")
        print("Current time:", datetime.datetime.now())
        time.sleep(5 * 60)
