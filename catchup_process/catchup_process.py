# run_catchup.py
import argparse
import subprocess
import sys
import time
from datetime import datetime

def run_spider(start_date: str, end_date: str):
    """
    Invoke the scrapy spider with the given date range.
    """
    cmd = [
        sys.executable, "-m", "scrapy", "crawl", "bse_spider",
        "-a", f"start_date={start_date}",
        "-a", f"end_date={end_date}",
        "-s", "LOG_LEVEL=INFO"
    ]
    # If you’re using feed exports, you can also add:
    #   "-O", f"announcements_{start_date}_{end_date}.json"
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)

def main():
    p = argparse.ArgumentParser(
        description="Periodically re-run the BSE spider to catch up missing PDFs"
    )
    p.add_argument(
        "--start_date", "-s",
        help="YYYYMMDD to start from (defaults to today)",
        default=datetime.today().strftime("%Y%m%d")
    )
    p.add_argument(
        "--end_date", "-e",
        help="YYYYMMDD to end at (defaults to today)",
        default=datetime.today().strftime("%Y%m%d")
    )
    p.add_argument(
        "--interval", "-i",
        help="Seconds between runs (default 1800 = 30 minutes)",
        type=int,
        default=1800
    )
    args = p.parse_args()

    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Starting catch-up loop:")
    print(f"  Date range: {args.start_date} → {args.end_date}")
    print(f"  Interval : {args.interval} s")
    try:
        while True:
            run_spider(args.start_date, args.end_date)
            print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Sleeping for {args.interval} s …")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nReceived interrupt; exiting.")

if __name__ == "__main__":
    main()