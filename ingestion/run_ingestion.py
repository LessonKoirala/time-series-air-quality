"""
Run full historical data ingestion: pollution and weather.
Usage: python ingestion/run_ingestion.py
"""
import time
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ingestion.laqn_collector import collect_all as collect_pollution
from ingestion.weather_collector import collect_all as collect_weather


def main():
    print("=" * 60)
    print("HISTORICAL DATA INGESTION")
    print("=" * 60)

    start = time.time()

    # Step 1: Pollution data (takes ~15 min due to API rate limiting)
    print("\n[1/2] POLLUTION DATA\n")
    collect_pollution()

    # Step 2: Weather data (fast — ~5 requests)
    print("\n[2/2] WEATHER DATA\n")
    collect_weather()

    elapsed = time.time() - start
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)

    print(f"\n{'=' * 60}")
    print(f"INGESTION COMPLETE — took {minutes}m {seconds}s")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
