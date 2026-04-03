"""
LAQN Pollution Data Collector
Pulls historical hourly pollution data from the London Air Quality Network API.
Stores it in the raw_pollution table.
"""
import requests
import time
import sqlite3
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import (
    LAQN_BASE_URL, STATIONS, SPECIES_MAP,
    HISTORICAL_START, HISTORICAL_END,
    LAQN_CHUNK_DAYS, REQUEST_DELAY,
)
from database.db_connector import get_connection, init_db


def fetch_site_data(site_code, start_date, end_date):
    """
    Fetch hourly pollution data for one station over a date range.
    Returns a dict: { timestamp: { 'no2': val, 'pm25': val, ... }, ... }
    """
    url = (
        f"{LAQN_BASE_URL}/Data/Site/SiteCode={site_code}"
        f"/StartDate={start_date}/EndDate={end_date}/Json"
    )

    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except (requests.RequestException, ValueError) as e:
        print(f"  ERROR fetching {site_code} ({start_date} to {end_date}): {e}")
        return {}

    # Parse the response — group readings by timestamp
    rows = {}
    aq_data = data.get("AirQualityData", {})
    records = aq_data.get("Data", [])

    # Handle case where API returns a single record (not a list)
    if isinstance(records, dict):
        records = [records]

    for record in records:
        species = record.get("@SpeciesCode", "")
        timestamp = record.get("@MeasurementDateGMT", "")
        value_str = record.get("@Value", "")

        # Skip species we don't care about
        if species not in SPECIES_MAP:
            continue

        # Parse value — empty string means missing data
        value = None
        if value_str and value_str.strip():
            try:
                value = float(value_str)
            except ValueError:
                value = None

        col_name = SPECIES_MAP[species]

        if timestamp not in rows:
            rows[timestamp] = {"no2": None, "pm25": None, "o3": None, "so2": None}
        rows[timestamp][col_name] = value

    return rows


def get_existing_dates(conn, site_code):
    """Get set of timestamps already in DB for this station."""
    cursor = conn.execute(
        "SELECT timestamp FROM raw_pollution WHERE site_code = ?",
        (site_code,)
    )
    return {row[0] for row in cursor.fetchall()}


def collect_station(site_code, station_name, start_str, end_str):
    """Pull all historical data for one station in chunks."""
    conn = get_connection()
    existing = get_existing_dates(conn, site_code)

    start = datetime.strptime(start_str, "%Y-%m-%d")
    end = datetime.strptime(end_str, "%Y-%m-%d")
    chunk = timedelta(days=LAQN_CHUNK_DAYS)

    total_inserted = 0
    total_skipped = 0
    current = start

    print(f"\n{'='*60}")
    print(f"Station: {site_code} — {station_name}")
    print(f"Range: {start_str} to {end_str}")
    print(f"{'='*60}")

    while current < end:
        chunk_end = min(current + chunk, end)
        chunk_start_str = current.strftime("%Y-%m-%d")
        chunk_end_str = chunk_end.strftime("%Y-%m-%d")

        print(f"  Fetching {chunk_start_str} to {chunk_end_str}...", end=" ")

        rows = fetch_site_data(site_code, chunk_start_str, chunk_end_str)

        inserted = 0
        skipped = 0
        for timestamp, values in rows.items():
            if timestamp in existing:
                skipped += 1
                continue

            conn.execute(
                """INSERT OR IGNORE INTO raw_pollution
                   (timestamp, site_code, no2, pm25, o3, so2)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (timestamp, site_code,
                 values["no2"], values["pm25"],
                 values["o3"], values["so2"])
            )
            existing.add(timestamp)
            inserted += 1

        conn.commit()
        total_inserted += inserted
        total_skipped += skipped
        print(f"{inserted} new, {skipped} skipped")

        current = chunk_end
        time.sleep(REQUEST_DELAY)

    conn.close()
    print(f"\nDone: {total_inserted} rows inserted, {total_skipped} skipped")
    return total_inserted


def collect_all():
    """Pull data for all configured stations."""
    init_db()

    print("LAQN Historical Data Collection")
    print(f"Stations: {', '.join(STATIONS.keys())}")
    print(f"Period: {HISTORICAL_START} to {HISTORICAL_END}")

    grand_total = 0
    for code, name in STATIONS.items():
        count = collect_station(code, name, HISTORICAL_START, HISTORICAL_END)
        grand_total += count

    print(f"\n{'='*60}")
    print(f"ALL DONE — {grand_total} total rows inserted across {len(STATIONS)} stations")
    print(f"{'='*60}")


if __name__ == "__main__":
    collect_all()
