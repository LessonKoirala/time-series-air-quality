"""
Open-Meteo Weather Data Collector
Pulls historical hourly weather data for central London.
Stores it in the raw_weather table.
"""
import requests
import sqlite3
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import (
    WEATHER_ARCHIVE_URL, WEATHER_FORECAST_URL, LONDON_LAT, LONDON_LON,
    WEATHER_VARIABLES, WEATHER_COLUMN_MAP,
    HISTORICAL_START, HISTORICAL_END,
)
from database.db_connector import get_connection, init_db


def fetch_weather_year(year):
    """Fetch one year of hourly weather data from Open-Meteo Archive API."""
    params = {
        "latitude": LONDON_LAT,
        "longitude": LONDON_LON,
        "start_date": f"{year}-01-01",
        "end_date": f"{year}-12-31",
        "hourly": ",".join(WEATHER_VARIABLES),
    }

    print(f"  Fetching {year}...", end=" ")

    try:
        resp = requests.get(WEATHER_ARCHIVE_URL, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except (requests.RequestException, ValueError) as e:
        print(f"ERROR: {e}")
        return []

    hourly = data.get("hourly", {})
    timestamps = hourly.get("time", [])

    if not timestamps:
        print("no data returned")
        return []

    # Build rows — parallel arrays to list of dicts
    rows = []
    for i, ts in enumerate(timestamps):
        row = {"timestamp": ts}
        for api_name, col_name in WEATHER_COLUMN_MAP.items():
            values = hourly.get(api_name, [])
            row[col_name] = values[i] if i < len(values) else None
        rows.append(row)

    print(f"{len(rows)} hours")
    return rows


def fetch_weather_recent(start_date, end_date):
    """Fetch recent actual weather from Open-Meteo Forecast API.
    Only keeps observations up to yesterday (excludes future forecasts)."""
    from datetime import date, datetime

    yesterday = date.today().isoformat()

    params = {
        "latitude": LONDON_LAT,
        "longitude": LONDON_LON,
        "hourly": ",".join(WEATHER_VARIABLES),
        "start_date": start_date,
        "end_date": yesterday,
    }

    print(f"  Fetching recent weather ({start_date} → {yesterday})...", end=" ")

    try:
        resp = requests.get(WEATHER_FORECAST_URL, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except (requests.RequestException, ValueError) as e:
        print(f"ERROR: {e}")
        return []

    hourly = data.get("hourly", {})
    timestamps = hourly.get("time", [])

    if not timestamps:
        print("no data returned")
        return []

    rows = []
    for i, ts in enumerate(timestamps):
        row = {"timestamp": ts}
        for api_name, col_name in WEATHER_COLUMN_MAP.items():
            values = hourly.get(api_name, [])
            row[col_name] = values[i] if i < len(values) else None
        rows.append(row)

    print(f"{len(rows)} hours")
    return rows


def get_existing_timestamps(conn):
    """Get set of timestamps already in raw_weather."""
    cursor = conn.execute("SELECT timestamp FROM raw_weather")
    return {row[0] for row in cursor.fetchall()}


def collect_all():
    """Pull weather data for all years in the historical range."""
    init_db()

    start_year = int(HISTORICAL_START[:4])
    end_year = int(HISTORICAL_END[:4])

    print("Open-Meteo Historical Weather Collection")
    print(f"Location: London ({LONDON_LAT}, {LONDON_LON})")
    print(f"Period: {start_year} to {end_year}")

    conn = get_connection()
    existing = get_existing_timestamps(conn)
    print(f"Existing rows in DB: {len(existing)}")

    grand_total = 0

    for year in range(start_year, end_year + 1):
        rows = fetch_weather_year(year)

        inserted = 0
        skipped = 0
        for row in rows:
            if row["timestamp"] in existing:
                skipped += 1
                continue

            conn.execute(
                """INSERT OR IGNORE INTO raw_weather
                   (timestamp, temperature, windspeed, winddirection,
                    precipitation, humidity)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (row["timestamp"], row["temperature"], row["windspeed"],
                 row["winddirection"], row["precipitation"], row["humidity"])
            )
            existing.add(row["timestamp"])
            inserted += 1

        conn.commit()
        grand_total += inserted
        print(f"    -> {inserted} new, {skipped} skipped")

    conn.close()

    print(f"\n{'='*60}")
    print(f"ALL DONE — {grand_total} weather rows inserted")
    print(f"{'='*60}")


if __name__ == "__main__":
    collect_all()
