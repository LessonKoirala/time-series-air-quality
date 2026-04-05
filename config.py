"""
Configuration: API URLs, station codes, date ranges and DB path.
"""
import os

# --- Database ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "database", "air_quality.db")
SCHEMA_PATH = os.path.join(BASE_DIR, "database", "schema.sql")

# --- LAQN API (pollution) ---
LAQN_BASE_URL = "https://api.erg.ic.ac.uk/AirQuality"

# Zone 1 stations
STATIONS = {
    "MY1": "Westminster - Marylebone Road",
    "WM0": "Westminster - Horseferry Road",
    "WM6": "Westminster - Oxford Street",
    "BL0": "Camden - Bloomsbury",
    "CD9": "Camden - Euston Road",
}

# LAQN species codes -> our column names
SPECIES_MAP = {
    "NO2":  "no2",
    "FINE": "pm25",   # PM2.5 is coded as 'FINE' in LAQN
    "O3":   "o3",
    "SO2":  "so2",
}

# --- Open-Meteo API (weather) ---
WEATHER_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
WEATHER_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

# Central London coordinates (grid point near Westminster)
LONDON_LAT = 51.51
LONDON_LON = -0.13

WEATHER_VARIABLES = [
    "temperature_2m",
    "wind_speed_10m",
    "wind_direction_10m",
    "precipitation",
    "relative_humidity_2m",
]

# Maps Open-Meteo variable names -> our DB column names
WEATHER_COLUMN_MAP = {
    "temperature_2m":        "temperature",
    "wind_speed_10m":        "windspeed",
    "wind_direction_10m":    "winddirection",
    "precipitation":         "precipitation",
    "relative_humidity_2m":  "humidity",
}

# --- Date ranges ---
HISTORICAL_START = "2019-01-01"
HISTORICAL_END = "2023-12-31"

# Backfill range (2024 → today)
BACKFILL_START = "2024-01-01"
BACKFILL_END = "2026-04-03"

# Chunk size for LAQN requests (days)
LAQN_CHUNK_DAYS = 7

# Delay between API requests (seconds) — be polite to LAQN
REQUEST_DELAY = 0.5
