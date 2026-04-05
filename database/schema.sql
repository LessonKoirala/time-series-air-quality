-- Air Quality London — Database Schema
-- SQLite version

CREATE TABLE IF NOT EXISTS raw_pollution (
    timestamp       TEXT    NOT NULL,   -- ISO 8601, UTC (e.g. '2019-01-01 00:00:00')
    site_code       TEXT    NOT NULL,   -- Station ID (MY1, BL0, WM0, WM6, CD9)
    no2             REAL,               -- Nitrogen dioxide (µg/m³)
    pm25            REAL,               -- Fine particulate matter (µg/m³)
    o3              REAL,               -- Ozone (µg/m³)
    so2             REAL,               -- Sulphur dioxide (µg/m³)
    PRIMARY KEY (timestamp, site_code)
);

CREATE TABLE IF NOT EXISTS raw_weather (
    timestamp       TEXT    NOT NULL,   -- ISO 8601, UTC (e.g. '2019-01-01T00:00')
    temperature     REAL,               -- Air temperature at 2m (°C)
    windspeed       REAL,               -- Wind speed at 10m (km/h)
    winddirection   REAL,               -- Wind direction at 10m (degrees 0-360)
    precipitation   REAL,               -- Rainfall (mm)
    humidity        REAL,               -- Relative humidity at 2m (%)
    PRIMARY KEY (timestamp)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_pollution_site ON raw_pollution(site_code);
CREATE INDEX IF NOT EXISTS idx_pollution_time ON raw_pollution(timestamp);
CREATE INDEX IF NOT EXISTS idx_weather_time   ON raw_weather(timestamp);
