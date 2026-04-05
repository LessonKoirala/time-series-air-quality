# London NO2 Air Quality Forecasting and MLOps Pipeline

An end-to-end time series forecasting system for nitrogen dioxide (NO2) pollution at Marylebone Road, London. From raw API data collection through to model validation, drift detection, robustness testing and business analysis.

## System Architecture

```
                    LAQN API                    Open-Meteo API
                  (pollution)                     (weather)
                      |                              |
                      v                              v
              laqn_collector.py            weather_collector.py
                      |                              |
                      +---------- SQLite DB ---------+
                                     |
                      +--------------+--------------+
                      |                             |
               01_data_cleaning              backfill_and_evaluate.py
               (2019 to 2023)                (2024 to 2026)
                      |                             |
          +-----------+-----------+                 |
          |     |     |     |     |                 |
        ARIMA SARIMA  VAR  GARCH  |          778 unseen days
          |     |     |     |     |          actual vs predicted
          +-----+-----+-----+    |                 |
                |                 |     +-----------+-----------+
          06_anomaly         07_class   |           |           |
                                     drift     residuals    robustness
                                   analysis   diagnostics    testing
                                        |           |           |
                                        +-----------+-----------+
                                                    |
                                          technical_report.ipynb
                                          (12 business questions)
```

## Project Structure

```
time-series-air-quality/
|
|-- config.py                              Configuration: APIs, stations, dates
|-- requirements.txt                       Python dependencies
|-- main.py                                Entry point (placeholder)
|
|-- database/
|   |-- schema.sql                         Table definitions
|   |-- db_connector.py                    SQLite connection helper
|   |-- air_quality.db                     381,000+ rows (2019 to Apr 2026)
|
|-- ingestion/
|   |-- laqn_collector.py                  LAQN pollution API collector
|   |-- weather_collector.py               Open-Meteo weather collector (archive + forecast)
|   |-- run_ingestion.py                   Runs both collectors
|
|-- processing/
|   |-- 01_data_cleaning.ipynb             EDA, stationarity, ACF/PACF, Fourier analysis
|
|-- models/
|   |-- 02_arima.ipynb                     ARIMA grid search and walk-forward validation
|   |-- 03_sarima.ipynb                    Seasonal ARIMA with weekly periodicity
|   |-- 04_var.ipynb                       Vector autoregression, Granger causality, FEVD
|   |-- 05_garch.ipynb                     Volatility modelling, dynamic confidence intervals
|   |-- saved/                             11 serialised model files (.pkl)
|
|-- anomaly/
|   |-- 06_anomaly_detection.ipynb         Z-score, GARCH-adaptive and rolling mean methods
|
|-- classification/
|   |-- 07_classification.ipynb            Gradient Boosting classifier (Good/Moderate/Unhealthy)
|
|-- monitoring/
|   |-- backfill_and_evaluate.py           Ingests 2024-2026 data, rolling predictions, drift check
|   |-- drift_analysis.ipynb              Bootstrap CI, Welch's t-test, KS, Levene, CUSUM
|   |-- residual_diagnostics.ipynb         Ljung-Box, Jarque-Bera, ARCH, bias testing
|   |-- robustness.ipynb                   Gaussian noise, Monte Carlo, missing data, outliers, seasonal
|   |-- logs/
|       |-- actual_vs_predicted.csv        778 days of predictions
|       |-- drift_report.txt               Drift evaluation summary
|
|-- analysis/
|   |-- technical_report.ipynb             12 stakeholder questions answered with data
|
|-- dashboard/                             Placeholder for visualisation layer
```

## Data

| Source | Records | Range | Variables |
|--------|---------|-------|-----------|
| LAQN API | 317,760 | 2019-01-01 to 2026-04-02 | NO2, O3, SO2 from 5 London stations |
| Open-Meteo API | 63,624 | 2019-01-01 to 2026-04-04 | Temperature, wind, precipitation, humidity |

Focused on **MY1 (Marylebone Road)**, one of London's busiest roadside monitors. Data stored in SQLite with hourly resolution, cleaned to daily means for modelling.

## Models and Results

| Model | Parameters | Walk-Forward RMSE | Notes |
|-------|-----------|-------------------|-------|
| **ARIMA(1,1,2)** | 3 | **11.61** | Best point forecast, selected for production |
| SARIMA(1,1,2)(1,1,2,7) | 7 | 11.97 | Lower AIC but worse forecast |
| VAR(12) | 432 | 31.14 | Multivariate, 63% worse than ARIMA |
| GARCH(1,1) | 3 | n/a | Models volatility, not mean. Dynamic CI |

## Unseen Data Validation (2024 to 2026)

The ARIMA model was evaluated on **778 days** it was never trained on:

- **RMSE: 9.98** (14% lower than the original test set)
- **No drift** across 10 consecutive quarters
- **Robust**: 3.9% RMSE degradation at 50% input noise (1,000 Monte Carlo simulations)
- Drift threshold derived from 95% bootstrap CI [9.79, 13.31], not an arbitrary percentage

## Key Findings

- NO2 at Marylebone Road dropped **52% since 2019** (62.8 to 30.6 ug/m3), a statistically significant decline (p < 10^-146)
- 2024-2026 pollution levels **match the first COVID lockdown** without any restrictions in place
- Weekdays average **8.5 ug/m3 higher** than weekends (p < 0.000001), confirming traffic as the primary driver
- NO2 is **93% self-driven** (FEVD). Weather explains only 7% of forecast variance
- Rain does **not** reduce NO2. Wind helps by ~6.5 ug/m3 on average
- The model **misses 73% of UK limit exceedance days** (sensitivity 27%). Not suitable for health warning systems without threshold adjustment
- February is the worst month (51 ug/m3 mean, 72% exceedance)
- Sensor outages beyond 3 days do not worsen predictions further. ARIMA converges to the mean within 3 steps

## Setup

```bash
git clone https://github.com/LessonKoirala/time-series-air-quality.git
cd time-series-air-quality
pip install -r requirements.txt
```

### Run the backfill pipeline

```bash
python monitoring/backfill_and_evaluate.py
```

### Notebooks

Open in Jupyter or VS Code. Run in order from `01_data_cleaning` through to `technical_report`.

## Requirements

- Python 3.10+
- pandas, numpy, matplotlib, seaborn
- statsmodels, scipy, scikit-learn
- arch (for GARCH)
- joblib (for parallel robustness testing)
- SQLite (built into Python)

## Further Work

- **LSTM networks** for capturing nonlinear dynamics and improving spike prediction
- **Bayesian time series** for calibrated uncertainty estimates
- Live monitoring pipeline with daily predictions via cron
- Interactive dashboard (HTML/CSS/JS)
- Compute optimisation and containerisation with Docker