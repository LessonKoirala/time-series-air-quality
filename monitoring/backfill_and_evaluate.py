"""
Backfill & Drift Evaluation Pipeline

1. Collect 2024-01-01 → 2026-04-03 from APIs into SQLite
2. Clean new data (same pipeline as Notebook 01)
3. Load saved ARIMA model (arima_best_fit.pkl)
4. Rolling predict 2024-2026, one day at a time
   → Save actual vs predicted to CSV
5. Analyse the CSV → RMSE, quarterly breakdown, drift decision
6. If drift → retrain on 2019-2026 → save new model

Usage: python monitoring/backfill_and_evaluate.py
"""
import sys
import os
import time
import pickle
import sqlite3
import numpy as np
import pandas as pd
from datetime import datetime
from statsmodels.tsa.arima.model import ARIMA
from sklearn.metrics import mean_squared_error, mean_absolute_error
import warnings
warnings.filterwarnings("ignore")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config import (
    DB_PATH, BACKFILL_START, BACKFILL_END,
    HISTORICAL_START, HISTORICAL_END,
    STATIONS,
)
from ingestion.laqn_collector import collect_station
from ingestion.weather_collector import fetch_weather_year
from database.db_connector import get_connection, init_db


SAVED_DIR = os.path.join(os.path.dirname(__file__), "..", "models", "saved")
MONITOR_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(MONITOR_DIR, exist_ok=True)

# Output files
PREDICTIONS_CSV = os.path.join(MONITOR_DIR, "actual_vs_predicted.csv")
DRIFT_REPORT = os.path.join(MONITOR_DIR, "drift_report.txt")


# ═══════════════════════════════════════════════════════════════
# STEP 1: Collect new data from APIs
# ═══════════════════════════════════════════════════════════════

def backfill_pollution():
    """Fetch pollution data for 2024 → 2026-04-03."""
    print("\n[Step 1a] Collecting pollution data from LAQN API...")
    init_db()
    total = 0
    for code, name in STATIONS.items():
        count = collect_station(code, name, BACKFILL_START, BACKFILL_END)
        total += count
    print(f"  Total pollution rows inserted: {total}")
    return total


def backfill_weather():
    """Fetch weather data for 2024 → 2026."""
    print("\n[Step 1b] Collecting weather data from Open-Meteo API...")
    init_db()
    conn = get_connection()

    cursor = conn.execute("SELECT timestamp FROM raw_weather")
    existing = {row[0] for row in cursor.fetchall()}

    start_year = int(BACKFILL_START[:4])
    end_year = int(BACKFILL_END[:4])

    total = 0
    for year in range(start_year, end_year + 1):
        rows = fetch_weather_year(year)
        inserted = 0
        for row in rows:
            if row["timestamp"] in existing:
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
        total += inserted
        print(f"  {year}: {inserted} new rows")

    conn.close()
    print(f"  Total weather rows inserted: {total}")
    return total


# ═══════════════════════════════════════════════════════════════
# STEP 2: Clean data (same pipeline as Notebook 01)
# ═══════════════════════════════════════════════════════════════

def prepare_clean_data():
    """
    Clean ALL data (2019-2026) using the exact same pipeline as Notebook 01:
    1. Filter MY1 station
    2. Force numeric types
    3. Keep no2, o3, so2
    4. Complete hourly reindex
    5. Forward fill ≤3 hours
    6. Drop remaining NaN
    7. Resample to daily mean
    8. Merge with daily weather
    """
    print("\n[Step 2] Cleaning data (same as Notebook 01)...")
    conn = sqlite3.connect(DB_PATH)

    pollution = pd.read_sql("SELECT * FROM raw_pollution WHERE site_code = 'MY1'",
                            conn, parse_dates=["timestamp"])
    pollution.set_index("timestamp", inplace=True)

    weather = pd.read_sql("SELECT * FROM raw_weather", conn, parse_dates=["timestamp"])
    weather.set_index("timestamp", inplace=True)
    conn.close()

    # Force numeric
    for col in ["no2", "pm25", "o3", "so2"]:
        if col in pollution.columns:
            pollution[col] = pd.to_numeric(pollution[col], errors="coerce")

    # Keep only no2, o3, so2
    pollutants = ["no2", "o3", "so2"]
    my1 = pollution[pollutants].copy().sort_index()

    # Complete hourly index (same as Notebook 01)
    full_idx = pd.date_range(my1.index.min(), my1.index.max(), freq="h")
    my1 = my1.reindex(full_idx)
    my1.index.name = "timestamp"

    # Forward fill ≤3 hours, drop rest (same as Notebook 01)
    my1 = my1.ffill(limit=3).dropna()

    # Resample to daily mean
    daily_poll = my1.resample("D").mean().dropna()
    daily_weather = weather.resample("D").mean()

    # Merge
    merged = daily_poll.join(daily_weather, how="inner")

    # Split into training and unseen
    train_data = merged.loc[:HISTORICAL_END]
    unseen_data = merged.loc[BACKFILL_START:]

    print(f"  Training data: {len(train_data)} days ({train_data.index.min().date()} → {train_data.index.max().date()})")
    print(f"  Unseen data:   {len(unseen_data)} days ({unseen_data.index.min().date()} → {unseen_data.index.max().date()})")

    # Verify cleaning matches Notebook 01
    print(f"\n  Verification:")
    print(f"    Training NO2 mean: {train_data['no2'].mean():.2f} (should be ~46.68)")
    print(f"    Training days: {len(train_data)} (should be ~1752)")

    return train_data, unseen_data


# ═══════════════════════════════════════════════════════════════
# STEP 3: Load saved model & rolling predict
# ═══════════════════════════════════════════════════════════════

def rolling_predict(train_data, unseen_data):
    """
    Use the saved ARIMA(1,1,2) model to predict 2024-2026 on a rolling basis.
    For each day:
      1. Predict tomorrow using the saved model's parameters
      2. Record actual vs predicted
      3. Add actual to history (expanding window)
    Save results to CSV.
    """
    print("\n[Step 3] Rolling prediction on unseen data...")

    if len(unseen_data) == 0:
        print("  No unseen data. Check if backfill succeeded.")
        return None

    # Check for cached results
    if os.path.exists(PREDICTIONS_CSV):
        results = pd.read_csv(PREDICTIONS_CSV, parse_dates=["date"], index_col="date")
        print(f"  ✓ Loaded cached predictions from {PREDICTIONS_CSV} ({len(results)} days)")
        return results

    # Load saved model to confirm the order
    model_path = os.path.join(SAVED_DIR, "arima_best_fit.pkl")
    if os.path.exists(model_path):
        saved_model = pickle.load(open(model_path, "rb"))
        order = saved_model.model.order
        print(f"  Loaded saved ARIMA model: order={order}")
    else:
        order = (1, 1, 2)
        print(f"  No saved model found. Using default order={order}")

    # Rolling prediction
    history = list(train_data["no2"].values)
    predictions = []
    actuals = []
    dates = []

    total = len(unseen_data)
    print(f"  Predicting {total} days rolling...")

    for i in range(total):
        # Fit ARIMA on history with same order as saved model
        try:
            model = ARIMA(history, order=order)
            fitted = model.fit()
            pred = fitted.forecast(steps=1)[0]
        except Exception:
            pred = np.mean(history[-30:])

        actual = unseen_data["no2"].values[i]
        predictions.append(pred)
        actuals.append(actual)
        dates.append(unseen_data.index[i])

        # Add actual to history (rolling: always use real data)
        history.append(actual)

        if (i + 1) % 60 == 0:
            rmse_so_far = np.sqrt(mean_squared_error(actuals, predictions))
            print(f"    Day {i+1}/{total} | Running RMSE: {rmse_so_far:.2f}")

    # Save to CSV
    results = pd.DataFrame({
        "date": dates,
        "actual": actuals,
        "predicted": predictions,
    })
    results.set_index("date", inplace=True)
    results["error"] = results["actual"] - results["predicted"]
    results["abs_error"] = np.abs(results["error"])

    results.to_csv(PREDICTIONS_CSV)
    print(f"\n  ✓ Saved actual vs predicted to {PREDICTIONS_CSV}")
    print(f"  Total predictions: {len(results)} days")

    return results


# ═══════════════════════════════════════════════════════════════
# STEP 4: Analyse drift
# ═══════════════════════════════════════════════════════════════

def analyse_drift(results):
    """
    Compare model performance on unseen 2024-2026 vs original 2023 test.
    Decide if retraining is needed.
    """
    print("\n[Step 4] Analysing model drift...")

    if results is None or len(results) == 0:
        print("  No results to analyse.")
        return None, False

    # Overall metrics on unseen data
    rmse = np.sqrt(mean_squared_error(results["actual"], results["predicted"]))
    mae = mean_absolute_error(results["actual"], results["predicted"])
    mape = mae / results["actual"].mean() * 100

    # Original metrics from Notebook 02 (2023 test set)
    original_rmse = 11.61
    original_mae = 9.31
    original_mape = 20.7

    print(f"\n  {'Metric':<10} {'2023 Test':<18} {'2024-2026 Unseen':<18} {'Change'}")
    print(f"  {'-'*65}")
    print(f"  {'RMSE':<10} {original_rmse:<18.2f} {rmse:<18.2f} {((rmse-original_rmse)/original_rmse*100):+.1f}%")
    print(f"  {'MAE':<10} {original_mae:<18.2f} {mae:<18.2f} {((mae-original_mae)/original_mae*100):+.1f}%")
    print(f"  {'MAPE':<10} {original_mape:<18.1f} {mape:<18.1f} {((mape-original_mape)/original_mape*100):+.1f}%")

    # Quarterly breakdown
    results_copy = results.copy()
    results_copy["quarter"] = results_copy.index.to_period("Q")
    quarterly = results_copy.groupby("quarter").agg(
        rmse=("abs_error", lambda x: np.sqrt((x**2).mean())),
        mae=("abs_error", "mean"),
        count=("actual", "count"),
    )

    print(f"\n  Quarterly Breakdown:")
    print(f"  {'Quarter':<12} {'RMSE':<10} {'MAE':<10} {'Days':<8} {'Status'}")
    print(f"  {'-'*50}")
    for idx, row in quarterly.iterrows():
        status = "⚠ DRIFT" if row["rmse"] > original_rmse * 1.5 else "OK"
        print(f"  {str(idx):<12} {row['rmse']:<10.2f} {row['mae']:<10.2f} {int(row['count']):<8} {status}")

    # Drift decision: >30% worse = drift
    drift_threshold = 1.3
    is_drifted = rmse > original_rmse * drift_threshold

    print(f"\n  {'='*50}")
    if is_drifted:
        print(f"  ⚠ MODEL DRIFT DETECTED")
        print(f"  RMSE increased by {((rmse-original_rmse)/original_rmse*100):.1f}%")
        print(f"  Action: RETRAIN on 2019-2026 data")
    else:
        print(f"  ✓ NO SIGNIFICANT DRIFT")
        print(f"  RMSE change: {((rmse-original_rmse)/original_rmse*100):+.1f}%")
        print(f"  Model is still performing within acceptable range")
    print(f"  {'='*50}")

    # Save drift report
    with open(DRIFT_REPORT, "w") as f:
        f.write(f"Drift Report — {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
        f.write(f"{'='*50}\n\n")
        f.write(f"Original RMSE (2023 test): {original_rmse}\n")
        f.write(f"Unseen RMSE (2024-2026):   {rmse:.2f}\n")
        f.write(f"Change: {((rmse-original_rmse)/original_rmse*100):+.1f}%\n")
        f.write(f"Drift detected: {is_drifted}\n\n")
        f.write(f"Quarterly:\n")
        for idx, row in quarterly.iterrows():
            f.write(f"  {str(idx)}: RMSE={row['rmse']:.2f}, MAE={row['mae']:.2f}, Days={int(row['count'])}\n")
    print(f"\n  Report saved to {DRIFT_REPORT}")

    return rmse, is_drifted


# ═══════════════════════════════════════════════════════════════
# STEP 5: Retrain if drift detected
# ═══════════════════════════════════════════════════════════════

def retrain_model(train_data, unseen_data):
    """
    Retrain ARIMA on 2019-2026 data with temporal 80/20 split.
    - Training: first 80% (2019 → ~2024-10)
    - Validation: last 20% (~2024-10 → 2026-04)
    Save new model trained on full data after validation.
    """
    print("\n[Step 5] Retraining with temporal 80/20 split...")
    import shutil

    # Combine all data
    all_no2 = pd.concat([train_data["no2"], unseen_data["no2"]])
    total_days = len(all_no2)

    # 80/20 temporal split
    split_idx = int(total_days * 0.8)
    retrain_train = all_no2.iloc[:split_idx]
    retrain_val = all_no2.iloc[split_idx:]

    print(f"  All data: {total_days} days ({all_no2.index.min().date()} → {all_no2.index.max().date()})")
    print(f"  Train (80%): {len(retrain_train)} days ({retrain_train.index.min().date()} → {retrain_train.index.max().date()})")
    print(f"  Validation (20%): {len(retrain_val)} days ({retrain_val.index.min().date()} → {retrain_val.index.max().date()})")

    # Train on 80%
    model_80 = ARIMA(retrain_train, order=(1, 1, 2))
    fit_80 = model_80.fit()

    # Validate: walk-forward on 20%
    history = list(retrain_train.values)
    val_preds = []
    for i in range(len(retrain_val)):
        try:
            m = ARIMA(history, order=(1, 1, 2))
            f = m.fit()
            pred = f.forecast(steps=1)[0]
        except Exception:
            pred = np.mean(history[-30:])
        val_preds.append(pred)
        history.append(retrain_val.values[i])

        if (i + 1) % 30 == 0:
            print(f"    Validation step {i+1}/{len(retrain_val)}...")

    val_rmse = np.sqrt(mean_squared_error(retrain_val.values, val_preds))
    val_mae = mean_absolute_error(retrain_val.values, val_preds)
    val_mape = val_mae / retrain_val.mean() * 100

    print(f"\n  Validation Results (20% holdout):")
    print(f"    RMSE: {val_rmse:.2f}")
    print(f"    MAE:  {val_mae:.2f}")
    print(f"    MAPE: {val_mape:.1f}%")

    # Now train final model on ALL data (2019-2026) for production
    print(f"\n  Training final model on all {total_days} days...")
    final_model = ARIMA(all_no2, order=(1, 1, 2))
    final_fit = final_model.fit()

    # Backup old model, save new one
    new_model_path = os.path.join(SAVED_DIR, "arima_best_fit.pkl")
    old_model_path = os.path.join(SAVED_DIR, "arima_best_fit_2023.pkl")

    if os.path.exists(new_model_path):
        shutil.copy(new_model_path, old_model_path)
        print(f"  Old model backed up to {old_model_path}")

    pickle.dump(final_fit, open(new_model_path, "wb"))
    print(f"  ✓ New model saved to {new_model_path}")
    print(f"  AIC: {final_fit.aic:.1f}")

    # Save validation results
    val_path = os.path.join(MONITOR_DIR, "retrain_validation.csv")
    val_df = pd.DataFrame({
        "date": retrain_val.index,
        "actual": retrain_val.values,
        "predicted": val_preds,
    })
    val_df.to_csv(val_path, index=False)
    print(f"  Validation results saved to {val_path}")

    return final_fit


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("BACKFILL & DRIFT EVALUATION PIPELINE")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    start_time = time.time()

    # Step 1: Collect new data from APIs
    backfill_pollution()
    backfill_weather()

    # Step 2: Clean data (same as Notebook 01)
    train_data, unseen_data = prepare_clean_data()

    # Step 3: Rolling predict on unseen data → save to CSV
    results = rolling_predict(train_data, unseen_data)

    # Step 4: Analyse drift
    rmse, is_drifted = analyse_drift(results)

    # Step 5: Retrain if drift detected
    if is_drifted:
        retrain_model(train_data, unseen_data)
    else:
        print("\n[Step 5] No retraining needed. Model is still good.")

    elapsed = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"PIPELINE COMPLETE — {int(elapsed//60)}m {int(elapsed%60)}s")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()