import os
import joblib

from ml.db import read_stock_data, insert_forecast

MODEL_PATH = "ml/models/linear_regression_model.pkl"
MODEL_NAME = "Multiple Linear Regression"

def predict_latest():
    if not os.path.exists(MODEL_PATH):
        print("Model file not found. Train model first.")
        return False

    df = read_stock_data()

    if df.empty:
        print("No stock data found for prediction.")
        return False

    features = ["open_price", "high_price", "low_price", "volume", "turnover"]

    df = df.dropna(subset=features + ["close_price"])

    if df.empty:
        print("No valid rows after removing null values.")
        return False

    model = joblib.load(MODEL_PATH)

    latest_rows = (
        df.sort_values("issue_date")
        .groupby("symbol")
        .tail(1)
    )

    for _, row in latest_rows.iterrows():
        X_latest = row[features].astype(float).values.reshape(1, -1)

        predicted_close = model.predict(X_latest)[0]

        insert_forecast(
            symbol=row["symbol"],
            actual_close=row["close_price"],
            predicted_close=predicted_close,
            model_name=MODEL_NAME
        )

        print(
            f"{row['symbol']} | Actual: {row['close_price']} | "
            f"Predicted: {round(float(predicted_close), 2)}"
        )

    return True


