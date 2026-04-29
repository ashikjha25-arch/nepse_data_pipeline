import os
import joblib

from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score

from ml.db import read_stock_data


MODEL_PATH = "ml/models/linear_regression_model.pkl"


def train_model():
    df = read_stock_data()

    if df.empty:
        print("No stock data found for training.")
        return False

    features = ["open_price", "high_price", "low_price", "volume", "turnover"]
    target = "close_price"

    df = df.dropna(subset=features + [target])

    if len(df) < 10:
        print("Not enough data for training. Need at least 10 records.")
        return False

    X = df[features].astype(float)
    y = df[target].astype(float)

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42
    )

    model = LinearRegression()
    model.fit(X_train, y_train)

    predictions = model.predict(X_test)

    mae = mean_absolute_error(y_test, predictions)
    r2 = r2_score(y_test, predictions)

    print(f"Linear Regression trained successfully.")
    print(f"MAE: {mae:.2f}")
    print(f"R2 Score: {r2:.4f}")

    os.makedirs("ml/models", exist_ok=True)
    joblib.dump(model, MODEL_PATH)

    print(f"Model saved at {MODEL_PATH}")

    return True