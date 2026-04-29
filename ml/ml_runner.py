import time

from ml.train_model import train_model
from ml.predict import predict_latest


ML_INTERVAL_SECONDS = 120  # 2 minutes


def run_ml_loop():
    print("Starting ML background service...")

    while True:
        try:
            print("Training Linear Regression model...")
            trained = train_model()

            if trained:
                print("Generating predictions...")
                predict_latest()
            else:
                print("Skipping prediction because model was not trained.")

        except Exception as e:
            print(f"ML service error: {e}")

        print(f"ML service sleeping for {ML_INTERVAL_SECONDS} seconds...")
        time.sleep(ML_INTERVAL_SECONDS)


if __name__ == "__main__":
    run_ml_loop()