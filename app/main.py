from fastapi import FastAPI
from kafka_layer.consumer import get_latest_data

app = FastAPI()

@app.get("/nepse_data")
def get_nepse_data():
    data = get_latest_data()
    if data is None:
        return {"message": "No data consumed yet"}
    return {"data": data}