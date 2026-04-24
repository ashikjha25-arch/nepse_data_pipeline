from fastapi import FastAPI
from db.repository import get_nepse_data

app = FastAPI()

@app.get("/nepse_data")
def get_nepse_data():
    data = get_nepse_data()
    if data is None:
        return {"message": "No data consumed yet"}
    return {"data": data}