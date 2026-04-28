from fastapi import FastAPI, HTTPException
from db import repository as repo
from services.batch_service import run_history_scrapper

app = FastAPI(title="NEPSE Market Data API")

@app.get("/")
def read_root():
    return {"message": "NEPSE Market Data API is running"}

@app.get("/get/market_status")
def market_status():
    try:
        data = repo.get_market_status()
        return {
            "success": True,
            "result": data
        }
    except Exception as e:
        print(f"Exception occurred in /market_status: {e}")
        return {
            "success": False,
            "result": "Exception occurred, please try again"
        }

@app.get("/get/company_info")
def company_info():
    try:
        data = repo.get_company_info()
        return {
            "success": True,
            "result": data
        }
    except Exception as e:
        print(f"Exception occurred in /company_info: {e}")
        return {
            "success": False,
            "result": "Exception occurred, please try again"
        }

@app.get("/get/stock_price")
def stock_price(symbol: str = None, limit: int = 100):
    try:
        data = repo.get_stock_price(symbol, limit)
        return {
            "success": True,
            "result": data
        }
    except Exception as e:
        print(f"Exception occurred in /stock_price: {e}")
        return {
            "success": False,
            "result": "Exception occurred, please try again"
        }

@app.get("/get/model_pred")
def model_pred(symbol: str = None, model_name: str = None, limit: int = 100):
    try:
        data = repo.get_model_pred(symbol, model_name, limit)
        return {
            "success": True,
            "result": data
        }
    except Exception as e:
        print(f"Exception occurred in /model_pred: {e}")
        return {
            "success": False,
            "result": "Exception occurred, please try again"
        }

@app.get("/store/prev_data")
def store_prev_data():
    symbols = ["HBLD86", "SHINED", "SBD89", "RMF1", "ICFCD88", "SBID89", "NABILD2089", "GIBF1", "SBID2090", "CMF2", "CBLD88"]
    for symbol in symbols:
        result = run_history_scrapper(symbol, start_date='2026-04-26', end_date='2026-04-27')
        print(f'Stored for {symbol}: {result}')
    
    return {'status': "Check logs"}


