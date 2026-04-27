from fastapi import FastAPI, HTTPException
from db import repository as repo

# Ensure DB schema is ready on startup
repo.ensure_tables_exist()

app = FastAPI(title="NEPSE Market Data API")

@app.get("/")
def read_root():
    return {"message": "NEPSE Market Data API is running"}

@app.get("/market/status")
def get_market_status():
    """Returns whether the market is currently open."""
    status = repo.get_market_status()
    return {"status": "open" if status else "closed", "raw": status}

@app.get("/inspect/stats")
def table_statistics():
    """Summary of data volume across all tables."""
    try:
        counts = repo.get_table_counts()
        return {"status": "success", "counts": counts}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/inspect/trades")
def preview_trades(limit: int = 10):
    """View recent trade data."""
    data = repo.inspect_table_data("nepse.daily_trades", limit)
    return {"data": data}

@app.get("/inspect/securities")
def preview_securities(limit: int = 50):
    """View list of registered companies/securities."""
    data = repo.inspect_table_data("nepse.securities", limit)
    return {"data": data}

@app.get("/inspect/price_history")
def preview_price_history(symbol: str, limit: int = 20):
    """View recent price history for a specific symbol."""
    data = repo.inspect_price_history(symbol, limit)
    return {"data": data}

@app.get("/inspect/top_stocks")
def preview_top_stocks(limit: int = 20):
    """View recent snapshots of top performers."""
    data = repo.inspect_table_data("nepse.top_stocks", limit)
    return {"data": data}

@app.get("/inspect/spark")
def preview_spark_results(limit: int = 20):
    """Fetch recent analytics from Spark computations."""
    data = repo.inspect_table_data("nepse.spark_analytics", limit)
    return {"data": data}

