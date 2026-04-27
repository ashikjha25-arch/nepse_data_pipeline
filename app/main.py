from fastapi import FastAPI
from db import repository as repo

app = FastAPI()

@app.get("/inspect/stats")
def table_statistics():
    # check row counts for all tables
    return {"status": "success", "counts": repo.get_table_counts()}

@app.get("/inspect/trades")
def preview_trades():
    # see the last 10 trades saved in the db
    return {"data": repo.inspect_table_data("market.daily_trades")}

@app.get("/inspect/securities")
def preview_securities():
    # see the list of companies registered in the db
    return {"data": repo.inspect_table_data("market.securities")}

@app.get("/inspect/spark")
def preview_spark_results():
    # verify if the spark teammate has saved any analysis
    return {"data": repo.inspect_table_data("market.spark_analytics")}