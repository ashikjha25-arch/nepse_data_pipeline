import time
from nepse_scrapper.nepse_ingestion import fetch_nepse_data

def run_scrapper():
    time.sleep(60)
    try:
        data = fetch_nepse_data()
        return {"data": data}
    except Exception as e:
        return {"exception": str(e)}