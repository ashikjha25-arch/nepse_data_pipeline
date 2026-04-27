import time
from datetime import datetime
from nepse_scrapper.nepse_ingestion import is_open

def run_scrapper():
    time.sleep(60)

    fetched_at = datetime.utcnow().isoformat()

    try:
        market_status_data = is_open()

        return [
            {
                "data_type": "market_open_status",
                "payload": market_status_data,
                "fetched_at": fetched_at
            }
        ]

    except Exception as e:
        return [
            {
                "data_type": "error",
                "payload": {
                    "exception": str(e)
                },
                "fetched_at": fetched_at
            }
        ]