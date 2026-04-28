import time
from datetime import datetime
from nepse_scrapper import nepse_ingestion
from db.repository import store_prev_data

def run_scrapper():
    """Main orchestration for data scraping."""
    time.sleep(5) 
    
    fetched_at = datetime.utcnow().isoformat()
    messages = []

    try:
        # 1. Market Status
        is_open = nepse_ingestion.is_market_open()
        messages.append({
            "data_type": "market_open_status",
            "payload": is_open,
            "fetched_at": fetched_at
        })

        # 2. Fetch Prices (Facts & Dimension Discovery)
        # We always check for prices because the list of listed companies 
        # (securites) is essentially discovered here.
        price_data = nepse_ingestion.fetch_today_prices()
        symbols = []
        if price_data:
            # Map for daily_trades table
            trades = []

            for item in price_data:
                # Scraper keys can vary
                symbol = item.get('symbol')
                open_p = item.get('openPrice')
                high_p = item.get('highPrice') 
                low_p = item.get('lowPrice')
                close_p = item.get('closePrice') 
                volume = item.get('totalTradedQuantity')
                turnover = item.get('totalTradedValue') 

                if symbol:

                    symbols.append(symbol)

                    # prepare stock price history
                    trades.append({
                        "business_date": datetime.now().date().isoformat(),
                        "symbol": symbol,
                        "open_price": open_p,
                        "high_price": high_p,
                        "low_price": low_p,
                        "close_price": close_p,
                        "volume": volume,
                        "turnover": turnover
                    })

            messages.append({"data_type": "stock_price_history", "payload": trades, "fetched_at": fetched_at})
        
        # fetch company info
        for symbol in symbols:
            company_data = nepse_ingestion.fetch_ticker_info(symbol)
            if company_data:
                messages.append({
                    "data_type": "company_info",
                    "payload": {
                        "symbol": symbol, 
                        "company_name": company_data['companyId']['companyName'],
                        "sector": company_data['companyId']['sectorMaster']['sectorDescription']
                    },
                })

    except Exception as e:
        messages.append({
            "data_type": "error",
            "payload": {"exception": str(e)},
            "fetched_at": fetched_at
        })

    return messages

fetched_at = datetime.utcnow().isoformat()

def run_history_scrapper(ticker, start_date, end_date):
    messages = []

    try:
        history = nepse_ingestion.scraper.get_ticker_price_history(
            ticker=ticker,
            start_date=start_date,
            end_date=end_date
        )

        records = history.get("content", [])

        trades = []

        for item in records:
            company = item.get("security", {}).get("companyId", {})
            sectorMaster = company.get("sectorMaster", {})
            sector = sectorMaster.get("sectorDescription", "Unknown")

            symbol = ticker

            # stock price
            trades.append({
                "business_date": item.get("businessDate"),
                "symbol": symbol,
                "open_price": item.get("openPrice"),
                "high_price": item.get("highPrice"), 
                "low_price": item.get("lowPrice"),
                "close_price": item.get("closePrice") or item.get("lastTradedPrice"),
                "volume": item.get("totalTradeQuantity"),
                "turnover": item.get("totalTradedValue")
            })

            # company info append
            messages.append({
                "data_type": "company_info",
                "payload": {
                    "symbol": symbol,
                    "company_name": company.get("companyName"),
                    "sector": sector
                },
                "fetched_at": fetched_at
            })
        # stock price history append
        messages.append({
            "data_type": "stock_price_history",
            "payload": trades,
            "fetched_at": fetched_at
        })

    except Exception as e:
        messages.append({
            "data_type": "error",
            "payload": {"exception": str(e)},
            "fetched_at": fetched_at
        }) 

    try:
        store_prev_data(messages)
        return True
    except Exception as e:
        print(f'Exception occurred while storing prev data: {e}')
        return False

