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
                # If response is wrapped like {"nabil_info": {...}}, unwrap it
                if len(company_data) == 1 and isinstance(next(iter(company_data.values())), dict):
                    company_data = next(iter(company_data.values()))
        
                security = company_data.get("security", {})
                company = security.get("companyId", {})
                sector_master = company.get("sectorMaster", {})
        
                company_name = company.get("companyName")
                sector = sector_master.get("sectorDescription", "Unknown")
        
                messages.append({
                    "data_type": "company_info",
                    "payload": {
                        "symbol": symbol,
                        "company_name": company_name,
                        "sector": sector
                    },
                    "fetched_at": fetched_at
                })

    except Exception as e:
        messages.append({
            "data_type": "error",
            "payload": {"exception": str(e)},
            "fetched_at": fetched_at
        })

    return messages

def run_history_scrapper(ticker, start_date, end_date):
    fetched_at = datetime.utcnow().isoformat()
    messages = []

    try:
        history = nepse_ingestion.scraper.get_ticker_price_history(
            ticker=ticker,
            start_date=start_date,
            end_date=end_date
        )

        records = history.get("content", [])

        trades = []
        company_info_added = False

        for item in records:
            company = item.get("security", {}).get("companyId", {})
            sector_master = company.get("sectorMaster", {})
            sector = sector_master.get("sectorDescription", "Unknown")

            symbol = ticker

            trades.append({
                "business_date": item.get("businessDate"),
                "symbol": symbol,
                "open_price": item.get("openPrice"),
                "high_price": item.get("highPrice"),
                "low_price": item.get("lowPrice"),
                "close_price": item.get("closePrice") or item.get("lastTradedPrice"),
                "volume": item.get("totalTradedQuantity"),
                "turnover": item.get("totalTradedValue")
            })

            if not company_info_added:
                messages.append({
                    "data_type": "company_info",
                    "payload": {
                        "symbol": symbol,
                        "company_name": company.get("companyName"),
                        "sector": sector
                    },
                    "fetched_at": fetched_at
                })
                company_info_added = True

        if trades:
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

    return messages

