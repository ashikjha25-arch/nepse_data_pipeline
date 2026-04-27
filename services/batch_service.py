import time
from datetime import datetime
from nepse_scrapper import nepse_ingestion

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
        
        if price_data:
            # Map for daily_trades table
            trades = []
            # Map for securities table (Dimensions)
            securities = []
            
            news = []


            for item in price_data:
                # Scraper keys can vary, we try common variations
                symbol = item.get('symbol') or item.get('ticker')
                open_p = item.get('openPrice') or item.get('open')
                high_p = item.get('highPrice') or item.get('maxPrice') or item.get('high')
                low_p = item.get('lowPrice') or item.get('minPrice') or item.get('low')
                close_p = item.get('closePrice') or item.get('lastTradedPrice') or item.get('ltp')
                qty = item.get('totalTradedQuantity') or item.get('quantity') or item.get('vol')
                value = item.get('totalTradedValue') or item.get('turnover')

                if symbol:
                    # Prepare trade fact (OHLCV)
                    trades.append({
                        "symbol": symbol,
                        "business_date": datetime.now().date().isoformat(),
                        "open_price": open_p,
                        "high_price": high_p,
                        "low_price": low_p,
                        "close_price": close_p,
                        "total_traded_quantity": qty,
                        "total_traded_value": value
                    })
                    # Prepare security dimension
                    securities.append({
                        "symbol": symbol,
                        "security_name": item.get('securityName') or item.get('companyName') or symbol,
                        "company_name": item.get('securityName') or item.get('companyName') or symbol
                    })

            messages.append({"data_type": "daily_trades", "payload": trades, "fetched_at": fetched_at})
            messages.append({"data_type": "securities", "payload": securities, "fetched_at": fetched_at})
        
        # 3. Fetch Top Stocks (Gainers & Losers)
        for cat in ["top_gainer", "top_loser"]:
            top_data = nepse_ingestion.fetch_top_stocks(cat)
            if top_data:
                messages.append({
                    "data_type": "top_stocks",
                    "payload": {"category": cat, "data": top_data},
                    "fetched_at": fetched_at
                })


        # 4. Fetch Broker Dimensions 
        brokers = nepse_ingestion.fetch_brokers()
        if brokers:
            formatted_brokers = [
                {
                    "member_code": b.get('memberCode'),
                    "member_name": b.get('memberName'),
                    "address": b.get('address')
                } for b in brokers
            ]
            messages.append({"data_type": "brokers", "payload": formatted_brokers, "fetched_at": fetched_at})

    except Exception as e:
        messages.append({
            "data_type": "error",
            "payload": {"exception": str(e)},
            "fetched_at": fetched_at
        })

    return messages
