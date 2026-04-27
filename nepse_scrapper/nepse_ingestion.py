from nepse_scraper import NepseScraper

# Initialize the client (SSL verification disabled per user request)
scraper = NepseScraper(verify_ssl=False)

def is_market_open():
    """Checks if the NEPSE market is currently open."""
    return scraper.is_market_open()

def fetch_today_prices():
    """Fetches today's price data for all companies."""
    try:
        return scraper.get_today_price()
    except Exception as e:
        print(f"Error fetching prices: {e}")
        return []

def fetch_brokers():
    """Fetches all registered brokers."""
    try:
        return scraper.get_brokers()
    except Exception as e:
        print(f"Error fetching brokers: {e}")
        return []

def fetch_top_stocks(category="top_gainer"):
    """Fetches top stocks based on category."""
    try:
        return scraper.get_top_stocks(category=category, show_all=False)
    except Exception as e:
        print(f"Error fetching top stocks ({category}): {e}")
        return []

def fetch_ticker_history(symbol, start_date, end_date):
    """Fetches historical price data for a specific ticker."""
    try:
        history = scraper.get_ticker_price_history(
            ticker=symbol,
            start_date=start_date,
            end_date=end_date
        )
        return history.get('content') if history else []
    except Exception as e:
        print(f"Error fetching history for {symbol}: {e}")
        return []

