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

def fetch_ticker_info(symbol):
    """Fetches company's info."""
    try:
        return scraper.get_ticker_info(symbol)
    except Exception as e:
        print(f"Error fetching prices: {e}")
        return []

