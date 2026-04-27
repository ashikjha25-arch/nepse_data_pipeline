from nepse_scraper import NepseScraper

# initialize the client (with SSL verification disabled as recommended)
scraper = NepseScraper(verify_ssl=False)

def is_open():
    # check if the market is open
    is_open = scraper.is_market_open()
    return is_open

def fetch_nepse_data():
    if is_open():   
        # fetch today's price data for all companies
        try:
            today_prices = scraper.get_today_price()
            if today_prices:
                # print(f'Data: {today_prices}')
                return today_prices
            return "Could not fetch today's data"
        except Exception as e:
            # print(f"Data: {e}")
            raise
    # print('Market not open')
    return 'Market not open'    

    