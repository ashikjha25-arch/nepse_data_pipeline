from db.db_connection import get_db_connection

def ensure_tables_exist():
    """Manually creates all tables if they don't exist (failsafe for init.sql)."""
    conn, cur = get_db_connection()
    try:
        cur.execute("CREATE SCHEMA IF NOT EXISTS nepse;")
        
        # 1. Status Log
        cur.execute("""
            CREATE TABLE IF NOT EXISTS nepse.status_log (
                checked_date date PRIMARY KEY DEFAULT CURRENT_DATE,
                is_open boolean NOT NULL,
                checked_at timestamp DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # 2. Securities
        cur.execute("""
            CREATE TABLE IF NOT EXISTS nepse.securities (
                symbol text PRIMARY KEY,
                security_name text,
                company_name text,
                updated_at timestamp DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # 3. Daily Trades
        cur.execute("""
            CREATE TABLE IF NOT EXISTS nepse.daily_trades (
                id bigserial PRIMARY KEY,
                symbol text REFERENCES nepse.securities(symbol),
                business_date date NOT NULL,
                open_price numeric(12, 2),
                high_price numeric(12, 2),
                low_price numeric(12, 2),
                close_price numeric(12, 2),
                total_traded_quantity bigint,
                total_traded_value numeric(20, 2),
                UNIQUE (symbol, business_date)
            );
        """)

        # 4. Brokers
        cur.execute("""
            CREATE TABLE IF NOT EXISTS nepse.brokers (
                member_code text PRIMARY KEY,
                member_name text,
                address text
            );
        """)

        # 5. Spark Analytics
        cur.execute("""
            CREATE TABLE IF NOT EXISTS nepse.spark_analytics (
                id serial PRIMARY KEY,
                symbol text REFERENCES nepse.securities(symbol),
                indicator_name text,
                value numeric(12, 2),
                calculated_at timestamp DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_view ON nepse.daily_trades (business_date DESC);")
        conn.commit()
    finally:
        cur.close()
        conn.close()

def get_market_status():
    """Checks if the market is open for current date."""
    conn, cur = get_db_connection()
    try:
        cur.execute("""
            SELECT is_open FROM nepse.status_log 
            WHERE checked_date = CURRENT_DATE;
        """)
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        cur.close()
        conn.close()

def get_table_counts():
    """Returns row counts for all tables."""
    conn, cur = get_db_connection()
    tables = ["status_log", "securities", "daily_trades", "brokers", "spark_analytics"]
    counts = {}
    try:
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM nepse.{table};")
            counts[table] = cur.fetchone()[0]
        return counts
    finally:
        cur.close()
        conn.close()

def inspect_table_data(table_name, limit=10):
    """Fetches sample data from specified table."""
    conn, cur = get_db_connection()
    try:
        cur.execute(f"SELECT * FROM {table_name} LIMIT %s;", (limit,))
        colnames = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        return [dict(zip(colnames, row)) for row in rows]
    finally:
        cur.close()
        conn.close()

def route_and_insert_data(cur, data_type, payload, fetched_at=None):
    """Routes incoming Kafka payload to specific insert functions."""
    if data_type == "market_open_status":
        insert_market_status(cur, payload, fetched_at)
    elif data_type == "daily_trades":
        insert_daily_trades(cur, payload)
    elif data_type == "securities":
        upsert_securities(cur, payload)
    elif data_type == "brokers":
        upsert_brokers(cur, payload)
    elif data_type == "spark_analytics":
        insert_spark_analytics(cur, payload)
    else:
        print(f"Unknown data_type: {data_type}")

def insert_market_status(cur, is_open, fetched_at):
    cur.execute("""
        INSERT INTO nepse.status_log (checked_date, is_open, checked_at)
        VALUES (CURRENT_DATE, %s, %s)
        ON CONFLICT (checked_date)
        DO UPDATE SET is_open = EXCLUDED.is_open, checked_at = EXCLUDED.checked_at;
    """, (is_open, fetched_at))

def insert_top_stocks(cur, payload):
    """Inserts a batch of top stocks by category."""
    category = payload.get('category')
    stocks = payload.get('data', [])
    for stock in stocks:
        cur.execute("""
            INSERT INTO nepse.top_stocks (category, symbol, ltp, point_change, percentage_change)
            VALUES (%s, %s, %s, %s, %s);
        """, (
            category, 
            stock.get('symbol'), 
            stock.get('ltp'), 
            stock.get('pointChange'), 
            stock.get('percentageChange')
        ))

def upsert_securities(cur, securities_list):
    """Upserts metadata for securities (companies)."""
    for sec in securities_list:
        cur.execute("""
            INSERT INTO nepse.securities (symbol, security_name, company_name, updated_at)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (symbol) DO UPDATE SET
                security_name = EXCLUDED.security_name,
                company_name = EXCLUDED.company_name,
                updated_at = CURRENT_TIMESTAMP;
        """, (sec['symbol'], sec.get('security_name'), sec.get('company_name')))

def insert_daily_trades(cur, trades_list):
    """Inserts daily trade data for provided symbols with OHLCV data."""
    for trade in trades_list:
        cur.execute("""
            INSERT INTO nepse.daily_trades 
            (symbol, business_date, open_price, high_price, low_price, close_price, total_traded_quantity, total_traded_value)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, business_date) DO UPDATE SET
                open_price = EXCLUDED.open_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                close_price = EXCLUDED.close_price,
                total_traded_quantity = EXCLUDED.total_traded_quantity,
                total_traded_value = EXCLUDED.total_traded_value;
        """, (
            trade['symbol'], 
            trade['business_date'], 
            trade.get('open_price'),
            trade.get('high_price'),
            trade.get('low_price'),
            trade['close_price'], 
            trade['total_traded_quantity'], 
            trade['total_traded_value']
        ))

def upsert_brokers(cur, brokers_list):
    """Upserts broker metadata."""
    for broker in brokers_list:
        cur.execute("""
            INSERT INTO nepse.brokers (member_code, member_name, address)
            VALUES (%s, %s, %s)
            ON CONFLICT (member_code) DO UPDATE SET
                member_name = EXCLUDED.member_name,
                address = EXCLUDED.address;
        """, (broker['member_code'], broker.get('member_name'), broker.get('address')))

def insert_spark_analytics(cur, metrics):
    """Stores calculated results from Spark."""
    cur.execute("""
        INSERT INTO nepse.spark_analytics (symbol, indicator_name, value, calculated_at)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP);
    """, (metrics['symbol'], metrics['indicator'], metrics['value']))
