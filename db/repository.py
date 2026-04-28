from db.db_connection import get_db_connection

def route_and_insert_data(cur, data_type, payload, fetched_at = None):
    if data_type == "stock_price_history":
        insert_stock_price(cur, payload, fetched_at)
    elif data_type == "company_info":
        insert_company_info(cur, payload, fetched_at)
    elif data_type == "market_open_status":
        insert_market_status(cur, payload, fetched_at)
    elif data_type == "stock_forecast":
        insert_stock_forecast(cur, payload, fetched_at)

def insert_market_status(cur, data, fetched_at):
    try:
        is_open = data

        cur.execute("""
            INSERT INTO nepse.status_log (
                checked_date,
                is_open,
                checked_at
            )
            VALUES (
                CURRENT_DATE,
                %s,
                %s
            )
            ON CONFLICT (checked_date)
            DO UPDATE SET
                is_open = EXCLUDED.is_open,
                checked_at = EXCLUDED.checked_at;
        """, (is_open, fetched_at))

    except Exception as e:
        print(f"Exception occurred in insert_market_status: {e}")

def insert_stock_price(cur, data, fetched_at):
    try:
        # If single dict comes, convert it into list
        if isinstance(data, dict):
            data = [data]

        for item in data:
            print(f"Inserting stock price data: {item['symbol']} on {item['business_date']}")

            cur.execute("""
                INSERT INTO nepse.stock_price_history (
                    issue_date,
                    symbol,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                    turnover
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (issue_date, symbol)
                DO UPDATE SET
                    open_price = EXCLUDED.open_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price,
                    volume = EXCLUDED.volume,
                    turnover = EXCLUDED.turnover;
            """, (
                item["business_date"],
                item["symbol"],
                item["open_price"],
                item["high_price"],
                item["low_price"],
                item["close_price"],
                item["volume"],
                item["turnover"]
            ))

    except Exception as e:
        print(f"Exception occurred in insert_stock_price: {e}")
        raise

def insert_company_info(cur, data, fetched_at):
    try:
        cur.execute("""
            INSERT INTO nepse.company_info (
                symbol,
                company_name,
                sector
            )
            VALUES (%s, %s, %s)
            ON CONFLICT (symbol)
            DO UPDATE SET
                company_name = EXCLUDED.company_name,
                sector = EXCLUDED.sector;
        """, (
            data.get("symbol"),
            data.get("company_name"),
            data.get("sector")  
        ))

    except Exception as e:
        print(f"Exception occurred in insert_company_info: {e}")

def insert_stock_forecast(cur, data, fetched_at):
    try:
        cur.execute("""
            INSERT INTO nepse.stock_forecast (
                forecast_date,
                symbol,
                actual_close,
                predicted_close,
                model_name
            )
            VALUES (
                CURRENT_DATE,
                %s,
                %s,
                %s,
                %s
            )
            ON CONFLICT (forecast_date, symbol, model_name)
            DO UPDATE SET
                actual_close = EXCLUDED.actual_close,
                predicted_close = EXCLUDED.predicted_close;
        """, (
            data.get("symbol"),
            data.get("actual_close"),
            data.get("predicted_close"),
            data.get("model_name")
        ))

    except Exception as e:
        print(f"Exception occurred in insert_stock_forecast: {e}")

def get_market_status():
    conn, cur = get_db_connection()

    try:
        cur.execute("""
            SELECT 
                checked_date,
                is_open,
                fetched_at
            FROM nepse.status_log
            ORDER BY checked_date DESC
            LIMIT 1;
        """)

        row = cur.fetchone()

        if not row:
            return None

        return {
            "checked_date": row[0],
            "is_open": row[1],
            "fetched_at": row[2]
        }

    except Exception as e:
        print(f"Exception occurred in get_market_status: {e}")
        return None

    finally:
        cur.close()
        conn.close()

def get_company_info():
    conn, cur = get_db_connection()

    try:
        cur.execute("""
            SELECT 
                symbol,
                company_name,
                sector
            FROM nepse.company_info
            ORDER BY symbol ASC;
        """)

        rows = cur.fetchall()

        return [
            {
                "symbol": row[0],
                "company_name": row[1],
                "sector": row[2]
            }
            for row in rows
        ]

    except Exception as e:
        print(f"Exception occurred in get_company_info: {e}")
        return []

    finally:
        cur.close()
        conn.close()

def get_stock_price(symbol=None, limit=100):
    conn, cur = get_db_connection()

    try:
        if symbol:
            cur.execute("""
                SELECT 
                    issue_date,
                    symbol,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                    turnover
                FROM nepse.stock_price_history
                WHERE symbol = %s
                ORDER BY issue_date DESC
                LIMIT %s;
            """, (symbol, limit))
        else:
            cur.execute("""
                SELECT 
                    issue_date,
                    symbol,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                    turnover
                FROM nepse.stock_price_history
                ORDER BY issue_date DESC
                LIMIT %s;
            """, (limit,))

        rows = cur.fetchall()

        return [
            {
                "issue_date": row[0],
                "symbol": row[1],
                "open_price": row[2],
                "high_price": row[3],
                "low_price": row[4],
                "close_price": row[5],
                "volume": row[6],
                "turnover": row[7]
            }
            for row in rows
        ]

    except Exception as e:
        print(f"Exception occurred in get_stock_price: {e}")
        return []

    finally:
        cur.close()
        conn.close()

def get_model_pred(symbol=None, model_name=None, limit=100):
    conn, cur = get_db_connection()

    try:
        query = """
            SELECT 
                forecast_date,
                symbol,
                actual_close,
                predicted_close,
                model_name
            FROM nepse.stock_forecast
            WHERE 1=1
        """

        params = []

        if symbol:
            query += " AND symbol = %s"
            params.append(symbol)

        if model_name:
            query += " AND model_name = %s"
            params.append(model_name)

        query += """
            ORDER BY forecast_date DESC
            LIMIT %s;
        """
        params.append(limit)

        cur.execute(query, tuple(params))

        rows = cur.fetchall()

        return [
            {
                "forecast_date": row[0],
                "symbol": row[1],
                "actual_close": row[2],
                "predicted_close": row[3],
                "model_name": row[4]
            }
            for row in rows
        ]

    except Exception as e:
        print(f"Exception occurred in get_model_pred: {e}")
        return []

    finally:
        cur.close()
        conn.close()

def store_prev_data(messages):
    conn, cur = get_db_connection()
    try:
        for message in messages:
            if message['data_type'] == "stock_price_history":
                insert_stock_price(
                    cur, 
                    message['payload'], 
                    message['fetched_at']
                )
            elif message['data_type'] == "company_info":
                insert_company_info(
                    cur, 
                    message['payload'], 
                    message['fetched_at']
                )
        conn.commit()  # Commit after each stock price insert to ensure data is saved
        print('Previous data stored successfully in the database.')
    except Exception as e:
        conn.rollback()  # Rollback in case of any error to maintain data integrity
        print(f"Exception occurred while storing previous data: {e}")
        raise
    finally:
        cur.close()
        conn.close()



