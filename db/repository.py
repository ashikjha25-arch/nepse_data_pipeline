from db.db_connection import get_db_connection

def get_market_status():
    conn, cur = get_db_connection()
    try:
        cur.execute("""
            SELECT is_open
            FROM nepse.status_log
            WHERE checked_date = CURRENT_DATE;
        """)

        row = cur.fetchone()
        return row[0] if row else None
    except Exception:
        raise
    finally:
        cur.close()
        conn.close()

def route_and_insert_data(cur, data_type, payload, fetched_at=None):
    if data_type == "market_open_status":
        insert_market_status(cur, payload, fetched_at)
    else:
        raise ValueError(f"Unknown data_type: {data_type}")

def insert_market_status(cur, payload, fetched_at):
    cur.execute("""
        INSERT INTO nepse.status_log (checked_date, is_open, checked_at)
        VALUES (CURRENT_DATE, %s, %s)
        ON CONFLICT (checked_date)
        DO UPDATE SET is_open = EXCLUDED.is_open;
    """, (payload, fetched_at))
