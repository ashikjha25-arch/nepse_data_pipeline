from db.db_connection import get_db_connection

def get_table_counts():
    # check how many rows are in each table to verify team progress
    conn, cur = get_db_connection()
    tables = ['market.status_log', 'market.securities', 'market.daily_trades', 'market.brokers', 'market.spark_analytics']
    results = {}
    try:
        for table in tables:
            cur.execute(f"select count(*) from {table};")
            results[table] = cur.fetchone()[0]
        return results
    finally:
        cur.close(); conn.close()

def inspect_table_data(table_name, limit=10):
    # peek inside any table to see the raw data
    conn, cur = get_db_connection()
    try:
        cur.execute(f"select * from {table_name} order by 1 desc limit %s;", (limit,))
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        return [dict(zip(cols, row)) for row in rows]
    except Exception as e:
        return {"error": str(e)}
    finally:
        cur.close(); conn.close()