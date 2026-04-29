import pandas as pd
from db.db_connection import get_db_connection


def read_stock_data():
    conn, cur = get_db_connection()

    query = """
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
        WHERE 
            open_price IS NOT NULL
            AND high_price IS NOT NULL
            AND low_price IS NOT NULL
            AND close_price IS NOT NULL
            AND volume IS NOT NULL
            AND turnover IS NOT NULL;
    """

    df = pd.read_sql(query, conn)

    cur.close()
    conn.close()

    return df


def insert_forecast(symbol, actual_close, predicted_close, model_name):
    conn, cur = get_db_connection()

    try:
        cur.execute("""
            INSERT INTO nepse.stock_forecast (
                symbol,
                actual_close,
                predicted_close,
                model_name
            )
            VALUES (%s, %s, %s, %s);
        """, (
            symbol,
            float(actual_close),
            round(float(predicted_close), 2),
            model_name
        ))

        conn.commit()

    except Exception as e:
        conn.rollback()
        print(f"Exception occurred while inserting forecast: {e}")
        raise

    finally:
        cur.close()
        conn.close()