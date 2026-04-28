-- create schema for isolation
create schema if not exists nepse;

-- Status Log (check if market is open or closed)
CREATE TABLE IF NOT EXISTS nepse.status_log (
    checked_date date PRIMARY KEY DEFAULT CURRENT_DATE,
    is_open boolean NOT NULL,
    checked_at timestamp DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE nepse.status_log
ADD CONSTRAINT status_log_checked_date_unique UNIQUE (checked_date);

-- Stock Price History
CREATE TABLE IF NOT EXISTS nepse.stock_price_history (
    stock_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    issue_date DATE DEFAULT CURRENT_DATE,
    symbol VARCHAR(10),
    open_price NUMERIC(12, 2),
    high_price NUMERIC(12, 2),
    low_price NUMERIC(12, 2),
    close_price NUMERIC(12, 2),
    volume INT,
    turnover NUMERIC(12, 2)
);


ALTER TABLE nepse.stock_price_history
ADD CONSTRAINT unique_stock_date_symbol UNIQUE (issue_date, symbol);

-- Company Information
create table if not exists nepse.company_info (
    symbol varchar(10) PRIMARY KEY,
    company_name text NOT NULL,
    sector text
);

-- Stock Forecast (for models)
CREATE TABLE IF NOT EXISTS nepse.stock_forecast (
    pred_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    forecast_date DATE DEFAULT CURRENT_DATE,
    symbol VARCHAR(10),
    actual_close NUMERIC(12, 2),
    predicted_close NUMERIC(12, 2),
    model_name VARCHAR(50)
);



