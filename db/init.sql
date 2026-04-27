-- create schema for isolation
create schema if not exists market;

-- 1. for operational tracking (is market open history)
create table if not exists market.status_log (
    id serial primary key,
    is_open boolean not null,
    checked_at timestamp default current_timestamp
);

-- 2. master table (target for kafka/spark dimension loading)
create table if not exists market.securities (
    symbol text primary key,
    security_name text,
    company_name text,
    updated_at timestamp default current_timestamp
);

-- 3. raw data table 
create table if not exists market.daily_trades (
    id bigserial primary key,
    symbol text references market.securities(symbol),
    business_date date not null,
    close_price numeric(12, 2),
    total_traded_quantity bigint,
    total_traded_value numeric(20, 2),
    unique (symbol, business_date)
);

-- 4. broker master table
create table if not exists market.brokers (
    member_code text primary key,
    member_name text,
    address text
);

-- 5. calculated data table 
create table if not exists market.spark_analytics (
    id serial primary key,
    symbol text references market.securities(symbol),
    indicator_name text, -- e.g., 'moving_average_7'
    value numeric(12, 2),
    calculated_at timestamp default current_timestamp
);

-- index for grafana query performance
create index if not exists idx_trades_view on market.daily_trades (business_date desc);