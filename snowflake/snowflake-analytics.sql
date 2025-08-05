USE ROLE ACCOUNTADMIN;
USE DATABASE KAFKA_STREAMING;
USE SCHEMA YAHOO_FINANCE;

-- Latest Price
CREATE OR REPLACE VIEW vw_latest_stock_prices AS
SELECT symbol, price, time
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY time DESC) AS rn
  FROM stock_prices
)
WHERE rn = 1;

-- 5-Min Moving Average
CREATE OR REPLACE DYNAMIC TABLE dt_moving_avg
TARGET_LAG = '1 minute'
WAREHOUSE = COMPUTE_WH
AS
WITH recent AS (
  SELECT 
    symbol,
    DATE_TRUNC('minute', time::TIMESTAMP_NTZ) AS minute_bucket,    
    AVG(price) OVER (PARTITION BY symbol ORDER BY minute_bucket ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS avg_price_5min
  FROM stock_prices
)
SELECT *
FROM recent;

-- Price Anomalies
CREATE OR REPLACE DYNAMIC TABLE dt_price_anomalies
TARGET_LAG = '1 minute'
WAREHOUSE = COMPUTE_WH
AS
WITH recent AS (
  SELECT 
    symbol,
    price,
    time,
    AVG(price) OVER (PARTITION BY symbol ORDER BY time ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS moving_avg
  FROM stock_prices
)
SELECT *
FROM recent
WHERE ABS(price - moving_avg) / NULLIF(moving_avg, 0) > 0.05;

-- Leaderboard by Latest Price
CREATE OR REPLACE VIEW vw_price_leaderboard AS
SELECT symbol, price, RANK() OVER (ORDER BY price DESC) AS price_rank
FROM (
  SELECT symbol, price
  FROM stock_prices
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY time DESC) = 1
);

SHOW DYNAMIC TABLES;

SELECT *
FROM TABLE(
  INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
    NAME => 'KAFKA_STREAMING.YAHOO_FINANCE.dt_moving_avg',
    RESULT_LIMIT => 20
  )
)
ORDER BY DATA_TIMESTAMP DESC;

SELECT *
FROM TABLE(
  INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
    NAME => 'KAFKA_STREAMING.YAHOO_FINANCE.dt_price_anomalies',
    RESULT_LIMIT => 20
  )
)
ORDER BY DATA_TIMESTAMP DESC;


