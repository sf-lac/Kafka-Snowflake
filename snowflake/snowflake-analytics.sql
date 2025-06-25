USE ROLE ACCOUNTADMIN;
USE DATABASE KAFKA_STREAMING;
USE SCHEMA YAHOO_FINANCE;

-- Latest price per symbol
CREATE OR REPLACE VIEW vw_latest_stock_prices AS
SELECT symbol, price, time
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY time DESC) AS rn
  FROM stock_prices
)
WHERE rn = 1;

-- Moving average (5-minute)
CREATE OR REPLACE VIEW vw_5min_moving_avg AS
SELECT 
  symbol,
  DATE_TRUNC('minute', time::TIMESTAMP_NTZ) AS minute_bucket,
  AVG(price) AS avg_price_5min
FROM stock_prices
WHERE time::TIMESTAMP_NTZ >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP)
GROUP BY symbol, minute_bucket;


-- Price spike/dip detection (5% deviation from 5-row moving avg)
CREATE OR REPLACE VIEW vw_price_anomalies AS
WITH recent AS (
  SELECT symbol, price, time,
         AVG(price) OVER (PARTITION BY symbol ORDER BY time ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS moving_avg
  FROM stock_prices
)
SELECT *
FROM recent
WHERE ABS(price - moving_avg) / NULLIF(moving_avg, 0) > 0.05;

-- Hourly trend (avg price)
CREATE OR REPLACE VIEW vw_hourly_avg_prices AS
SELECT 
  symbol,
  DATE_TRUNC('hour', time::TIMESTAMP_NTZ) AS hour_bucket,
  AVG(price) AS avg_price_hour
FROM stock_prices
WHERE time::TIMESTAMP_NTZ >= DATEADD(DAY, -1, CURRENT_TIMESTAMP)
GROUP BY symbol, hour_bucket;

-- Stock leaderboard by latest price
CREATE OR REPLACE VIEW vw_price_leaderboard AS
SELECT symbol, price, RANK() OVER (ORDER BY price DESC) AS price_rank
FROM (
  SELECT symbol, price
  FROM stock_prices
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY time DESC) = 1
);

SELECT * FROM vw_5min_moving_avg LIMIT 10;