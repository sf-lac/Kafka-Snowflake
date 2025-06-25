USE ROLE SYSADMIN;

CREATE OR REPLACE DATABASE KAFKA_STREAMING;

CREATE OR REPLACE SCHEMA YAHOO_FINANCE;

CREATE OR REPLACE TABLE KAFKA_STREAMING.YAHOO_FINANCE.STOCK_PRICES (
  symbol STRING,
  price FLOAT,
  currency STRING,
  time STRING
);

-- Create and grant a custom kafka role

USE ROLE ACCOUNTADMIN;

CREATE ROLE kafka_role;

-- Grant required permissions
GRANT ROLE KAFKA_ROLE TO ROLE SYSADMIN;


GRANT USAGE ON DATABASE KAFKA_STREAMING TO ROLE kafka_role;
GRANT USAGE ON SCHEMA KAFKA_STREAMING.YAHOO_FINANCE TO ROLE kafka_role;
GRANT INSERT ON TABLE KAFKA_STREAMING.YAHOO_FINANCE.STOCK_PRICES TO ROLE kafka_role;

GRANT OWNERSHIP ON DATABASE KAFKA_STREAMING TO ROLE kafka_role REVOKE CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA KAFKA_STREAMING.YAHOO_FINANCE TO ROLE kafka_role REVOKE CURRENT GRANTS;
GRANT OWNERSHIP ON TABLE KAFKA_STREAMING.YAHOO_FINANCE.STOCK_PRICES TO ROLE kafka_role REVOKE CURRENT GRANTS;

-- Create kafka connector user
CREATE USER kafka_connector_user
  PASSWORD = '****' 
  DEFAULT_ROLE = kafka_role
  MUST_CHANGE_PASSWORD = FALSE;

-- Assign role to user
GRANT ROLE kafka_role TO USER kafka_connector_user;


SHOW USERS IN ACCOUNT;

-- Add the public key to the user
ALTER USER kafka_connector_user SET RSA_PUBLIC_KEY='MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAzD3iljGLMZOAme...';

-- DATA NOT STREAMED DIRECTLY TO THE STOCK_PRICE TARGET TABLE UNLESS KAFKA MESSAGE MATCHES THE TARGET STRUCTURE --

SHOW TABLES LIKE '%_TOPIC_%';

SELECT * FROM YAHOO_FINANCE_TOPIC_1140052305;

-- TABLE name from SHOW TABLES
SET kafka_staging_table = 'YAHOO_FINANCE_TOPIC_1140052305';

-- Create or replace a stream
CREATE OR REPLACE STREAM kafka_finance_stream
ON TABLE IDENTIFIER($kafka_staging_table);

-- Create the target table if not exists
/*CREATE TABLE IF NOT EXISTS stock_prices (
  symbol STRING,
  price FLOAT,
  currency STRING,
  time STRING
);*/

-- Create a task to copy data every minute
CREATE OR REPLACE TASK move_kafka_data_to_snowflake_stock_prices
  WAREHOUSE = COMPUTE_WH  
  SCHEDULE = '1 MINUTE'
AS
INSERT INTO stock_prices (symbol, price, currency, time)
SELECT
  RECORD_CONTENT:"symbol"::STRING,
  RECORD_CONTENT:"price"::FLOAT,
  RECORD_CONTENT:"currency"::STRING,
  RECORD_CONTENT:"time"::STRING
FROM kafka_finance_stream;

-- Start the task
ALTER TASK move_kafka_data_to_snowflake_stock_prices RESUME;

SELECT * FROM stock_prices ORDER BY time DESC;