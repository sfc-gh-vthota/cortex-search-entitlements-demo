-- Create and populate Transactions table with 10,000+ records across 10 regions
-- This script creates a transactions table with various transaction types and regions

-- Create database if it doesn't exist
CREATE DATABASE IF NOT EXISTS CORTEX_SEARCH_ENTITLEMENT_DB;
USE DATABASE CORTEX_SEARCH_ENTITLEMENT_DB;

-- Create schema
CREATE SCHEMA IF NOT EXISTS TRANSACTIONS;
USE SCHEMA TRANSACTIONS;

-- Create the Transactions table
CREATE OR REPLACE TABLE TRANSACTIONS (
    TRANSACTION_ID VARCHAR(50) PRIMARY KEY,
    USER_ID VARCHAR(50) NOT NULL,
    TRANSACTION_DATE TIMESTAMP NOT NULL,
    AMOUNT DECIMAL(12,2) NOT NULL,
    TRANSACTION_TYPE VARCHAR(50) NOT NULL,
    DESCRIPTION VARCHAR(500),
    REGION_NAME VARCHAR(50) NOT NULL,
    MERCHANT_NAME VARCHAR(100),
    CATEGORY VARCHAR(50),
    STATUS VARCHAR(20) DEFAULT 'COMPLETED',
    REGION_USER_IDS ARRAY
);

-- Generate 10,000 transaction records using Snowflake native functions for better performance

-- Generate 10,000 transaction records spread across regions using Snowflake native functions
INSERT INTO TRANSACTIONS (
    TRANSACTION_ID,
    USER_ID,
    TRANSACTION_DATE,
    AMOUNT,
    TRANSACTION_TYPE,
    DESCRIPTION,
    REGION_NAME,
    MERCHANT_NAME,
    CATEGORY,
    STATUS,
    REGION_USER_IDS
)
SELECT 
    'TXN_' || LPAD(seq4(), 6, '0') AS transaction_id,
    'USER_' || LPAD(ABS(RANDOM()) % 1000 + 1, 4, '0') AS user_id,
    DATEADD(day, ABS(RANDOM()) % 730, '2023-01-01'::date)::timestamp AS transaction_date,
    ROUND((ABS(RANDOM()) % 4999 + 1) + (ABS(RANDOM()) % 100) / 100.0, 2) AS amount,
    CASE ABS(RANDOM()) % 6
        WHEN 0 THEN 'Purchase'
        WHEN 1 THEN 'Refund'
        WHEN 2 THEN 'Transfer'
        WHEN 3 THEN 'Payment'
        WHEN 4 THEN 'Withdrawal'
        ELSE 'Deposit'
    END AS transaction_type,
    CASE ABS(RANDOM()) % 6
        WHEN 0 THEN 'Purchase'
        WHEN 1 THEN 'Refund'
        WHEN 2 THEN 'Transfer'
        WHEN 3 THEN 'Payment'
        WHEN 4 THEN 'Withdrawal'
        ELSE 'Deposit'
    END || ' at ' ||
    CASE ABS(RANDOM()) % 15
        WHEN 0 THEN 'Amazon'
        WHEN 1 THEN 'Walmart'
        WHEN 2 THEN 'Target'
        WHEN 3 THEN 'Starbucks'
        WHEN 4 THEN 'McDonalds'
        WHEN 5 THEN 'Shell'
        WHEN 6 THEN 'Exxon'
        WHEN 7 THEN 'Best Buy'
        WHEN 8 THEN 'Home Depot'
        WHEN 9 THEN 'Costco'
        WHEN 10 THEN 'Netflix'
        WHEN 11 THEN 'Spotify'
        WHEN 12 THEN 'Uber'
        WHEN 13 THEN 'Lyft'
        ELSE 'Hotels.com'
    END || ' in ' ||
    CASE (seq4() - 1) % 10
        WHEN 0 THEN 'North America'
        WHEN 1 THEN 'Europe'
        WHEN 2 THEN 'Asia Pacific'
        WHEN 3 THEN 'Latin America'
        WHEN 4 THEN 'Middle East'
        WHEN 5 THEN 'Africa'
        WHEN 6 THEN 'Oceania'
        WHEN 7 THEN 'Nordic'
        WHEN 8 THEN 'Eastern Europe'
        ELSE 'Southeast Asia'
    END AS description,
    CASE (seq4() - 1) % 10
        WHEN 0 THEN 'North America'
        WHEN 1 THEN 'Europe'
        WHEN 2 THEN 'Asia Pacific'
        WHEN 3 THEN 'Latin America'
        WHEN 4 THEN 'Middle East'
        WHEN 5 THEN 'Africa'
        WHEN 6 THEN 'Oceania'
        WHEN 7 THEN 'Nordic'
        WHEN 8 THEN 'Eastern Europe'
        ELSE 'Southeast Asia'
    END AS region_name,
    CASE ABS(RANDOM()) % 15
        WHEN 0 THEN 'Amazon'
        WHEN 1 THEN 'Walmart'
        WHEN 2 THEN 'Target'
        WHEN 3 THEN 'Starbucks'
        WHEN 4 THEN 'McDonalds'
        WHEN 5 THEN 'Shell'
        WHEN 6 THEN 'Exxon'
        WHEN 7 THEN 'Best Buy'
        WHEN 8 THEN 'Home Depot'
        WHEN 9 THEN 'Costco'
        WHEN 10 THEN 'Netflix'
        WHEN 11 THEN 'Spotify'
        WHEN 12 THEN 'Uber'
        WHEN 13 THEN 'Lyft'
        ELSE 'Hotels.com'
    END AS merchant_name,
    CASE ABS(RANDOM()) % 8
        WHEN 0 THEN 'Retail'
        WHEN 1 THEN 'Groceries'
        WHEN 2 THEN 'Gas'
        WHEN 3 THEN 'Restaurant'
        WHEN 4 THEN 'Online'
        WHEN 5 THEN 'Entertainment'
        WHEN 6 THEN 'Travel'
        ELSE 'Healthcare'
    END AS category,
    CASE ABS(RANDOM()) % 20
        WHEN 0 THEN 'PENDING'
        WHEN 1 THEN 'FAILED'
        ELSE 'COMPLETED'
    END AS status,
    NULL AS region_user_ids  -- Will be populated after USER_REGION_MAPPING is created
FROM TABLE(GENERATOR(ROWCOUNT => 10000));

-- Note: Snowflake does not use traditional indexes
-- It automatically optimizes queries using micro-partitions and metadata

-- Verify the data distribution across regions
SELECT 
    REGION_NAME,
    COUNT(*) as TRANSACTION_COUNT,
    AVG(AMOUNT) as AVG_AMOUNT,
    SUM(AMOUNT) as TOTAL_AMOUNT
FROM TRANSACTIONS 
GROUP BY REGION_NAME 
ORDER BY REGION_NAME;

-- Show sample data
SELECT * FROM TRANSACTIONS LIMIT 20;

-- NOTE: Run this UPDATE statement AFTER creating and populating USER_REGION_MAPPING table
-- This populates the REGION_USER_IDS array with all user IDs from the same region
UPDATE TRANSACTIONS 
SET REGION_USER_IDS = (
    SELECT ARRAY_AGG(USER_ID) 
    FROM USER_REGION_MAPPING 
    WHERE USER_REGION_MAPPING.REGION_NAME = TRANSACTIONS.REGION_NAME
    AND STATUS = 'ACTIVE'
);

-- Verify the array column is populated
SELECT 
    TRANSACTION_ID,
    REGION_NAME,
    REGION_USER_IDS,
    ARRAY_SIZE(REGION_USER_IDS) as USERS_IN_REGION_COUNT
FROM TRANSACTIONS 
LIMIT 10;

-- Show array elements for a specific region
SELECT DISTINCT
    REGION_NAME,
    REGION_USER_IDS,
    ARRAY_SIZE(REGION_USER_IDS) as TOTAL_USERS_IN_REGION
FROM TRANSACTIONS 
WHERE REGION_NAME = 'North America'
LIMIT 1;

-- NOTE: For Cortex Search functionality, run the following scripts in order:
-- 1. This script (creates TRANSACTIONS table and populates REGION_USER_IDS array)  
-- 2. 02_create_user_region_mapping.sql (creates USER_REGION_MAPPING table)
-- 3. 03_create_cortex_search_service.sql (creates Cortex Search service)
-- 4. 04_cortex_search_examples.sql (demonstrates search with entitlements)

COMMIT;
