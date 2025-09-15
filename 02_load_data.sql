-- =============================================================================
-- CORTEX SEARCH ENTITLEMENTS DEMO - Data Loading Script
-- =============================================================================
-- This script loads the generated credit card transaction data into Snowflake
-- Run this script after completing 01_setup_database_and_table.sql

-- Set context
USE DATABASE CORTEX_SEARCH_ENTITLEMENTS_DB;
USE SCHEMA ENTITLEMENTS;
USE WAREHOUSE ENTITLEMENTS_WH;

-- =============================================================================
-- STEP 1: UPLOAD DATA TO STAGE
-- =============================================================================
-- First, you need to upload the credit_card_transactions.csv file to the stage
-- You can do this through SnowSQL, Snowsight, or programmatically

-- For SnowSQL command line:
-- PUT file://credit_card_transactions.csv @DEMO_STAGE;

-- For Snowsight: Use the Load Data wizard or the following approach:
-- 1. Go to Data > Databases > CORTEX_SEARCH_ENTITLEMENTS_DEMO > DEMO > Stages > DEMO_STAGE
-- 2. Click "Load Files" and upload the credit_card_transactions.csv file

-- Verify files in stage (run after uploading)
LIST @DEMO_STAGE;

-- =============================================================================
-- STEP 2: COPY DATA FROM STAGE TO TABLE
-- =============================================================================
-- Load data from the stage into the table
COPY INTO CREDIT_CARD_TRANSACTIONS
FROM @DEMO_STAGE/credit_card_transactions.csv
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
ON_ERROR = 'SKIP_FILE'
PURGE = FALSE; -- Keep files in stage for reference

-- Check the copy results
SELECT *
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE STATUS != 'LOADED';

-- =============================================================================
-- STEP 3: VERIFY DATA LOADING
-- =============================================================================
-- Count total records loaded
SELECT COUNT(*) AS TOTAL_RECORDS
FROM CREDIT_CARD_TRANSACTIONS;

-- Basic data quality checks
SELECT 
    'Transaction Date Range' AS CHECK_TYPE,
    MIN(TRANSACTION_DATE) AS MIN_VALUE,
    MAX(TRANSACTION_DATE) AS MAX_VALUE,
    COUNT(*) AS RECORD_COUNT
FROM CREDIT_CARD_TRANSACTIONS

UNION ALL

SELECT 
    'Amount Range' AS CHECK_TYPE,
    MIN(AMOUNT)::VARCHAR AS MIN_VALUE,
    MAX(AMOUNT)::VARCHAR AS MAX_VALUE,
    COUNT(*) AS RECORD_COUNT
FROM CREDIT_CARD_TRANSACTIONS

UNION ALL

SELECT 
    'Unique Customers' AS CHECK_TYPE,
    COUNT(DISTINCT CUSTOMER_ID)::VARCHAR AS MIN_VALUE,
    '' AS MAX_VALUE,
    COUNT(*) AS RECORD_COUNT
FROM CREDIT_CARD_TRANSACTIONS

UNION ALL

SELECT 
    'Unique Merchants' AS CHECK_TYPE,
    COUNT(DISTINCT MERCHANT_ID)::VARCHAR AS MIN_VALUE,
    '' AS MAX_VALUE,
    COUNT(*) AS RECORD_COUNT
FROM CREDIT_CARD_TRANSACTIONS;

-- =============================================================================
-- STEP 4: ANALYZE ENTITLEMENT DIMENSIONS
-- =============================================================================
-- Show distribution across entitlement dimensions
SELECT 'REGION DISTRIBUTION' AS DIMENSION, NULL AS CATEGORY, NULL AS COUNT, NULL AS PERCENTAGE
UNION ALL
SELECT '', REGION_CODE, COUNT(*), ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM CREDIT_CARD_TRANSACTIONS), 2)
FROM CREDIT_CARD_TRANSACTIONS 
GROUP BY REGION_CODE
UNION ALL
SELECT '', '', NULL, NULL
UNION ALL
SELECT 'CUSTOMER TIER DISTRIBUTION' AS DIMENSION, NULL, NULL, NULL
UNION ALL
SELECT '', CUSTOMER_TIER, COUNT(*), ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM CREDIT_CARD_TRANSACTIONS), 2)
FROM CREDIT_CARD_TRANSACTIONS 
GROUP BY CUSTOMER_TIER
UNION ALL
SELECT '', '', NULL, NULL
UNION ALL
SELECT 'SENSITIVITY LEVEL DISTRIBUTION' AS DIMENSION, NULL, NULL, NULL
UNION ALL
SELECT '', SENSITIVITY_LEVEL, COUNT(*), ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM CREDIT_CARD_TRANSACTIONS), 2)
FROM CREDIT_CARD_TRANSACTIONS 
GROUP BY SENSITIVITY_LEVEL
UNION ALL
SELECT '', '', NULL, NULL
UNION ALL
SELECT 'DEPARTMENT DISTRIBUTION' AS DIMENSION, NULL, NULL, NULL
UNION ALL
SELECT '', PRIMARY_DEPARTMENT, COUNT(*), ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM CREDIT_CARD_TRANSACTIONS), 2)
FROM CREDIT_CARD_TRANSACTIONS 
GROUP BY PRIMARY_DEPARTMENT
UNION ALL
SELECT '', '', NULL, NULL
UNION ALL
SELECT 'RISK LEVEL DISTRIBUTION' AS DIMENSION, NULL, NULL, NULL
UNION ALL
SELECT '', RISK_LEVEL, COUNT(*), ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM CREDIT_CARD_TRANSACTIONS), 2)
FROM CREDIT_CARD_TRANSACTIONS 
GROUP BY RISK_LEVEL
ORDER BY DIMENSION, CATEGORY;

-- =============================================================================
-- STEP 5: SAMPLE DATA PREVIEW
-- =============================================================================
-- Show sample records to verify data structure
SELECT 
    TRANSACTION_ID,
    CUSTOMER_ID,
    TRANSACTION_DATE,
    AMOUNT,
    CARD_BRAND,
    MERCHANT_CATEGORY,
    TRANSACTION_STATUS,
    REGION_CODE,
    CUSTOMER_TIER,
    PRIMARY_DEPARTMENT,
    SENSITIVITY_LEVEL,
    RISK_LEVEL
FROM CREDIT_CARD_TRANSACTIONS 
ORDER BY TRANSACTION_DATE DESC
LIMIT 10;

-- Show records across different sensitivity levels for verification
SELECT 
    SENSITIVITY_LEVEL,
    COUNT(*) AS COUNT,
    ROUND(AVG(AMOUNT), 2) AS AVG_AMOUNT,
    STRING_AGG(DISTINCT REGION_CODE, ', ') AS REGIONS,
    STRING_AGG(DISTINCT CUSTOMER_TIER, ', ') AS TIERS,
    STRING_AGG(DISTINCT RISK_LEVEL, ', ') AS RISK_LEVELS
FROM CREDIT_CARD_TRANSACTIONS
GROUP BY SENSITIVITY_LEVEL
ORDER BY 
    CASE SENSITIVITY_LEVEL
        WHEN 'PUBLIC' THEN 1
        WHEN 'INTERNAL' THEN 2
        WHEN 'CONFIDENTIAL' THEN 3
        WHEN 'RESTRICTED' THEN 4
    END;

-- =============================================================================
-- DATA LOADING COMPLETE
-- =============================================================================
SELECT 
    'Data loading complete!' AS STATUS,
    COUNT(*) AS TOTAL_RECORDS,
    COUNT(DISTINCT REGION_CODE) AS UNIQUE_REGIONS,
    COUNT(DISTINCT CUSTOMER_TIER) AS UNIQUE_TIERS,
    COUNT(DISTINCT SENSITIVITY_LEVEL) AS UNIQUE_SENSITIVITY_LEVELS,
    COUNT(DISTINCT PRIMARY_DEPARTMENT) AS UNIQUE_DEPARTMENTS
FROM CREDIT_CARD_TRANSACTIONS;
