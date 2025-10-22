-- Create TPCDS Tables with Change Tracking Enabled
-- ====================================================
-- This script materializes shared TPCDS tables into local tables
-- with change tracking enabled for Cortex Search compatibility
--
-- IMPORTANT: Run this script FIRST before comprehensive_cortex_search_demo.sql
--
-- WHY THIS IS NEEDED:
-- - Cortex Search requires tables with CHANGE_TRACKING enabled
-- - Shared SAMPLE_DATA tables cannot have change tracking enabled
-- - This script creates local copies with change tracking
--
-- TABLES CREATED (with _TAB suffix):
-- 1. STORE_SALES_TAB        - Transaction/sales data
-- 2. ITEM_TAB               - Product catalog
-- 3. STORE_TAB              - Store information
-- 4. CUSTOMER_TAB           - Customer master data
-- 5. CUSTOMER_DEMOGRAPHICS_TAB - Customer demographic details
-- 6. DATE_DIM_TAB           - Date dimension
--
-- TARGET DATABASE: TPCDS_CORTEX_DB
-- TARGET SCHEMA: TPCDS_DATA

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- Create dedicated database and schema for TPCDS data
CREATE DATABASE IF NOT EXISTS TPCDS_CORTEX_DB;
CREATE SCHEMA IF NOT EXISTS TPCDS_CORTEX_DB.TPCDS_DATA;

USE DATABASE TPCDS_CORTEX_DB;
USE SCHEMA TPCDS_DATA;

-- =============================================================================
-- STEP 1: CREATE TABLES WITH CHANGE TRACKING
-- =============================================================================

SELECT 'Creating STORE_SALES_TAB with change tracking...' as status;

-- 1. STORE_SALES Table (Primary transaction data)
CREATE OR REPLACE TABLE STORE_SALES_TAB 
CHANGE_TRACKING = TRUE
AS 
SELECT * 
FROM SAMPLE_DATA.TPCDS_SF10TCL.STORE_SALES
WHERE ss_sold_date_sk IS NOT NULL;  -- Filter out rows with no date

SELECT 'Creating ITEM_TAB with change tracking...' as status;

-- 2. ITEM Table (Product catalog)
CREATE OR REPLACE TABLE ITEM_TAB
CHANGE_TRACKING = TRUE
AS 
SELECT * 
FROM SAMPLE_DATA.TPCDS_SF10TCL.ITEM;

SELECT 'Creating STORE_TAB with change tracking...' as status;

-- 3. STORE Table (Store information)
CREATE OR REPLACE TABLE STORE_TAB
CHANGE_TRACKING = TRUE
AS 
SELECT * 
FROM SAMPLE_DATA.TPCDS_SF10TCL.STORE;

SELECT 'Creating CUSTOMER_TAB with change tracking...' as status;

-- 4. CUSTOMER Table (Customer information)
CREATE OR REPLACE TABLE CUSTOMER_TAB
CHANGE_TRACKING = TRUE
AS 
SELECT * 
FROM SAMPLE_DATA.TPCDS_SF10TCL.CUSTOMER;

SELECT 'Creating CUSTOMER_DEMOGRAPHICS_TAB with change tracking...' as status;

-- 5. CUSTOMER_DEMOGRAPHICS Table (Customer demographic data)
CREATE OR REPLACE TABLE CUSTOMER_DEMOGRAPHICS_TAB
CHANGE_TRACKING = TRUE
AS 
SELECT * 
FROM SAMPLE_DATA.TPCDS_SF10TCL.CUSTOMER_DEMOGRAPHICS;

SELECT 'Creating DATE_DIM_TAB with change tracking...' as status;

-- 6. DATE_DIM Table (Date dimension)
CREATE OR REPLACE TABLE DATE_DIM_TAB
CHANGE_TRACKING = TRUE
AS 
SELECT * 
FROM SAMPLE_DATA.TPCDS_SF10TCL.DATE_DIM
WHERE d_year >= 2000;  -- Limit to recent data for performance

-- =============================================================================
-- STEP 2: VERIFY TABLE CREATION AND CHANGE TRACKING
-- =============================================================================

SELECT 'Verifying table creation and change tracking...' as status;

-- Check table counts
SELECT 'STORE_SALES_TAB' as table_name, COUNT(*) as row_count FROM STORE_SALES_TAB
UNION ALL
SELECT 'ITEM_TAB', COUNT(*) FROM ITEM_TAB
UNION ALL
SELECT 'STORE_TAB', COUNT(*) FROM STORE_TAB
UNION ALL
SELECT 'CUSTOMER_TAB', COUNT(*) FROM CUSTOMER_TAB
UNION ALL
SELECT 'CUSTOMER_DEMOGRAPHICS_TAB', COUNT(*) FROM CUSTOMER_DEMOGRAPHICS_TAB
UNION ALL
SELECT 'DATE_DIM_TAB', COUNT(*) FROM DATE_DIM_TAB;

-- Verify change tracking is enabled
SHOW TABLES LIKE '%_TAB';

-- =============================================================================
-- STEP 3: CREATE INDEXES FOR BETTER JOIN PERFORMANCE (Optional)
-- =============================================================================

SELECT 'Creating sample indexes for performance...' as status;

-- Note: Snowflake automatically optimizes joins, but you can add clustering keys if needed
-- ALTER TABLE STORE_SALES_TAB CLUSTER BY (ss_sold_date_sk, ss_item_sk);
-- ALTER TABLE ITEM_TAB CLUSTER BY (i_item_sk);
-- ALTER TABLE DATE_DIM_TAB CLUSTER BY (d_date_sk);

-- =============================================================================
-- STEP 4: GRANT PERMISSIONS
-- =============================================================================

SELECT 'Granting permissions...' as status;

GRANT USAGE ON DATABASE TPCDS_CORTEX_DB TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA TPCDS_CORTEX_DB.TPCDS_DATA TO ROLE PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA TPCDS_CORTEX_DB.TPCDS_DATA TO ROLE PUBLIC;

-- =============================================================================
-- COMPLETION MESSAGE
-- =============================================================================

SELECT 
    '✅ TPCDS tables created successfully with change tracking enabled!' as status,
    'Database: TPCDS_CORTEX_DB' as database_name,
    'Schema: TPCDS_DATA' as schema_name,
    'Tables created: 6 (STORE_SALES_TAB, ITEM_TAB, STORE_TAB, CUSTOMER_TAB, CUSTOMER_DEMOGRAPHICS_TAB, DATE_DIM_TAB)' as details,
    'Next step: Run comprehensive_cortex_search_demo.sql' as next_action;

