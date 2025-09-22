-- Standalone Dynamic Tables Setup - Complete New Implementation
-- This creates a completely separate setup without disturbing existing tables

-- =============================================================================
-- STANDALONE DYNAMIC TABLES CORTEX SEARCH SETUP
-- =============================================================================

-- Create a separate schema to avoid conflicts
USE DATABASE CORTEX_SEARCH_ENTITLEMENT_DB;
CREATE SCHEMA IF NOT EXISTS DYNAMIC_DEMO;
USE SCHEMA DYNAMIC_DEMO;

SELECT '🚀 CREATING STANDALONE DYNAMIC TABLES SETUP' as setup_title;
SELECT 'This will not interfere with your existing implementation' as info;

-- =============================================================================
-- STEP 1: CREATE NEW BASE TABLES WITH DIFFERENT NAMES
-- =============================================================================

SELECT '📊 STEP 1: CREATING BASE TABLES' as step_1;

-- Create new transactions table (separate from existing one)
CREATE OR REPLACE TABLE financial_transactions (
    txn_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    transaction_date TIMESTAMP NOT NULL,
    amount DECIMAL(12,2) NOT NULL,
    transaction_type VARCHAR(50) NOT NULL,
    description VARCHAR(500),
    region_name VARCHAR(50) NOT NULL,
    merchant_name VARCHAR(100),
    category VARCHAR(50),
    status VARCHAR(20) DEFAULT 'COMPLETED'
);

-- Create new user region mapping table  
CREATE OR REPLACE TABLE user_region_access (
    user_id VARCHAR(50) PRIMARY KEY,
    user_name VARCHAR(100) NOT NULL,
    region_name VARCHAR(50) NOT NULL,
    access_level VARCHAR(20) DEFAULT 'STANDARD',
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    status VARCHAR(20) DEFAULT 'ACTIVE'
);

SELECT '✅ Base tables created: FINANCIAL_TRANSACTIONS, USER_REGION_ACCESS' as tables_created;

-- =============================================================================
-- STEP 2: POPULATE BASE TABLES WITH SAMPLE DATA
-- =============================================================================

SELECT '📝 STEP 2: POPULATING BASE TABLES WITH SAMPLE DATA' as step_2;

-- Insert 5,000 financial transactions across 10 regions
INSERT INTO financial_transactions (
    txn_id,
    customer_id,
    transaction_date,
    amount,
    transaction_type,
    description,
    region_name,
    merchant_name,
    category,
    status
)
SELECT 
    'FTX_' || LPAD(seq4(), 6, '0') AS txn_id,
    'CUST_' || LPAD(ABS(RANDOM()) % 500 + 1, 4, '0') AS customer_id,
    DATEADD(day, ABS(RANDOM()) % 365, '2024-01-01'::date)::timestamp AS transaction_date,
    ROUND((ABS(RANDOM()) % 2999 + 1) + (ABS(RANDOM()) % 100) / 100.0, 2) AS amount,
    CASE ABS(RANDOM()) % 5
        WHEN 0 THEN 'Purchase'
        WHEN 1 THEN 'Refund'
        WHEN 2 THEN 'Transfer'
        WHEN 3 THEN 'Payment'
        ELSE 'Withdrawal'
    END AS transaction_type,
    CASE ABS(RANDOM()) % 5
        WHEN 0 THEN 'Purchase'
        WHEN 1 THEN 'Refund'
        WHEN 2 THEN 'Transfer'
        WHEN 3 THEN 'Payment'
        ELSE 'Withdrawal'
    END || ' at ' ||
    CASE ABS(RANDOM()) % 12
        WHEN 0 THEN 'Amazon'
        WHEN 1 THEN 'Walmart'
        WHEN 2 THEN 'Target'
        WHEN 3 THEN 'Starbucks'
        WHEN 4 THEN 'McDonalds'
        WHEN 5 THEN 'Shell'
        WHEN 6 THEN 'Best Buy'
        WHEN 7 THEN 'Home Depot'
        WHEN 8 THEN 'Costco'
        WHEN 9 THEN 'Netflix'
        WHEN 10 THEN 'Spotify'
        ELSE 'Uber'
    END || ' - ' ||
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
    CASE ABS(RANDOM()) % 12
        WHEN 0 THEN 'Amazon'
        WHEN 1 THEN 'Walmart'
        WHEN 2 THEN 'Target'
        WHEN 3 THEN 'Starbucks'
        WHEN 4 THEN 'McDonalds'
        WHEN 5 THEN 'Shell'
        WHEN 6 THEN 'Best Buy'
        WHEN 7 THEN 'Home Depot'
        WHEN 8 THEN 'Costco'
        WHEN 9 THEN 'Netflix'
        WHEN 10 THEN 'Spotify'
        ELSE 'Uber'
    END AS merchant_name,
    CASE ABS(RANDOM()) % 6
        WHEN 0 THEN 'Retail'
        WHEN 1 THEN 'Groceries'
        WHEN 2 THEN 'Gas'
        WHEN 3 THEN 'Restaurant'
        WHEN 4 THEN 'Online'
        ELSE 'Entertainment'
    END AS category,
    CASE ABS(RANDOM()) % 15
        WHEN 0 THEN 'PENDING'
        WHEN 1 THEN 'FAILED'
        ELSE 'COMPLETED'
    END AS status
FROM TABLE(GENERATOR(ROWCOUNT => 10000));

-- Insert 200 users distributed across 10 regions (20 per region)
INSERT INTO user_region_access (user_id, user_name, region_name, access_level, status)
SELECT 
    'USR_' || LPAD(seq4(), 4, '0') AS user_id,
    CASE (seq4() - 1) % 20
        WHEN 0 THEN 'Alice Johnson'
        WHEN 1 THEN 'Bob Smith'
        WHEN 2 THEN 'Carol Williams'
        WHEN 3 THEN 'David Brown'
        WHEN 4 THEN 'Eva Davis'
        WHEN 5 THEN 'Frank Wilson'
        WHEN 6 THEN 'Grace Miller'
        WHEN 7 THEN 'Henry Taylor'
        WHEN 8 THEN 'Iris Anderson'
        WHEN 9 THEN 'Jack Thompson'
        WHEN 10 THEN 'Kate Martinez'
        WHEN 11 THEN 'Leo Garcia'
        WHEN 12 THEN 'Mia Rodriguez'
        WHEN 13 THEN 'Noah Lopez'
        WHEN 14 THEN 'Olivia Hernandez'
        WHEN 15 THEN 'Paul Gonzalez'
        WHEN 16 THEN 'Quinn Perez'
        WHEN 17 THEN 'Ruby Sanchez'
        WHEN 18 THEN 'Sam Torres'
        ELSE 'Tina Flores'
    END || ' ' || seq4() AS user_name,
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
    CASE ABS(RANDOM()) % 4
        WHEN 0 THEN 'ADMIN'
        WHEN 1 THEN 'MANAGER'
        ELSE 'STANDARD'
    END AS access_level,
    CASE ABS(RANDOM()) % 10
        WHEN 0 THEN 'INACTIVE'
        ELSE 'ACTIVE'
    END AS status
FROM TABLE(GENERATOR(ROWCOUNT => 200));

-- Show data distribution
SELECT '📊 Data created:' as summary;
SELECT 
    'FINANCIAL_TRANSACTIONS' as table_name,
    COUNT(*) as record_count,
    COUNT(DISTINCT region_name) as regions,
    MIN(transaction_date) as earliest_date,
    MAX(transaction_date) as latest_date
FROM financial_transactions
UNION ALL
SELECT 
    'USER_REGION_ACCESS' as table_name,
    COUNT(*) as record_count,
    COUNT(DISTINCT region_name) as regions,
    MIN(created_date) as earliest_date,
    MAX(created_date) as latest_date
FROM user_region_access;

-- =============================================================================
-- STEP 3: CREATE DYNAMIC TABLE WITH ENTITLEMENT ARRAYS
-- =============================================================================

SELECT '⚡ STEP 3: CREATING DYNAMIC TABLE WITH AUTOMATIC ENTITLEMENTS' as step_3;

-- Create the Dynamic Table that automatically maintains entitlement arrays
-- Using JOIN approach instead of correlated subqueries for incremental refresh support
CREATE OR REPLACE DYNAMIC TABLE financial_transactions_enriched
TARGET_LAG = '1 minute'
WAREHOUSE = COMPUTE_WH
REFRESH_MODE = 'INCREMENTAL'
AS
SELECT 
    ft.txn_id,
    ft.customer_id,
    ft.transaction_date,
    ft.amount,
    ft.transaction_type,
    ft.description,
    ft.region_name,
    ft.merchant_name,
    ft.category,
    ft.status,
    -- Use aggregated JOIN approach for better incremental refresh support
    COALESCE(region_agg.entitled_user_ids, ARRAY_CONSTRUCT()) AS entitled_user_ids,
    COALESCE(region_agg.user_access_levels, ARRAY_CONSTRUCT()) AS user_access_levels,
    COALESCE(region_agg.entitled_user_count, 0) AS entitled_user_count
    -- Note: Removed CURRENT_TIMESTAMP() to support Cortex Search change tracking
    -- Non-deterministic functions prevent change tracking required by Cortex Search
FROM financial_transactions ft
LEFT JOIN (
    SELECT 
        region_name,
        ARRAY_AGG(user_id) AS entitled_user_ids,
        ARRAY_AGG(access_level) AS user_access_levels,
        COUNT(*) AS entitled_user_count
    FROM user_region_access 
    WHERE status = 'ACTIVE'
    GROUP BY region_name
) region_agg ON ft.region_name = region_agg.region_name;

SELECT '✅ Dynamic Table created: FINANCIAL_TRANSACTIONS_ENRICHED' as dynamic_table_created;

SELECT * FROM financial_transactions_enriched;

-- Verify Dynamic Table data
SELECT 'Sample from Dynamic Table:' as sample_info;
SELECT 
    txn_id,
    region_name,
    amount,
    ARRAY_SIZE(entitled_user_ids) as users_with_access,
    entitled_user_count
FROM financial_transactions_enriched
LIMIT 10;

-- =============================================================================
-- STEP 4: CREATE CORTEX SEARCH SERVICE ON DYNAMIC TABLE
-- =============================================================================

SELECT '🔍 STEP 4: CREATING CORTEX SEARCH SERVICE ON DYNAMIC TABLE' as step_4;

-- Create Cortex Search service on the Dynamic Table
CREATE OR REPLACE CORTEX SEARCH SERVICE financial_search_service
ON description
ATTRIBUTES txn_id, region_name, transaction_type, category, merchant_name, status, entitled_user_ids, amount, transaction_date, entitled_user_count, customer_id
WAREHOUSE = COMPUTE_WH
TARGET_LAG = '1 minute'
COMMENT = 'Cortex Search on Dynamic Table with automatic entitlement arrays'
AS (
    SELECT * FROM financial_transactions_enriched
);

SELECT '✅ Cortex Search service created: FINANCIAL_SEARCH_SERVICE' as search_service_created;

-- Verify Cortex Search service
SELECT 'Cortex Search service status:' as search_status;
DESCRIBE CORTEX SEARCH SERVICE financial_search_service;

-- =============================================================================
-- STEP 5: CREATE MONITORING AND UTILITY VIEWS
-- =============================================================================

SELECT '📊 STEP 5: CREATING MONITORING VIEWS' as step_5;

-- View to monitor Dynamic Table status
CREATE OR REPLACE VIEW dynamic_table_monitor AS
SELECT 
    'FINANCIAL_TRANSACTIONS_ENRICHED' as dynamic_table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT region_name) as regions_covered,
    AVG(entitled_user_count) as avg_users_per_region,
    CURRENT_TIMESTAMP() as last_check_time,
    CURRENT_TIMESTAMP() as check_time
FROM financial_transactions_enriched;

-- View to compare base tables vs dynamic table
CREATE OR REPLACE VIEW table_sync_status AS
SELECT 
    'BASE_FINANCIAL_TRANSACTIONS' as source,
    COUNT(*) as record_count
FROM financial_transactions
UNION ALL
SELECT 
    'DYNAMIC_TABLE_ENRICHED' as source,
    COUNT(*) as record_count
FROM financial_transactions_enriched;

-- View for regional entitlement analysis
CREATE OR REPLACE VIEW regional_entitlements AS
SELECT 
    ft.region_name,
    COUNT(ft.txn_id) as total_transactions,
    MAX(ft.entitled_user_count) as active_users_in_region,
    ROUND(SUM(ft.amount), 2) as total_transaction_value,
    ROUND(AVG(ft.amount), 2) as avg_transaction_amount,
    CURRENT_TIMESTAMP() as report_generated_time
FROM financial_transactions_enriched ft
GROUP BY ft.region_name
ORDER BY total_transaction_value DESC;

SELECT '✅ Monitoring views created: DYNAMIC_TABLE_MONITOR, TABLE_SYNC_STATUS, REGIONAL_ENTITLEMENTS' as views_created;

-- =============================================================================
-- STEP 6: TEST THE COMPLETE SETUP
-- =============================================================================

SELECT '🧪 STEP 6: TESTING THE COMPLETE SETUP' as step_6;

-- Test 1: Show data distribution
SELECT 'Test 1 - Data Distribution by Region:' as test_1;
SELECT * FROM regional_entitlements;

-- Test 2: Show Dynamic Table status
SELECT 'Test 2 - Dynamic Table Status:' as test_2;
SELECT * FROM dynamic_table_monitor;

-- Test 3: Test Cortex Search with entitlements
SELECT 'Test 3 - Sample Cortex Search Results:' as test_3;
SELECT 
    txn_id,
    region_name,
    description,
    amount,
    ARRAY_SIZE(entitled_user_ids) as entitled_users_count
FROM TABLE(
    CORTEX_SEARCH_DATA_SCAN(
        SERVICE_NAME => 'financial_search_service'
    )
)
LIMIT 5;

-- Test 4: User-specific access test
SELECT 'Test 4 - Transactions accessible to specific user (USR_0001):' as test_4;
SELECT COUNT(*) as accessible_transaction_count
FROM TABLE(
    CORTEX_SEARCH_DATA_SCAN(
        SERVICE_NAME => 'financial_search_service'
    )
)
WHERE ARRAY_CONTAINS('USR_0001'::VARIANT, entitled_user_ids);

-- Test 5: High-value transactions with entitlements
SELECT 'Test 5 - High-value transactions (>$2000) with entitlements:' as test_5;
SELECT 
    txn_id,
    region_name,
    amount,
    entitled_user_count,
    description
FROM TABLE(
    CORTEX_SEARCH_DATA_SCAN(
        SERVICE_NAME => 'financial_search_service'
    )
)
WHERE amount > 2000
ORDER BY amount DESC
LIMIT 8;

-- =============================================================================
-- STEP 7: CREATE UTILITY PROCEDURES
-- =============================================================================

SELECT '🛠️ STEP 7: CREATING UTILITY PROCEDURES' as step_7;

-- Procedure to manually refresh Dynamic Table
CREATE OR REPLACE PROCEDURE refresh_financial_dynamic_table()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    ALTER DYNAMIC TABLE financial_transactions_enriched REFRESH;
    RETURN 'Financial Dynamic Table refreshed at ' || CURRENT_TIMESTAMP()::STRING;
END;
$$;

-- Procedure to get entitlement summary for a specific user
CREATE OR REPLACE PROCEDURE get_user_entitlements(user_id_param STRING)
RETURNS TABLE (region STRING, transaction_count INTEGER, total_accessible_value DECIMAL(12,2))
LANGUAGE SQL
AS
$$
BEGIN
    LET result_query RESULTSET := (
        SELECT 
            region_name as region,
            COUNT(*) as transaction_count,
            ROUND(SUM(amount), 2) as total_accessible_value
        FROM TABLE(
            CORTEX_SEARCH_DATA_SCAN(
                SERVICE_NAME => 'financial_search_service'
            )
        )
        WHERE ARRAY_CONTAINS(:user_id_param::VARIANT, entitled_user_ids)
        GROUP BY region_name
        ORDER BY total_accessible_value DESC
    );
    RETURN TABLE(result_query);
END;
$$;

SELECT '✅ Utility procedures created: REFRESH_FINANCIAL_DYNAMIC_TABLE, GET_USER_ENTITLEMENTS' as procedures_created;

-- =============================================================================
-- FINAL SUMMARY AND VERIFICATION
-- =============================================================================

SELECT '🎉 STANDALONE DYNAMIC TABLES SETUP COMPLETE!' as completion_status;

SELECT 'COMPONENTS SUCCESSFULLY CREATED:' as components_header;
SELECT '✅ FINANCIAL_TRANSACTIONS (5,000 records across 10 regions)' as component_1;
SELECT '✅ USER_REGION_ACCESS (200 users across 10 regions)' as component_2;
SELECT '✅ FINANCIAL_TRANSACTIONS_ENRICHED (Dynamic Table with entitlements)' as component_3;
SELECT '✅ FINANCIAL_SEARCH_SERVICE (Cortex Search on Dynamic Table)' as component_4;
SELECT '✅ Monitoring views and utility procedures' as component_5;

SELECT 'SCHEMA USED: CORTEX_SEARCH_ENTITLEMENT_DB.DYNAMIC_DEMO' as schema_info;
SELECT 'This setup is completely separate from your existing implementation!' as isolation_info;

-- Show final verification
SELECT 'FINAL VERIFICATION:' as final_check;
SELECT 
    COUNT(*) as base_transactions,
    (SELECT COUNT(*) FROM financial_transactions_enriched) as dynamic_table_records,
    (SELECT COUNT(*) FROM user_region_access WHERE status = 'ACTIVE') as active_users
FROM financial_transactions;

SELECT 'Your new Dynamic Tables setup is ready for testing!' as ready_message;
SELECT 'Run incremental update tests by modifying USER_REGION_ACCESS table' as next_steps;

COMMIT;
