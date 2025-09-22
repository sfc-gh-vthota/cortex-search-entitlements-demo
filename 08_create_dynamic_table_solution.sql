-- Dynamic Table Implementation for Incremental Cortex Search Updates
-- This script creates a Dynamic Table approach that automatically maintains
-- the REGION_USER_IDS array and keeps Cortex Search updated

USE DATABASE CORTEX_SEARCH_ENTITLEMENT_DB;
USE SCHEMA TRANSACTIONS;

-- =============================================================================
-- CREATE DYNAMIC TABLE FOR TRANSACTIONS WITH AUTO-UPDATED ENTITLEMENTS
-- =============================================================================

-- Create a Dynamic Table that automatically maintains the REGION_USER_IDS array
-- This table will automatically refresh when either TRANSACTIONS or USER_REGION_MAPPING changes
CREATE OR REPLACE DYNAMIC TABLE transactions_with_entitlements
TARGET_LAG = '1 minute'
WAREHOUSE = COMPUTE_WH
AS
SELECT 
    t.transaction_id,
    t.user_id,
    t.transaction_date,
    t.amount,
    t.transaction_type,
    t.description,
    t.region_name,
    t.merchant_name,
    t.category,
    t.status,
    -- Automatically populate REGION_USER_IDS array from USER_REGION_MAPPING
    COALESCE(
        (SELECT ARRAY_AGG(urm.user_id) 
         FROM user_region_mapping urm 
         WHERE urm.region_name = t.region_name 
         AND urm.status = 'ACTIVE'),
        ARRAY_CONSTRUCT()
    ) AS region_user_ids,
    CURRENT_TIMESTAMP() as last_updated
FROM transactions t;

-- =============================================================================
-- CREATE CORTEX SEARCH SERVICE ON DYNAMIC TABLE
-- =============================================================================

-- Drop existing Cortex Search service if it exists
DROP CORTEX SEARCH SERVICE IF EXISTS transactions_search_service;

-- Create Cortex Search service on the Dynamic Table
CREATE OR REPLACE CORTEX SEARCH SERVICE transactions_dynamic_search_service
ON description
ATTRIBUTES transaction_id, region_name, transaction_type, category, merchant_name, status, region_user_ids, amount, transaction_date
WAREHOUSE = COMPUTE_WH
TARGET_LAG = '1 minute'
COMMENT = 'Cortex Search service on Dynamic Table with automatic entitlement updates'
AS (
    SELECT * FROM transactions_with_entitlements
);

-- =============================================================================
-- VERIFICATION AND MONITORING VIEWS
-- =============================================================================

-- View to monitor Dynamic Table refresh status
CREATE OR REPLACE VIEW dynamic_table_status AS
SELECT 
    'TRANSACTIONS_WITH_ENTITLEMENTS' as table_name,
    CURRENT_TIMESTAMP() as check_time,
    COUNT(*) as total_records
FROM transactions_with_entitlements;

-- View to compare base table vs dynamic table record counts
CREATE OR REPLACE VIEW table_comparison AS
SELECT 
    'BASE_TRANSACTIONS' as source,
    COUNT(*) as record_count,
    MAX(COALESCE(ARRAY_SIZE(region_user_ids), 0)) as max_users_per_region
FROM transactions
UNION ALL
SELECT 
    'DYNAMIC_TABLE' as source,
    COUNT(*) as record_count,
    MAX(ARRAY_SIZE(region_user_ids)) as max_users_per_region
FROM transactions_with_entitlements;

-- View to monitor entitlement array population
CREATE OR REPLACE VIEW entitlement_summary AS
SELECT 
    region_name,
    COUNT(*) as transaction_count,
    MAX(ARRAY_SIZE(region_user_ids)) as users_in_region,
    MIN(ARRAY_SIZE(region_user_ids)) as min_users,
    AVG(ARRAY_SIZE(region_user_ids)) as avg_users_per_transaction
FROM transactions_with_entitlements
GROUP BY region_name
ORDER BY region_name;

-- =============================================================================
-- UTILITY PROCEDURES FOR DYNAMIC TABLE MANAGEMENT
-- =============================================================================

-- Procedure to manually refresh Dynamic Table (if needed)
CREATE OR REPLACE PROCEDURE refresh_dynamic_table()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    ALTER DYNAMIC TABLE transactions_with_entitlements REFRESH;
    RETURN 'Dynamic table refreshed successfully at ' || CURRENT_TIMESTAMP()::STRING;
END;
$$;

-- Procedure to get Dynamic Table information
CREATE OR REPLACE PROCEDURE get_dynamic_table_info()
RETURNS TABLE (property STRING, value STRING)
LANGUAGE SQL
AS
$$
BEGIN
    LET result_table RESULTSET := (
        SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
    );
    
    -- Show Dynamic Table details
    SHOW DYNAMIC TABLES LIKE 'transactions_with_entitlements';
    
    RETURN TABLE(result_table);
END;
$$;

-- =============================================================================
-- VERIFICATION QUERIES
-- =============================================================================

SELECT '🔍 VERIFYING DYNAMIC TABLE SETUP' as status;

-- Check Dynamic Table status
SELECT 'Dynamic Table Status:' as info;
SELECT * FROM dynamic_table_status;

-- Compare record counts
SELECT 'Record Count Comparison:' as info;
SELECT * FROM table_comparison;

-- Show entitlement summary
SELECT 'Entitlement Array Summary by Region:' as info;
SELECT * FROM entitlement_summary;

-- Show sample data from Dynamic Table
SELECT 'Sample Data from Dynamic Table:' as info;
SELECT 
    transaction_id,
    region_name,
    amount,
    ARRAY_SIZE(region_user_ids) as users_with_access,
    last_updated
FROM transactions_with_entitlements
LIMIT 10;

-- Verify Cortex Search service status
SELECT 'Cortex Search Service Status:' as info;
DESCRIBE CORTEX SEARCH SERVICE transactions_dynamic_search_service;

-- Test Cortex Search on Dynamic Table
SELECT 'Testing Cortex Search on Dynamic Table:' as info;
SELECT 
    transaction_id,
    region_name,
    description,
    amount,
    ARRAY_SIZE(region_user_ids) as entitled_users_count
FROM TABLE(
    CORTEX_SEARCH_DATA_SCAN(
        SERVICE_NAME => 'transactions_dynamic_search_service'
    )
)
LIMIT 5;

-- =============================================================================
-- BENEFITS OF DYNAMIC TABLE APPROACH
-- =============================================================================

SELECT '✅ DYNAMIC TABLE BENEFITS:' as benefits;
SELECT '1. Automatic incremental refresh - no manual stream/task management' as benefit_1;
SELECT '2. Built-in change detection and processing' as benefit_2;
SELECT '3. Optimized refresh logic handled by Snowflake' as benefit_3;
SELECT '4. Simpler architecture - no stored procedures needed' as benefit_4;
SELECT '5. Better performance for complex transformations' as benefit_5;
SELECT '6. Automatic dependency management' as benefit_6;
SELECT '7. Cortex Search automatically picks up Dynamic Table changes' as benefit_7;

-- =============================================================================
-- MONITORING AND MAINTENANCE
-- =============================================================================

-- Show Dynamic Tables in the schema
SELECT 'Current Dynamic Tables:' as info;
SHOW DYNAMIC TABLES;

-- Show Dynamic Table refresh history (if available)
SELECT 'Dynamic Table refresh can be monitored through:' as monitoring_info;
SELECT '- Information Schema views' as method_1;
SELECT '- Account usage views' as method_2;
SELECT '- Query history for automatic refreshes' as method_3;

COMMIT;

-- =============================================================================
-- NEXT STEPS
-- =============================================================================

SELECT '🎯 DYNAMIC TABLE SETUP COMPLETE!' as completion_status;
SELECT 'Your incremental pipeline now uses:' as architecture;
SELECT '✓ Dynamic Table: transactions_with_entitlements (auto-refreshes every minute)' as component_1;
SELECT '✓ Cortex Search: transactions_dynamic_search_service (indexes Dynamic Table)' as component_2;
SELECT '✓ Automatic entitlement updates when USER_REGION_MAPPING changes' as component_3;
SELECT '✓ No manual streams/tasks needed - all handled automatically!' as component_4;

SELECT 'Run 09_test_dynamic_table_incremental.sql to test the functionality!' as next_step;

