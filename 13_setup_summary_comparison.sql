-- Setup Summary and Comparison - Original vs Standalone
-- This script shows what you now have in your database

-- =============================================================================
-- CORTEX SEARCH ENTITLEMENTS - SETUP SUMMARY
-- =============================================================================

SELECT '🏗️ YOUR CORTEX SEARCH ENTITLEMENTS IMPLEMENTATIONS' as summary_title;
SELECT '' as separator;

-- =============================================================================
-- ORIGINAL SETUP (Your existing implementation)
-- =============================================================================

SELECT '🔄 ORIGINAL SETUP (Existing Implementation)' as original_header;
SELECT 'Schema: CORTEX_SEARCH_ENTITLEMENT_DB.TRANSACTIONS' as original_schema;
SELECT '' as separator;

-- Check if original tables exist
SELECT 'Original Setup Tables:' as original_tables_header;

BEGIN
    SELECT 'TRANSACTIONS' as table_name, COUNT(*) as records 
    FROM CORTEX_SEARCH_ENTITLEMENT_DB.TRANSACTIONS.TRANSACTIONS;
EXCEPTION
    WHEN OTHER THEN
        SELECT 'TRANSACTIONS' as table_name, 'NOT FOUND' as records;
END;

BEGIN
    SELECT 'USER_REGION_MAPPING' as table_name, COUNT(*) as records 
    FROM CORTEX_SEARCH_ENTITLEMENT_DB.TRANSACTIONS.USER_REGION_MAPPING;
EXCEPTION
    WHEN OTHER THEN
        SELECT 'USER_REGION_MAPPING' as table_name, 'NOT FOUND' as records;
END;

-- Check original Cortex Search services
SELECT 'Original Cortex Search Services:' as original_search_header;

BEGIN
    DESCRIBE CORTEX SEARCH SERVICE CORTEX_SEARCH_ENTITLEMENT_DB.TRANSACTIONS.transactions_search_service;
    SELECT '✅ transactions_search_service (Streams & Tasks)' as service_status;
EXCEPTION
    WHEN OTHER THEN
        SELECT '❌ transactions_search_service (Not found)' as service_status;
END;

BEGIN
    DESCRIBE CORTEX SEARCH SERVICE CORTEX_SEARCH_ENTITLEMENT_DB.TRANSACTIONS.transactions_dynamic_search_service;
    SELECT '✅ transactions_dynamic_search_service (Dynamic Tables)' as service_status;
EXCEPTION
    WHEN OTHER THEN
        SELECT '❌ transactions_dynamic_search_service (Not found)' as service_status;
END;

-- Check for streams and tasks (if using that approach)
SELECT 'Original Streams & Tasks (if implemented):' as original_streams_header;

BEGIN
    SHOW STREAMS IN SCHEMA CORTEX_SEARCH_ENTITLEMENT_DB.TRANSACTIONS;
    SELECT 'Streams found - check output above' as streams_status;
EXCEPTION
    WHEN OTHER THEN
        SELECT 'No streams found or schema not accessible' as streams_status;
END;

BEGIN
    SHOW TASKS IN SCHEMA CORTEX_SEARCH_ENTITLEMENT_DB.TRANSACTIONS;
    SELECT 'Tasks found - check output above' as tasks_status;
EXCEPTION
    WHEN OTHER THEN
        SELECT 'No tasks found or schema not accessible' as tasks_status;
END;

SELECT '' as separator;

-- =============================================================================
-- NEW STANDALONE SETUP 
-- =============================================================================

USE DATABASE CORTEX_SEARCH_ENTITLEMENT_DB;
USE SCHEMA DYNAMIC_DEMO;

SELECT '⚡ NEW STANDALONE SETUP (Just Created)' as standalone_header;
SELECT 'Schema: CORTEX_SEARCH_ENTITLEMENT_DB.DYNAMIC_DEMO' as standalone_schema;
SELECT '' as separator;

-- Check standalone tables
SELECT 'Standalone Setup Tables:' as standalone_tables_header;

BEGIN
    SELECT 'FINANCIAL_TRANSACTIONS' as table_name, COUNT(*) as records 
    FROM financial_transactions;
EXCEPTION
    WHEN OTHER THEN
        SELECT 'FINANCIAL_TRANSACTIONS' as table_name, 'NOT FOUND' as records;
END;

BEGIN
    SELECT 'USER_REGION_ACCESS' as table_name, COUNT(*) as records 
    FROM user_region_access;
EXCEPTION
    WHEN OTHER THEN
        SELECT 'USER_REGION_ACCESS' as table_name, 'NOT FOUND' as records;
END;

-- Check Dynamic Table
SELECT 'Standalone Dynamic Table:' as dynamic_table_header;

BEGIN
    SELECT 'FINANCIAL_TRANSACTIONS_ENRICHED' as table_name, COUNT(*) as records 
    FROM financial_transactions_enriched;
    SELECT '✅ Dynamic Table with automatic entitlement arrays' as dynamic_status;
EXCEPTION
    WHEN OTHER THEN
        SELECT 'FINANCIAL_TRANSACTIONS_ENRICHED' as table_name, 'NOT FOUND' as records;
        SELECT '❌ Dynamic Table not found' as dynamic_status;
END;

-- Check standalone Cortex Search service
SELECT 'Standalone Cortex Search Service:' as standalone_search_header;

BEGIN
    DESCRIBE CORTEX SEARCH SERVICE financial_search_service;
    SELECT '✅ financial_search_service (Active)' as standalone_service_status;
EXCEPTION
    WHEN OTHER THEN
        SELECT '❌ financial_search_service (Not found)' as standalone_service_status;
END;

-- Show Dynamic Table info
SELECT 'Dynamic Table Monitoring:' as monitoring_header;
BEGIN
    SELECT * FROM dynamic_table_monitor;
EXCEPTION
    WHEN OTHER THEN
        SELECT 'Dynamic Table monitoring not available' as monitor_status;
END;

SELECT '' as separator;

-- =============================================================================
-- SIDE-BY-SIDE COMPARISON
-- =============================================================================

SELECT '📊 SETUP COMPARISON' as comparison_header;
SELECT '' as separator;

WITH comparison AS (
    SELECT 'Schema' as feature, 'TRANSACTIONS' as original_setup, 'DYNAMIC_DEMO' as standalone_setup
    UNION ALL
    SELECT 'Main Table', 'TRANSACTIONS', 'FINANCIAL_TRANSACTIONS'
    UNION ALL  
    SELECT 'User Table', 'USER_REGION_MAPPING', 'USER_REGION_ACCESS'
    UNION ALL
    SELECT 'Dynamic Table', 'transactions_with_entitlements (maybe)', 'FINANCIAL_TRANSACTIONS_ENRICHED'
    UNION ALL
    SELECT 'Search Service', 'transactions_*_search_service', 'financial_search_service'
    UNION ALL
    SELECT 'Approach', 'Streams/Tasks OR Dynamic Tables', 'Dynamic Tables Only'
    UNION ALL
    SELECT 'Records', '10,000 transactions', '5,000 transactions'
    UNION ALL
    SELECT 'Users', '1,000 users', '200 users'
    UNION ALL
    SELECT 'Regions', '10 regions', '10 regions'
    UNION ALL
    SELECT 'Isolation', 'Original implementation', 'Completely separate'
)
SELECT * FROM comparison;

SELECT '' as separator;

-- =============================================================================
-- USAGE RECOMMENDATIONS
-- =============================================================================

SELECT '🎯 USAGE RECOMMENDATIONS' as recommendations_header;
SELECT '' as separator;

SELECT '✅ STANDALONE SETUP (DYNAMIC_DEMO):' as standalone_recommend;
SELECT '   • Use for testing Dynamic Tables approach' as standalone_use_1;
SELECT '   • Perfect for learning and experimentation' as standalone_use_2;
SELECT '   • Completely isolated - no conflicts' as standalone_use_3;
SELECT '   • Simpler data set (5K transactions, 200 users)' as standalone_use_4;
SELECT '   • Run: @11_standalone_dynamic_tables_setup.sql' as standalone_setup_cmd;
SELECT '   • Test: @12_test_standalone_incremental_updates.sql' as standalone_test_cmd;
SELECT '' as separator;

SELECT '🔄 ORIGINAL SETUP (TRANSACTIONS):' as original_recommend;
SELECT '   • Your main implementation with full data set' as original_use_1;
SELECT '   • Can use either Streams/Tasks OR Dynamic Tables approach' as original_use_2;
SELECT '   • Production-ready with 10K transactions, 1K users' as original_use_3;
SELECT '   • Choose from existing scripts 04 or 08' as original_use_4;
SELECT '' as separator;

-- =============================================================================
-- QUICK START COMMANDS
-- =============================================================================

SELECT '🚀 QUICK START COMMANDS' as quickstart_header;
SELECT '' as separator;

SELECT '⚡ TO USE THE NEW STANDALONE SETUP:' as standalone_quickstart;
SELECT '@11_standalone_dynamic_tables_setup.sql   -- Creates everything' as standalone_cmd_1;
SELECT '@12_test_standalone_incremental_updates.sql  -- Tests incremental updates' as standalone_cmd_2;
SELECT '' as separator;

SELECT '🔄 TO USE YOUR ORIGINAL SETUP (if not done yet):' as original_quickstart;
SELECT '-- Choose ONE of these approaches:' as original_choice;
SELECT '@08_create_dynamic_table_solution.sql     -- Dynamic Tables (Recommended)' as original_cmd_1;
SELECT '@04_create_incremental_pipeline.sql       -- Streams & Tasks (Traditional)' as original_cmd_2;
SELECT '' as separator;

-- =============================================================================
-- TESTING BOTH SETUPS
-- =============================================================================

SELECT '🧪 TESTING BOTH SETUPS' as testing_header;
SELECT '' as separator;

SELECT 'Test Standalone Setup (DYNAMIC_DEMO):' as test_standalone;
SELECT 'USE SCHEMA CORTEX_SEARCH_ENTITLEMENT_DB.DYNAMIC_DEMO;' as test_standalone_cmd_1;
SELECT '@12_test_standalone_incremental_updates.sql' as test_standalone_cmd_2;
SELECT '' as separator;

SELECT 'Test Original Setup (TRANSACTIONS):' as test_original;
SELECT 'USE SCHEMA CORTEX_SEARCH_ENTITLEMENT_DB.TRANSACTIONS;' as test_original_cmd_1;
SELECT '@09_test_dynamic_table_incremental.sql  -- If using Dynamic Tables' as test_original_cmd_2;
SELECT '@06_test_incremental_pipeline.sql       -- If using Streams & Tasks' as test_original_cmd_3;
SELECT '' as separator;

-- =============================================================================
-- CURRENT STATUS CHECK
-- =============================================================================

SELECT '📋 CURRENT STATUS CHECK' as status_header;
SELECT '' as separator;

SELECT 'Standalone Setup Status:' as standalone_status;
SELECT 
    CURRENT_DATABASE() as current_database,
    CURRENT_SCHEMA() as current_schema;

SELECT 
    (SELECT COUNT(*) FROM financial_transactions) as financial_transactions,
    (SELECT COUNT(*) FROM user_region_access) as user_region_access,
    (SELECT COUNT(*) FROM financial_transactions_enriched) as dynamic_table_records;

SELECT '' as separator;

SELECT '🎉 YOU NOW HAVE TWO COMPLETE IMPLEMENTATIONS!' as final_status;
SELECT '   1️⃣  Original setup (TRANSACTIONS schema) - Full featured' as implementation_1;
SELECT '   2️⃣  Standalone setup (DYNAMIC_DEMO schema) - Testing focused' as implementation_2;
SELECT '   Both demonstrate Cortex Search with Dynamic Tables!' as both_feature;
SELECT '' as separator;

SELECT '🎯 NEXT STEPS:' as next_steps;
SELECT '• Test the standalone setup with @12_test_standalone_incremental_updates.sql' as next_1;
SELECT '• Compare approaches with @10_comparison_streams_vs_dynamic_tables.sql' as next_2;  
SELECT '• Explore search capabilities with @05_cortex_search_examples.sql' as next_3;

COMMIT;

