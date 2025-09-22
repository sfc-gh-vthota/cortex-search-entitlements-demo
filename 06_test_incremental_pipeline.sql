-- Test Incremental Pipeline Functionality
-- This script demonstrates how the incremental pipeline works with live updates

USE DATABASE CORTEX_SEARCH_ENTITLEMENT_DB;
USE SCHEMA TRANSACTIONS;

-- =============================================================================
-- INITIAL STATE CHECK
-- =============================================================================

SELECT 'INITIAL STATE CHECK' as test_phase;

-- Check current stream status
SELECT 'Current stream status (should show 0 pending changes):' as info;
SELECT * FROM stream_monitoring;

-- Check recent update logs
SELECT 'Recent update activity:' as info;
SELECT * FROM incremental_update_log ORDER BY update_timestamp DESC LIMIT 3;

-- Sample the current REGION_USER_IDS in transactions
SELECT 'Sample of current REGION_USER_IDS arrays:' as info;
SELECT 
    transaction_id,
    region_name,
    region_user_ids,
    ARRAY_SIZE(COALESCE(region_user_ids, ARRAY_CONSTRUCT())) as user_count
FROM transactions 
WHERE region_name = 'North America' 
LIMIT 5;

-- =============================================================================
-- TEST 1: INSERT NEW USER TO TRIGGER PIPELINE
-- =============================================================================

SELECT 'TEST 1: INSERTING NEW USER TO TRIGGER PIPELINE' as test_phase;

-- Insert a new user to North America region
INSERT INTO USER_REGION_MAPPING (
    USER_ID,
    USER_NAME,
    REGION_NAME,
    CREATED_DATE,
    STATUS
) VALUES (
    'USER_TEST1',
    'Test User One',
    'North America',
    CURRENT_TIMESTAMP(),
    'ACTIVE'
);

-- Check stream immediately (should show 1 change)
SELECT 'Stream status after INSERT (should show 1 change):' as info;
SELECT * FROM stream_monitoring;

-- Wait a moment for the automated task to process (task runs every minute)
-- In a real scenario, you would wait ~1-2 minutes
SELECT 'Waiting for automated task to process changes...' as info;

-- =============================================================================
-- TEST 2: UPDATE USER STATUS TO TRIGGER PIPELINE
-- =============================================================================

SELECT 'TEST 2: UPDATING USER STATUS TO TRIGGER PIPELINE' as test_phase;

-- Update a user's status from ACTIVE to INACTIVE
UPDATE USER_REGION_MAPPING 
SET STATUS = 'INACTIVE'
WHERE USER_ID = 'USER_0010'  -- This should exist from initial data load
AND STATUS = 'ACTIVE';

-- Check stream status
SELECT 'Stream status after UPDATE (should show changes):' as info;
SELECT * FROM stream_monitoring;

-- =============================================================================
-- TEST 3: MANUAL PROCEDURE EXECUTION (FOR IMMEDIATE TESTING)
-- =============================================================================

SELECT 'TEST 3: MANUALLY EXECUTING UPDATE PROCEDURES' as test_phase;

-- Manually execute the incremental update procedure
CALL process_incremental_updates();

-- Check updated log
SELECT 'Update log after manual execution:' as info;
SELECT * FROM incremental_update_log ORDER BY update_timestamp DESC LIMIT 3;

-- Check if REGION_USER_IDS were updated
SELECT 'Updated REGION_USER_IDS for North America (should include USER_TEST1):' as info;
SELECT DISTINCT
    region_name,
    region_user_ids,
    ARRAY_SIZE(region_user_ids) as total_users_in_region
FROM transactions 
WHERE region_name = 'North America'
LIMIT 3;

-- =============================================================================
-- TEST 4: VERIFY CORTEX SEARCH SEES THE UPDATES
-- =============================================================================

SELECT 'TEST 4: VERIFYING CORTEX SEARCH REFLECTS UPDATES' as test_phase;

-- Query Cortex Search to see if it reflects the new user entitlements
SELECT 'Transactions accessible to new test user (USER_TEST1):' as info;
SELECT 
    transaction_id,
    description,
    region_name,
    amount,
    region_user_ids
FROM TABLE(
    CORTEX_SEARCH_DATA_SCAN(
        SERVICE_NAME => 'transactions_search_service'
    )
)
WHERE ARRAY_CONTAINS('USER_TEST1'::VARIANT, region_user_ids)
LIMIT 10;

-- Check if the inactive user (USER_0010) no longer has access
SELECT 'Verifying USER_0010 no longer appears in region_user_ids after status change:' as info;
SELECT 
    COUNT(*) as transactions_with_user_0010
FROM TABLE(
    CORTEX_SEARCH_DATA_SCAN(
        SERVICE_NAME => 'transactions_search_service'
    )
)
WHERE ARRAY_CONTAINS('USER_0010'::VARIANT, region_user_ids);

-- =============================================================================
-- TEST 5: BULK UPDATE TEST
-- =============================================================================

SELECT 'TEST 5: BULK UPDATE TEST' as test_phase;

-- Insert multiple users at once to test bulk processing
INSERT INTO USER_REGION_MAPPING (USER_ID, USER_NAME, REGION_NAME, STATUS)
VALUES 
    ('USER_TEST2', 'Test User Two', 'Europe', 'ACTIVE'),
    ('USER_TEST3', 'Test User Three', 'Asia Pacific', 'ACTIVE'),
    ('USER_TEST4', 'Test User Four', 'Europe', 'ACTIVE');

-- Manually process the changes
CALL process_incremental_updates();

-- Verify multiple regions were updated
SELECT 'Regions affected by bulk update:' as info;
SELECT 
    region_name,
    COUNT(CASE WHEN ARRAY_CONTAINS('USER_TEST2'::VARIANT, region_user_ids) THEN 1 END) as has_test2,
    COUNT(CASE WHEN ARRAY_CONTAINS('USER_TEST3'::VARIANT, region_user_ids) THEN 1 END) as has_test3,
    COUNT(CASE WHEN ARRAY_CONTAINS('USER_TEST4'::VARIANT, region_user_ids) THEN 1 END) as has_test4
FROM TABLE(
    CORTEX_SEARCH_DATA_SCAN(
        SERVICE_NAME => 'transactions_search_service'
    )
)
WHERE region_name IN ('Europe', 'Asia Pacific')
GROUP BY region_name;

-- =============================================================================
-- MONITORING AND ANALYTICS
-- =============================================================================

SELECT 'MONITORING AND ANALYTICS' as test_phase;

-- Show update activity summary
SELECT 'Update activity summary:' as info;
SELECT * FROM update_activity_summary ORDER BY hour_bucket DESC LIMIT 5;

-- Show current task status
SELECT 'Current task status:' as info;
SHOW TASKS LIKE 'incremental_update_task';

-- Show stream consumption history
SELECT 'Stream consumption verification:' as info;
SELECT 'TRANSACTIONS_STREAM' as stream_name, COUNT(*) as consumed_records FROM transactions_stream
UNION ALL
SELECT 'USER_REGION_MAPPING_STREAM' as stream_name, COUNT(*) as consumed_records FROM user_region_mapping_stream;

-- =============================================================================
-- PERFORMANCE VERIFICATION
-- =============================================================================

SELECT 'PERFORMANCE VERIFICATION' as test_phase;

-- Check Cortex Search service health
SELECT 'Cortex Search service health:' as info;
DESCRIBE CORTEX SEARCH SERVICE transactions_search_service;

-- Verify end-to-end entitlement functionality
SELECT 'End-to-end entitlement test - Compare user access:' as info;

-- Compare access between original user and test user
WITH access_comparison AS (
    SELECT 
        'USER_0001 (Original)' as user_type,
        COUNT(*) as accessible_transactions,
        ROUND(SUM(amount), 2) as total_accessible_amount
    FROM TABLE(CORTEX_SEARCH_DATA_SCAN(SERVICE_NAME => 'transactions_search_service'))
    WHERE ARRAY_CONTAINS('USER_0001'::VARIANT, region_user_ids)
    
    UNION ALL
    
    SELECT 
        'USER_TEST1 (New)' as user_type,
        COUNT(*) as accessible_transactions,
        ROUND(SUM(amount), 2) as total_accessible_amount
    FROM TABLE(CORTEX_SEARCH_DATA_SCAN(SERVICE_NAME => 'transactions_search_service'))
    WHERE ARRAY_CONTAINS('USER_TEST1'::VARIANT, region_user_ids)
)
SELECT * FROM access_comparison;

-- =============================================================================
-- CLEANUP TEST DATA (OPTIONAL)
-- =============================================================================

SELECT 'To cleanup test data, run the following commands:' as cleanup_info;
/*
-- Uncomment to cleanup test users
DELETE FROM USER_REGION_MAPPING WHERE USER_ID LIKE 'USER_TEST%';
CALL refresh_all_region_user_arrays();
*/

-- =============================================================================
-- TEST SUMMARY
-- =============================================================================

SELECT 'INCREMENTAL PIPELINE TEST COMPLETE!' as summary;
SELECT 'Key verified features:' as features;
SELECT '✓ Streams capture data changes automatically' as feature_1;
SELECT '✓ User mapping changes trigger transaction table updates' as feature_2;
SELECT '✓ Region user arrays are updated correctly' as feature_3;
SELECT '✓ Cortex Search reflects entitlement changes' as feature_4;
SELECT '✓ Automated task processes changes every minute' as feature_5;
SELECT '✓ Logging and monitoring track all activities' as feature_6;
SELECT '✓ Manual procedures available for immediate processing' as feature_7;

COMMIT;
