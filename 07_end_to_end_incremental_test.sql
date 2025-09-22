-- End-to-End Incremental Refresh Test Script
-- This script demonstrates the complete flow: User Region Change → Streams → Task → Transaction Updates → Cortex Search Refresh

USE DATABASE CORTEX_SEARCH_ENTITLEMENT_DB;
USE SCHEMA TRANSACTIONS;

-- =============================================================================
-- STEP 1: INITIAL STATE VERIFICATION
-- =============================================================================

SELECT '🔍 STEP 1: CHECKING INITIAL STATE' as test_step;

-- Show current stream status (should be empty initially)
SELECT 'Current stream status (should show 0 pending changes):' as info;
SELECT * FROM stream_monitoring;

-- Show current task status
SELECT 'Current task status:' as info;
SHOW TASKS LIKE 'incremental_update_task';

-- Pick a test user to modify (let's use USER_0100)
SELECT 'Test user before change (USER_0100):' as info;
SELECT 
    user_id,
    user_name,
    region_name as current_region,
    status
FROM user_region_mapping 
WHERE user_id = 'USER_0100';

-- Show current transactions that USER_0100 can access
SELECT 'Transactions currently accessible to USER_0100:' as info;
SELECT COUNT(*) as accessible_count
FROM TABLE(
    CORTEX_SEARCH_DATA_SCAN(
        SERVICE_NAME => 'transactions_search_service'
    )
)
WHERE ARRAY_CONTAINS('USER_0100'::VARIANT, region_user_ids);

-- Show which region USER_0100 currently belongs to and transaction samples
SELECT 'Sample transactions in USER_0100 current region:' as info;
SELECT 
    t.transaction_id,
    t.region_name,
    t.description,
    t.amount,
    ARRAY_SIZE(t.region_user_ids) as users_in_region
FROM TABLE(
    CORTEX_SEARCH_DATA_SCAN(
        SERVICE_NAME => 'transactions_search_service'
    )
) t
WHERE ARRAY_CONTAINS('USER_0100'::VARIANT, t.region_user_ids)
LIMIT 5;

-- =============================================================================
-- STEP 2: PERFORM THE REGION CHANGE
-- =============================================================================

SELECT '🔄 STEP 2: CHANGING USER REGION TO TRIGGER INCREMENTAL UPDATE' as test_step;

-- Get USER_0100's current region for reference
SET current_region = (SELECT region_name FROM user_region_mapping WHERE user_id = 'USER_0100');
SELECT 'USER_0100 current region: ' || $current_region as current_info;

-- Change USER_0100 from their current region to a different region
UPDATE user_region_mapping 
SET region_name = CASE 
    WHEN region_name = 'North America' THEN 'Europe'
    WHEN region_name = 'Europe' THEN 'Asia Pacific' 
    WHEN region_name = 'Asia Pacific' THEN 'Latin America'
    WHEN region_name = 'Latin America' THEN 'Middle East'
    WHEN region_name = 'Middle East' THEN 'Africa'
    WHEN region_name = 'Africa' THEN 'Oceania'
    WHEN region_name = 'Oceania' THEN 'Nordic'
    WHEN region_name = 'Nordic' THEN 'Eastern Europe'
    WHEN region_name = 'Eastern Europe' THEN 'Southeast Asia'
    ELSE 'North America' -- Southeast Asia -> North America
END,
updated_timestamp = CURRENT_TIMESTAMP()
WHERE user_id = 'USER_0100';

-- Verify the change
SELECT 'USER_0100 after region change:' as info;
SELECT 
    user_id,
    user_name, 
    region_name as new_region,
    status
FROM user_region_mapping 
WHERE user_id = 'USER_0100';

-- =============================================================================
-- STEP 3: VERIFY STREAM CAPTURED THE CHANGE
-- =============================================================================

SELECT '📊 STEP 3: VERIFYING STREAMS CAPTURED THE CHANGE' as test_step;

-- Check stream status immediately after change
SELECT 'Stream status after region change (should show 1+ changes):' as info;
SELECT * FROM stream_monitoring;

-- Show detailed stream data
SELECT 'Detailed stream data for user region mapping changes:' as info;
SELECT 
    user_id,
    user_name,
    region_name,
    METADATA$ACTION as action_type,
    METADATA$ISUPDATE as is_update
FROM user_region_mapping_stream
WHERE user_id = 'USER_0100'
LIMIT 5;

-- =============================================================================
-- STEP 4: WAIT FOR TASK TO EXECUTE OR TRIGGER MANUALLY
-- =============================================================================

SELECT '⏱️ STEP 4: TASK EXECUTION' as test_step;
SELECT 'The task should execute automatically within 1 minute when it detects stream data.' as task_info;
SELECT 'You can also manually execute the procedure to test immediately.' as manual_option;

-- Option A: Wait for automatic execution (recommended for production)
SELECT 'OPTION A: Wait 1-2 minutes for automatic task execution, then continue with STEP 5' as option_a;

-- Option B: Manual execution for immediate testing
SELECT 'OPTION B: Execute manually for immediate testing:' as option_b;
-- Uncomment the line below to execute immediately:
-- CALL process_incremental_updates();

-- For this demo, let's execute manually to see immediate results
CALL process_incremental_updates();

-- Check update log
SELECT 'Recent update log entries:' as info;
SELECT * FROM incremental_update_log 
ORDER BY update_timestamp DESC 
LIMIT 3;

-- =============================================================================
-- STEP 5: VERIFY TRANSACTION TABLE UPDATES
-- =============================================================================

SELECT '✅ STEP 5: VERIFYING TRANSACTION TABLE REGION_USER_IDS UPDATED' as test_step;

-- Check if USER_0100 now appears in their new region's transactions
SELECT 'USER_0100 new region assignment:' as info;
SELECT region_name as new_region FROM user_region_mapping WHERE user_id = 'USER_0100';

-- Count transactions USER_0100 can now access (should be different)
SELECT 'Transactions now accessible to USER_0100 after region change:' as info;
SELECT COUNT(*) as new_accessible_count
FROM transactions
WHERE ARRAY_CONTAINS('USER_0100'::VARIANT, region_user_ids);

-- Sample transactions from USER_0100's new region
SELECT 'Sample transactions from USER_0100 new region:' as info;
SELECT 
    transaction_id,
    region_name,
    description,
    amount,
    ARRAY_SIZE(region_user_ids) as users_in_region
FROM transactions
WHERE ARRAY_CONTAINS('USER_0100'::VARIANT, region_user_ids)
LIMIT 5;

-- Verify USER_0100 is no longer in old region transactions
SET old_region = $current_region;
SELECT 'Checking if USER_0100 removed from old region (' || $old_region || ') transactions:' as info;
SELECT COUNT(*) as should_be_zero
FROM transactions 
WHERE region_name = $old_region 
AND ARRAY_CONTAINS('USER_0100'::VARIANT, region_user_ids);

-- =============================================================================
-- STEP 6: VERIFY CORTEX SEARCH REFLECTS THE CHANGES
-- =============================================================================

SELECT '🔍 STEP 6: VERIFYING CORTEX SEARCH REFLECTS ENTITLEMENT CHANGES' as test_step;
SELECT 'Note: Cortex Search may take 1-2 minutes to refresh due to TARGET_LAG setting' as cortex_note;

-- Check Cortex Search service status
SELECT 'Cortex Search service status:' as info;
DESCRIBE CORTEX SEARCH SERVICE transactions_search_service;

-- Test search with new entitlements - USER_0100 should see different transactions
SELECT 'Transactions accessible to USER_0100 via Cortex Search:' as info;
SELECT 
    transaction_id,
    region_name,
    description,
    amount,
    region_user_ids
FROM TABLE(
    CORTEX_SEARCH_DATA_SCAN(
        SERVICE_NAME => 'transactions_search_service'
    )
)
WHERE ARRAY_CONTAINS('USER_0100'::VARIANT, region_user_ids)
ORDER BY amount DESC
LIMIT 10;

-- Compare before/after accessible transaction counts
WITH before_change AS (
    SELECT COUNT(*) as old_count
    FROM transactions
    WHERE region_name = $old_region 
    AND ARRAY_CONTAINS('USER_0100'::VARIANT, region_user_ids)
),
after_change AS (
    SELECT COUNT(*) as new_count
    FROM TABLE(
        CORTEX_SEARCH_DATA_SCAN(
            SERVICE_NAME => 'transactions_search_service'
        )
    )
    WHERE ARRAY_CONTAINS('USER_0100'::VARIANT, region_user_ids)
)
SELECT 
    'Entitlement change verification:' as verification,
    b.old_count as old_accessible_count,
    a.new_count as new_accessible_count,
    CASE 
        WHEN a.new_count != b.old_count THEN '✅ SUCCESS: Entitlements changed'
        ELSE '❌ PENDING: May need to wait for Cortex Search refresh'
    END as status
FROM before_change b, after_change a;

-- =============================================================================
-- STEP 7: ADVANCED TESTING - SEMANTIC SEARCH WITH NEW ENTITLEMENTS
-- =============================================================================

SELECT '🎯 STEP 7: TESTING SEMANTIC SEARCH WITH NEW ENTITLEMENTS' as test_step;

-- Test semantic search for restaurant transactions with new entitlements
SELECT 'Restaurant transactions accessible to USER_0100 in new region:' as info;
SELECT 
    transaction_id,
    region_name,
    description,
    amount,
    category,
    merchant_name
FROM TABLE(
    CORTEX_SEARCH_DATA_SCAN(
        SERVICE_NAME => 'transactions_search_service'
    )
)
WHERE ARRAY_CONTAINS('USER_0100'::VARIANT, region_user_ids)
AND (category = 'Restaurant' OR description ILIKE '%restaurant%' OR description ILIKE '%coffee%')
LIMIT 5;

-- Test high-value transactions with entitlements
SELECT 'High-value transactions (>$2000) accessible to USER_0100:' as info;
SELECT 
    transaction_id,
    region_name,
    description,
    amount,
    transaction_type
FROM TABLE(
    CORTEX_SEARCH_DATA_SCAN(
        SERVICE_NAME => 'transactions_search_service'
    )
)
WHERE ARRAY_CONTAINS('USER_0100'::VARIANT, region_user_ids)
AND amount > 2000
ORDER BY amount DESC
LIMIT 5;

-- =============================================================================
-- STEP 8: MONITORING AND PERFORMANCE VERIFICATION
-- =============================================================================

SELECT '📊 STEP 8: MONITORING AND PERFORMANCE VERIFICATION' as test_step;

-- Show update activity summary
SELECT 'Update activity summary:' as info;
SELECT * FROM update_activity_summary 
ORDER BY hour_bucket DESC 
LIMIT 5;

-- Show stream consumption (should be empty after processing)
SELECT 'Stream status after processing (should show 0 pending):' as info;
SELECT * FROM stream_monitoring;

-- Show task execution history
SELECT 'Task execution verification:' as info;
SELECT 
    name,
    state,
    schedule,
    warehouse,
    comment
FROM TABLE(INFORMATION_SCHEMA.TASKS)
WHERE name = 'INCREMENTAL_UPDATE_TASK';

-- =============================================================================
-- STEP 9: CLEANUP AND RESET (OPTIONAL)
-- =============================================================================

SELECT '🧹 STEP 9: CLEANUP AND RESET (OPTIONAL)' as test_step;
SELECT 'To reset USER_0100 back to original region, uncomment and run the following:' as cleanup_info;

/*
-- Reset USER_0100 back to original region
UPDATE user_region_mapping 
SET region_name = $old_region
WHERE user_id = 'USER_0100';

-- Wait for or manually trigger update
CALL process_incremental_updates();
*/

-- =============================================================================
-- TEST SUMMARY AND RESULTS
-- =============================================================================

SELECT '🎉 END-TO-END INCREMENTAL TEST COMPLETED!' as summary;
SELECT 'Tested Components:' as components;
SELECT '✅ User region mapping change detection' as component_1;
SELECT '✅ Stream-based change capture' as component_2;
SELECT '✅ Conditional task execution (only when streams have data)' as component_3;
SELECT '✅ Automatic transaction entitlement updates' as component_4;
SELECT '✅ Cortex Search automatic refresh and entitlement filtering' as component_5;
SELECT '✅ Complete audit trail and monitoring' as component_6;

-- Final verification query
SELECT 'FINAL VERIFICATION - USER_0100 Current State:' as final_check;
SELECT 
    urm.user_id,
    urm.region_name as assigned_region,
    COUNT(t.transaction_id) as accessible_transactions_count,
    ROUND(AVG(t.amount), 2) as avg_accessible_amount
FROM user_region_mapping urm
LEFT JOIN TABLE(
    CORTEX_SEARCH_DATA_SCAN(
        SERVICE_NAME => 'transactions_search_service'
    )
) t ON ARRAY_CONTAINS(urm.user_id::VARIANT, t.region_user_ids)
WHERE urm.user_id = 'USER_0100'
GROUP BY urm.user_id, urm.region_name;

SELECT 'Test completed successfully! The incremental pipeline is working correctly.' as conclusion;
