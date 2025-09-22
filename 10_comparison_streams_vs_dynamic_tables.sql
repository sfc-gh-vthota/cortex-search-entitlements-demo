-- Comparison: Streams & Tasks vs Dynamic Tables for Incremental Cortex Search
-- This script demonstrates both approaches and their key differences

USE DATABASE CORTEX_SEARCH_ENTITLEMENT_DB;
USE SCHEMA TRANSACTIONS;

-- =============================================================================
-- APPROACH COMPARISON OVERVIEW
-- =============================================================================

SELECT '📊 INCREMENTAL CORTEX SEARCH: STREAMS vs DYNAMIC TABLES COMPARISON' as title;

-- =============================================================================
-- APPROACH 1: STREAMS & TASKS (Traditional Event-Driven)
-- =============================================================================

SELECT '🔄 APPROACH 1: STREAMS & TASKS (Event-Driven Architecture)' as approach_1;

-- Show current streams setup
SELECT 'Current Streams Configuration:' as streams_info;
SHOW STREAMS;

-- Show current tasks setup  
SELECT 'Current Tasks Configuration:' as tasks_info;
SHOW TASKS;

-- Show stored procedures
SELECT 'Custom Stored Procedures:' as procedures_info;
SHOW PROCEDURES LIKE '%incremental%';

-- =============================================================================
-- APPROACH 2: DYNAMIC TABLES (Declarative Automatic)
-- =============================================================================

SELECT '⚡ APPROACH 2: DYNAMIC TABLES (Declarative Architecture)' as approach_2;

-- Show Dynamic Tables setup
SELECT 'Current Dynamic Tables:' as dynamic_info;
SHOW DYNAMIC TABLES;

-- Show Dynamic Table definition
SELECT 'Dynamic Table Structure:' as structure_info;
DESCRIBE TABLE transactions_with_entitlements;

-- =============================================================================
-- FEATURE BY FEATURE COMPARISON
-- =============================================================================

SELECT '📋 DETAILED FEATURE COMPARISON' as comparison_title;

-- Create comparison table
WITH feature_comparison AS (
    SELECT 'Change Detection' as feature, 'Manual Streams' as streams_approach, 'Automatic' as dynamic_tables_approach, 'Dynamic Tables win - built-in change detection' as winner
    UNION ALL
    SELECT 'Setup Complexity', 'High - Streams + Tasks + Procedures', 'Low - Single Dynamic Table', 'Dynamic Tables win - simpler setup'
    UNION ALL  
    SELECT 'Maintenance', 'Manual - Monitor streams, tasks, procedures', 'Automatic - Snowflake managed', 'Dynamic Tables win - zero maintenance'
    UNION ALL
    SELECT 'Error Handling', 'Custom exception handling required', 'Built-in error handling', 'Dynamic Tables win - robust error handling'
    UNION ALL
    SELECT 'Performance', 'Good - but requires tuning', 'Excellent - Snowflake optimized', 'Dynamic Tables win - optimized by Snowflake'
    UNION ALL
    SELECT 'Refresh Control', 'High - Custom logic possible', 'Medium - Declarative only', 'Streams win - more flexibility'
    UNION ALL
    SELECT 'Monitoring', 'Custom monitoring required', 'Built-in monitoring', 'Dynamic Tables win - integrated monitoring'
    UNION ALL
    SELECT 'Cost Efficiency', 'Good - runs only when needed', 'Excellent - optimized refresh patterns', 'Dynamic Tables win - Snowflake optimization'
    UNION ALL
    SELECT 'Learning Curve', 'Steep - Multiple concepts', 'Gentle - Single concept', 'Dynamic Tables win - easier to learn'
    UNION ALL
    SELECT 'Debugging', 'Complex - Multiple components', 'Simple - Single table to check', 'Dynamic Tables win - easier debugging'
)
SELECT * FROM feature_comparison;

-- =============================================================================
-- PERFORMANCE COMPARISON
-- =============================================================================

SELECT '🏃 PERFORMANCE COMPARISON TEST' as perf_test;

-- Test 1: Count transactions accessible to a specific user
-- Streams & Tasks approach (using base table + manual logic)
SELECT 'Test 1 - User Access Count (Streams Approach):' as test_1a;
SELECT COUNT(*) as streams_result
FROM transactions t
WHERE EXISTS (
    SELECT 1 FROM user_region_mapping urm 
    WHERE urm.region_name = t.region_name 
    AND urm.user_id = 'USER_0100' 
    AND urm.status = 'ACTIVE'
);

-- Dynamic Tables approach (using pre-computed array)  
SELECT 'Test 1 - User Access Count (Dynamic Tables Approach):' as test_1b;
SELECT COUNT(*) as dynamic_result
FROM transactions_with_entitlements
WHERE ARRAY_CONTAINS('USER_0100'::VARIANT, region_user_ids);

-- Test 2: Regional summary with user counts
-- Streams & Tasks approach
SELECT 'Test 2 - Regional Summary (Streams Approach):' as test_2a;
SELECT 
    t.region_name,
    COUNT(t.transaction_id) as transaction_count,
    COUNT(DISTINCT urm.user_id) as active_users
FROM transactions t
LEFT JOIN user_region_mapping urm ON t.region_name = urm.region_name AND urm.status = 'ACTIVE'
GROUP BY t.region_name
ORDER BY t.region_name
LIMIT 5;

-- Dynamic Tables approach
SELECT 'Test 2 - Regional Summary (Dynamic Tables Approach):' as test_2b;
SELECT 
    region_name,
    COUNT(*) as transaction_count,
    MAX(ARRAY_SIZE(region_user_ids)) as active_users
FROM transactions_with_entitlements
GROUP BY region_name  
ORDER BY region_name
LIMIT 5;

-- =============================================================================
-- ARCHITECTURE COMPARISON
-- =============================================================================

SELECT '🏗️ ARCHITECTURE COMPARISON' as arch_comparison;

-- Streams & Tasks Architecture
SELECT 'STREAMS & TASKS ARCHITECTURE:' as streams_arch;
SELECT '├── TRANSACTIONS (Base Table)' as streams_comp_1;
SELECT '├── USER_REGION_MAPPING (Base Table)' as streams_comp_2;
SELECT '├── transactions_stream (Change Detection)' as streams_comp_3;
SELECT '├── user_region_mapping_stream (Change Detection)' as streams_comp_4;
SELECT '├── update_transaction_user_arrays() (Stored Procedure)' as streams_comp_5;
SELECT '├── process_incremental_updates() (Stored Procedure)' as streams_comp_6;
SELECT '├── incremental_update_task (Scheduled Task)' as streams_comp_7;
SELECT '├── incremental_update_log (Monitoring Table)' as streams_comp_8;
SELECT '└── transactions_search_service (Cortex Search)' as streams_comp_9;

-- Dynamic Tables Architecture  
SELECT 'DYNAMIC TABLES ARCHITECTURE:' as dynamic_arch;
SELECT '├── TRANSACTIONS (Source Table)' as dynamic_comp_1;
SELECT '├── USER_REGION_MAPPING (Source Table)' as dynamic_comp_2;
SELECT '├── transactions_with_entitlements (Dynamic Table - Auto-refresh)' as dynamic_comp_3;
SELECT '└── transactions_dynamic_search_service (Cortex Search)' as dynamic_comp_4;

-- =============================================================================
-- COST COMPARISON
-- =============================================================================

SELECT '💰 COST ANALYSIS' as cost_analysis;

-- Streams & Tasks Costs
SELECT 'STREAMS & TASKS COSTS:' as streams_costs;
SELECT '• Compute: Task execution every minute (even when no changes)' as streams_cost_1;
SELECT '• Storage: Stream storage + Log table storage' as streams_cost_2;  
SELECT '• Maintenance: Developer time for monitoring and debugging' as streams_cost_3;
SELECT '• Complexity: Higher operational overhead' as streams_cost_4;

-- Dynamic Tables Costs
SELECT 'DYNAMIC TABLES COSTS:' as dynamic_costs;
SELECT '• Compute: Only when source data changes (optimized)' as dynamic_cost_1;
SELECT '• Storage: Dynamic Table storage (similar to view materialization)' as dynamic_cost_2;
SELECT '• Maintenance: Minimal - Snowflake managed' as dynamic_cost_3;
SELECT '• Complexity: Lower operational overhead' as dynamic_cost_4;

-- =============================================================================
-- USE CASE RECOMMENDATIONS
-- =============================================================================

SELECT '🎯 WHEN TO USE EACH APPROACH' as recommendations;

-- When to use Streams & Tasks
SELECT 'USE STREAMS & TASKS WHEN:' as streams_when;
SELECT '✓ You need complex custom logic in the transformation' as streams_use_1;
SELECT '✓ You need fine-grained control over refresh timing' as streams_use_2;
SELECT '✓ You need custom error handling and retry logic' as streams_use_3;
SELECT '✓ You have complex multi-step data processing workflows' as streams_use_4;
SELECT '✓ You need to integrate with external systems during processing' as streams_use_5;

-- When to use Dynamic Tables
SELECT 'USE DYNAMIC TABLES WHEN:' as dynamic_when;
SELECT '✅ You have declarative transformations (SQL-based)' as dynamic_use_1;
SELECT '✅ You want minimal operational overhead' as dynamic_use_2;
SELECT '✅ You need automatic optimization and performance tuning' as dynamic_use_3;
SELECT '✅ You want built-in monitoring and observability' as dynamic_use_4;
SELECT '✅ You have straightforward incremental refresh requirements' as dynamic_use_5;
SELECT '✅ You want Snowflake to handle all complexity automatically' as dynamic_use_6;

-- =============================================================================
-- MIGRATION STRATEGY
-- =============================================================================

SELECT '🔄 MIGRATION STRATEGY: STREAMS → DYNAMIC TABLES' as migration;

-- Step-by-step migration approach
SELECT 'MIGRATION STEPS:' as migration_steps;
SELECT '1. Create Dynamic Table alongside existing Streams setup' as step_1;
SELECT '2. Test Dynamic Table functionality in parallel' as step_2;
SELECT '3. Update Cortex Search to use Dynamic Table' as step_3;
SELECT '4. Monitor both approaches for a period' as step_4;
SELECT '5. Once confident, disable Streams/Tasks' as step_5;
SELECT '6. Clean up old Streams, Tasks, and Procedures' as step_6;

-- Side-by-side comparison script
SELECT 'SIDE-BY-SIDE VERIFICATION:' as verification;

-- Compare both approaches for same user
WITH streams_access AS (
    SELECT COUNT(*) as count_streams
    FROM transactions t
    JOIN user_region_mapping urm ON t.region_name = urm.region_name
    WHERE urm.user_id = 'USER_0100' AND urm.status = 'ACTIVE'
),
dynamic_access AS (
    SELECT COUNT(*) as count_dynamic
    FROM transactions_with_entitlements
    WHERE ARRAY_CONTAINS('USER_0100'::VARIANT, region_user_ids)
)
SELECT 
    s.count_streams,
    d.count_dynamic,
    CASE 
        WHEN s.count_streams = d.count_dynamic THEN '✅ Results Match'
        ELSE '❌ Results Differ - Investigation Needed'
    END as validation_status
FROM streams_access s, dynamic_access d;

-- =============================================================================
-- FINAL RECOMMENDATION
-- =============================================================================

SELECT '🏆 FINAL RECOMMENDATION FOR CORTEX SEARCH ENTITLEMENTS' as final_rec;

SELECT 'FOR THIS USE CASE (Cortex Search + Entitlements):' as use_case;
SELECT '🥇 WINNER: DYNAMIC TABLES' as winner;
SELECT '' as separator;
SELECT 'REASONS:' as reasons_header;
SELECT '✅ Perfect fit for declarative entitlement logic' as reason_1;
SELECT '✅ Automatic optimization by Snowflake' as reason_2;
SELECT '✅ Seamless Cortex Search integration' as reason_3;
SELECT '✅ Minimal operational overhead' as reason_4;  
SELECT '✅ Built-in monitoring and observability' as reason_5;
SELECT '✅ Cost-effective incremental refresh' as reason_6;
SELECT '✅ Future-proof architecture' as reason_7;

SELECT 'Dynamic Tables provide the ideal solution for maintaining' as conclusion_1;
SELECT 'entitlement arrays and keeping Cortex Search synchronized!' as conclusion_2;

COMMIT;

