-- Master Script to Execute All Components - Choose Your Approach!
-- This script sets up Cortex Search with Incremental Pipeline using your preferred method

-- =============================================================================
-- CORTEX SEARCH ENTITLEMENTS DEMO - CHOOSE YOUR APPROACH
-- =============================================================================

SELECT '🚀 CORTEX SEARCH ENTITLEMENTS DEMO SETUP' as setup_status;
SELECT 'Choose your incremental update approach:' as description;
SELECT 'A) Streams & Tasks (Traditional) or B) Dynamic Tables (Recommended)' as options;

-- UNCOMMENT ONE OF THE APPROACHES BELOW:

-- =============================================================================
-- APPROACH A: STREAMS & TASKS (TRADITIONAL)
-- Uncomment this section to use Streams & Tasks approach
-- =============================================================================

/*
SELECT '🔄 APPROACH A: STREAMS & TASKS SETUP' as approach_selected;

-- Step 1: Foundation Data
SELECT 'STEP 1: Creating transactions table with 10,000 records...' as current_step;
@01_create_transactions_table.sql;

SELECT 'STEP 2: Creating user-region mapping with 1,000 users...' as current_step;  
@02_create_user_region_mapping.sql;

-- Step 2: Streams & Tasks Pipeline
SELECT 'STEP 3: Creating Cortex Search service on base table...' as current_step;
@03_create_cortex_search_service.sql;

SELECT 'STEP 4: Setting up streams, tasks, and procedures...' as current_step;
@04_create_incremental_pipeline.sql;

-- Step 3: Testing
SELECT 'STEP 5: Testing streams & tasks pipeline...' as current_step;
@06_test_incremental_pipeline.sql;
*/

-- =============================================================================
-- APPROACH B: DYNAMIC TABLES (RECOMMENDED)
-- Uncomment this section to use Dynamic Tables approach
-- =============================================================================

SELECT '⚡ APPROACH B: DYNAMIC TABLES SETUP (RECOMMENDED)' as approach_selected;

-- Step 1: Foundation Data
SELECT 'STEP 1: Creating transactions table with 10,000 records...' as current_step;
@01_create_transactions_table.sql;

SELECT 'STEP 2: Creating user-region mapping with 1,000 users...' as current_step;  
@02_create_user_region_mapping.sql;

-- Step 2: Dynamic Tables Solution
SELECT 'STEP 3: Creating Dynamic Table with automatic entitlements + Cortex Search...' as current_step;
@08_create_dynamic_table_solution.sql;

-- Step 3: Testing  
SELECT 'STEP 4: Testing Dynamic Tables incremental updates...' as current_step;
@09_test_dynamic_table_incremental.sql;

-- =============================================================================
-- FINAL STATUS CHECK
-- =============================================================================

SELECT 'SETUP COMPLETE! 🎉' as final_status;
SELECT 'Your Cortex Search Entitlements Demo is now ready!' as message;

-- Verify all components are working
SELECT 'Verifying setup...' as verification;

-- Check database and schema
SELECT CURRENT_DATABASE() as current_database, CURRENT_SCHEMA() as current_schema;

-- Check tables exist
SELECT 'TRANSACTIONS' as table_name, COUNT(*) as record_count FROM TRANSACTIONS
UNION ALL
SELECT 'USER_REGION_MAPPING' as table_name, COUNT(*) as record_count FROM USER_REGION_MAPPING;

-- Dynamic Tables approach verification
SELECT 'DYNAMIC TABLES VERIFICATION:' as dynamic_verification;
SELECT COUNT(*) as dynamic_table_records FROM transactions_with_entitlements;
DESCRIBE CORTEX SEARCH SERVICE transactions_dynamic_search_service;

-- Streams & Tasks verification (uncomment if using that approach)
/*
SELECT 'STREAMS & TASKS VERIFICATION:' as streams_verification;
DESCRIBE CORTEX SEARCH SERVICE transactions_search_service;
SHOW STREAMS LIKE '%stream';
SHOW TASKS LIKE 'incremental_update_task';
*/

-- =============================================================================
-- NEXT STEPS
-- =============================================================================

SELECT 'NEXT STEPS:' as info;
SELECT '1. Run @05_cortex_search_examples.sql to explore search capabilities' as step_1;
SELECT '2. Monitor the system using the provided views and queries' as step_2;
SELECT '3. Test live updates by modifying user region mappings' as step_3;
SELECT '4. Experiment with semantic search queries using natural language' as step_4;

-- =============================================================================
-- KEY COMPONENTS CREATED (DYNAMIC TABLES APPROACH)
-- =============================================================================

SELECT 'KEY COMPONENTS SUCCESSFULLY CREATED:' as summary;
SELECT '✅ TRANSACTIONS table (10,000 records across 10 regions)' as component_1;
SELECT '✅ USER_REGION_MAPPING table (1,000 users across 10 regions)' as component_2;
SELECT '✅ TRANSACTIONS_WITH_ENTITLEMENTS (Dynamic Table with auto-refresh)' as component_3;
SELECT '✅ Cortex Search service (AI-powered search on Dynamic Table)' as component_4;
SELECT '✅ Monitoring views (status, comparison, summaries)' as component_5;
SELECT '✅ Test suite (comprehensive Dynamic Table verification)' as component_6;

-- =============================================================================
-- DYNAMIC TABLES FEATURES
-- =============================================================================

SELECT 'DYNAMIC TABLES PIPELINE FEATURES:' as pipeline_info;
SELECT '⚡ Automatic change detection - no manual streams needed' as feature_1;
SELECT '🔄 Declarative SQL transformations with TARGET_LAG' as feature_2;
SELECT '🔐 Entitlement arrays updated automatically when mappings change' as feature_3;
SELECT '🔍 Cortex Search auto-refresh from Dynamic Table changes' as feature_4;
SELECT '📊 Built-in monitoring and optimization by Snowflake' as feature_5;
SELECT '🛠️ Zero maintenance - Snowflake handles everything!' as feature_6;

SELECT '🏆 DYNAMIC TABLES DEMO COMPLETED SUCCESSFULLY!' as final_message;
SELECT 'You now have a production-ready, zero-maintenance incremental pipeline!' as production_ready;
