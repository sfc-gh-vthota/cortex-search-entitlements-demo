-- Deploy Cortex Search Streamlit Application
-- This script creates and deploys the Streamlit app in Snowflake

USE DATABASE CORTEX_SEARCH_ENTITLEMENT_DB;
USE SCHEMA DYNAMIC_DEMO;

-- =============================================================================
-- STEP 1: CREATE STAGE FOR STREAMLIT FILES (MUST BE FIRST)
-- =============================================================================

SELECT '🚀 DEPLOYING CORTEX SEARCH STREAMLIT APPLICATION' as deployment_title;

-- Create internal stage for Streamlit files FIRST (required before creating Streamlit app)
CREATE OR REPLACE STAGE streamlit_stage
COMMENT = 'Stage for Cortex Search Streamlit application files';

SELECT '✅ Stage created: streamlit_stage' as stage_created;

-- =============================================================================
-- STEP 2: CREATE STREAMLIT APPLICATION IN SNOWFLAKE
-- =============================================================================

-- Create the Streamlit application (now that stage exists)
CREATE OR REPLACE STREAMLIT cortex_search_dashboard
ROOT_LOCATION = '@CORTEX_SEARCH_ENTITLEMENT_DB.DYNAMIC_DEMO.streamlit_stage'
MAIN_FILE = 'cortex_search_app.py'
QUERY_WAREHOUSE = COMPUTE_WH
COMMENT = 'Interactive dashboard for Cortex Search with user entitlements and performance monitoring';

SELECT '✅ Streamlit app created: cortex_search_dashboard' as streamlit_created;

-- =============================================================================
-- STEP 3: UPLOAD STREAMLIT APPLICATION FILE
-- =============================================================================

-- Note: You'll need to upload the cortex_search_app.py file to the stage
-- This can be done via:
-- 1. Snowflake Web UI (Data > Databases > Stages > Upload Files)
-- 2. SnowSQL command: PUT file://cortex_search_app.py @streamlit_stage
-- 3. Python connector with put_file method

SELECT 'Upload cortex_search_app.py to the streamlit_stage' as upload_instruction;

-- =============================================================================
-- STEP 4: GRANT PERMISSIONS FOR STREAMLIT APP
-- =============================================================================

-- Grant usage on database and schema
GRANT USAGE ON DATABASE CORTEX_SEARCH_ENTITLEMENT_DB TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA DYNAMIC_DEMO TO ROLE SYSADMIN;

-- Grant select permissions on tables and views
GRANT SELECT ON TABLE financial_transactions TO ROLE SYSADMIN;
GRANT SELECT ON TABLE user_region_access TO ROLE SYSADMIN;
GRANT SELECT ON TABLE financial_transactions_enriched TO ROLE SYSADMIN;
GRANT SELECT ON VIEW regional_entitlements TO ROLE SYSADMIN;

-- Grant usage on Cortex Search service
GRANT USAGE ON CORTEX SEARCH SERVICE financial_search_service TO ROLE SYSADMIN;

-- Grant usage on warehouse
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE SYSADMIN;

-- Grant usage on stage
GRANT READ ON STAGE streamlit_stage TO ROLE SYSADMIN;

-- =============================================================================
-- STEP 5: CREATE ADDITIONAL UTILITY FUNCTIONS FOR STREAMLIT
-- =============================================================================

-- Function to get user statistics for the dashboard
CREATE OR REPLACE FUNCTION get_user_stats()
RETURNS TABLE (
    total_users INT,
    active_users INT, 
    inactive_users INT,
    regions_count INT,
    access_levels VARIANT
)
LANGUAGE SQL
AS
$$
    SELECT 
        COUNT(*) as total_users,
        COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END) as active_users,
        COUNT(CASE WHEN status = 'INACTIVE' THEN 1 END) as inactive_users,
        COUNT(DISTINCT region_name) as regions_count,
        ARRAY_AGG(DISTINCT access_level) as access_levels
    FROM user_region_access
$$;

-- Function to get transaction statistics
CREATE OR REPLACE FUNCTION get_transaction_stats()
RETURNS TABLE (
    total_transactions INT,
    total_value NUMBER(15,2),
    avg_transaction NUMBER(10,2),
    regions_with_transactions INT,
    categories_count INT
)
LANGUAGE SQL
AS
$$
    SELECT 
        COUNT(*) as total_transactions,
        SUM(amount) as total_value,
        AVG(amount) as avg_transaction,
        COUNT(DISTINCT region_name) as regions_with_transactions,
        COUNT(DISTINCT category) as categories_count
    FROM financial_transactions_enriched
$$;

-- Procedure to refresh all dependent objects for Streamlit
CREATE OR REPLACE PROCEDURE refresh_dashboard_data()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Refresh dynamic table
    CALL refresh_financial_dynamic_table();
    
    -- Return success message
    RETURN 'Dashboard data refreshed successfully';
END;
$$;

-- =============================================================================
-- STEP 6: CREATE SAMPLE QUERIES FOR TESTING
-- =============================================================================

-- Test query to verify Streamlit can access data
SELECT 'Testing Streamlit data access...' as test_status;

-- Test user access
SELECT COUNT(*) as active_users_count 
FROM user_region_access 
WHERE status = 'ACTIVE';

-- Test Cortex Search access
SELECT COUNT(*) as search_accessible_transactions
FROM TABLE(
    CORTEX_SEARCH_DATA_SCAN(
        SERVICE_NAME => 'financial_search_service'
    )
)
LIMIT 5;

-- Test dynamic table access
SELECT COUNT(*) as enriched_transactions_count
FROM financial_transactions_enriched;

-- =============================================================================
-- STEP 7: DEPLOYMENT VERIFICATION AND INSTRUCTIONS
-- =============================================================================

SELECT '✅ STREAMLIT APPLICATION SETUP COMPLETED' as setup_status;

SELECT 'DEPLOYMENT INSTRUCTIONS:' as instructions_header;
SELECT '1. Upload cortex_search_app.py to the streamlit_stage' as instruction_1;
SELECT '2. Run: SHOW STREAMLIT cortex_search_dashboard to verify creation' as instruction_2;
SELECT '3. Open the Streamlit app from Snowflake UI or use direct URL' as instruction_3;
SELECT '4. Test with different users and search queries' as instruction_4;

-- Show Streamlit application details
SHOW STREAMLIT cortex_search_dashboard;

-- =============================================================================
-- STEP 8: ALTERNATIVE DEPLOYMENT METHODS
-- =============================================================================

SELECT '📋 ALTERNATIVE DEPLOYMENT OPTIONS:' as alt_deployment_header;

-- Option 1: Using SnowSQL to upload file
SELECT 'Option 1 - SnowSQL Upload:' as option_1_header;
SELECT 'snowsql -c <connection> -q "PUT file://cortex_search_app.py @CORTEX_SEARCH_ENTITLEMENT_DB.DYNAMIC_DEMO.streamlit_stage"' as snowsql_command;

-- Option 2: Using Python Snowpark
SELECT 'Option 2 - Python Snowpark:' as option_2_header;
SELECT 'session.file.put("cortex_search_app.py", "@CORTEX_SEARCH_ENTITLEMENT_DB.DYNAMIC_DEMO.streamlit_stage")' as python_command;

-- Option 3: Web UI Upload
SELECT 'Option 3 - Web UI:' as option_3_header;
SELECT 'Navigate to Data > Databases > CORTEX_SEARCH_ENTITLEMENT_DB > DYNAMIC_DEMO > Stages > streamlit_stage > Upload Files' as webui_path;

-- =============================================================================
-- STEP 9: TROUBLESHOOTING AND MONITORING
-- =============================================================================

-- Create view for monitoring Streamlit usage (if needed)
CREATE OR REPLACE VIEW streamlit_monitoring AS
SELECT 
    'Streamlit App Monitoring' as monitor_type,
    CURRENT_TIMESTAMP() as check_time,
    (SELECT COUNT(*) FROM user_region_access WHERE status = 'ACTIVE') as active_users,
    (SELECT COUNT(*) FROM financial_transactions_enriched) as total_transactions,
    (SELECT MAX(transaction_date) FROM financial_transactions_enriched) as latest_transaction_date;

-- Test the monitoring view
SELECT * FROM streamlit_monitoring;

SELECT '🔧 TROUBLESHOOTING TIPS:' as troubleshooting_header;
SELECT 'If app fails to load, check: 1) File uploaded to stage, 2) Permissions granted, 3) Warehouse running' as tip_1;
SELECT 'For performance issues: 1) Check warehouse size, 2) Optimize queries, 3) Use result caching' as tip_2;
SELECT 'For data issues: 1) Verify dynamic table is current, 2) Check user entitlements, 3) Refresh search service' as tip_3;

-- =============================================================================
-- STEP 10: SUCCESS MESSAGE AND NEXT STEPS
-- =============================================================================

SELECT '🎉 STREAMLIT DEPLOYMENT SCRIPT COMPLETED!' as completion_message;
SELECT 'Next steps: 1) Upload Python file, 2) Access via Snowflake UI, 3) Test functionality' as next_steps;
SELECT 'The Streamlit app provides: User selection, Cortex Search integration, Performance metrics, Interactive visualizations' as app_features;

COMMIT;
