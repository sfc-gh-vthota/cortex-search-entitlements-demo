-- Create Incremental Pipeline for Cortex Search Updates
-- This script sets up streams, procedures, and tasks for automatic updates

USE DATABASE CORTEX_SEARCH_ENTITLEMENT_DB;
USE SCHEMA TRANSACTIONS;

-- =============================================================================
-- CREATE STREAMS TO CAPTURE DATA CHANGES
-- =============================================================================

-- Stream to capture changes in TRANSACTIONS table
CREATE OR REPLACE STREAM transactions_stream ON TABLE TRANSACTIONS
COMMENT = 'Stream to capture changes in transactions table for Cortex Search updates';

-- Stream to capture changes in USER_REGION_MAPPING table
CREATE OR REPLACE STREAM user_region_mapping_stream ON TABLE USER_REGION_MAPPING
COMMENT = 'Stream to capture changes in user region mapping table';

-- =============================================================================
-- CREATE STORED PROCEDURES FOR INCREMENTAL UPDATES
-- =============================================================================

-- Procedure to update REGION_USER_IDS in transactions table when user mapping changes
CREATE OR REPLACE PROCEDURE update_transaction_user_arrays()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    affected_regions VARCHAR;
    update_count INTEGER DEFAULT 0;
    result_message VARCHAR;
BEGIN
    -- Get list of affected regions from the stream
    SELECT LISTAGG(DISTINCT region_name, ', ') INTO affected_regions
    FROM user_region_mapping_stream
    WHERE region_name IS NOT NULL;
    
    -- If no changes, exit early
    IF (affected_regions IS NULL OR affected_regions = '') THEN
        RETURN 'No user region mapping changes detected';
    END IF;
    
    -- Update REGION_USER_IDS array for affected regions
    UPDATE TRANSACTIONS 
    SET REGION_USER_IDS = (
        SELECT ARRAY_AGG(USER_ID) 
        FROM USER_REGION_MAPPING 
        WHERE USER_REGION_MAPPING.REGION_NAME = TRANSACTIONS.REGION_NAME
        AND STATUS = 'ACTIVE'
    )
    WHERE REGION_NAME IN (
        SELECT DISTINCT REGION_NAME 
        FROM user_region_mapping_stream 
        WHERE REGION_NAME IS NOT NULL
    );
    
    -- Get count of updated rows
    SELECT ROW_COUNT() INTO update_count;
    
    result_message := 'Updated ' || update_count || ' transaction records for regions: ' || affected_regions;
    RETURN result_message;
END;
$$;

-- Procedure to handle incremental Cortex Search updates
-- This procedure only runs when task detects stream data, so we know there are changes
CREATE OR REPLACE PROCEDURE process_incremental_updates()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    txn_changes INTEGER;
    user_changes INTEGER;
    result_msg VARCHAR(5000);
    update_result VARCHAR(5000);
BEGIN
    -- Initialize variables
    txn_changes := 0;
    user_changes := 0;
    result_msg := '';
    update_result := '';
    
    -- Get count of changes (for logging purposes only)
    SELECT COUNT(*) INTO :user_changes FROM user_region_mapping_stream;
    SELECT COUNT(*) INTO :txn_changes FROM transactions_stream;
    
    -- Process user mapping changes (this updates transaction table)
    -- Only call if there are user mapping changes
    IF (:user_changes > 0) THEN
        CALL update_transaction_user_arrays() INTO :update_result;
        result_msg := 'User mapping changes: ' || :user_changes || '. ' || :update_result || '. ';
    END IF;
    
    -- Add transaction changes info
    IF (:txn_changes > 0) THEN
        result_msg := :result_msg || 'Direct transaction changes: ' || :txn_changes || '. ';
    END IF;
    
    -- Log the changes for monitoring
    INSERT INTO incremental_update_log (
        update_timestamp,
        transactions_changes,
        user_mapping_changes,
        update_result,
        status
    ) VALUES (
        CURRENT_TIMESTAMP(),
        :txn_changes,
        :user_changes,
        :result_msg,
        'SUCCESS'
    );
    
    -- The Cortex Search service will automatically pick up changes due to TARGET_LAG setting
    result_msg := :result_msg || 'Cortex Search will refresh automatically within target lag period.';
    
    RETURN result_msg;
EXCEPTION
    WHEN OTHER THEN
        -- Log the error with proper error handling
        BEGIN
            INSERT INTO incremental_update_log (
                update_timestamp,
                transactions_changes,
                user_mapping_changes,
                update_result,
                status,
                error_message
            ) VALUES (
                CURRENT_TIMESTAMP(),
                COALESCE(:txn_changes, 0),
                COALESCE(:user_changes, 0),
                COALESCE(:result_msg, ''),
                'ERROR',
                SQLERRM
            );
        EXCEPTION
            WHEN OTHER THEN
                -- If logging fails, just return the error
                NULL;
        END;
        
        RETURN 'Error processing updates: ' || SQLERRM;
END;
$$;

-- =============================================================================
-- CREATE LOGGING TABLE
-- =============================================================================

-- Table to log incremental update activities
CREATE OR REPLACE TABLE incremental_update_log (
    log_id INTEGER AUTOINCREMENT PRIMARY KEY,
    update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    transactions_changes INTEGER,
    user_mapping_changes INTEGER,
    update_result VARCHAR(5000),
    status VARCHAR(50),
    error_message VARCHAR(5000),
    processing_time_ms INTEGER
);

-- =============================================================================
-- CREATE AUTOMATED TASK FOR PROCESSING CHANGES
-- =============================================================================

-- Task to automatically process incremental updates ONLY when streams have data
CREATE OR REPLACE TASK incremental_update_task
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
COMMENT = 'Task to process incremental updates for Cortex Search - runs only when streams have data'
WHEN SYSTEM$STREAM_HAS_DATA('user_region_mapping_stream') OR SYSTEM$STREAM_HAS_DATA('transactions_stream')
AS
CALL process_incremental_updates();

-- Enable the task
ALTER TASK incremental_update_task RESUME;

-- =============================================================================
-- CREATE MONITORING VIEWS
-- =============================================================================

-- View to monitor stream activity
CREATE OR REPLACE VIEW stream_monitoring AS
SELECT 
    'TRANSACTIONS_STREAM' as stream_name,
    COUNT(*) as pending_changes,
    CURRENT_TIMESTAMP() as check_time
FROM transactions_stream
UNION ALL
SELECT 
    'USER_REGION_MAPPING_STREAM' as stream_name,
    COUNT(*) as pending_changes,
    CURRENT_TIMESTAMP() as check_time
FROM user_region_mapping_stream;

-- View to monitor update log
CREATE OR REPLACE VIEW update_activity_summary AS
SELECT 
    DATE_TRUNC('HOUR', update_timestamp) as hour_bucket,
    COUNT(*) as total_updates,
    SUM(transactions_changes) as total_transaction_changes,
    SUM(user_mapping_changes) as total_user_mapping_changes,
    COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) as successful_updates,
    COUNT(CASE WHEN status = 'ERROR' THEN 1 END) as failed_updates
FROM incremental_update_log
GROUP BY DATE_TRUNC('HOUR', update_timestamp)
ORDER BY hour_bucket DESC;

-- =============================================================================
-- VERIFICATION AND TESTING QUERIES
-- =============================================================================

-- Check current stream status
SELECT 'Current stream status:' as info;
SELECT * FROM stream_monitoring;

-- Check task status
SELECT 'Task status:' as info;
SHOW TASKS LIKE 'incremental_update_task';

-- Test the update procedure manually (optional)
-- CALL process_incremental_updates();

-- View recent log entries
SELECT 'Recent update log entries:' as info;
SELECT * FROM incremental_update_log ORDER BY update_timestamp DESC LIMIT 5;

-- =============================================================================
-- UTILITY PROCEDURES FOR MANUAL OPERATIONS
-- =============================================================================

-- Procedure to force a full refresh of REGION_USER_IDS
CREATE OR REPLACE PROCEDURE refresh_all_region_user_arrays()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    update_count INTEGER DEFAULT 0;
BEGIN
    UPDATE TRANSACTIONS 
    SET REGION_USER_IDS = (
        SELECT ARRAY_AGG(USER_ID) 
        FROM USER_REGION_MAPPING 
        WHERE USER_REGION_MAPPING.REGION_NAME = TRANSACTIONS.REGION_NAME
        AND STATUS = 'ACTIVE'
    );
    
    SELECT ROW_COUNT() INTO update_count;
    
    RETURN 'Refreshed REGION_USER_IDS for ' || update_count || ' transaction records';
END;
$$;

-- Procedure to manually trigger Cortex Search refresh
CREATE OR REPLACE PROCEDURE manual_cortex_refresh()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- This will be automatically handled by Snowflake due to TARGET_LAG setting
    -- But you can check service status
    RETURN 'Cortex Search service refreshes automatically based on TARGET_LAG setting. Check service status with: DESCRIBE CORTEX SEARCH SERVICE transactions_search_service';
END;
$$;

COMMIT;

-- =============================================================================
-- SETUP COMPLETE - PIPELINE IS NOW ACTIVE
-- =============================================================================

SELECT 'Incremental pipeline setup complete!' as status;
SELECT 'The following components are now active:' as info;
SELECT '1. Streams capturing changes in both tables' as component_1;
SELECT '2. Stored procedures for processing updates' as component_2;  
SELECT '3. Automated task running every minute' as component_3;
SELECT '4. Logging and monitoring views' as component_4;
SELECT '5. Cortex Search service with 1-minute refresh lag' as component_5;
