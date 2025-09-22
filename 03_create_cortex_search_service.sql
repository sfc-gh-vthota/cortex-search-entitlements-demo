-- Create Cortex Search Service for incremental updates
-- This script creates the Cortex Search service and sets up the initial index

USE DATABASE CORTEX_SEARCH_ENTITLEMENT_DB;
USE SCHEMA TRANSACTIONS;

-- =============================================================================
-- CREATE CORTEX SEARCH SERVICE
-- =============================================================================

-- Drop service if it exists (for testing purposes)
-- DROP CORTEX SEARCH SERVICE IF EXISTS transactions_search_service;

-- Create the Cortex Search service on the transactions table
CREATE or replace CORTEX SEARCH SERVICE transactions_search_service
ON description
ATTRIBUTES region_name, transaction_type, category, merchant_name, status, region_user_ids
WAREHOUSE = COMPUTE_WH
TARGET_LAG = '1 minute'
COMMENT = 'Cortex Search service for transactions with region-based entitlements'
AS (
    SELECT * FROM TRANSACTIONS
);

-- =============================================================================
-- VERIFY SERVICE CREATION AND STATUS
-- =============================================================================

-- Check service status
SELECT 'Checking service status...' as info;
DESCRIBE CORTEX SEARCH SERVICE transactions_search_service;

-- Show service details
SHOW CORTEX SEARCH SERVICES;

-- Wait for initial indexing to complete
-- Note: The service needs time to build the initial index
-- Check status with: DESCRIBE CORTEX SEARCH SERVICE transactions_search_service;

-- =============================================================================
-- TEST BASIC FUNCTIONALITY
-- =============================================================================

-- Test basic data scan to ensure service is working
SELECT 'Testing basic data scan functionality...' as test_info;

SELECT 
    *
   -- ARRAY_SIZE(COALESCE(region_user_ids, ARRAY_CONSTRUCT())) as users_in_region_count
FROM TABLE(
    CORTEX_SEARCH_DATA_SCAN(
        SERVICE_NAME => 'transactions_search_service'
    )
)
LIMIT 5;

-- =============================================================================
-- SERVICE MANAGEMENT QUERIES
-- =============================================================================

-- Query to check service health
CREATE OR REPLACE VIEW SERVICE_HEALTH_CHECK AS
SELECT 
    'transactions_search_service' as service_name,
    CURRENT_TIMESTAMP() as check_time,
    COUNT(*) as indexed_records
FROM TABLE(
    CORTEX_SEARCH_DATA_SCAN(
        SERVICE_NAME => 'transactions_search_service'
    )
);

-- View service health
SELECT * FROM SERVICE_HEALTH_CHECK;

COMMIT;

-- =============================================================================
-- NOTES FOR INCREMENTAL PIPELINE
-- =============================================================================
/*
This service will be automatically updated by:
1. Streams that capture changes in TRANSACTIONS and USER_REGION_MAPPING tables
2. Stored procedures that handle the incremental updates
3. Tasks that run the update procedures automatically

The TARGET_LAG of '1 minute' means the service will refresh within 1 minute
of detecting changes in the underlying table.

Next steps:
- Run 04_create_incremental_pipeline.sql to set up streams, procedures, and automation
- Run 04_cortex_search_examples.sql to test search functionality
*/