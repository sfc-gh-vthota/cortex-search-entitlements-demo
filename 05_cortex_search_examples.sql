-- Cortex Search Examples with Region-Based Entitlements
-- This script demonstrates various ways to use the Cortex Search service
-- with region-based access control for different users

-- PREREQUISITES:
-- 1. Run scripts 01, 02, and 03 first
-- 2. Ensure transactions_search_service is created and populated

-- Use the same database and schema
USE DATABASE CORTEX_SEARCH_ENTITLEMENT_DB;
USE SCHEMA TRANSACTIONS;

-- =============================================================================
-- EXAMPLE 1: Basic Data Scan with Entitlement Filtering
-- Show all transactions that a specific user can access based on their region
-- =============================================================================

SELECT 'EXAMPLE 1: All transactions accessible to USER_0001' as example_title;

SELECT 
    transaction_id,
    description,
    region_name,
    amount,
    transaction_date,
    category,
    region_user_ids,
    ARRAY_SIZE(region_user_ids) as total_users_in_region
FROM TABLE(
    CORTEX_SEARCH_DATA_SCAN(
        SERVICE_NAME => 'transactions_search_service'
    )
)
WHERE ARRAY_CONTAINS('USER_0001'::VARIANT, region_user_ids)
ORDER BY transaction_date DESC
LIMIT 20;

-- =============================================================================
-- EXAMPLE 2: Semantic Search with Entitlement Filtering
-- Perform semantic search for specific types of transactions
-- =============================================================================

SELECT 'EXAMPLE 2: Semantic search for restaurant/food transactions for USER_0050' as example_title;

-- Uncomment and modify this query to perform actual semantic search:
/*
SELECT 
    transaction_id,
    description,
    region_name,
    amount,
    category,
    merchant_name,
    SCORE
FROM TABLE(
    CORTEX_SEARCH(
        SERVICE_NAME => 'transactions_search_service',
        QUERY => 'restaurant dining food purchases meals coffee',
        LIMIT => 30
    )
)
WHERE ARRAY_CONTAINS('USER_0050'::VARIANT, region_user_ids)
ORDER BY SCORE DESC;
*/

-- Alternative: Filter by category for now (until you run semantic search)
SELECT 
    transaction_id,
    description,
    region_name,
    amount,
    category,
    merchant_name
FROM TABLE(
    CORTEX_SEARCH_DATA_SCAN(
        SERVICE_NAME => 'transactions_search_service'
    )
)
WHERE ARRAY_CONTAINS('USER_0050'::VARIANT, region_user_ids)
AND category = 'Restaurant'
ORDER BY amount DESC
LIMIT 15;

-- =============================================================================
-- EXAMPLE 3: Multi-User Comparison
-- Show how different users see different transaction sets based on their regions
-- =============================================================================

SELECT 'EXAMPLE 3: Transaction access comparison across different users' as example_title;

-- Compare what different users can see for gas station transactions
WITH user_transaction_access AS (
  SELECT 
    'USER_0001' as user_id,
    COUNT(*) as accessible_transactions,
    SUM(amount) as total_accessible_amount
  FROM TABLE(
      CORTEX_SEARCH_DATA_SCAN(
          SERVICE_NAME => 'transactions_search_service'
      )
  )
  WHERE category = 'Gas' 
  AND ARRAY_CONTAINS('USER_0001'::VARIANT, region_user_ids)

  UNION ALL

  SELECT 
    'USER_0150' as user_id,
    COUNT(*) as accessible_transactions,
    SUM(amount) as total_accessible_amount
  FROM TABLE(
      CORTEX_SEARCH_DATA_SCAN(
          SERVICE_NAME => 'transactions_search_service'
      )
  )
  WHERE category = 'Gas' 
  AND ARRAY_CONTAINS('USER_0150'::VARIANT, region_user_ids)

  UNION ALL

  SELECT 
    'USER_0300' as user_id,
    COUNT(*) as accessible_transactions,
    SUM(amount) as total_accessible_amount
  FROM TABLE(
      CORTEX_SEARCH_DATA_SCAN(
          SERVICE_NAME => 'transactions_search_service'
      )
  )
  WHERE category = 'Gas' 
  AND ARRAY_CONTAINS('USER_0300'::VARIANT, region_user_ids)
)
SELECT 
  user_id,
  accessible_transactions,
  ROUND(total_accessible_amount, 2) as total_amount,
  'Gas station transactions they can view' as transaction_type
FROM user_transaction_access
ORDER BY accessible_transactions DESC;

-- =============================================================================
-- EXAMPLE 4: Region-Based Analytics
-- Show transaction statistics per region that users can access
-- =============================================================================

SELECT 'EXAMPLE 4: Transaction statistics by region (with user perspective)' as example_title;

-- Show what each region looks like from different user perspectives
SELECT 
    region_name,
    COUNT(*) as transaction_count,
    ROUND(AVG(amount), 2) as avg_amount,
    ROUND(SUM(amount), 2) as total_amount,
    COUNT(DISTINCT category) as categories_available,
    ARRAY_SIZE(MAX(region_user_ids)) as users_in_region
FROM TABLE(
    CORTEX_SEARCH_DATA_SCAN(
        SERVICE_NAME => 'transactions_search_service'
    )
)
GROUP BY region_name
ORDER BY transaction_count DESC;

-- =============================================================================
-- EXAMPLE 5: High-Value Transaction Search with Entitlements
-- Find high-value transactions that specific users can access
-- =============================================================================

SELECT 'EXAMPLE 5: High-value transactions (>$3000) accessible to USER_0200' as example_title;

SELECT 
    transaction_id,
    description,
    region_name,
    amount,
    transaction_type,
    merchant_name,
    transaction_date
FROM TABLE(
    CORTEX_SEARCH_DATA_SCAN(
        SERVICE_NAME => 'transactions_search_service'
    )
)
WHERE ARRAY_CONTAINS('USER_0200'::VARIANT, region_user_ids)
AND amount > 3000
ORDER BY amount DESC
LIMIT 10;

-- =============================================================================
-- EXAMPLE 6: Merchant Analysis with Region Filtering
-- Show merchant transaction patterns for specific user's accessible data
-- =============================================================================

SELECT 'EXAMPLE 6: Merchant analysis for USER_0001 accessible transactions' as example_title;

SELECT 
    merchant_name,
    COUNT(*) as transaction_count,
    ROUND(AVG(amount), 2) as avg_transaction_amount,
    ROUND(SUM(amount), 2) as total_spent,
    MIN(transaction_date) as first_transaction,
    MAX(transaction_date) as last_transaction
FROM TABLE(
    CORTEX_SEARCH_DATA_SCAN(
        SERVICE_NAME => 'transactions_search_service'
    )
)
WHERE ARRAY_CONTAINS('USER_0001'::VARIANT, region_user_ids)
GROUP BY merchant_name
HAVING COUNT(*) >= 5  -- Only merchants with 5+ transactions
ORDER BY total_spent DESC
LIMIT 10;

-- =============================================================================
-- UTILITY QUERIES
-- =============================================================================

-- Check service health and status
SELECT 'SERVICE HEALTH CHECK' as info;
DESCRIBE CORTEX SEARCH SERVICE transactions_search_service;

-- Count total records in service vs. accessible records for a user
SELECT 
    'Total records in service' as metric,
    COUNT(*) as count
FROM TABLE(
    CORTEX_SEARCH_DATA_SCAN(
        SERVICE_NAME => 'transactions_search_service'
    )
)

UNION ALL

SELECT 
    'Records accessible to USER_0100' as metric,
    COUNT(*) as count
FROM TABLE(
    CORTEX_SEARCH_DATA_SCAN(
        SERVICE_NAME => 'transactions_search_service'
    )
)
WHERE ARRAY_CONTAINS('USER_0100'::VARIANT, region_user_ids);

COMMIT;

