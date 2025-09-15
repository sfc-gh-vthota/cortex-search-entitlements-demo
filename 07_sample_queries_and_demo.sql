-- =============================================================================
-- CORTEX SEARCH ENTITLEMENTS DEMO - Sample Queries and Demonstrations
-- =============================================================================
-- This script contains comprehensive examples showing how to use Cortex Search
-- with fine-grained entitlements for user-specific filtering
-- Run these queries after all previous setup is complete and search indexes are refreshed

-- Set context
USE ROLE ACCOUNTADMIN;
USE DATABASE CORTEX_SEARCH_ENTITLEMENTS_DB;
USE SCHEMA ENTITLEMENTS;
USE WAREHOUSE ENTITLEMENTS_WH;

-- =============================================================================
-- DEMO SECTION 1: BASIC USER-FILTERED SEARCH EXAMPLES
-- =============================================================================

-- Example 1: Executive User Search (Global Access)
-- CEO Jane Smith searches for high-value transactions
SELECT 'DEMO 1: Executive Search - High Value Transactions' AS DEMO_TITLE;

SELECT 
    TRANSACTION_ID,
    RELEVANCE_SCORE,
    AMOUNT,
    MERCHANT_NAME,
    MERCHANT_CATEGORY,
    REGION_CODE,
    CUSTOMER_TIER,
    RISK_LEVEL,
    AUTHORIZED_USERS
FROM TABLE(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        TRANSACTION_SEARCH_SERVICE,
        'high value premium customer transaction over $1000',
        20
    )
)
WHERE ARRAY_CONTAINS('ceo_jane_smith'::VARIANT, AUTHORIZED_USERS)
ORDER BY RELEVANCE_SCORE DESC;

-- Example 2: Regional Manager Search (Regional Access)
-- US East Manager searches for transactions in their region
SELECT 'DEMO 2: Regional Manager Search - US East Region' AS DEMO_TITLE;

SELECT 
    TRANSACTION_ID,
    RELEVANCE_SCORE,
    AMOUNT,
    MERCHANT_NAME,
    REGION_CODE,
    CUSTOMER_TIER,
    TRANSACTION_STATUS,
    AUTHORIZED_USERS
FROM TABLE(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        TRANSACTION_SEARCH_SERVICE,
        'restaurant transaction declined payment',
        15
    )
)
WHERE ARRAY_CONTAINS('mgr_sarah_davis_us_east'::VARIANT, AUTHORIZED_USERS)
  AND REGION_CODE = 'US_EAST'
ORDER BY RELEVANCE_SCORE DESC;

-- Example 3: Department-Specific Search (Fraud Department)
-- Fraud analyst searches for suspicious transactions
SELECT 'DEMO 3: Fraud Department Search - High Risk Transactions' AS DEMO_TITLE;

SELECT 
    TRANSACTION_ID,
    RELEVANCE_SCORE,
    AMOUNT,
    RISK_LEVEL,
    RISK_SCORE,
    MERCHANT_CATEGORY,
    TRANSACTION_STATUS,
    SENSITIVITY_LEVEL,
    AUTHORIZED_USERS
FROM TABLE(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        FRAUD_RISK_TRANSACTION_SEARCH,
        'high risk critical transaction fraud suspicious',
        10
    )
)
WHERE ARRAY_CONTAINS('fraud_analyst_james_taylor'::VARIANT, AUTHORIZED_USERS)
  AND RISK_LEVEL IN ('HIGH', 'CRITICAL')
ORDER BY RELEVANCE_SCORE DESC;

-- Example 4: Customer Tier Specialist Search
-- Premium customer specialist searches for premium customer issues
SELECT 'DEMO 4: Premium Specialist Search - Premium Customer Transactions' AS DEMO_TITLE;

SELECT 
    TRANSACTION_ID,
    RELEVANCE_SCORE,
    AMOUNT,
    CUSTOMER_TIER,
    CARD_BRAND,
    MERCHANT_NAME,
    TRANSACTION_STATUS,
    AUTHORIZED_USERS
FROM TABLE(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        HIGH_VALUE_TRANSACTION_SEARCH,
        'premium customer declined transaction credit card',
        10
    )
)
WHERE ARRAY_CONTAINS('premium_specialist_john_clark'::VARIANT, AUTHORIZED_USERS)
  AND CUSTOMER_TIER = 'PREMIUM'
ORDER BY RELEVANCE_SCORE DESC;

-- Example 5: External Partner Search (Limited Access)
-- External partner searches for public data only
SELECT 'DEMO 5: External Partner Search - Public Data Only' AS DEMO_TITLE;

SELECT 
    TRANSACTION_ID,
    RELEVANCE_SCORE,
    AMOUNT,
    MERCHANT_CATEGORY,
    REGION_CODE,
    TRANSACTION_STATUS,
    SENSITIVITY_LEVEL,
    AUTHORIZED_USERS
FROM TABLE(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        TRANSACTION_SEARCH_SERVICE,
        'grocery store transaction approved',
        10
    )
)
WHERE ARRAY_CONTAINS('partner_vendor_alex_jones'::VARIANT, AUTHORIZED_USERS)
  AND SENSITIVITY_LEVEL = 'PUBLIC'
ORDER BY RELEVANCE_SCORE DESC;

-- =============================================================================
-- DEMO SECTION 2: ADVANCED SEARCH SCENARIOS
-- =============================================================================

-- Example 6: Multi-User Access Pattern Analysis
-- Show transactions accessible to multiple user types
SELECT 'DEMO 6: Multi-User Access Analysis' AS DEMO_TITLE;

SELECT 
    t.TRANSACTION_ID,
    t.AMOUNT,
    t.REGION_CODE,
    t.CUSTOMER_TIER,
    t.SENSITIVITY_LEVEL,
    t.AUTHORIZED_USER_COUNT,
    
    -- Check specific user types
    CASE WHEN ARRAY_SIZE(t.EXECUTIVE_USERS) > 0 THEN 'YES' ELSE 'NO' END AS EXECUTIVE_ACCESS,
    CASE WHEN ARRAY_SIZE(t.MANAGER_USERS) > 0 THEN 'YES' ELSE 'NO' END AS MANAGER_ACCESS,
    CASE WHEN ARRAY_SIZE(t.EXTERNAL_USERS) > 0 THEN 'YES' ELSE 'NO' END AS EXTERNAL_ACCESS,
    
    t.AUTHORIZED_USERS
FROM CORTEX_SEARCH_TRANSACTIONS t
WHERE t.AUTHORIZED_USER_COUNT >= 5
ORDER BY t.AUTHORIZED_USER_COUNT DESC
LIMIT 10;

-- Example 7: Search with Dynamic User Context
-- Function that adapts search based on user's access level
SELECT 'DEMO 7: Dynamic Search Based on User Access Level' AS DEMO_TITLE;

-- Create a dynamic search function
CREATE OR REPLACE FUNCTION GET_USER_SEARCH_RESULTS(
    USERNAME STRING,
    SEARCH_QUERY STRING
)
RETURNS TABLE (
    TRANSACTION_ID STRING,
    RELEVANCE_SCORE FLOAT,
    AMOUNT FLOAT,
    MERCHANT_NAME STRING,
    ACCESS_REASON STRING
)
LANGUAGE SQL
AS
$$
    WITH user_info AS (
        SELECT 
            USERNAME as user_name,
            ACCESS_LEVEL,
            IS_EXECUTIVE,
            IS_MANAGER,
            IS_EXTERNAL
        FROM USER_ACCESS_MAPPING 
        WHERE USERNAME = USERNAME
    )
    SELECT 
        sr.TRANSACTION_ID,
        sr.RELEVANCE_SCORE,
        sr.AMOUNT,
        sr.MERCHANT_NAME,
        
        CASE 
            WHEN ui.IS_EXECUTIVE THEN 'Executive Access'
            WHEN ui.IS_MANAGER THEN 'Manager Access' 
            WHEN ui.ACCESS_LEVEL = 'SPECIALIZED' THEN 'Specialist Access'
            WHEN ui.IS_EXTERNAL THEN 'Partner Access'
            ELSE 'Department Access'
        END AS ACCESS_REASON
        
    FROM TABLE(
        SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
            TRANSACTION_SEARCH_SERVICE,
            SEARCH_QUERY,
            50
        )
    ) sr
    CROSS JOIN user_info ui
    WHERE ARRAY_CONTAINS(USERNAME::VARIANT, sr.AUTHORIZED_USERS)
    ORDER BY sr.RELEVANCE_SCORE DESC
    LIMIT 20
$$;

-- Test the dynamic search function with different users
SELECT * FROM TABLE(GET_USER_SEARCH_RESULTS('ceo_jane_smith', 'credit card transaction'));
SELECT * FROM TABLE(GET_USER_SEARCH_RESULTS('mgr_sarah_davis_us_east', 'restaurant payment'));
SELECT * FROM TABLE(GET_USER_SEARCH_RESULTS('partner_vendor_alex_jones', 'grocery store'));

-- =============================================================================
-- DEMO SECTION 3: ENTITLEMENT VERIFICATION QUERIES  
-- =============================================================================

-- Example 8: Verify Entitlement Logic
-- Show how different users see different result sets for the same query
SELECT 'DEMO 8: Entitlement Verification - Same Query, Different Users' AS DEMO_TITLE;

WITH search_results AS (
    SELECT 
        TRANSACTION_ID,
        RELEVANCE_SCORE,
        AMOUNT,
        SENSITIVITY_LEVEL,
        RISK_LEVEL,
        REGION_CODE,
        AUTHORIZED_USERS
    FROM TABLE(
        SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
            TRANSACTION_SEARCH_SERVICE,
            'high value transaction',
            100
        )
    )
)
SELECT 
    'Executive (Global Access)' AS USER_TYPE,
    'ceo_jane_smith' AS USERNAME,
    COUNT(*) AS VISIBLE_RESULTS,
    STRING_AGG(DISTINCT SENSITIVITY_LEVEL, ', ') AS SENSITIVITY_LEVELS_SEEN,
    STRING_AGG(DISTINCT REGION_CODE, ', ') AS REGIONS_SEEN
FROM search_results 
WHERE ARRAY_CONTAINS('ceo_jane_smith'::VARIANT, AUTHORIZED_USERS)

UNION ALL

SELECT 
    'Regional Manager (US East)',
    'mgr_sarah_davis_us_east',
    COUNT(*),
    STRING_AGG(DISTINCT SENSITIVITY_LEVEL, ', '),
    STRING_AGG(DISTINCT REGION_CODE, ', ')
FROM search_results
WHERE ARRAY_CONTAINS('mgr_sarah_davis_us_east'::VARIANT, AUTHORIZED_USERS)

UNION ALL

SELECT 
    'External Partner (Limited)',
    'partner_vendor_alex_jones', 
    COUNT(*),
    STRING_AGG(DISTINCT SENSITIVITY_LEVEL, ', '),
    STRING_AGG(DISTINCT REGION_CODE, ', ')
FROM search_results
WHERE ARRAY_CONTAINS('partner_vendor_alex_jones'::VARIANT, AUTHORIZED_USERS);

-- Example 9: Access Pattern Audit
-- Show detailed access patterns for compliance reporting
SELECT 'DEMO 9: Access Pattern Audit Report' AS DEMO_TITLE;

SELECT 
    u.USERNAME,
    u.FULL_NAME,
    u.DEPARTMENT,
    u.ACCESS_LEVEL,
    
    -- Search access statistics
    COUNT(CASE WHEN ARRAY_CONTAINS(u.USERNAME::VARIANT, t.AUTHORIZED_USERS) THEN 1 END) AS ACCESSIBLE_TRANSACTIONS,
    
    -- Sensitivity breakdown
    COUNT(CASE WHEN ARRAY_CONTAINS(u.USERNAME::VARIANT, t.AUTHORIZED_USERS) AND t.SENSITIVITY_LEVEL = 'RESTRICTED' THEN 1 END) AS RESTRICTED_ACCESS,
    COUNT(CASE WHEN ARRAY_CONTAINS(u.USERNAME::VARIANT, t.AUTHORIZED_USERS) AND t.SENSITIVITY_LEVEL = 'CONFIDENTIAL' THEN 1 END) AS CONFIDENTIAL_ACCESS,
    COUNT(CASE WHEN ARRAY_CONTAINS(u.USERNAME::VARIANT, t.AUTHORIZED_USERS) AND t.SENSITIVITY_LEVEL = 'INTERNAL' THEN 1 END) AS INTERNAL_ACCESS,
    COUNT(CASE WHEN ARRAY_CONTAINS(u.USERNAME::VARIANT, t.AUTHORIZED_USERS) AND t.SENSITIVITY_LEVEL = 'PUBLIC' THEN 1 END) AS PUBLIC_ACCESS,
    
    -- Risk level access
    COUNT(CASE WHEN ARRAY_CONTAINS(u.USERNAME::VARIANT, t.AUTHORIZED_USERS) AND t.RISK_LEVEL = 'CRITICAL' THEN 1 END) AS CRITICAL_RISK_ACCESS,
    
    -- High value access
    COUNT(CASE WHEN ARRAY_CONTAINS(u.USERNAME::VARIANT, t.AUTHORIZED_USERS) AND t.AMOUNT > 10000 THEN 1 END) AS HIGH_VALUE_ACCESS
    
FROM USER_ACCESS_MAPPING u
CROSS JOIN CORTEX_SEARCH_TRANSACTIONS t
GROUP BY u.USERNAME, u.FULL_NAME, u.DEPARTMENT, u.ACCESS_LEVEL
ORDER BY ACCESSIBLE_TRANSACTIONS DESC;

-- =============================================================================
-- DEMO SECTION 4: PERFORMANCE AND OPTIMIZATION EXAMPLES
-- =============================================================================

-- Example 10: Optimized Search with Pre-filtering
-- Show how to optimize searches by pre-filtering on user access
SELECT 'DEMO 10: Optimized User-Specific Search' AS DEMO_TITLE;

-- Create an optimized search stored procedure
CREATE OR REPLACE PROCEDURE OPTIMIZED_USER_SEARCH(
    USERNAME STRING,
    SEARCH_QUERY STRING,
    RESULT_LIMIT INTEGER DEFAULT 20
)
RETURNS TABLE (
    TRANSACTION_ID STRING,
    RELEVANCE_SCORE FLOAT,
    AMOUNT FLOAT,
    MERCHANT_NAME STRING,
    REGION_CODE STRING,
    RISK_LEVEL STRING,
    AUTHORIZED_USER_COUNT INTEGER
)
LANGUAGE SQL
AS
$$
DECLARE
    user_access_query STRING;
    final_query STRING;
BEGIN
    -- Get user's access parameters
    SELECT 
        CONCAT(
            'SELECT TRANSACTION_ID, RELEVANCE_SCORE, AMOUNT, MERCHANT_NAME, REGION_CODE, RISK_LEVEL, AUTHORIZED_USER_COUNT ',
            'FROM TABLE(SNOWFLAKE.CORTEX.SEARCH_PREVIEW(TRANSACTION_SEARCH_SERVICE, ''', SEARCH_QUERY, ''', ', RESULT_LIMIT, ')) ',
            'WHERE ARRAY_CONTAINS(''', USERNAME, '''::VARIANT, AUTHORIZED_USERS) ',
            CASE 
                WHEN ACCESS_LEVEL = 'LIMITED' THEN 'AND SENSITIVITY_LEVEL = ''PUBLIC'' '
                WHEN ACCESS_LEVEL = 'REGIONAL' THEN 'AND REGION_CODE IN (''' || ARRAY_TO_STRING(ALLOWED_REGIONS, ''',''') || ''') '
                WHEN ACCESS_LEVEL = 'SPECIALIZED' AND DEPARTMENT = 'FRAUD' THEN 'AND RISK_LEVEL IN (''HIGH'', ''CRITICAL'') '
                ELSE ''
            END,
            'ORDER BY RELEVANCE_SCORE DESC'
        )
    INTO final_query
    FROM USER_ACCESS_MAPPING
    WHERE USERNAME = :USERNAME;
    
    -- Execute the dynamic query
    RETURN TABLE(RESULT_SCAN(final_query));
END;
$$;

-- Test the optimized search
CALL OPTIMIZED_USER_SEARCH('fraud_analyst_james_taylor', 'suspicious transaction', 10);

-- =============================================================================
-- DEMO SECTION 5: INTEGRATION EXAMPLES FOR APPLICATIONS
-- =============================================================================

-- Example 11: API-Ready Search Function
-- Create functions that can be easily called from applications
SELECT 'DEMO 11: Application Integration Examples' AS DEMO_TITLE;

CREATE OR REPLACE FUNCTION SEARCH_FOR_APPLICATION(
    USERNAME STRING,
    SEARCH_QUERY STRING, 
    FILTERS OBJECT DEFAULT NULL
)
RETURNS ARRAY
LANGUAGE SQL
AS
$$
    SELECT ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'transaction_id', TRANSACTION_ID,
            'relevance_score', RELEVANCE_SCORE,
            'amount', AMOUNT,
            'merchant_name', MERCHANT_NAME,
            'merchant_category', MERCHANT_CATEGORY,
            'transaction_date', TRANSACTION_DATE,
            'region', REGION_CODE,
            'customer_tier', CUSTOMER_TIER,
            'risk_level', RISK_LEVEL,
            'authorized_users', AUTHORIZED_USERS
        )
    )
    FROM TABLE(
        SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
            TRANSACTION_SEARCH_SERVICE,
            SEARCH_QUERY,
            50
        )
    )
    WHERE ARRAY_CONTAINS(USERNAME::VARIANT, AUTHORIZED_USERS)
    ORDER BY RELEVANCE_SCORE DESC
$$;

-- Test the application-ready function
SELECT SEARCH_FOR_APPLICATION('mgr_sarah_davis_us_east', 'restaurant transaction', NULL);

-- =============================================================================
-- DEMO SECTION 6: MONITORING AND ANALYTICS
-- =============================================================================

-- Example 12: Search Usage Analytics
-- Track search patterns for optimization and security
SELECT 'DEMO 12: Search Analytics and Monitoring' AS DEMO_TITLE;

-- Create a search usage tracking table (for production use)
CREATE OR REPLACE TABLE SEARCH_USAGE_LOG (
    LOG_ID STRING DEFAULT UUID_STRING(),
    USERNAME STRING,
    SEARCH_QUERY STRING,
    SEARCH_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    RESULTS_COUNT INTEGER,
    MAX_RELEVANCE_SCORE FLOAT,
    USER_ACCESS_LEVEL STRING,
    SEARCH_DURATION_MS INTEGER
);

-- Create a logging procedure for searches
CREATE OR REPLACE PROCEDURE LOG_SEARCH_USAGE(
    USERNAME STRING,
    SEARCH_QUERY STRING,
    RESULTS_COUNT INTEGER,
    MAX_RELEVANCE_SCORE FLOAT
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO SEARCH_USAGE_LOG (USERNAME, SEARCH_QUERY, RESULTS_COUNT, MAX_RELEVANCE_SCORE, USER_ACCESS_LEVEL)
    SELECT 
        USERNAME,
        SEARCH_QUERY,
        RESULTS_COUNT,
        MAX_RELEVANCE_SCORE,
        u.ACCESS_LEVEL
    FROM USER_ACCESS_MAPPING u
    WHERE u.USERNAME = :USERNAME;
    
    RETURN 'Search usage logged successfully';
END;
$$;

-- =============================================================================
-- FINAL DEMO SUMMARY AND VALIDATION
-- =============================================================================

SELECT '========================================' AS DEMO_SECTION;
SELECT 'CORTEX SEARCH ENTITLEMENTS DEMO SUMMARY' AS DEMO_SECTION;
SELECT '========================================' AS DEMO_SECTION;

-- Summary statistics
SELECT 
    'Total Transactions in System' AS METRIC,
    COUNT(*)::STRING AS VALUE
FROM CREDIT_CARD_TRANSACTIONS

UNION ALL

SELECT 
    'Total Users in System',
    COUNT(*)::STRING
FROM USER_ACCESS_MAPPING

UNION ALL

SELECT 
    'Average Users Per Transaction',
    ROUND(AVG(AUTHORIZED_USER_COUNT), 2)::STRING
FROM CORTEX_SEARCH_TRANSACTIONS

UNION ALL

SELECT 
    'Cortex Search Services Created',
    COUNT(*)::STRING
FROM INFORMATION_SCHEMA.CORTEX_SEARCH_SERVICES
WHERE SEARCH_SERVICE_SCHEMA = 'DEMO'

UNION ALL

SELECT 
    'Transactions with Restricted Access',
    (COUNT(CASE WHEN AUTHORIZED_USER_COUNT <= 5 THEN 1 END) * 100.0 / COUNT(*))::STRING || '%'
FROM CORTEX_SEARCH_TRANSACTIONS;

-- Show sample searches for different user personas
SELECT '=== SAMPLE SEARCH RESULTS BY USER TYPE ===' AS DEMO_SECTION;

-- Quick validation searches
SELECT 
    'Executive Search Results' AS USER_TYPE,
    COUNT(*) AS VISIBLE_TRANSACTIONS
FROM TABLE(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        TRANSACTION_SEARCH_SERVICE,
        'credit card transaction',
        100
    )
)
WHERE ARRAY_CONTAINS('ceo_jane_smith'::VARIANT, AUTHORIZED_USERS)

UNION ALL

SELECT 
    'Manager Search Results',
    COUNT(*)
FROM TABLE(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        TRANSACTION_SEARCH_SERVICE,
        'credit card transaction', 
        100
    )
)
WHERE ARRAY_CONTAINS('mgr_sarah_davis_us_east'::VARIANT, AUTHORIZED_USERS)

UNION ALL

SELECT 
    'Partner Search Results',
    COUNT(*)
FROM TABLE(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        TRANSACTION_SEARCH_SERVICE,
        'credit card transaction',
        100
    )
)
WHERE ARRAY_CONTAINS('partner_vendor_alex_jones'::VARIANT, AUTHORIZED_USERS);

SELECT 'Cortex Search Entitlements Demo Complete!' AS STATUS;
SELECT 'Use the above queries as templates for building entitlement-aware search applications.' AS NEXT_STEPS;
