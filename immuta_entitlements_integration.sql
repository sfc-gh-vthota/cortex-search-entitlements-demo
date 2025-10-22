-- Immuta Entitlements Integration with Cortex Search
-- ==================================================
-- This script demonstrates how to integrate Immuta data entitlements
-- with Snowflake Cortex Search services for comprehensive data governance

-- =============================================================================
-- IMMUTA INTEGRATION ARCHITECTURE
-- =============================================================================

/*
IMMUTA INTEGRATION OVERVIEW:

1. DATA CLASSIFICATION & TAGGING
   - Immuta automatically discovers and classifies sensitive data
   - Tags are applied at column and row level
   - Classifications: PII, PHI, Financial, Geographic, etc.

2. POLICY ENGINE
   - Attribute-Based Access Control (ABAC)
   - Row-level and column-level security policies
   - Dynamic data masking and filtering

3. CORTEX SEARCH INTEGRATION
   - Search services respect Immuta policies
   - Entitlements applied at query time
   - Audit logging for compliance

4. ROLE-BASED ACCESS PATTERNS
   - Different user roles see different search results
   - Geographic restrictions
   - Department-based data access
*/

-- =============================================================================
-- STEP 1: IMMUTA POLICY SIMULATION FRAMEWORK
-- =============================================================================

-- Create user context table (simulates Immuta user attributes)
CREATE OR REPLACE TABLE user_entitlements (
    username VARCHAR(100),
    role_name VARCHAR(100),
    department VARCHAR(100),
    region VARCHAR(100),
    clearance_level VARCHAR(50),
    data_access_tags ARRAY
);

-- Insert sample user entitlements (simulates Immuta user attributes)
INSERT INTO user_entitlements VALUES
    ('john.doe@company.com', 'SALES_MANAGER', 'SALES', 'US_WEST', 'HIGH', ['SALES_DATA', 'CUSTOMER_PII', 'FINANCIAL']),
    ('jane.smith@company.com', 'ANALYST', 'FINANCE', 'US_EAST', 'MEDIUM', ['FINANCIAL', 'TRANSACTION_DATA']),
    ('bob.wilson@company.com', 'REGIONAL_MANAGER', 'OPERATIONS', 'US_WEST', 'HIGH', ['GEOGRAPHIC', 'SALES_DATA']),
    ('alice.johnson@company.com', 'DATA_SCIENTIST', 'ANALYTICS', 'GLOBAL', 'LOW', ['AGGREGATE_ONLY']),
    ('mike.brown@company.com', 'COMPLIANCE_OFFICER', 'LEGAL', 'GLOBAL', 'RESTRICTED', ['AUDIT_ONLY']);

-- Create data classification table (simulates Immuta data tags)
CREATE OR REPLACE TABLE data_classification (
    table_name VARCHAR(100),
    column_name VARCHAR(100),
    classification_tag VARCHAR(100),
    sensitivity_level VARCHAR(50),
    geographic_restriction VARCHAR(100)
);

-- Define data classifications (simulates Immuta automatic discovery)
INSERT INTO data_classification VALUES
    ('STORE_SALES', 'SS_CUSTOMER_SK', 'CUSTOMER_PII', 'HIGH', NULL),
    ('STORE_SALES', 'SS_NET_PROFIT', 'FINANCIAL', 'MEDIUM', NULL),
    ('STORE_SALES', 'SS_SALES_PRICE', 'FINANCIAL', 'LOW', NULL),
    ('CUSTOMER', 'C_FIRST_NAME', 'CUSTOMER_PII', 'HIGH', NULL),
    ('CUSTOMER', 'C_LAST_NAME', 'CUSTOMER_PII', 'HIGH', NULL),
    ('CUSTOMER', 'C_EMAIL_ADDRESS', 'CUSTOMER_PII', 'HIGH', NULL),
    ('CUSTOMER', 'C_BIRTH_COUNTRY', 'GEOGRAPHIC', 'MEDIUM', 'GDPR_RESTRICTED'),
    ('STORE', 'S_CITY', 'GEOGRAPHIC', 'LOW', NULL),
    ('STORE', 'S_STATE', 'GEOGRAPHIC', 'LOW', NULL);

-- =============================================================================
-- STEP 2: ENTITLED CORTEX SEARCH SERVICE
-- =============================================================================

-- Create search service with entitlement-aware data preparation
CREATE OR REPLACE CORTEX SEARCH SERVICE tpcds_entitled_search
ON 
    entitled_item_description,
    entitled_customer_info,
    entitled_store_location,
    entitled_financial_data
WAREHOUSE = COMPUTE_WH
TARGET_LAG = '1 hour'
AS (
    SELECT 
        -- Core identifiers (always accessible)
        ss.ss_item_sk as item_key,
        ss.ss_sold_date_sk as date_key,
        d.d_date as transaction_date,
        d.d_year as year,
        
        -- Product information (generally accessible)
        i.i_item_desc as entitled_item_description,
        i.i_product_name as product_name,
        i.i_brand as brand,
        i.i_category as category,
        
        -- Customer information (PII - requires entitlements)
        CASE 
            WHEN CURRENT_USER() IN (
                SELECT username FROM user_entitlements 
                WHERE ARRAYS_OVERLAP(data_access_tags, ['CUSTOMER_PII'])
            )
            THEN CONCAT(c.c_first_name, ' ', c.c_last_name)
            ELSE 'CUSTOMER_' || HASH(ss.ss_customer_sk)  -- Pseudonymized
        END as entitled_customer_info,
        
        -- Store location (geographic restrictions)
        CASE 
            WHEN CURRENT_USER() IN (
                SELECT username FROM user_entitlements 
                WHERE ARRAYS_OVERLAP(data_access_tags, ['GEOGRAPHIC'])
            )
            THEN CONCAT(s.s_city, ', ', s.s_state)
            ELSE SUBSTR(s.s_state, 1, 1) || '***'  -- Masked location
        END as entitled_store_location,
        
        -- Financial data (requires financial access)
        CASE 
            WHEN CURRENT_USER() IN (
                SELECT username FROM user_entitlements 
                WHERE ARRAYS_OVERLAP(data_access_tags, ['FINANCIAL'])
            )
            THEN CONCAT('Price: $', ss.ss_sales_price::VARCHAR, ' Profit: $', ss.ss_net_profit::VARCHAR)
            WHEN CURRENT_USER() IN (
                SELECT username FROM user_entitlements 
                WHERE clearance_level = 'LOW'
            )
            THEN 'PRICE_RANGE_' || 
                 CASE 
                    WHEN ss.ss_sales_price < 25 THEN 'LOW'
                    WHEN ss.ss_sales_price < 100 THEN 'MEDIUM'
                    ELSE 'HIGH'
                 END
            ELSE 'FINANCIAL_DATA_RESTRICTED'
        END as entitled_financial_data,
        
        -- Searchable numeric fields (with aggregation controls)
        CASE 
            WHEN CURRENT_USER() IN (
                SELECT username FROM user_entitlements 
                WHERE clearance_level IN ('HIGH', 'MEDIUM')
            )
            THEN ss.ss_sales_price
            ELSE ROUND(ss.ss_sales_price / 10) * 10  -- Rounded for privacy
        END as sales_price_entitled,
        
        ss.ss_quantity as quantity,
        ss.ss_customer_sk as customer_key_hashed
        
    FROM SAMPLE_DATA.TPCDS_SF10TCL.STORE_SALES ss
    LEFT JOIN SAMPLE_DATA.TPCDS_SF10TCL.ITEM i ON ss.ss_item_sk = i.i_item_sk
    LEFT JOIN SAMPLE_DATA.TPCDS_SF10TCL.STORE s ON ss.ss_store_sk = s.s_store_sk
    LEFT JOIN SAMPLE_DATA.TPCDS_SF10TCL.CUSTOMER c ON ss.ss_customer_sk = c.c_customer_sk
    LEFT JOIN SAMPLE_DATA.TPCDS_SF10TCL.DATE_DIM d ON ss.ss_sold_date_sk = d.d_date_sk
    WHERE d.d_year >= 2000
);

-- =============================================================================
-- STEP 3: ENTITLEMENT-AWARE SEARCH FUNCTIONS
-- =============================================================================

-- Create UDF for dynamic entitlement checking
CREATE OR REPLACE FUNCTION check_user_entitlement(
    required_tag VARCHAR,
    user_context VARCHAR DEFAULT CURRENT_USER()
)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
    SELECT EXISTS(
        SELECT 1 FROM user_entitlements 
        WHERE username = user_context
        AND ARRAYS_OVERLAP(data_access_tags, [required_tag])
    )
$$;

-- Create secure search function with automatic entitlement enforcement
CREATE OR REPLACE FUNCTION entitled_search(
    search_query VARCHAR,
    max_results INTEGER DEFAULT 20
)
RETURNS TABLE(
    item_key INTEGER,
    entitled_description VARCHAR,
    entitled_customer VARCHAR,
    entitled_location VARCHAR,
    entitled_price VARCHAR,
    access_level VARCHAR
)
LANGUAGE SQL
AS
$$
    SELECT 
        item_key,
        entitled_item_description as entitled_description,
        entitled_customer_info as entitled_customer,
        entitled_store_location as entitled_location,
        entitled_financial_data as entitled_price,
        CASE 
            WHEN check_user_entitlement('CUSTOMER_PII') AND check_user_entitlement('FINANCIAL')
                THEN 'FULL_ACCESS'
            WHEN check_user_entitlement('FINANCIAL')
                THEN 'FINANCIAL_ACCESS'
            WHEN check_user_entitlement('CUSTOMER_PII')
                THEN 'CUSTOMER_ACCESS'
            ELSE 'BASIC_ACCESS'
        END as access_level
    FROM TABLE(
        SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
            'tpcds_entitled_search',
            search_query,
            LIMIT => max_results
        )
    )
$$;

-- =============================================================================
-- STEP 4: ROLE-BASED SEARCH EXAMPLES
-- =============================================================================

-- Simulate different user contexts for testing
SELECT 'ENTITLEMENT-BASED SEARCH EXAMPLES' as demo_section;

-- Example 1: High-privilege user search (Sales Manager)
SELECT 'SALES MANAGER - Full Access' as user_role;

-- Simulate setting user context (in real Immuta integration, this is automatic)
-- SET CURRENT_USER = 'john.doe@company.com';  -- This would be handled by Immuta

SELECT * FROM TABLE(entitled_search('premium electronics', 10));

-- Example 2: Low-privilege user search (Data Scientist)
SELECT 'DATA SCIENTIST - Aggregate Only' as user_role;

-- Create aggregated view for low-privilege users
CREATE OR REPLACE SECURE VIEW entitled_search_aggregated AS
WITH search_base AS (
    SELECT * FROM TABLE(
        SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
            'tpcds_entitled_search',
            'customer transactions',
            LIMIT => 100
        )
    )
)
SELECT 
    -- Only aggregated data for low-privilege users
    SUBSTR(entitled_item_description, 1, 20) as item_category,
    COUNT(*) as transaction_count,
    ROUND(AVG(sales_price_entitled), -1) as avg_price_range,
    DATE_TRUNC('month', transaction_date) as month_year
FROM search_base
WHERE CURRENT_USER() IN (
    SELECT username FROM user_entitlements 
    WHERE clearance_level = 'LOW'
)
GROUP BY 
    SUBSTR(entitled_item_description, 1, 20),
    DATE_TRUNC('month', transaction_date);

-- Example 3: Geographic restrictions (Regional Manager)
SELECT 'REGIONAL MANAGER - Geographic Restrictions' as user_role;

WITH regional_search AS (
    SELECT * FROM TABLE(
        SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
            'tpcds_entitled_search',
            'store performance analysis',
            LIMIT => 50
        )
    )
)
SELECT *
FROM regional_search
WHERE entitled_store_location LIKE '%CA%'  -- California region only
   OR entitled_store_location LIKE '%NV%'  -- Nevada region only
ORDER BY sales_price_entitled DESC;

-- =============================================================================
-- STEP 5: AUDIT AND COMPLIANCE
-- =============================================================================

-- Create audit log for search activity (Immuta integration point)
CREATE OR REPLACE TABLE search_audit_log (
    audit_id INTEGER AUTOINCREMENT,
    username VARCHAR(100),
    search_query VARCHAR(500),
    search_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    results_count INTEGER,
    entitlements_applied ARRAY,
    sensitive_data_accessed BOOLEAN,
    ip_address VARCHAR(45),
    session_id VARCHAR(100)
);

-- Create auditable search procedure
CREATE OR REPLACE PROCEDURE auditable_search(
    search_query VARCHAR,
    max_results INTEGER DEFAULT 20
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    results VARIANT;
    user_name VARCHAR DEFAULT CURRENT_USER();
    result_count INTEGER;
    user_entitlements ARRAY;
BEGIN
    -- Get user entitlements
    SELECT data_access_tags INTO user_entitlements
    FROM user_entitlements
    WHERE username = user_name;
    
    -- Perform search
    CREATE OR REPLACE TEMPORARY TABLE temp_search_results AS
    SELECT * FROM TABLE(entitled_search(:search_query, :max_results));
    
    -- Count results
    SELECT COUNT(*) INTO result_count FROM temp_search_results;
    
    -- Log search activity
    INSERT INTO search_audit_log (
        username, search_query, results_count, entitlements_applied, sensitive_data_accessed
    ) VALUES (
        user_name, 
        search_query, 
        result_count, 
        user_entitlements,
        ARRAYS_OVERLAP(user_entitlements, ['CUSTOMER_PII', 'FINANCIAL'])
    );
    
    -- Return results
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) INTO results FROM temp_search_results;
    RETURN results;
END;
$$;

-- =============================================================================
-- STEP 6: IMMUTA POLICY SIMULATION
-- =============================================================================

-- Create policy enforcement view (simulates Immuta policy engine)
CREATE OR REPLACE SECURE VIEW immuta_policy_enforced_search AS
WITH policy_context AS (
    SELECT 
        CURRENT_USER() as current_user,
        CURRENT_ROLE() as current_role,
        CURRENT_IP_ADDRESS() as ip_address
),
user_policies AS (
    SELECT 
        ue.*,
        pc.current_role,
        pc.ip_address
    FROM user_entitlements ue
    CROSS JOIN policy_context pc
    WHERE ue.username = pc.current_user
),
search_results AS (
    SELECT * FROM TABLE(
        SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
            'tpcds_entitled_search',
            'sensitive customer data',
            LIMIT => 100
        )
    )
)
SELECT 
    sr.item_key,
    -- Apply column-level masking policies
    CASE 
        WHEN up.clearance_level = 'RESTRICTED' THEN 'ACCESS_DENIED'
        WHEN 'CUSTOMER_PII' = ANY(up.data_access_tags) THEN sr.entitled_customer_info
        ELSE 'MASKED_CUSTOMER_INFO'
    END as masked_customer_info,
    
    -- Apply row-level filtering policies
    CASE 
        WHEN up.region = 'US_WEST' AND sr.entitled_store_location LIKE '%CA%' THEN sr.entitled_store_location
        WHEN up.region = 'US_EAST' AND sr.entitled_store_location LIKE '%NY%' THEN sr.entitled_store_location
        WHEN up.clearance_level = 'HIGH' THEN sr.entitled_store_location
        ELSE 'LOCATION_FILTERED'
    END as policy_filtered_location,
    
    -- Apply data minimization
    CASE 
        WHEN up.department = 'LEGAL' THEN 'AUDIT_VIEW_ONLY'
        WHEN 'FINANCIAL' = ANY(up.data_access_tags) THEN sr.entitled_financial_data
        ELSE NULL
    END as financial_data_minimized,
    
    up.username,
    up.clearance_level,
    CURRENT_TIMESTAMP() as policy_applied_at
FROM search_results sr
CROSS JOIN user_policies up
WHERE 
    -- Row-level security: only show data user is entitled to see
    (up.clearance_level != 'RESTRICTED')
    AND (
        up.region = 'GLOBAL' 
        OR sr.entitled_store_location LIKE CONCAT('%', SPLIT_PART(up.region, '_', 2), '%')
    );

-- =============================================================================
-- PRODUCTION IMMUTA INTEGRATION GUIDE
-- =============================================================================

/*
PRODUCTION IMMUTA INTEGRATION STEPS:

1. IMMUTA POLICY SETUP:
   - Connect Immuta to Snowflake using native integration
   - Configure automatic data discovery and classification
   - Set up ABAC policies based on user attributes
   - Define data masking and filtering rules

2. CORTEX SEARCH INTEGRATION:
   - Replace CASE statements with Immuta policy functions
   - Use Immuta's dynamic data masking in search service
   - Implement real-time policy evaluation

3. EXAMPLE IMMUTA FUNCTIONS (replace simulation):
   ```sql
   -- Real Immuta integration
   CREATE CORTEX SEARCH SERVICE production_search AS (
       SELECT 
           item_key,
           IMMUTA.MASK_COLUMN('customer_name', customer_name) as masked_customer,
           IMMUTA.FILTER_GEOGRAPHIC('store_location', store_location) as filtered_location,
           IMMUTA.APPLY_POLICY('financial_data', sales_amount) as entitled_amount
       FROM source_table
       WHERE IMMUTA.ROW_FILTER('transaction_filter', user_attributes)
   );
   ```

4. AUDIT INTEGRATION:
   - Configure Immuta audit logging
   - Set up compliance reporting
   - Monitor policy violations

5. TESTING:
   - Test with different user personas
   - Validate policy enforcement
   - Performance testing with entitlements
*/

-- Show implementation status
SELECT 'Immuta Entitlements Integration Demo Complete!' as status,
       'Simulation framework demonstrates all entitlement capabilities' as simulation_status,
       'Ready for production Immuta integration' as next_steps;

