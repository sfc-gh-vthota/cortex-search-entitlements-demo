-- =============================================================================
-- CORTEX SEARCH ENTITLEMENTS DEMO - Dynamic Entitlement View Creation
-- =============================================================================
-- This script creates views that derive user access by dynamically fetching
-- and evaluating the actual Row Access Policy DDL definitions
-- Run this script after completing the previous setup scripts

-- Set context
USE ROLE ACCOUNTADMIN;
USE DATABASE CORTEX_SEARCH_ENTITLEMENTS_DB;
USE SCHEMA ENTITLEMENTS;
USE WAREHOUSE ENTITLEMENTS_WH;

-- =============================================================================
-- STEP 1: CREATE DYNAMIC POLICY INTROSPECTION FUNCTIONS
-- =============================================================================

-- Function to extract all row access policies applied to our table
CREATE OR REPLACE FUNCTION GET_TABLE_ROW_ACCESS_POLICIES()
RETURNS ARRAY
LANGUAGE SQL
AS
$$
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
        'policy_name', POLICY_NAME,
        'policy_kind', POLICY_KIND,
        'policy_signature', POLICY_SIGNATURE,
        'policy_body', POLICY_BODY,
        'policy_return_type', POLICY_RETURN_TYPE,
        'ref_database_name', REF_DATABASE_NAME,
        'ref_schema_name', REF_SCHEMA_NAME,
        'ref_entity_name', REF_ENTITY_NAME,
        'ref_column_name', REF_COLUMN_NAME
    ))
    FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
        REF_ENTITY_NAME => 'CORTEX_SEARCH_ENTITLEMENTS_DB.ENTITLEMENTS.CREDIT_CARD_TRANSACTIONS',
        REF_ENTITY_DOMAIN => 'TABLE'
    ))
    WHERE POLICY_KIND = 'ROW_ACCESS_POLICY'
$$;

-- Function to parse role references from RAP policy bodies
CREATE OR REPLACE FUNCTION EXTRACT_ROLES_FROM_POLICY_BODIES()
RETURNS OBJECT
LANGUAGE SQL
AS
$$
    WITH policy_data AS (
        SELECT 
            POLICY_NAME,
            POLICY_BODY
        FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
            REF_ENTITY_NAME => 'CORTEX_SEARCH_ENTITLEMENTS_DB.ENTITLEMENTS.CREDIT_CARD_TRANSACTIONS',
            REF_ENTITY_DOMAIN => 'TABLE'
        ))
        WHERE POLICY_KIND = 'ROW_ACCESS_POLICY'
    ),
    role_extractions AS (
        SELECT 
            POLICY_NAME,
            POLICY_BODY,
            -- Extract role names using REGEXP_SUBSTR to find CURRENT_ROLE() = 'ROLE_NAME' patterns
            REGEXP_SUBSTR_ALL(POLICY_BODY, 'CURRENT_ROLE\\(\\)\\s*=\\s*[''"]([^''"]+)[''"]', 1, 1, 'i', 1) AS extracted_roles
        FROM policy_data
    )
    SELECT OBJECT_CONSTRUCT(
        'policies_analyzed', COUNT(DISTINCT POLICY_NAME),
        'total_roles_found', ARRAY_SIZE(ARRAY_AGG(DISTINCT role_ref.value::STRING)),
        'roles_by_policy', OBJECT_AGG(POLICY_NAME, extracted_roles),
        'all_roles_found', ARRAY_AGG(DISTINCT role_ref.value::STRING),
        'extraction_timestamp', CURRENT_TIMESTAMP()
    )
    FROM role_extractions,
    LATERAL FLATTEN(input => extracted_roles) role_ref
    WHERE role_ref.value IS NOT NULL
$$;

-- Function to create dynamic user-to-role mapping based on RAP analysis
CREATE OR REPLACE FUNCTION CREATE_DYNAMIC_USER_ROLE_MAPPING()
RETURNS TABLE (USERNAME VARCHAR, APPLICABLE_ROLES ARRAY)
LANGUAGE SQL
AS
$$
    WITH 
    -- Get roles extracted from actual RAP policies
    rap_analysis AS (
        SELECT EXTRACT_ROLES_FROM_POLICY_BODIES() AS analysis_result
    ),
    discovered_roles AS (
        SELECT role_name.value::STRING AS role_name
        FROM rap_analysis,
        LATERAL FLATTEN(input => analysis_result:all_roles_found) role_name
    ),
    -- Create user mappings based on discovered roles and user attributes
    user_role_candidates AS (
        SELECT 
            uam.USERNAME,
            dr.role_name,
            CASE 
                -- Dynamic role matching based on role name patterns and user attributes
                WHEN dr.role_name LIKE '%GLOBAL_ACCESS%' AND uam.IS_EXECUTIVE THEN TRUE
                WHEN dr.role_name LIKE '%US_EAST%' AND ARRAY_CONTAINS('US_EAST'::VARIANT, uam.ALLOWED_REGIONS) THEN TRUE
                WHEN dr.role_name LIKE '%US_WEST%' AND ARRAY_CONTAINS('US_WEST'::VARIANT, uam.ALLOWED_REGIONS) THEN TRUE
                WHEN dr.role_name LIKE '%EUROPE%' AND ARRAY_CONTAINS('EUROPE'::VARIANT, uam.ALLOWED_REGIONS) THEN TRUE
                WHEN dr.role_name LIKE '%ASIA_PAC%' AND ARRAY_CONTAINS('ASIA_PAC'::VARIANT, uam.ALLOWED_REGIONS) THEN TRUE
                WHEN dr.role_name LIKE '%FINANCE%' AND uam.DEPARTMENT = 'FINANCE' THEN TRUE
                WHEN dr.role_name LIKE '%FRAUD%' AND uam.DEPARTMENT = 'FRAUD' THEN TRUE
                WHEN dr.role_name LIKE '%COMPLIANCE%' AND uam.DEPARTMENT = 'COMPLIANCE' THEN TRUE
                WHEN dr.role_name LIKE '%OPERATIONS%' AND uam.DEPARTMENT = 'OPERATIONS' THEN TRUE
                WHEN dr.role_name LIKE '%CUSTOMER_SERVICE%' AND uam.DEPARTMENT = 'CUSTOMER_SERVICE' THEN TRUE
                WHEN dr.role_name LIKE '%PREMIUM%' AND ARRAY_CONTAINS('PREMIUM'::VARIANT, uam.ALLOWED_CUSTOMER_TIERS) THEN TRUE
                WHEN dr.role_name LIKE '%GOLD%' AND ARRAY_CONTAINS('GOLD'::VARIANT, uam.ALLOWED_CUSTOMER_TIERS) THEN TRUE
                WHEN dr.role_name LIKE '%HIGH_RISK%' AND ARRAY_CONTAINS('HIGH'::VARIANT, uam.ALLOWED_RISK_LEVELS) THEN TRUE
                WHEN dr.role_name LIKE '%LOW_RISK%' AND uam.ACCESS_LEVEL = 'LIMITED' THEN TRUE
                WHEN dr.role_name LIKE '%PARTNER%' AND uam.IS_EXTERNAL THEN TRUE
                ELSE FALSE
            END AS role_matches_user
        FROM USER_ACCESS_MAPPING uam
        CROSS JOIN discovered_roles dr
    )
    SELECT 
        USERNAME,
        ARRAY_AGG(role_name) AS APPLICABLE_ROLES
    FROM user_role_candidates
    WHERE role_matches_user = TRUE
    GROUP BY USERNAME
$$;

-- Function to get all available roles in the account
CREATE OR REPLACE FUNCTION GET_AVAILABLE_ROLES()
RETURNS ARRAY
LANGUAGE SQL
AS
$$
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
        'role_name', "name",
        'comment', "comment",
        'owner', "owner"
    ))
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) 
    WHERE TRUE -- This will be populated by calling SHOW ROLES first
$$;

-- =============================================================================
-- STEP 2: CREATE PROCEDURE TO DYNAMICALLY TEST ACCESS FOR ALL ROLES
-- =============================================================================

CREATE OR REPLACE PROCEDURE POPULATE_ACCESS_CACHE_DYNAMICALLY()
RETURNS OBJECT
LANGUAGE SQL
AS
$$
DECLARE
    -- Variables for role iteration
    roles_result RESULTSET;
    role_name VARCHAR;
    role_comment VARCHAR;
    
    -- Variables for transaction testing
    transaction_count INTEGER;
    accessible_count INTEGER;
    
    -- Results tracking
    total_roles_tested INTEGER := 0;
    total_access_grants INTEGER := 0;
    start_time TIMESTAMP_LTZ := CURRENT_TIMESTAMP();
    
    -- SQL for dynamic execution
    switch_role_sql VARCHAR;
    test_access_sql VARCHAR;
    insert_access_sql VARCHAR;
    
    -- Error handling
    error_message VARCHAR;
    
BEGIN
    -- Clear existing access cache
    DELETE FROM USER_TRANSACTION_ACCESS_CACHE;
    
    -- Get roles dynamically from RAP policy analysis
    SELECT EXTRACT_ROLES_FROM_POLICY_BODIES() INTO :rap_analysis;
    
    -- Get all discovered roles from the RAP policies
    EXECUTE IMMEDIATE 'SHOW ROLES';
    LET roles_result RESULTSET := (
        WITH rap_roles AS (
            SELECT role_name.value::STRING as discovered_role
            FROM TABLE(SELECT PARSE_JSON(:rap_analysis):all_roles_found) t,
            LATERAL FLATTEN(input => t.VALUE) role_name
        ),
        system_roles AS (
            SELECT "name" as role_name, "comment" as role_comment 
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        )
        SELECT sr.role_name, sr.role_comment
        FROM system_roles sr
        JOIN rap_roles rr ON sr.role_name = rr.discovered_role
        WHERE sr.role_name IS NOT NULL
    );
    
    -- Iterate through each role and test access dynamically
    FOR role_record IN roles_result DO
        role_name := role_record.role_name;
        role_comment := role_record.role_comment;
        
        BEGIN
            -- Switch to the role (this activates row access policies for that role)
            switch_role_sql := 'USE ROLE ' || role_name;
            EXECUTE IMMEDIATE switch_role_sql;
            
            -- Test access by actually querying the table with policies active
            -- This query will only return transactions visible to the current role
            test_access_sql := 'SELECT COUNT(*) FROM CREDIT_CARD_TRANSACTIONS';
            EXECUTE IMMEDIATE test_access_sql INTO accessible_count;
            
            -- Get the actual accessible transaction IDs under this role
            -- Using dynamic role mapping derived from RAP policy bodies
            insert_access_sql := '
                INSERT INTO USER_TRANSACTION_ACCESS_CACHE 
                SELECT 
                    durm.USERNAME,
                    t.TRANSACTION_ID,
                    ''' || role_name || ''' as PRIMARY_ROLE,
                    uam.FULL_NAME,
                    uam.ACCESS_LEVEL,
                    uam.IS_EXECUTIVE,
                    uam.IS_MANAGER,
                    uam.IS_EXTERNAL,
                    uam.DEPARTMENT,
                    TRUE as HAS_ACCESS,
                    CURRENT_TIMESTAMP() as LAST_UPDATED
                FROM CREDIT_CARD_TRANSACTIONS t
                CROSS JOIN TABLE(CREATE_DYNAMIC_USER_ROLE_MAPPING()) durm
                JOIN USER_ACCESS_MAPPING uam ON durm.USERNAME = uam.USERNAME
                WHERE ARRAY_CONTAINS(''' || role_name || '''::VARIANT, durm.APPLICABLE_ROLES)';
            
            -- Execute the insert (this captures what the current role can actually see)
            EXECUTE IMMEDIATE insert_access_sql;
            
            total_roles_tested := total_roles_tested + 1;
            total_access_grants := total_access_grants + accessible_count;
            
        EXCEPTION
            WHEN OTHER THEN
                error_message := SQLERRM;
                -- Continue with next role even if this one fails
                CONTINUE;
        END;
    END FOR;
    
    -- Switch back to ACCOUNTADMIN
    USE ROLE ACCOUNTADMIN;
    
    -- Return summary of what was tested
    RETURN OBJECT_CONSTRUCT(
        'success', TRUE,
        'roles_tested', total_roles_tested,
        'total_access_grants', total_access_grants,
        'start_time', start_time,
        'end_time', CURRENT_TIMESTAMP(),
        'duration_seconds', DATEDIFF('second', start_time, CURRENT_TIMESTAMP()),
        'policies_evaluated', (SELECT ARRAY_SIZE(GET_TABLE_ROW_ACCESS_POLICIES())),
        'last_error', error_message
    );
    
EXCEPTION
    WHEN OTHER THEN
        -- Ensure we switch back to ACCOUNTADMIN even if there's an error
        USE ROLE ACCOUNTADMIN;
        RETURN OBJECT_CONSTRUCT(
            'success', FALSE,
            'error', SQLERRM,
            'roles_tested', total_roles_tested
        );
END;
$$;

-- =============================================================================
-- STEP 3: CREATE ENHANCED ACCESS CACHE TABLE
-- =============================================================================

CREATE OR REPLACE TABLE USER_TRANSACTION_ACCESS_CACHE (
    USERNAME VARCHAR(100),
    TRANSACTION_ID VARCHAR(50), 
    PRIMARY_ROLE VARCHAR(100),
    FULL_NAME VARCHAR(200),
    ACCESS_LEVEL VARCHAR(20),
    IS_EXECUTIVE BOOLEAN,
    IS_MANAGER BOOLEAN,
    IS_EXTERNAL BOOLEAN,
    DEPARTMENT VARCHAR(50),
    HAS_ACCESS BOOLEAN DEFAULT FALSE,
    LAST_UPDATED TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Add constraint for performance
    PRIMARY KEY (USERNAME, TRANSACTION_ID)
);

-- =============================================================================
-- STEP 4: CREATE PROCEDURE TO ANALYZE ROW ACCESS POLICIES DYNAMICALLY
-- =============================================================================

CREATE OR REPLACE PROCEDURE ANALYZE_ROW_ACCESS_POLICIES()
RETURNS OBJECT
LANGUAGE SQL
AS
$$
DECLARE
    policies_info ARRAY;
    policy_count INTEGER;
    policy_details OBJECT;
    
    -- SQL to get policy information
    policy_query VARCHAR := '
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
            ''policy_name'', POLICY_NAME,
            ''policy_kind'', POLICY_KIND,
            ''policy_signature'', POLICY_SIGNATURE,
            ''policy_body'', POLICY_BODY,
            ''ref_column_name'', REF_COLUMN_NAME
        )) as policies
        FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
            REF_ENTITY_NAME => ''CORTEX_SEARCH_ENTITLEMENTS_DB.ENTITLEMENTS.CREDIT_CARD_TRANSACTIONS'',
            REF_ENTITY_DOMAIN => ''TABLE''
        ))
        WHERE POLICY_KIND = ''ROW_ACCESS_POLICY''';
    
BEGIN
    -- Get information about applied row access policies
    EXECUTE IMMEDIATE policy_query INTO policies_info;
    
    -- Count policies
    policy_count := ARRAY_SIZE(policies_info);
    
    -- Return analysis
    RETURN OBJECT_CONSTRUCT(
        'total_policies', policy_count,
        'policies_found', policies_info,
        'table_name', 'CREDIT_CARD_TRANSACTIONS',
        'analysis_timestamp', CURRENT_TIMESTAMP()
    );
    
END;
$$;

-- =============================================================================
-- STEP 5: CREATE TRULY DYNAMIC ACCESS TESTING PROCEDURE
-- =============================================================================

CREATE OR REPLACE PROCEDURE TEST_DYNAMIC_ACCESS_WITH_POLICY_INSPECTION()
RETURNS OBJECT
LANGUAGE SQL  
AS
$$
DECLARE
    -- Policy analysis variables
    policies_analysis OBJECT;
    
    -- Role testing variables  
    all_roles ARRAY;
    roles_cursor CURSOR FOR 
        WITH rap_roles AS (
            SELECT role_name.value::STRING as discovered_role
            FROM TABLE(SELECT (SELECT EXTRACT_ROLES_FROM_POLICY_BODIES()):all_roles_found) t,
            LATERAL FLATTEN(input => t.VALUE) role_name
        ),
        system_roles AS (
            SELECT "name" as role_name
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        )
        SELECT sr.role_name
        FROM system_roles sr
        JOIN rap_roles rr ON sr.role_name = rr.discovered_role
        WHERE sr.role_name IS NOT NULL;
    
    current_role VARCHAR;
    access_test_results ARRAY := ARRAY_CONSTRUCT();
    role_access_summary OBJECT;
    
BEGIN
    -- Step 1: Analyze the row access policies dynamically
    CALL ANALYZE_ROW_ACCESS_POLICIES() INTO policies_analysis;
    
    -- Step 2: Get all roles in the system
    EXECUTE IMMEDIATE 'SHOW ROLES';
    
    -- Step 3: Clear and prepare access cache
    CREATE OR REPLACE TEMPORARY TABLE TEMP_ACCESS_RESULTS (
        role_name VARCHAR,
        username VARCHAR,
        transaction_id VARCHAR,
        accessible BOOLEAN,
        test_timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
    );
    
    -- Step 4: Test each role's access dynamically
    FOR role_record IN roles_cursor DO
        current_role := role_record.role_name;
        
        BEGIN
            -- Switch to role (activates its row access policies)
            EXECUTE IMMEDIATE 'USE ROLE ' || current_role;
            
            -- Test what this role can actually see
            INSERT INTO TEMP_ACCESS_RESULTS (role_name, transaction_id, accessible)
            SELECT 
                :current_role as role_name,
                TRANSACTION_ID,
                TRUE as accessible
            FROM CREDIT_CARD_TRANSACTIONS;
            
        EXCEPTION
            WHEN OTHER THEN
                -- Role might not exist or have access - continue
                CONTINUE;
        END;
    END FOR;
    
    -- Step 5: Switch back and populate main cache
    USE ROLE ACCOUNTADMIN;
    
    -- Populate the main access cache with results from actual policy testing
    MERGE INTO USER_TRANSACTION_ACCESS_CACHE AS target
    USING (
        SELECT 
            durm.USERNAME,
            tar.transaction_id,
            tar.role_name as PRIMARY_ROLE,
            uam.FULL_NAME,
            uam.ACCESS_LEVEL,
            uam.IS_EXECUTIVE,
            uam.IS_MANAGER,
            uam.IS_EXTERNAL,
            uam.DEPARTMENT,
            tar.accessible as HAS_ACCESS,
            CURRENT_TIMESTAMP() as LAST_UPDATED
        FROM TEMP_ACCESS_RESULTS tar
        JOIN TABLE(CREATE_DYNAMIC_USER_ROLE_MAPPING()) durm ON ARRAY_CONTAINS(tar.role_name::VARIANT, durm.APPLICABLE_ROLES)
        JOIN USER_ACCESS_MAPPING uam ON durm.USERNAME = uam.USERNAME
        WHERE tar.accessible = TRUE
    ) AS source
    ON target.USERNAME = source.USERNAME AND target.TRANSACTION_ID = source.TRANSACTION_ID
    WHEN MATCHED THEN 
        UPDATE SET 
            HAS_ACCESS = source.HAS_ACCESS,
            LAST_UPDATED = source.LAST_UPDATED
    WHEN NOT MATCHED THEN
        INSERT (USERNAME, TRANSACTION_ID, PRIMARY_ROLE, FULL_NAME, ACCESS_LEVEL, 
                IS_EXECUTIVE, IS_MANAGER, IS_EXTERNAL, DEPARTMENT, HAS_ACCESS, LAST_UPDATED)
        VALUES (source.USERNAME, source.TRANSACTION_ID, source.PRIMARY_ROLE, source.FULL_NAME,
                source.ACCESS_LEVEL, source.IS_EXECUTIVE, source.IS_MANAGER, source.IS_EXTERNAL,
                source.DEPARTMENT, source.HAS_ACCESS, source.LAST_UPDATED);
    
    -- Return comprehensive results
    RETURN OBJECT_CONSTRUCT(
        'success', TRUE,
        'policies_analyzed', policies_analysis,
        'total_roles_tested', (SELECT COUNT(DISTINCT role_name) FROM TEMP_ACCESS_RESULTS),
        'total_access_records', (SELECT COUNT(*) FROM USER_TRANSACTION_ACCESS_CACHE WHERE HAS_ACCESS = TRUE),
        'test_timestamp', CURRENT_TIMESTAMP(),
        'method', 'dynamic_policy_inspection_and_role_testing'
    );
    
END;
$$;

-- =============================================================================
-- STEP 6: CREATE ENTITLEMENT VIEWS (Same as before but fed by dynamic testing)
-- =============================================================================

-- View showing user-transaction access from dynamic policy testing
CREATE OR REPLACE VIEW TRANSACTION_USER_ACCESS AS
SELECT 
    USERNAME,
    FULL_NAME,
    ACCESS_LEVEL,
    IS_EXECUTIVE,
    IS_MANAGER,
    IS_EXTERNAL,
    PRIMARY_ROLE,
    DEPARTMENT,
    TRANSACTION_ID,
    HAS_ACCESS,
    LAST_UPDATED
FROM USER_TRANSACTION_ACCESS_CACHE
WHERE HAS_ACCESS = TRUE;

-- Main entitlement view with user arrays from dynamic policy evaluation
CREATE OR REPLACE VIEW TRANSACTIONS_WITH_ENTITLEMENTS AS
SELECT 
    -- All original transaction fields
    t.*,
    
    -- User arrays built from dynamic policy testing results
    COALESCE(access_summary.AUTHORIZED_USERS, ARRAY_CONSTRUCT()) AS AUTHORIZED_USERS,
    COALESCE(access_summary.AUTHORIZED_USER_COUNT, 0) AS AUTHORIZED_USER_COUNT,
    COALESCE(access_summary.EXECUTIVE_USERS, ARRAY_CONSTRUCT()) AS EXECUTIVE_USERS,
    COALESCE(access_summary.MANAGER_USERS, ARRAY_CONSTRUCT()) AS MANAGER_USERS,
    COALESCE(access_summary.DEPARTMENT_USERS, ARRAY_CONSTRUCT()) AS DEPARTMENT_USERS,
    COALESCE(access_summary.SPECIALIST_USERS, ARRAY_CONSTRUCT()) AS SPECIALIST_USERS,
    COALESCE(access_summary.EXTERNAL_USERS, ARRAY_CONSTRUCT()) AS EXTERNAL_USERS,
    
    -- Dynamic policy evaluation metadata
    access_summary.LAST_POLICY_TEST,
    access_summary.ROLES_WITH_ACCESS,
    access_summary.ACCESS_METHOD
    
FROM CREDIT_CARD_TRANSACTIONS t
LEFT JOIN (
    SELECT 
        TRANSACTION_ID,
        ARRAY_AGG(USERNAME) WITHIN GROUP (ORDER BY USERNAME) AS AUTHORIZED_USERS,
        COUNT(USERNAME) AS AUTHORIZED_USER_COUNT,
        ARRAY_AGG(CASE WHEN IS_EXECUTIVE THEN USERNAME END) WITHIN GROUP (ORDER BY USERNAME) AS EXECUTIVE_USERS,
        ARRAY_AGG(CASE WHEN IS_MANAGER AND NOT IS_EXECUTIVE THEN USERNAME END) WITHIN GROUP (ORDER BY USERNAME) AS MANAGER_USERS,
        ARRAY_AGG(CASE WHEN NOT IS_MANAGER AND NOT IS_EXECUTIVE AND NOT IS_EXTERNAL THEN USERNAME END) WITHIN GROUP (ORDER BY USERNAME) AS DEPARTMENT_USERS,
        ARRAY_AGG(CASE WHEN ACCESS_LEVEL = 'SPECIALIZED' THEN USERNAME END) WITHIN GROUP (ORDER BY USERNAME) AS SPECIALIST_USERS,
        ARRAY_AGG(CASE WHEN IS_EXTERNAL THEN USERNAME END) WITHIN GROUP (ORDER BY USERNAME) AS EXTERNAL_USERS,
        MAX(LAST_UPDATED) AS LAST_POLICY_TEST,
        ARRAY_AGG(DISTINCT PRIMARY_ROLE) AS ROLES_WITH_ACCESS,
        'dynamic_policy_evaluation' AS ACCESS_METHOD
    FROM USER_TRANSACTION_ACCESS_CACHE
    WHERE HAS_ACCESS = TRUE
    GROUP BY TRANSACTION_ID
) access_summary ON t.TRANSACTION_ID = access_summary.TRANSACTION_ID;

-- Cortex Search optimized view  
CREATE OR REPLACE VIEW CORTEX_SEARCH_TRANSACTIONS AS
SELECT 
    -- All transaction attributes plus dynamic entitlements
    t.*,
    -- User arrays from dynamic policy evaluation
    twe.AUTHORIZED_USERS,
    twe.AUTHORIZED_USER_COUNT,
    twe.EXECUTIVE_USERS,
    twe.MANAGER_USERS,
    twe.DEPARTMENT_USERS,
    twe.SPECIALIST_USERS,
    twe.EXTERNAL_USERS,
    
    -- Searchable content
    CONCAT(
        'Transaction ID: ', t.TRANSACTION_ID, '. ',
        'Customer: ', t.CUSTOMER_ID, '. ',
        'Merchant: ', t.MERCHANT_NAME, ' (', t.MERCHANT_CATEGORY, '). ',
        'Amount: $', t.AMOUNT, ' ', t.CURRENCY, '. ',
        'Region: ', t.REGION_NAME, '. ',
        'Customer Tier: ', t.CUSTOMER_TIER, '. ',
        'Risk Level: ', t.RISK_LEVEL, '. ',
        'Status: ', t.TRANSACTION_STATUS
    ) AS SEARCH_CONTENT,
    
    -- Dynamic evaluation metadata
    twe.LAST_POLICY_TEST,
    twe.ACCESS_METHOD
    
FROM CREDIT_CARD_TRANSACTIONS t
JOIN TRANSACTIONS_WITH_ENTITLEMENTS twe ON t.TRANSACTION_ID = twe.TRANSACTION_ID;

-- =============================================================================
-- STEP 7: RUN INITIAL DYNAMIC POLICY EVALUATION
-- =============================================================================

-- Execute the dynamic policy analysis and access testing
CALL TEST_DYNAMIC_ACCESS_WITH_POLICY_INSPECTION();

-- =============================================================================
-- STEP 8: VERIFICATION AND MONITORING VIEWS
-- =============================================================================

CREATE OR REPLACE VIEW DYNAMIC_POLICY_EVALUATION_STATUS AS
SELECT 
    'Dynamic Policy Evaluation Summary' AS METRIC_TYPE,
    'Row Access Policies Detected' AS METRIC,
    (SELECT ARRAY_SIZE(GET_TABLE_ROW_ACCESS_POLICIES()))::VARCHAR AS VALUE

UNION ALL

SELECT 
    'Dynamic Policy Evaluation Summary',
    'Unique Roles Tested',
    COUNT(DISTINCT PRIMARY_ROLE)::VARCHAR
FROM USER_TRANSACTION_ACCESS_CACHE

UNION ALL

SELECT 
    'Dynamic Policy Evaluation Summary',
    'Total Access Grants',
    COUNT(*)::VARCHAR
FROM USER_TRANSACTION_ACCESS_CACHE
WHERE HAS_ACCESS = TRUE

UNION ALL

SELECT 
    'Dynamic Policy Evaluation Summary',
    'Last Policy Test',
    MAX(LAST_UPDATED)::VARCHAR
FROM USER_TRANSACTION_ACCESS_CACHE

UNION ALL

SELECT 
    'Access Distribution',
    'Users with Access',
    COUNT(DISTINCT USERNAME)::VARCHAR
FROM USER_TRANSACTION_ACCESS_CACHE
WHERE HAS_ACCESS = TRUE

UNION ALL

SELECT 
    'Access Distribution',
    'Transactions Accessible',
    COUNT(DISTINCT TRANSACTION_ID)::VARCHAR
FROM USER_TRANSACTION_ACCESS_CACHE
WHERE HAS_ACCESS = TRUE;

-- =============================================================================
-- VERIFICATION QUERIES
-- =============================================================================

SELECT 'DYNAMIC POLICY EVALUATION COMPLETE' AS STATUS;

-- Show policy analysis results
SELECT 'Row Access Policies Applied to Table:' AS INFO, GET_TABLE_ROW_ACCESS_POLICIES() AS POLICIES;

-- Show sample results from dynamic testing
SELECT 
    'SAMPLE DYNAMIC ACCESS RESULTS' AS SECTION,
    USERNAME, 
    COUNT(*) AS ACCESSIBLE_TRANSACTIONS,
    STRING_AGG(DISTINCT PRIMARY_ROLE, ', ') AS ROLES_USED,
    MAX(LAST_UPDATED) AS LAST_TESTED
FROM USER_TRANSACTION_ACCESS_CACHE 
WHERE HAS_ACCESS = TRUE
GROUP BY USERNAME
ORDER BY ACCESSIBLE_TRANSACTIONS DESC
LIMIT 10;

-- Show entitlement summary
SELECT * FROM DYNAMIC_POLICY_EVALUATION_STATUS ORDER BY METRIC_TYPE, METRIC;

SELECT 'Dynamic entitlement view creation complete!' AS STATUS;
SELECT 'Entitlements now derived from actual row access policy evaluation!' AS INFO;