-- =============================================================================
-- CORTEX SEARCH ENTITLEMENTS DEMO - Sample Users Creation
-- =============================================================================
-- This script creates sample users and assigns them to the roles created in the previous step
-- Run this script after completing 03_create_roles_and_access_policies.sql

-- Set context
USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- STEP 1: CREATE SAMPLE USERS
-- =============================================================================
-- Create users representing different personas in the organization

-- Executive Users
CREATE USER IF NOT EXISTS ceo_jane_smith
  PASSWORD = 'DemoPassword123!'
  DEFAULT_ROLE = 'EXEC_GLOBAL_ACCESS'
  DEFAULT_WAREHOUSE = 'ENTITLEMENTS_WH'
  COMMENT = 'CEO with global access to all transaction data';

CREATE USER IF NOT EXISTS cfo_michael_johnson
  PASSWORD = 'DemoPassword123!'
  DEFAULT_ROLE = 'EXEC_GLOBAL_ACCESS'  
  DEFAULT_WAREHOUSE = 'ENTITLEMENTS_WH'
  COMMENT = 'CFO with global financial oversight access';

-- Regional Managers
CREATE USER IF NOT EXISTS mgr_sarah_davis_us_east
  PASSWORD = 'DemoPassword123!'
  DEFAULT_ROLE = 'MANAGER_US_EAST'
  DEFAULT_WAREHOUSE = 'ENTITLEMENTS_WH'
  COMMENT = 'Regional manager for US East operations';

CREATE USER IF NOT EXISTS mgr_robert_wilson_us_west  
  PASSWORD = 'DemoPassword123!'
  DEFAULT_ROLE = 'MANAGER_US_WEST'
  DEFAULT_WAREHOUSE = 'ENTITLEMENTS_WH'
  COMMENT = 'Regional manager for US West operations';

CREATE USER IF NOT EXISTS mgr_emma_brown_europe
  PASSWORD = 'DemoPassword123!'
  DEFAULT_ROLE = 'MANAGER_EUROPE'
  DEFAULT_WAREHOUSE = 'ENTITLEMENTS_WH'
  COMMENT = 'Regional manager for Europe operations';

CREATE USER IF NOT EXISTS mgr_david_lee_asia_pac
  PASSWORD = 'DemoPassword123!'
  DEFAULT_ROLE = 'MANAGER_ASIA_PAC'
  DEFAULT_WAREHOUSE = 'ENTITLEMENTS_WH'  
  COMMENT = 'Regional manager for Asia Pacific operations';

-- Department Heads and Staff
CREATE USER IF NOT EXISTS finance_director_lisa_martinez
  PASSWORD = 'DemoPassword123!'
  DEFAULT_ROLE = 'DEPT_FINANCE'
  DEFAULT_WAREHOUSE = 'ENTITLEMENTS_WH'
  COMMENT = 'Finance director with access to financial transaction data';

CREATE USER IF NOT EXISTS fraud_analyst_james_taylor
  PASSWORD = 'DemoPassword123!'
  DEFAULT_ROLE = 'DEPT_FRAUD'
  DEFAULT_WAREHOUSE = 'ENTITLEMENTS_WH'
  COMMENT = 'Senior fraud analyst with access to high-risk transactions';

CREATE USER IF NOT EXISTS compliance_officer_maria_garcia  
  PASSWORD = 'DemoPassword123!'
  DEFAULT_ROLE = 'DEPT_COMPLIANCE'
  DEFAULT_WAREHOUSE = 'ENTITLEMENTS_WH'
  COMMENT = 'Compliance officer with regulatory oversight access';

CREATE USER IF NOT EXISTS ops_manager_kevin_anderson
  PASSWORD = 'DemoPassword123!'
  DEFAULT_ROLE = 'DEPT_OPERATIONS'
  DEFAULT_WAREHOUSE = 'ENTITLEMENTS_WH'
  COMMENT = 'Operations manager with access to operational transaction data';

CREATE USER IF NOT EXISTS customer_service_lead_amy_thomas
  PASSWORD = 'DemoPassword123!'
  DEFAULT_ROLE = 'DEPT_CUSTOMER_SERVICE'
  DEFAULT_WAREHOUSE = 'ENTITLEMENTS_WH'
  COMMENT = 'Customer service lead with customer transaction access';

-- Customer Tier Specialists
CREATE USER IF NOT EXISTS premium_specialist_john_clark
  PASSWORD = 'DemoPassword123!'
  DEFAULT_ROLE = 'SPECIALIST_PREMIUM'
  DEFAULT_WAREHOUSE = 'ENTITLEMENTS_WH'
  COMMENT = 'Premium customer specialist';

CREATE USER IF NOT EXISTS gold_specialist_jennifer_white
  PASSWORD = 'DemoPassword123!'
  DEFAULT_ROLE = 'SPECIALIST_GOLD'
  DEFAULT_WAREHOUSE = 'ENTITLEMENTS_WH'
  COMMENT = 'Gold customer specialist';

-- Risk Analysts
CREATE USER IF NOT EXISTS risk_analyst_daniel_moore
  PASSWORD = 'DemoPassword123!'
  DEFAULT_ROLE = 'ANALYST_HIGH_RISK'
  DEFAULT_WAREHOUSE = 'ENTITLEMENTS_WH'
  COMMENT = 'Senior risk analyst focused on high-risk transactions';

CREATE USER IF NOT EXISTS junior_analyst_rachel_miller
  PASSWORD = 'DemoPassword123!'
  DEFAULT_ROLE = 'ANALYST_LOW_RISK'
  DEFAULT_WAREHOUSE = 'ENTITLEMENTS_WH'
  COMMENT = 'Junior analyst with access to low-risk transactions only';

-- External Partners
CREATE USER IF NOT EXISTS partner_vendor_alex_jones
  PASSWORD = 'DemoPassword123!'
  DEFAULT_ROLE = 'PARTNER_LIMITED'
  DEFAULT_WAREHOUSE = 'ENTITLEMENTS_WH'
  COMMENT = 'External partner with limited access to public data';

-- =============================================================================
-- STEP 2: GRANT ROLES TO USERS
-- =============================================================================
-- Grant appropriate roles to users (some users may have multiple roles)

-- Executive Users - Full Access
GRANT ROLE EXEC_GLOBAL_ACCESS TO USER ceo_jane_smith;
GRANT ROLE EXEC_GLOBAL_ACCESS TO USER cfo_michael_johnson;

-- Regional Managers - Region-Specific Access
GRANT ROLE MANAGER_US_EAST TO USER mgr_sarah_davis_us_east;
GRANT ROLE MANAGER_US_WEST TO USER mgr_robert_wilson_us_west;
GRANT ROLE MANAGER_EUROPE TO USER mgr_emma_brown_europe;
GRANT ROLE MANAGER_ASIA_PAC TO USER mgr_david_lee_asia_pac;

-- Department Staff - Department-Specific Access
GRANT ROLE DEPT_FINANCE TO USER finance_director_lisa_martinez;
GRANT ROLE DEPT_FRAUD TO USER fraud_analyst_james_taylor;
GRANT ROLE DEPT_COMPLIANCE TO USER compliance_officer_maria_garcia;
GRANT ROLE DEPT_OPERATIONS TO USER ops_manager_kevin_anderson;
GRANT ROLE DEPT_CUSTOMER_SERVICE TO USER customer_service_lead_amy_thomas;

-- Specialists - Tier-Specific Access
GRANT ROLE SPECIALIST_PREMIUM TO USER premium_specialist_john_clark;
GRANT ROLE SPECIALIST_GOLD TO USER gold_specialist_jennifer_white;

-- Risk Analysts - Risk-Level Access
GRANT ROLE ANALYST_HIGH_RISK TO USER risk_analyst_daniel_moore;
GRANT ROLE ANALYST_LOW_RISK TO USER junior_analyst_rachel_miller;

-- External Partners - Limited Access
GRANT ROLE PARTNER_LIMITED TO USER partner_vendor_alex_jones;

-- =============================================================================
-- STEP 3: GRANT ADDITIONAL CROSS-FUNCTIONAL ROLES (Optional)
-- =============================================================================
-- Some users might need access to multiple roles for their job functions

-- Finance director also needs compliance oversight
GRANT ROLE DEPT_COMPLIANCE TO USER finance_director_lisa_martinez;

-- Regional managers also get operations role for their regions
GRANT ROLE DEPT_OPERATIONS TO USER mgr_sarah_davis_us_east;
GRANT ROLE DEPT_OPERATIONS TO USER mgr_robert_wilson_us_west;
GRANT ROLE DEPT_OPERATIONS TO USER mgr_emma_brown_europe;
GRANT ROLE DEPT_OPERATIONS TO USER mgr_david_lee_asia_pac;

-- Senior fraud analyst also gets high-risk analysis role
GRANT ROLE ANALYST_HIGH_RISK TO USER fraud_analyst_james_taylor;

-- Premium specialist also gets access to compliance role for premium customers
GRANT ROLE DEPT_COMPLIANCE TO USER premium_specialist_john_clark;

-- =============================================================================
-- STEP 4: CREATE USER MAPPING TABLE FOR ENTITLEMENTS
-- =============================================================================
-- Create a table that maps users to their access characteristics
-- This will be used later for creating the entitlement view

CREATE OR REPLACE TABLE USER_ACCESS_MAPPING (
    USERNAME VARCHAR(100) PRIMARY KEY,
    FULL_NAME VARCHAR(200),
    EMAIL VARCHAR(200),
    DEPARTMENT VARCHAR(50),
    ROLE_TYPE VARCHAR(50),
    ACCESS_LEVEL VARCHAR(20),
    ALLOWED_REGIONS ARRAY,
    ALLOWED_CUSTOMER_TIERS ARRAY,  
    ALLOWED_SENSITIVITY_LEVELS ARRAY,
    ALLOWED_RISK_LEVELS ARRAY,
    ALLOWED_DEPARTMENTS ARRAY,
    MAX_AMOUNT_ACCESS NUMBER,
    IS_EXECUTIVE BOOLEAN DEFAULT FALSE,
    IS_MANAGER BOOLEAN DEFAULT FALSE,
    IS_EXTERNAL BOOLEAN DEFAULT FALSE,
    CREATED_DATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert user access mapping data
INSERT INTO USER_ACCESS_MAPPING VALUES
-- Executives (Global Access)
('ceo_jane_smith', 'Jane Smith', 'jane.smith@company.com', 'EXECUTIVE', 'CEO', 'GLOBAL', 
 ['US_EAST', 'US_WEST', 'EUROPE', 'ASIA_PAC'], ['PREMIUM', 'GOLD', 'SILVER', 'STANDARD'], 
 ['RESTRICTED', 'CONFIDENTIAL', 'INTERNAL', 'PUBLIC'], ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW'], 
 ['FINANCE', 'FRAUD', 'COMPLIANCE', 'OPERATIONS', 'CUSTOMER_SERVICE'], NULL, TRUE, TRUE, FALSE, CURRENT_TIMESTAMP()),

('cfo_michael_johnson', 'Michael Johnson', 'michael.johnson@company.com', 'FINANCE', 'CFO', 'GLOBAL',
 ['US_EAST', 'US_WEST', 'EUROPE', 'ASIA_PAC'], ['PREMIUM', 'GOLD', 'SILVER', 'STANDARD'],
 ['RESTRICTED', 'CONFIDENTIAL', 'INTERNAL', 'PUBLIC'], ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW'],
 ['FINANCE', 'FRAUD', 'COMPLIANCE', 'OPERATIONS', 'CUSTOMER_SERVICE'], NULL, TRUE, TRUE, FALSE, CURRENT_TIMESTAMP()),

-- Regional Managers
('mgr_sarah_davis_us_east', 'Sarah Davis', 'sarah.davis@company.com', 'OPERATIONS', 'REGIONAL_MANAGER', 'REGIONAL',
 ['US_EAST'], ['PREMIUM', 'GOLD', 'SILVER', 'STANDARD'], ['INTERNAL', 'PUBLIC'], ['HIGH', 'MEDIUM', 'LOW'],
 ['OPERATIONS', 'CUSTOMER_SERVICE'], 50000, FALSE, TRUE, FALSE, CURRENT_TIMESTAMP()),

('mgr_robert_wilson_us_west', 'Robert Wilson', 'robert.wilson@company.com', 'OPERATIONS', 'REGIONAL_MANAGER', 'REGIONAL',
 ['US_WEST'], ['PREMIUM', 'GOLD', 'SILVER', 'STANDARD'], ['INTERNAL', 'PUBLIC'], ['HIGH', 'MEDIUM', 'LOW'],
 ['OPERATIONS', 'CUSTOMER_SERVICE'], 50000, FALSE, TRUE, FALSE, CURRENT_TIMESTAMP()),

('mgr_emma_brown_europe', 'Emma Brown', 'emma.brown@company.com', 'OPERATIONS', 'REGIONAL_MANAGER', 'REGIONAL',
 ['EUROPE'], ['PREMIUM', 'GOLD', 'SILVER', 'STANDARD'], ['INTERNAL', 'PUBLIC'], ['HIGH', 'MEDIUM', 'LOW'],
 ['OPERATIONS', 'CUSTOMER_SERVICE'], 50000, FALSE, TRUE, FALSE, CURRENT_TIMESTAMP()),

('mgr_david_lee_asia_pac', 'David Lee', 'david.lee@company.com', 'OPERATIONS', 'REGIONAL_MANAGER', 'REGIONAL',
 ['ASIA_PAC'], ['PREMIUM', 'GOLD', 'SILVER', 'STANDARD'], ['INTERNAL', 'PUBLIC'], ['HIGH', 'MEDIUM', 'LOW'],
 ['OPERATIONS', 'CUSTOMER_SERVICE'], 50000, FALSE, TRUE, FALSE, CURRENT_TIMESTAMP()),

-- Department Staff
('finance_director_lisa_martinez', 'Lisa Martinez', 'lisa.martinez@company.com', 'FINANCE', 'DIRECTOR', 'DEPARTMENTAL',
 ['US_EAST', 'US_WEST', 'EUROPE', 'ASIA_PAC'], ['PREMIUM', 'GOLD'], ['CONFIDENTIAL', 'INTERNAL', 'PUBLIC'], ['HIGH', 'MEDIUM', 'LOW'],
 ['FINANCE', 'COMPLIANCE'], NULL, FALSE, TRUE, FALSE, CURRENT_TIMESTAMP()),

('fraud_analyst_james_taylor', 'James Taylor', 'james.taylor@company.com', 'FRAUD', 'SENIOR_ANALYST', 'SPECIALIZED',
 ['US_EAST', 'US_WEST', 'EUROPE', 'ASIA_PAC'], ['PREMIUM', 'GOLD', 'SILVER', 'STANDARD'], ['RESTRICTED', 'CONFIDENTIAL', 'INTERNAL', 'PUBLIC'], ['CRITICAL', 'HIGH'],
 ['FRAUD'], NULL, FALSE, FALSE, FALSE, CURRENT_TIMESTAMP()),

('compliance_officer_maria_garcia', 'Maria Garcia', 'maria.garcia@company.com', 'COMPLIANCE', 'OFFICER', 'SPECIALIZED',
 ['US_EAST', 'US_WEST', 'EUROPE', 'ASIA_PAC'], ['PREMIUM', 'GOLD', 'SILVER', 'STANDARD'], ['CONFIDENTIAL', 'INTERNAL', 'PUBLIC'], ['CRITICAL', 'HIGH', 'MEDIUM'],
 ['COMPLIANCE', 'FRAUD'], 10000, FALSE, FALSE, FALSE, CURRENT_TIMESTAMP()),

('ops_manager_kevin_anderson', 'Kevin Anderson', 'kevin.anderson@company.com', 'OPERATIONS', 'MANAGER', 'DEPARTMENTAL',
 ['US_EAST', 'US_WEST'], ['PREMIUM', 'GOLD', 'SILVER', 'STANDARD'], ['INTERNAL', 'PUBLIC'], ['MEDIUM', 'LOW'],
 ['OPERATIONS', 'CUSTOMER_SERVICE'], 25000, FALSE, TRUE, FALSE, CURRENT_TIMESTAMP()),

('customer_service_lead_amy_thomas', 'Amy Thomas', 'amy.thomas@company.com', 'CUSTOMER_SERVICE', 'LEAD', 'DEPARTMENTAL',
 ['US_EAST', 'US_WEST', 'EUROPE', 'ASIA_PAC'], ['PREMIUM', 'GOLD', 'SILVER', 'STANDARD'], ['INTERNAL', 'PUBLIC'], ['MEDIUM', 'LOW'],
 ['CUSTOMER_SERVICE'], 15000, FALSE, FALSE, FALSE, CURRENT_TIMESTAMP()),

-- Specialists
('premium_specialist_john_clark', 'John Clark', 'john.clark@company.com', 'CUSTOMER_SERVICE', 'SPECIALIST', 'SPECIALIZED',
 ['US_EAST', 'US_WEST', 'EUROPE', 'ASIA_PAC'], ['PREMIUM'], ['CONFIDENTIAL', 'INTERNAL', 'PUBLIC'], ['HIGH', 'MEDIUM', 'LOW'],
 ['CUSTOMER_SERVICE', 'COMPLIANCE'], NULL, FALSE, FALSE, FALSE, CURRENT_TIMESTAMP()),

('gold_specialist_jennifer_white', 'Jennifer White', 'jennifer.white@company.com', 'CUSTOMER_SERVICE', 'SPECIALIST', 'SPECIALIZED',
 ['US_EAST', 'US_WEST', 'EUROPE', 'ASIA_PAC'], ['GOLD'], ['INTERNAL', 'PUBLIC'], ['MEDIUM', 'LOW'],
 ['CUSTOMER_SERVICE'], 75000, FALSE, FALSE, FALSE, CURRENT_TIMESTAMP()),

-- Risk Analysts
('risk_analyst_daniel_moore', 'Daniel Moore', 'daniel.moore@company.com', 'FRAUD', 'SENIOR_ANALYST', 'SPECIALIZED',
 ['US_EAST', 'US_WEST', 'EUROPE', 'ASIA_PAC'], ['PREMIUM', 'GOLD', 'SILVER', 'STANDARD'], ['RESTRICTED', 'CONFIDENTIAL', 'INTERNAL'], ['CRITICAL', 'HIGH'],
 ['FRAUD'], NULL, FALSE, FALSE, FALSE, CURRENT_TIMESTAMP()),

('junior_analyst_rachel_miller', 'Rachel Miller', 'rachel.miller@company.com', 'OPERATIONS', 'JUNIOR_ANALYST', 'LIMITED',
 ['US_EAST', 'US_WEST'], ['SILVER', 'STANDARD'], ['PUBLIC'], ['LOW'],
 ['OPERATIONS'], 5000, FALSE, FALSE, FALSE, CURRENT_TIMESTAMP()),

-- External Partners
('partner_vendor_alex_jones', 'Alex Jones', 'alex.jones@vendorpartner.com', 'EXTERNAL', 'PARTNER', 'LIMITED',
 ['US_EAST', 'US_WEST'], ['STANDARD'], ['PUBLIC'], ['LOW'],
 ['OPERATIONS', 'CUSTOMER_SERVICE'], 1000, FALSE, FALSE, TRUE, CURRENT_TIMESTAMP());

-- =============================================================================
-- STEP 5: VERIFICATION QUERIES
-- =============================================================================
-- Show created users
SELECT 'USER CREATION SUMMARY' AS SECTION, NULL AS USERNAME, NULL AS ROLES_COUNT;
SELECT '', NAME AS USERNAME, NULL AS ROLES_COUNT
FROM TABLE(INFORMATION_SCHEMA.APPLICABLE_ROLES) 
WHERE GRANTEE_TYPE = 'USER' 
  AND GRANTEE_NAME LIKE '%smith%' 
   OR GRANTEE_NAME LIKE '%johnson%'
   OR GRANTEE_NAME LIKE '%davis%'
   OR GRANTEE_NAME LIKE '%wilson%'
   OR GRANTEE_NAME LIKE '%brown%'
   OR GRANTEE_NAME LIKE '%lee%'
ORDER BY USERNAME;

-- Show user access mapping summary
SELECT 
    'USER ACCESS MAPPING SUMMARY' AS SECTION,
    COUNT(*) AS TOTAL_USERS,
    COUNT(CASE WHEN IS_EXECUTIVE THEN 1 END) AS EXECUTIVES,
    COUNT(CASE WHEN IS_MANAGER THEN 1 END) AS MANAGERS,
    COUNT(CASE WHEN IS_EXTERNAL THEN 1 END) AS EXTERNAL_USERS
FROM USER_ACCESS_MAPPING;

-- Show access levels distribution
SELECT 
    ACCESS_LEVEL,
    COUNT(*) AS USER_COUNT,
    STRING_AGG(USERNAME, ', ') AS USERS
FROM USER_ACCESS_MAPPING
GROUP BY ACCESS_LEVEL
ORDER BY ACCESS_LEVEL;

SELECT 'Sample users and access mapping complete!' AS STATUS;
