author: Michael Baron
id: cortex_search_entitlements_demo
summary: A comprehensive demonstration of implementing fine-grained access control with Snowflake Cortex Search using credit card transaction data
categories: cortex-search,security,data-governance,rbac
environments: web
status: Published
feedback link: https://github.com/sfc-gh-mbaron/mb_demos/issues
tags: cortex-search, entitlements, rbac, access-control, data-governance, search, credit-card-transactions

# Cortex Search Fine-Grained Entitlements Demo
<!-- ------------------------ -->

## Overview
Duration: 5

This comprehensive demo showcases how to implement sophisticated fine-grained access control with Snowflake Cortex Search using realistic credit card transaction data. You'll learn to build an entitlements system where search results are dynamically filtered based on user roles, permissions, and organizational context.

### What You'll Learn
- How to implement row-level security with multiple access dimensions
- Creating user entitlement arrays for search filtering  
- Building Cortex Search services with embedded access control
- Designing role-based access control (RBAC) for search applications
- Demonstrating real-world entitlement scenarios

### What You'll Build
- A complete entitlements system with 5,000+ sample credit card transactions
- 15+ organizational roles with different access levels
- Sophisticated row access policies across multiple dimensions
- Cortex Search services with user-based filtering
- Production-ready search functions with embedded entitlements

### Prerequisites
- Snowflake account with Cortex Search enabled
- ACCOUNTADMIN privileges for setup
- Basic understanding of Snowflake SQL and RBAC concepts
- Python 3.8+ (for data generation)

### What You'll Need
- 15-20 minutes for complete setup
- 10-15 minutes for demo walkthrough
- Web browser for Snowsight access

<!-- ------------------------ -->

## Generate Sample Dataset
Duration: 5

We'll start by creating realistic credit card transaction data with rich attributes for demonstrating fine-grained entitlements.

### Step 1: Setup Python Environment

First, set up a Python virtual environment and install dependencies:

```bash
cd cortex-search-entitlements-demo
python3 -m venv venv
source venv/bin/activate
pip install pandas numpy
```

### Step 2: Generate Transaction Data

Run the data generation script to create 5,000 sample transactions:

```bash
python generate_sample_data.py
```

This creates:
- **credit_card_transactions.csv** - 5,000 realistic transactions with multiple entitlement dimensions
- **dataset_metadata.json** - Comprehensive dataset documentation

### Data Attributes for Entitlements

The generated dataset includes multiple dimensions for access control:

| Dimension | Values | Purpose |
|-----------|--------|---------|
| **Geographic** | US_EAST, US_WEST, EUROPE, ASIA_PAC | Regional access control |
| **Customer Tiers** | PREMIUM, GOLD, SILVER, STANDARD | Customer-based filtering |
| **Sensitivity** | RESTRICTED, CONFIDENTIAL, INTERNAL, PUBLIC | Data classification |
| **Risk Levels** | CRITICAL, HIGH, MEDIUM, LOW | Risk-based access |
| **Departments** | FINANCE, FRAUD, COMPLIANCE, OPERATIONS, CUSTOMER_SERVICE | Department ownership |

> **Positive**
> The sample data is designed to be realistic and production-like, with proper distributions and relationships between attributes that mirror real credit card transaction patterns.

<!-- ------------------------ -->

## Setup Snowflake Environment
Duration: 3

Now we'll create the foundational database structure in Snowflake.

### Step 1: Create Database and Schema

Execute the database setup script in Snowsight:

```sql
-- File: 01_setup_database_and_table.sql
USE ROLE ACCOUNTADMIN;

-- Create database and schema for the demo
CREATE DATABASE IF NOT EXISTS CORTEX_SEARCH_ENTITLEMENTS_DB;
CREATE SCHEMA IF NOT EXISTS CORTEX_SEARCH_ENTITLEMENTS_DB.ENTITLEMENTS;

-- Use the demo database and schema
USE DATABASE CORTEX_SEARCH_ENTITLEMENTS_DB;
USE SCHEMA ENTITLEMENTS;

-- Create warehouse for processing
CREATE WAREHOUSE IF NOT EXISTS ENTITLEMENTS_WH
  WITH WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE;

USE WAREHOUSE ENTITLEMENTS_WH;
```

### Step 2: Create Transactions Table

The script creates a comprehensive table with all necessary attributes:

```sql
CREATE OR REPLACE TABLE CREDIT_CARD_TRANSACTIONS (
    -- Transaction Identifiers
    TRANSACTION_ID VARCHAR(50) PRIMARY KEY,
    CUSTOMER_ID VARCHAR(50) NOT NULL,
    MERCHANT_ID VARCHAR(50) NOT NULL,
    
    -- Transaction Details
    TRANSACTION_DATE TIMESTAMP_NTZ NOT NULL,
    AMOUNT DECIMAL(10,2) NOT NULL,
    CURRENCY VARCHAR(3) DEFAULT 'USD',
    
    -- Access Control Attributes (Critical for Entitlements)
    REGION_CODE VARCHAR(20) NOT NULL,
    CUSTOMER_TIER VARCHAR(20) NOT NULL,
    PRIMARY_DEPARTMENT VARCHAR(50) NOT NULL,
    SENSITIVITY_LEVEL VARCHAR(20) NOT NULL,
    RISK_LEVEL VARCHAR(20) NOT NULL,
    
    -- Additional transaction attributes...
);
```

> **Negative**
> Make sure to run the complete script `01_setup_database_and_table.sql` which includes all necessary table columns, file formats, and stages for data loading.

<!-- ------------------------ -->

## Load Transaction Data
Duration: 2

Load the generated CSV data into your Snowflake table.

### Step 1: Upload CSV to Stage

In Snowsight, navigate to **Data** > **Databases** > **CORTEX_SEARCH_ENTITLEMENTS_DB** > **ENTITLEMENTS** > **Stages** > **DEMO_STAGE** and upload `credit_card_transactions.csv`.

Alternatively, use SnowSQL:
```bash
PUT file://credit_card_transactions.csv @DEMO_STAGE;
```

### Step 2: Load Data

Execute the data loading script:

```sql
-- File: 02_load_data.sql
COPY INTO CREDIT_CARD_TRANSACTIONS
FROM @DEMO_STAGE/credit_card_transactions.csv
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
ON_ERROR = 'SKIP_FILE';
```

### Step 3: Verify Data Loading

Check that all 5,000 records loaded successfully:

```sql
SELECT COUNT(*) AS TOTAL_RECORDS FROM CREDIT_CARD_TRANSACTIONS;
-- Should return: 5000

-- View sample data with entitlement attributes
SELECT 
    TRANSACTION_ID,
    AMOUNT,
    REGION_CODE,
    CUSTOMER_TIER,
    SENSITIVITY_LEVEL,
    RISK_LEVEL,
    PRIMARY_DEPARTMENT
FROM CREDIT_CARD_TRANSACTIONS 
LIMIT 10;
```

<!-- ------------------------ -->

## Create Roles and Access Policies
Duration: 4

Build a sophisticated role-based access control system with row access policies.

### Step 1: Create Organizational Roles

The demo creates 15+ roles representing different organizational functions:

```sql
-- File: 03_create_roles_and_access_policies.sql

-- Executive Level - Full Access
CREATE ROLE IF NOT EXISTS EXEC_GLOBAL_ACCESS;

-- Regional Management Roles
CREATE ROLE IF NOT EXISTS MANAGER_US_EAST;
CREATE ROLE IF NOT EXISTS MANAGER_US_WEST;
CREATE ROLE IF NOT EXISTS MANAGER_EUROPE;
CREATE ROLE IF NOT EXISTS MANAGER_ASIA_PAC;

-- Department-Specific Roles
CREATE ROLE IF NOT EXISTS DEPT_FINANCE;
CREATE ROLE IF NOT EXISTS DEPT_FRAUD;
CREATE ROLE IF NOT EXISTS DEPT_COMPLIANCE;
CREATE ROLE IF NOT EXISTS DEPT_OPERATIONS;
CREATE ROLE IF NOT EXISTS DEPT_CUSTOMER_SERVICE;

-- Specialist Roles
CREATE ROLE IF NOT EXISTS SPECIALIST_PREMIUM;
CREATE ROLE IF NOT EXISTS SPECIALIST_GOLD;

-- Risk Analysis Roles
CREATE ROLE IF NOT EXISTS ANALYST_HIGH_RISK;
CREATE ROLE IF NOT EXISTS ANALYST_LOW_RISK;

-- External Partner Role
CREATE ROLE IF NOT EXISTS PARTNER_LIMITED;
```

### Step 2: Create Row Access Policies

Implement three complementary access policies:

**1. Regional Access Policy**
```sql
CREATE OR REPLACE ROW ACCESS POLICY REGIONAL_ACCESS_POLICY 
AS (REGION_CODE VARCHAR) RETURNS BOOLEAN ->
  CASE
    WHEN CURRENT_ROLE() = 'EXEC_GLOBAL_ACCESS' THEN TRUE
    WHEN CURRENT_ROLE() = 'MANAGER_US_EAST' AND REGION_CODE = 'US_EAST' THEN TRUE
    WHEN CURRENT_ROLE() = 'MANAGER_US_WEST' AND REGION_CODE = 'US_WEST' THEN TRUE
    -- Additional regional logic...
    ELSE FALSE
  END;
```

**2. Sensitivity Access Policy**
```sql
CREATE OR REPLACE ROW ACCESS POLICY SENSITIVITY_ACCESS_POLICY 
AS (SENSITIVITY_LEVEL VARCHAR, CUSTOMER_TIER VARCHAR, RISK_LEVEL VARCHAR) 
RETURNS BOOLEAN ->
  CASE
    WHEN CURRENT_ROLE() = 'EXEC_GLOBAL_ACCESS' THEN TRUE
    WHEN CURRENT_ROLE() = 'DEPT_FRAUD' 
         AND SENSITIVITY_LEVEL IN ('RESTRICTED', 'CONFIDENTIAL', 'INTERNAL', 'PUBLIC') THEN TRUE
    WHEN CURRENT_ROLE() = 'PARTNER_LIMITED' 
         AND SENSITIVITY_LEVEL = 'PUBLIC' THEN TRUE
    -- Additional sensitivity logic...
    ELSE FALSE
  END;
```

### Step 3: Apply Policies to Table

```sql
-- Apply all three policies (they work together with AND logic)
ALTER TABLE CREDIT_CARD_TRANSACTIONS 
ADD ROW ACCESS POLICY REGIONAL_ACCESS_POLICY ON (REGION_CODE);

ALTER TABLE CREDIT_CARD_TRANSACTIONS 
ADD ROW ACCESS POLICY SENSITIVITY_ACCESS_POLICY ON (SENSITIVITY_LEVEL, CUSTOMER_TIER, RISK_LEVEL);

ALTER TABLE CREDIT_CARD_TRANSACTIONS 
ADD ROW ACCESS POLICY DEPARTMENT_ACCESS_POLICY ON (PRIMARY_DEPARTMENT, RISK_LEVEL, AMOUNT);
```

> **Positive**
> Row access policies in Snowflake are applied at query time, providing real-time access control without performance overhead or data duplication.

<!-- ------------------------ -->

## Create Sample Users
Duration: 2

Create realistic user personas representing different organizational roles.

### Sample User Creation

The demo includes 16 users with different access patterns:

```sql
-- File: 04_create_sample_users.sql

-- Executive Users (Global Access)
CREATE USER IF NOT EXISTS ceo_jane_smith
  PASSWORD = 'DemoPassword123!'
  DEFAULT_ROLE = 'EXEC_GLOBAL_ACCESS'
  DEFAULT_WAREHOUSE = 'ENTITLEMENTS_WH';

-- Regional Managers (Regional Access)
CREATE USER IF NOT EXISTS mgr_sarah_davis_us_east
  PASSWORD = 'DemoPassword123!'
  DEFAULT_ROLE = 'MANAGER_US_EAST'
  DEFAULT_WAREHOUSE = 'ENTITLEMENTS_WH';

-- Department Staff (Department Access)
CREATE USER IF NOT EXISTS fraud_analyst_james_taylor
  PASSWORD = 'DemoPassword123!'
  DEFAULT_ROLE = 'DEPT_FRAUD'
  DEFAULT_WAREHOUSE = 'ENTITLEMENTS_WH';

-- External Partners (Limited Access)
CREATE USER IF NOT EXISTS partner_vendor_alex_jones
  PASSWORD = 'DemoPassword123!'
  DEFAULT_ROLE = 'PARTNER_LIMITED'
  DEFAULT_WAREHOUSE = 'ENTITLEMENTS_WH';
```

### User Access Mapping

Create a mapping table that defines each user's access characteristics:

```sql
CREATE OR REPLACE TABLE USER_ACCESS_MAPPING (
    USERNAME VARCHAR(100) PRIMARY KEY,
    FULL_NAME VARCHAR(200),
    DEPARTMENT VARCHAR(50),
    ACCESS_LEVEL VARCHAR(20),
    ALLOWED_REGIONS ARRAY,
    ALLOWED_CUSTOMER_TIERS ARRAY,
    ALLOWED_SENSITIVITY_LEVELS ARRAY,
    ALLOWED_RISK_LEVELS ARRAY,
    MAX_AMOUNT_ACCESS NUMBER,
    IS_EXECUTIVE BOOLEAN DEFAULT FALSE,
    IS_EXTERNAL BOOLEAN DEFAULT FALSE
);
```

This table drives the entitlement calculations in the next step.

<!-- ------------------------ -->

## Build Entitlement Views
Duration: 4

Create views that calculate user access permissions and generate user arrays for each transaction.

### Step 1: Create Access Calculation Functions

Build helper functions for complex access logic:

```sql
-- File: 05_create_entitlement_view.sql

-- Function to check region access
CREATE OR REPLACE FUNCTION CHECK_REGION_ACCESS(
    USER_REGIONS ARRAY,
    TRANSACTION_REGION VARCHAR
) RETURNS BOOLEAN
LANGUAGE SQL
AS $$ ARRAY_CONTAINS(TRANSACTION_REGION::VARIANT, USER_REGIONS) $$;

-- Function to check sensitivity access
CREATE OR REPLACE FUNCTION CHECK_SENSITIVITY_ACCESS(
    USER_SENSITIVITY_LEVELS ARRAY,
    TRANSACTION_SENSITIVITY VARCHAR
) RETURNS BOOLEAN  
LANGUAGE SQL
AS $$ ARRAY_CONTAINS(TRANSACTION_SENSITIVITY::VARIANT, USER_SENSITIVITY_LEVELS) $$;
```

### Step 2: Create User-Transaction Access View

Calculate access for each user-transaction combination:

```sql
CREATE OR REPLACE VIEW TRANSACTION_USER_ACCESS AS
SELECT 
    t.TRANSACTION_ID,
    u.USERNAME,
    u.ACCESS_LEVEL,
    
    -- Individual access checks
    CHECK_REGION_ACCESS(u.ALLOWED_REGIONS, t.REGION_CODE) AS HAS_REGION_ACCESS,
    CHECK_SENSITIVITY_ACCESS(u.ALLOWED_SENSITIVITY_LEVELS, t.SENSITIVITY_LEVEL) AS HAS_SENSITIVITY_ACCESS,
    
    -- Overall access (all conditions must be true)
    (CHECK_REGION_ACCESS(u.ALLOWED_REGIONS, t.REGION_CODE) AND
     CHECK_SENSITIVITY_ACCESS(u.ALLOWED_SENSITIVITY_LEVELS, t.SENSITIVITY_LEVEL) AND
     CHECK_DEPARTMENT_ACCESS(u.ALLOWED_DEPARTMENTS, t.PRIMARY_DEPARTMENT) AND
     CHECK_AMOUNT_ACCESS(u.MAX_AMOUNT_ACCESS, t.AMOUNT)) AS HAS_ACCESS
     
FROM CREDIT_CARD_TRANSACTIONS t
CROSS JOIN USER_ACCESS_MAPPING u;
```

### Step 3: Create Main Entitlement View

Generate the final view with user arrays:

```sql
CREATE OR REPLACE VIEW TRANSACTIONS_WITH_ENTITLEMENTS AS
SELECT 
    t.*,  -- All original transaction fields
    
    -- User arrays for Cortex Search filtering
    access_summary.AUTHORIZED_USERS,
    access_summary.AUTHORIZED_USER_COUNT,
    access_summary.EXECUTIVE_USERS,
    access_summary.MANAGER_USERS,
    access_summary.EXTERNAL_USERS
    
FROM CREDIT_CARD_TRANSACTIONS t
LEFT JOIN (
    SELECT 
        TRANSACTION_ID,
        ARRAY_AGG(USERNAME) AS AUTHORIZED_USERS,
        COUNT(USERNAME) AS AUTHORIZED_USER_COUNT,
        ARRAY_AGG(CASE WHEN IS_EXECUTIVE THEN USERNAME END) AS EXECUTIVE_USERS,
        ARRAY_AGG(CASE WHEN IS_EXTERNAL THEN USERNAME END) AS EXTERNAL_USERS
    FROM TRANSACTION_USER_ACCESS
    WHERE HAS_ACCESS = TRUE
    GROUP BY TRANSACTION_ID
) access_summary ON t.TRANSACTION_ID = access_summary.TRANSACTION_ID;
```

> **Positive**
> This approach pre-calculates user access arrays, providing excellent performance for Cortex Search while maintaining flexibility for complex entitlement logic.

<!-- ------------------------ -->

## Create Cortex Search Indexes
Duration: 3

Build Cortex Search services with embedded entitlement filtering.

### Step 1: Create Main Search Service

```sql
-- File: 06_create_cortex_search_index.sql

CREATE OR REPLACE CORTEX SEARCH SERVICE TRANSACTION_SEARCH_SERVICE
ON SEARCH_CONTENT
WAREHOUSE = ENTITLEMENTS_WH
TARGET_LAG = '1 minute'
AS (
    SELECT 
        TRANSACTION_ID,
        
        -- Searchable content
        CONCAT(
            'Transaction ID: ', TRANSACTION_ID, '. ',
            'Merchant: ', MERCHANT_NAME, ' (', MERCHANT_CATEGORY, '). ',
            'Amount: $', AMOUNT, ' ', CURRENCY, '. ',
            'Customer Tier: ', CUSTOMER_TIER, '. ',
            'Risk Level: ', RISK_LEVEL
        ) AS SEARCH_CONTENT,
        
        -- Entitlement arrays for filtering (CRITICAL)
        AUTHORIZED_USERS,
        AUTHORIZED_USER_COUNT,
        EXECUTIVE_USERS,
        MANAGER_USERS,
        EXTERNAL_USERS,
        
        -- Transaction attributes for filtering
        AMOUNT, MERCHANT_NAME, REGION_CODE, CUSTOMER_TIER,
        RISK_LEVEL, SENSITIVITY_LEVEL, TRANSACTION_DATE
        
    FROM CORTEX_SEARCH_TRANSACTIONS
);
```

### Step 2: Create Specialized Search Services

Build targeted search services for specific use cases:

```sql
-- High-value transaction search
CREATE OR REPLACE CORTEX SEARCH SERVICE HIGH_VALUE_TRANSACTION_SEARCH
ON SEARCH_CONTENT
WAREHOUSE = ENTITLEMENTS_WH
TARGET_LAG = '1 minute'
AS (
    SELECT TRANSACTION_ID, SEARCH_CONTENT, AUTHORIZED_USERS, AMOUNT, CUSTOMER_TIER
    FROM CORTEX_SEARCH_TRANSACTIONS
    WHERE AMOUNT >= 1000
);

-- Fraud and risk search
CREATE OR REPLACE CORTEX SEARCH SERVICE FRAUD_RISK_TRANSACTION_SEARCH
ON SEARCH_CONTENT  
WAREHOUSE = ENTITLEMENTS_WH
TARGET_LAG = '1 minute'
AS (
    SELECT TRANSACTION_ID, SEARCH_CONTENT, AUTHORIZED_USERS, RISK_LEVEL, RISK_SCORE
    FROM CORTEX_SEARCH_TRANSACTIONS
    WHERE RISK_LEVEL IN ('HIGH', 'CRITICAL')
);
```

### Step 3: Grant Search Service Access

```sql
-- Grant access to search services based on roles
GRANT USAGE ON CORTEX SEARCH SERVICE TRANSACTION_SEARCH_SERVICE TO ROLE EXEC_GLOBAL_ACCESS;
GRANT USAGE ON CORTEX SEARCH SERVICE TRANSACTION_SEARCH_SERVICE TO ROLE MANAGER_US_EAST;
GRANT USAGE ON CORTEX SEARCH SERVICE TRANSACTION_SEARCH_SERVICE TO ROLE DEPT_FRAUD;
-- Continue for all relevant roles...

-- Specialized services get more restrictive grants
GRANT USAGE ON CORTEX SEARCH SERVICE HIGH_VALUE_TRANSACTION_SEARCH TO ROLE EXEC_GLOBAL_ACCESS;
GRANT USAGE ON CORTEX SEARCH SERVICE HIGH_VALUE_TRANSACTION_SEARCH TO ROLE DEPT_FINANCE;
GRANT USAGE ON CORTEX SEARCH SERVICE FRAUD_RISK_TRANSACTION_SEARCH TO ROLE DEPT_FRAUD;
```

> **Negative**  
> Wait for search services to complete their initial refresh before testing. Check status with `SHOW CORTEX SEARCH SERVICES` and ensure all services show `RUNNING` status.

<!-- ------------------------ -->

## Test User-Filtered Search
Duration: 4

Demonstrate how different users see different search results for the same queries.

### Step 1: Executive Search (Global Access)

CEO Jane Smith can see all transactions:

```sql
-- File: 07_sample_queries_and_demo.sql
-- Executive search sees all high-value transactions
SELECT 
    TRANSACTION_ID, RELEVANCE_SCORE, AMOUNT, MERCHANT_NAME, 
    REGION_CODE, CUSTOMER_TIER, RISK_LEVEL, AUTHORIZED_USERS
FROM TABLE(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        TRANSACTION_SEARCH_SERVICE,
        'high value premium customer transaction over $1000',
        20
    )
)
WHERE ARRAY_CONTAINS('ceo_jane_smith'::VARIANT, AUTHORIZED_USERS)
ORDER BY RELEVANCE_SCORE DESC;
```

### Step 2: Regional Manager Search (Regional Limited)

US East Manager only sees their region:

```sql
-- Regional manager search limited to US East
SELECT 
    TRANSACTION_ID, RELEVANCE_SCORE, AMOUNT, MERCHANT_NAME,
    REGION_CODE, CUSTOMER_TIER, AUTHORIZED_USERS
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
```

### Step 3: Fraud Analyst Search (Risk-Based)

Fraud analyst sees high-risk transactions:

```sql
-- Fraud analyst search focused on high-risk transactions
SELECT 
    TRANSACTION_ID, RELEVANCE_SCORE, AMOUNT, RISK_LEVEL,
    RISK_SCORE, MERCHANT_CATEGORY, AUTHORIZED_USERS
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
```

### Step 4: External Partner Search (Public Only)

External partner only sees public data:

```sql
-- External partner search limited to public data
SELECT 
    TRANSACTION_ID, RELEVANCE_SCORE, AMOUNT, MERCHANT_CATEGORY,
    REGION_CODE, SENSITIVITY_LEVEL, AUTHORIZED_USERS
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
```

### Step 5: Verify Different Results

Compare result counts to demonstrate entitlement filtering:

```sql
-- Show how different users see different result counts for the same query
WITH search_results AS (
    SELECT TRANSACTION_ID, AUTHORIZED_USERS, SENSITIVITY_LEVEL, REGION_CODE
    FROM TABLE(
        SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
            TRANSACTION_SEARCH_SERVICE, 'high value transaction', 100
        )
    )
)
SELECT 
    'Executive (Global)' AS USER_TYPE,
    COUNT(*) AS VISIBLE_RESULTS
FROM search_results 
WHERE ARRAY_CONTAINS('ceo_jane_smith'::VARIANT, AUTHORIZED_USERS)

UNION ALL

SELECT 
    'Regional Manager (US East)',
    COUNT(*)
FROM search_results
WHERE ARRAY_CONTAINS('mgr_sarah_davis_us_east'::VARIANT, AUTHORIZED_USERS)

UNION ALL

SELECT 
    'External Partner (Limited)',
    COUNT(*)
FROM search_results
WHERE ARRAY_CONTAINS('partner_vendor_alex_jones'::VARIANT, AUTHORIZED_USERS);
```

> **Positive**
> You should see significantly different result counts, demonstrating that the entitlement system is working correctly to filter search results based on user access rights.

<!-- ------------------------ -->

## Advanced Search Functions
Duration: 2

Create reusable functions for application integration.

### Dynamic Search Function

Create a function that adapts to user context:

```sql
CREATE OR REPLACE FUNCTION SEARCH_WITH_ENTITLEMENTS(
    SEARCH_QUERY STRING,
    USERNAME STRING
) RETURNS TABLE (
    TRANSACTION_ID STRING,
    RELEVANCE_SCORE FLOAT,
    AMOUNT FLOAT,
    MERCHANT_NAME STRING,
    REGION_CODE STRING
) LANGUAGE SQL
AS $$
    SELECT 
        TRANSACTION_ID, RELEVANCE_SCORE, AMOUNT, MERCHANT_NAME, REGION_CODE
    FROM TABLE(
        SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
            TRANSACTION_SEARCH_SERVICE, SEARCH_QUERY, 50
        )
    )
    WHERE ARRAY_CONTAINS(USERNAME::VARIANT, AUTHORIZED_USERS)
    ORDER BY RELEVANCE_SCORE DESC
    LIMIT 20
$$;
```

### Usage Examples

Test the function with different users:

```sql
-- Executive search
SELECT * FROM TABLE(SEARCH_WITH_ENTITLEMENTS(
    'credit card fraud suspicious', 'ceo_jane_smith'
));

-- Regional manager search  
SELECT * FROM TABLE(SEARCH_WITH_ENTITLEMENTS(
    'restaurant payment declined', 'mgr_sarah_davis_us_east'
));

-- Partner search
SELECT * FROM TABLE(SEARCH_WITH_ENTITLEMENTS(
    'grocery store purchase', 'partner_vendor_alex_jones'
));
```

### Application-Ready JSON Function

Create a function that returns JSON for easy application integration:

```sql
CREATE OR REPLACE FUNCTION SEARCH_FOR_APPLICATION(
    USERNAME STRING,
    SEARCH_QUERY STRING
) RETURNS ARRAY
LANGUAGE SQL
AS $$
    SELECT ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'transaction_id', TRANSACTION_ID,
            'relevance_score', RELEVANCE_SCORE,
            'amount', AMOUNT,
            'merchant_name', MERCHANT_NAME,
            'region', REGION_CODE,
            'customer_tier', CUSTOMER_TIER
        )
    )
    FROM TABLE(
        SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
            TRANSACTION_SEARCH_SERVICE, SEARCH_QUERY, 50
        )
    )
    WHERE ARRAY_CONTAINS(USERNAME::VARIANT, AUTHORIZED_USERS)
$$;
```

<!-- ------------------------ -->

## Demo Validation & Analytics
Duration: 2

Verify the demo is working correctly and explore usage analytics.

### Validation Queries

Check that entitlements are working:

```sql
-- Verify user access distribution
SELECT 
    USERNAME, FULL_NAME, ACCESS_LEVEL,
    COUNT(CASE WHEN ARRAY_CONTAINS(USERNAME::VARIANT, AUTHORIZED_USERS) THEN 1 END) AS ACCESSIBLE_TRANSACTIONS,
    ROUND(COUNT(CASE WHEN ARRAY_CONTAINS(USERNAME::VARIANT, AUTHORIZED_USERS) THEN 1 END) * 100.0 / 5000, 2) AS ACCESS_PERCENTAGE
FROM USER_ACCESS_MAPPING u
CROSS JOIN CORTEX_SEARCH_TRANSACTIONS t
GROUP BY USERNAME, FULL_NAME, ACCESS_LEVEL
ORDER BY ACCESSIBLE_TRANSACTIONS DESC;
```

Expected results should show:
- Executives: ~90-100% access
- Regional managers: ~25% access (their region only)
- Department staff: 40-80% access (varies by department)
- External partners: ~20% access (public data only)

### Analytics Dashboard

Create views for monitoring search usage:

```sql
-- Search capability summary by user
CREATE OR REPLACE VIEW USER_SEARCH_ANALYTICS AS
SELECT 
    u.USERNAME,
    u.ACCESS_LEVEL,
    COUNT(DISTINCT t.REGION_CODE) AS ACCESSIBLE_REGIONS,
    COUNT(DISTINCT t.SENSITIVITY_LEVEL) AS ACCESSIBLE_SENSITIVITY_LEVELS,
    AVG(t.AMOUNT) AS AVG_ACCESSIBLE_AMOUNT,
    COUNT(*) AS TOTAL_ACCESSIBLE_TRANSACTIONS
FROM USER_ACCESS_MAPPING u
LEFT JOIN CORTEX_SEARCH_TRANSACTIONS t 
    ON ARRAY_CONTAINS(u.USERNAME::VARIANT, t.AUTHORIZED_USERS)
GROUP BY u.USERNAME, u.ACCESS_LEVEL;
```

### Final Demo Summary

```sql
-- Complete demo statistics
SELECT 
    'Demo Statistics' AS METRIC_TYPE,
    'Total Transactions' AS METRIC,
    COUNT(*)::STRING AS VALUE
FROM CREDIT_CARD_TRANSACTIONS

UNION ALL

SELECT 'Demo Statistics', 'Total Users', COUNT(*)::STRING
FROM USER_ACCESS_MAPPING

UNION ALL

SELECT 'Demo Statistics', 'Cortex Search Services', COUNT(*)::STRING  
FROM INFORMATION_SCHEMA.CORTEX_SEARCH_SERVICES
WHERE SEARCH_SERVICE_SCHEMA = 'ENTITLEMENTS'

UNION ALL

SELECT 'Access Control', 'Avg Users Per Transaction', 
       ROUND(AVG(AUTHORIZED_USER_COUNT), 2)::STRING
FROM CORTEX_SEARCH_TRANSACTIONS;
```

> **Positive**
> Congratulations! You've successfully built a production-ready entitlement system with Cortex Search. The demo shows how to implement sophisticated access control that scales with your organization's security requirements.

<!-- ------------------------ -->

## Conclusion & Next Steps
Duration: 2

### What You've Accomplished

You've built a comprehensive fine-grained entitlements demo that demonstrates:

✅ **Realistic Data Generation** - Created 5,000+ credit card transactions with multi-dimensional attributes  
✅ **Sophisticated RBAC** - Implemented 15+ roles with complex access policies  
✅ **Row-Level Security** - Built three complementary access policies  
✅ **User Entitlements** - Generated user arrays for efficient search filtering  
✅ **Cortex Search Integration** - Created search services with embedded access control  
✅ **Production Functions** - Built reusable search functions for applications  

### Key Takeaways

- **User Arrays are Powerful**: Pre-calculating authorized user arrays provides excellent performance while maintaining security
- **Multi-Policy Approach**: Combining multiple row access policies gives fine-grained control across different dimensions
- **Search Service Design**: Different search services can be optimized for different use cases and access patterns
- **Function-Based API**: Creating reusable functions makes integration with applications seamless

### Next Steps

**Extend the Demo**:
- Add time-based access controls (business hours, temporary access)
- Implement dynamic risk scoring integration
- Add data masking for sensitive fields
- Create ML-driven access recommendations

**Production Considerations**:
- Implement proper password policies and user lifecycle management  
- Add comprehensive audit logging and access monitoring
- Consider performance optimization for large-scale deployments
- Implement data retention policies aligned with compliance requirements

**Integration Opportunities**:
- Build Streamlit apps with embedded search
- Create REST APIs using Snowflake functions
- Integrate with external identity providers
- Add real-time alerting for unusual access patterns

### Resources

- [Snowflake Cortex Search Documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search)
- [Row Access Policies Guide](https://docs.snowflake.com/en/user-guide/security-row-access-policies)
- [RBAC Best Practices](https://docs.snowflake.com/en/user-guide/security-access-control-overview)

### Feedback

Have questions or suggestions? [Create an issue](https://github.com/sfc-gh-mbaron/mb_demos/issues) in the GitHub repository.

---

**Thank you for completing the Cortex Search Fine-Grained Entitlements Demo!**
