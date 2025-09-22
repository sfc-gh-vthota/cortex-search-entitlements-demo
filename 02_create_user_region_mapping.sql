-- Create User-Region mapping table with 1000 users distributed across 10 regions
-- This script creates a mapping table showing which users belong to which regions
-- Users are mapped to match the USER_IDs used in the transactions table

-- Use the same database and schema
USE DATABASE CORTEX_SEARCH_ENTITLEMENT_DB;
USE SCHEMA TRANSACTIONS;

-- Create the User-Region mapping table
CREATE OR REPLACE TABLE USER_REGION_MAPPING (
    USER_ID VARCHAR(50) PRIMARY KEY,
    USER_NAME VARCHAR(100) NOT NULL,
    REGION_NAME VARCHAR(50) NOT NULL,
    CREATED_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    STATUS VARCHAR(20) DEFAULT 'ACTIVE'
);

-- Generate 1000 users distributed across 10 regions using Snowflake native functions
INSERT INTO USER_REGION_MAPPING (
    USER_ID,
    USER_NAME,
    REGION_NAME,
    CREATED_DATE,
    STATUS
)
SELECT 
    'USER_' || LPAD(seq4(), 4, '0') AS user_id,
    CASE (seq4() - 1) % 50
        WHEN 0 THEN 'John Smith'
        WHEN 1 THEN 'Jane Johnson'
        WHEN 2 THEN 'Michael Williams'
        WHEN 3 THEN 'Sarah Brown'
        WHEN 4 THEN 'David Jones'
        WHEN 5 THEN 'Lisa Garcia'
        WHEN 6 THEN 'Robert Miller'
        WHEN 7 THEN 'Emily Davis'
        WHEN 8 THEN 'James Rodriguez'
        WHEN 9 THEN 'Jessica Martinez'
        WHEN 10 THEN 'William Hernandez'
        WHEN 11 THEN 'Ashley Lopez'
        WHEN 12 THEN 'Christopher Gonzalez'
        WHEN 13 THEN 'Amanda Wilson'
        WHEN 14 THEN 'Daniel Anderson'
        WHEN 15 THEN 'Stephanie Thomas'
        WHEN 16 THEN 'Matthew Taylor'
        WHEN 17 THEN 'Jennifer Moore'
        WHEN 18 THEN 'Anthony Jackson'
        WHEN 19 THEN 'Samantha Martin'
        WHEN 20 THEN 'Mark Lee'
        WHEN 21 THEN 'Rachel Perez'
        WHEN 22 THEN 'Donald Thompson'
        WHEN 23 THEN 'Melissa White'
        WHEN 24 THEN 'Steven Harris'
        WHEN 25 THEN 'Michelle Sanchez'
        WHEN 26 THEN 'Paul Clark'
        WHEN 27 THEN 'Laura Ramirez'
        WHEN 28 THEN 'Andrew Lewis'
        WHEN 29 THEN 'Kimberly Robinson'
        WHEN 30 THEN 'Kenneth Walker'
        WHEN 31 THEN 'Deborah Young'
        WHEN 32 THEN 'Brian Allen'
        WHEN 33 THEN 'Dorothy King'
        WHEN 34 THEN 'George Wright'
        WHEN 35 THEN 'Amy Scott'
        WHEN 36 THEN 'Edward Torres'
        WHEN 37 THEN 'Angela Nguyen'
        WHEN 38 THEN 'Ronald Hill'
        WHEN 39 THEN 'Helen Flores'
        WHEN 40 THEN 'Timothy Green'
        WHEN 41 THEN 'Brenda Adams'
        WHEN 42 THEN 'Jason Nelson'
        WHEN 43 THEN 'Emma Baker'
        WHEN 44 THEN 'Jeffrey Hall'
        WHEN 45 THEN 'Olivia Rivera'
        WHEN 46 THEN 'Ryan Campbell'
        WHEN 47 THEN 'Cynthia Mitchell'
        WHEN 48 THEN 'Jacob Carter'
        ELSE 'Marie Roberts'
    END || ' ' || (seq4()) AS user_name,
    CASE (seq4() - 1) % 10
        WHEN 0 THEN 'North America'
        WHEN 1 THEN 'Europe'
        WHEN 2 THEN 'Asia Pacific'
        WHEN 3 THEN 'Latin America'
        WHEN 4 THEN 'Middle East'
        WHEN 5 THEN 'Africa'
        WHEN 6 THEN 'Oceania'
        WHEN 7 THEN 'Nordic'
        WHEN 8 THEN 'Eastern Europe'
        ELSE 'Southeast Asia'
    END AS region_name,
    DATEADD(day, ABS(RANDOM()) % 365, '2024-01-01'::date)::timestamp AS created_date,
    CASE ABS(RANDOM()) % 20
        WHEN 0 THEN 'INACTIVE'
        ELSE 'ACTIVE'
    END AS status
FROM TABLE(GENERATOR(ROWCOUNT => 1000));

-- Note: Snowflake does not use traditional indexes
-- It automatically optimizes queries using micro-partitions and metadata

-- Verify the distribution across regions
SELECT 
    REGION_NAME,
    COUNT(*) as USER_COUNT,
    COUNT(CASE WHEN STATUS = 'ACTIVE' THEN 1 END) as ACTIVE_USERS,
    COUNT(CASE WHEN STATUS = 'INACTIVE' THEN 1 END) as INACTIVE_USERS
FROM USER_REGION_MAPPING 
GROUP BY REGION_NAME 
ORDER BY REGION_NAME;

-- Show sample users with their regions (first 20)
SELECT 
    USER_ID,
    USER_NAME,
    REGION_NAME,
    CREATED_DATE,
    STATUS
FROM USER_REGION_MAPPING 
ORDER BY USER_ID
LIMIT 20;

-- Summary statistics
SELECT 
    COUNT(*) as TOTAL_USERS,
    COUNT(DISTINCT REGION_NAME) as TOTAL_REGIONS,
    COUNT(CASE WHEN STATUS = 'ACTIVE' THEN 1 END) as TOTAL_ACTIVE_USERS,
    COUNT(CASE WHEN STATUS = 'INACTIVE' THEN 1 END) as TOTAL_INACTIVE_USERS,
    ROUND(COUNT(*) / COUNT(DISTINCT REGION_NAME), 2) as AVG_USERS_PER_REGION
FROM USER_REGION_MAPPING;

-- Verify alignment with transactions table users
SELECT 
    'Users in mapping but not in transactions' as check_type,
    COUNT(*) as count_discrepancy
FROM USER_REGION_MAPPING urm
LEFT JOIN TRANSACTIONS t ON urm.USER_ID = t.USER_ID
WHERE t.USER_ID IS NULL

UNION ALL

SELECT 
    'Users in transactions but not in mapping' as check_type,
    COUNT(DISTINCT t.USER_ID) as count_discrepancy
FROM TRANSACTIONS t
LEFT JOIN USER_REGION_MAPPING urm ON t.USER_ID = urm.USER_ID
WHERE urm.USER_ID IS NULL;

COMMIT;
