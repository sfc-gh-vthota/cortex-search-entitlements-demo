-- =============================================================================
-- CORTEX SEARCH ENTITLEMENTS DEMO - Database Setup and Table Creation
-- =============================================================================
-- This script sets up the foundational database structure for the demo
-- Run this script first to establish the environment

-- Set context and create database
USE ROLE ACCOUNTADMIN;

-- Create database and schema for the demo
CREATE DATABASE IF NOT EXISTS CORTEX_SEARCH_ENTITLEMENTS_DB;
CREATE SCHEMA IF NOT EXISTS CORTEX_SEARCH_ENTITLEMENTS_DB.ENTITLEMENTS;

-- Use the demo database and schema
USE DATABASE CORTEX_SEARCH_ENTITLEMENTS_DB;
USE SCHEMA ENTITLEMENTS;

-- Create warehouse for processing (if not already exists)
CREATE WAREHOUSE IF NOT EXISTS ENTITLEMENTS_WH
  WITH WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE
  COMMENT = 'Warehouse for Cortex Search Entitlements Demo';

USE WAREHOUSE ENTITLEMENTS_WH;

-- =============================================================================
-- MAIN TRANSACTIONS TABLE
-- =============================================================================
-- Create the main credit card transactions table
CREATE OR REPLACE TABLE CREDIT_CARD_TRANSACTIONS (
    -- Transaction Identifiers
    TRANSACTION_ID VARCHAR(50) PRIMARY KEY,
    CUSTOMER_ID VARCHAR(50) NOT NULL,
    MERCHANT_ID VARCHAR(50) NOT NULL,
    
    -- Transaction Details
    TRANSACTION_DATE TIMESTAMP_NTZ NOT NULL,
    AMOUNT DECIMAL(10,2) NOT NULL,
    CURRENCY VARCHAR(3) DEFAULT 'USD',
    
    -- Card Information
    CARD_NUMBER VARCHAR(50), -- Masked for security
    CARD_BRAND VARCHAR(20),
    CARD_TYPE VARCHAR(20),
    CARD_SUBTYPE VARCHAR(30),
    
    -- Merchant Information
    MERCHANT_NAME VARCHAR(200),
    MERCHANT_CATEGORY_CODE VARCHAR(10),
    MERCHANT_CATEGORY VARCHAR(50),
    MERCHANT_DESCRIPTION VARCHAR(200),
    
    -- Transaction Status and Processing
    TRANSACTION_STATUS VARCHAR(20),
    AUTHORIZATION_CODE VARCHAR(20),
    PROCESSOR_RESPONSE_CODE VARCHAR(10),
    
    -- Geographic Information (Key for Region-based Entitlements)
    REGION_CODE VARCHAR(20) NOT NULL,
    REGION_NAME VARCHAR(100),
    COUNTRY_CODE VARCHAR(5),
    TIMEZONE VARCHAR(50),
    IS_INTERNATIONAL BOOLEAN DEFAULT FALSE,
    
    -- Customer Attributes (Key for Customer-based Entitlements)
    CUSTOMER_TIER VARCHAR(20) NOT NULL,
    CUSTOMER_CREDIT_LIMIT DECIMAL(12,2),
    
    -- Access Control Attributes
    PRIMARY_DEPARTMENT VARCHAR(50) NOT NULL,
    SENSITIVITY_LEVEL VARCHAR(20) NOT NULL,
    
    -- Risk and Fraud Attributes (Key for Risk-based Entitlements)
    RISK_LEVEL VARCHAR(20) NOT NULL,
    RISK_SCORE DECIMAL(5,2),
    
    -- Additional Attributes
    IS_ONLINE BOOLEAN DEFAULT FALSE,
    
    -- Audit Timestamps
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Add comments to the table and key columns for documentation
COMMENT ON TABLE CREDIT_CARD_TRANSACTIONS IS 'Credit card transaction data with rich attributes for fine-grained entitlement demonstration';
COMMENT ON COLUMN CREDIT_CARD_TRANSACTIONS.REGION_CODE IS 'Region code for geographic-based access control (US_EAST, US_WEST, EUROPE, ASIA_PAC)';
COMMENT ON COLUMN CREDIT_CARD_TRANSACTIONS.CUSTOMER_TIER IS 'Customer tier for tier-based access control (PREMIUM, GOLD, SILVER, STANDARD)';
COMMENT ON COLUMN CREDIT_CARD_TRANSACTIONS.PRIMARY_DEPARTMENT IS 'Primary responsible department (FINANCE, OPERATIONS, MARKETING, COMPLIANCE, FRAUD, CUSTOMER_SERVICE)';
COMMENT ON COLUMN CREDIT_CARD_TRANSACTIONS.SENSITIVITY_LEVEL IS 'Data sensitivity level (PUBLIC, INTERNAL, CONFIDENTIAL, RESTRICTED)';
COMMENT ON COLUMN CREDIT_CARD_TRANSACTIONS.RISK_LEVEL IS 'Transaction risk level (LOW, MEDIUM, HIGH, CRITICAL)';

-- =============================================================================
-- FILE FORMAT AND STAGE FOR DATA LOADING
-- =============================================================================
-- Create file format for CSV loading
CREATE OR REPLACE FILE FORMAT CSV_FORMAT
  TYPE = 'CSV'
  COMPRESSION = 'NONE'
  FIELD_DELIMITER = ','
  RECORD_DELIMITER = '\n'
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  ESCAPE = 'NONE'
  ESCAPE_UNENCLOSED_FIELD = '\134'
  DATE_FORMAT = 'AUTO'
  TIMESTAMP_FORMAT = 'AUTO'
  NULL_IF = ('NULL', 'null', '', 'N/A');

-- Create internal stage for loading data
CREATE OR REPLACE STAGE DEMO_STAGE
  COMMENT = 'Internal stage for loading credit card transaction data';

-- =============================================================================
-- INDEXES FOR PERFORMANCE (Optional but recommended)
-- =============================================================================
-- Note: Snowflake automatically manages clustering, but these are logical groupings for access patterns

-- Display table structure for verification
DESCRIBE TABLE CREDIT_CARD_TRANSACTIONS;

-- Show the created objects
SHOW TABLES LIKE 'CREDIT_CARD_TRANSACTIONS';
SHOW FILE FORMATS LIKE 'CSV_FORMAT';
SHOW STAGES LIKE 'DEMO_STAGE';

-- Grant basic permissions (adjust based on your role structure)
-- GRANT USAGE ON DATABASE CORTEX_SEARCH_ENTITLEMENTS_DB TO ROLE <your_role>;
-- GRANT USAGE ON SCHEMA CORTEX_SEARCH_ENTITLEMENTS_DB.ENTITLEMENTS TO ROLE <your_role>;
-- GRANT SELECT ON TABLE CREDIT_CARD_TRANSACTIONS TO ROLE <your_role>;

SELECT 'Database and table setup complete!' AS STATUS;
