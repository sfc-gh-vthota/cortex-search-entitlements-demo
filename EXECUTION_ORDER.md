# Cortex Search Demo - Execution Order

This guide explains the correct order to run the scripts for the Cortex Search demonstration.

## Problem Solved

**Issue**: Cortex Search requires tables with `CHANGE_TRACKING` enabled, but the shared `SAMPLE_DATA.TPCDS_SF10TCL` tables don't allow enabling change tracking.

**Solution**: Materialize the shared tables into local tables with change tracking enabled.

---

## Execution Steps

### Step 1: Create Tables with Change Tracking
**Script**: `00_create_tpcds_tables_with_change_tracking.sql`

Run this script **FIRST** to:
- Create database `TPCDS_CORTEX_DB` and schema `TPCDS_DATA`
- Materialize 6 tables from `SAMPLE_DATA.TPCDS_SF10TCL` with `_TAB` suffix
- Enable `CHANGE_TRACKING` on all tables
- Grant necessary permissions

**Tables Created**:
```
TPCDS_CORTEX_DB.TPCDS_DATA.STORE_SALES_TAB
TPCDS_CORTEX_DB.TPCDS_DATA.ITEM_TAB
TPCDS_CORTEX_DB.TPCDS_DATA.STORE_TAB
TPCDS_CORTEX_DB.TPCDS_DATA.CUSTOMER_TAB
TPCDS_CORTEX_DB.TPCDS_DATA.CUSTOMER_DEMOGRAPHICS_TAB
TPCDS_CORTEX_DB.TPCDS_DATA.DATE_DIM_TAB
```

**Estimated Time**: 2-5 minutes (depending on data volume)

---

### Step 2: Run Comprehensive Cortex Search Demo
**Script**: `comprehensive_cortex_search_demo.sql`

After Step 1 completes successfully, run this script to:
- Create 2 Cortex Search services:
  - `tpcds_comprehensive_search` - Multi-column search across all dimensions
  - `tpcds_product_search` - Product-specific search
- Demonstrate all search capabilities:
  - Basic search (exact, partial, multi-column)
  - Advanced search (semantic, aggregations)
  - Filtering (numeric, categorical, temporal)
  - Geospatial search simulation
  - Entitlements integration examples

**Estimated Time**: 
- Service creation: 10-30 minutes (initial indexing)
- Query execution: 1-2 minutes

---

## Key Changes Made

### Database & Schema
- **OLD**: `SAMPLE_DATA.TPCDS_SF10TCL`
- **NEW**: `TPCDS_CORTEX_DB.TPCDS_DATA`

### Table Names
All table names now have `_TAB` suffix:
- `STORE_SALES` → `STORE_SALES_TAB`
- `ITEM` → `ITEM_TAB`
- `STORE` → `STORE_TAB`
- `CUSTOMER` → `CUSTOMER_TAB`
- `CUSTOMER_DEMOGRAPHICS` → `CUSTOMER_DEMOGRAPHICS_TAB`
- `DATE_DIM` → `DATE_DIM_TAB`

### Cortex Search Service Syntax
Fixed to use correct syntax:
- **ON clause**: Single column for text search (`search_content`)
- **ATTRIBUTES clause**: Additional columns for filtering/display

---

## Verification

After running both scripts, verify success:

```sql
-- Check tables exist with change tracking
USE DATABASE TPCDS_CORTEX_DB;
USE SCHEMA TPCDS_DATA;
SHOW TABLES LIKE '%_TAB';

-- Check Cortex Search services
SHOW CORTEX SEARCH SERVICES;

-- Check service status
DESCRIBE CORTEX SEARCH SERVICE tpcds_comprehensive_search;
DESCRIBE CORTEX SEARCH SERVICE tpcds_product_search;

-- Test a simple search
SELECT * 
FROM TABLE(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'tpcds_comprehensive_search',
        'electronics',
        LIMIT => 10
    )
);
```

---

## Troubleshooting

### If Step 1 Fails
- Ensure you have `ACCOUNTADMIN` role or sufficient privileges
- Check warehouse `COMPUTE_WH` exists and is active
- Verify access to `SAMPLE_DATA.TPCDS_SF10TCL`

### If Step 2 Fails
- Ensure Step 1 completed successfully
- Wait for change tracking to be fully enabled (check `SHOW TABLES`)
- Verify Cortex Search service limits haven't been exceeded

### If Search Service Takes Long to Build
- Normal for large datasets (SF10TCL has millions of rows)
- Check status: `DESCRIBE CORTEX SEARCH SERVICE <service_name>`
- Services can be queried once status shows as "READY"

---

## Next Steps

Once both scripts complete successfully:
1. Run the example queries in `comprehensive_cortex_search_demo.sql`
2. Experiment with different search terms
3. Test filtering and aggregations
4. Integrate with Immuta for production entitlements
5. Deploy Streamlit app for user interface

---

## Support

For issues or questions:
- Check Snowflake documentation: https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search
- Review script comments for detailed explanations
- Ensure all prerequisites are met before execution


