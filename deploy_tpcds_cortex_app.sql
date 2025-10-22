-- Deploy TPCDS Cortex Search App (Following cortex_search_app.py Pattern)
-- ===================================================================
-- This script deploys the TPCDS search app using the same structure as the working cortex_search_app.py

-- Prerequisites check
USE ROLE ACCOUNTADMIN; -- Or role with CORTEX_USER privileges and Streamlit app creation rights

-- Ensure the enhanced search service exists
SELECT 'Checking for tpcds_comprehensive_search service...' as status;

SHOW CORTEX SEARCH SERVICES LIKE 'tpcds_comprehensive_search' IN DATABASE SAMPLE_DATA SCHEMA TPCDS_SF10TCL;

-- =============================================================================
-- STEP 1: CREATE TPCDS CORTEX SEARCH STREAMLIT APP
-- =============================================================================

-- Use the correct database and schema
USE DATABASE SAMPLE_DATA;
USE SCHEMA TPCDS_SF10TCL;

-- Create the TPCDS Cortex Search application (following the working pattern)
CREATE OR REPLACE STREAMLIT tpcds_cortex_search_app
  ROOT_LOCATION = '@~/streamlit'
  MAIN_FILE = '/tpcds_cortex_search_app.py'
  QUERY_WAREHOUSE = 'COMPUTE_WH'
  COMMENT = 'TPCDS Cortex Search App - Following cortex_search_app.py pattern with same imports and structure';

-- Grant necessary permissions
GRANT USAGE ON STREAMLIT tpcds_cortex_search_app TO ROLE PUBLIC;

-- =============================================================================
-- STEP 2: DEPLOYMENT INSTRUCTIONS
-- =============================================================================

SELECT '✅ TPCDS CORTEX SEARCH APP DEPLOYED!' as status,
       'Upload tpcds_cortex_search_app.py with same packages as cortex_search_app.py' as instructions;

/*
📋 DEPLOYMENT INSTRUCTIONS (Following Working Pattern):

🎯 **USING THE SAME STRUCTURE AS cortex_search_app.py:**
This new app follows the EXACT same pattern as your working cortex_search_app.py:
- Same imports (including plotly, numpy, pandas)
- Same function structure and naming
- Same UI layout and styling
- Same Python API approach with snowflake.core

🚀 **DEPLOYMENT STEPS:**

1. **Upload the TPCDS App File:**
   - Navigate to Snowsight → Projects → Streamlit
   - Find the "tpcds_cortex_search_app" app
   - Upload the file: tpcds_cortex_search_app.py
   - Set it as the main file

2. **Required Packages (SAME AS cortex_search_app.py):**
   Since your cortex_search_app.py works perfectly, use the SAME packages:
   - pandas
   - plotly  
   - numpy
   - streamlit
   - snowflake (if needed)
   
   Use whatever package configuration you have for cortex_search_app.py!

3. **App Features (Identical Structure):**
   ✅ Multi-dimensional search across TPCDS products, demographics, geography
   ✅ Sidebar controls with search input and filters (same as your working app)
   ✅ Performance metrics display (same format as cortex_search_app.py)
   ✅ Interactive analytics with Plotly visualizations
   ✅ Export capabilities (CSV, JSON)
   ✅ Same responsive UI and styling

4. **Key Differences from cortex_search_app.py:**
   - Database: SAMPLE_DATA instead of CORTEX_SEARCH_ENTITLEMENT_DB
   - Schema: TPCDS_SF10TCL instead of DYNAMIC_DEMO  
   - Service: tpcds_comprehensive_search instead of financial_search_service
   - Data: Products/customers instead of financial transactions
   - Filters: Price, categories, demographics instead of user entitlements

5. **Search Capabilities:**
   - Product search: Items, brands, categories, descriptions
   - Demographics: Customer gender, marital status
   - Geography: Store locations, cities, states  
   - Temporal: Transaction dates, years, quarters
   - Combined: Mix any dimensions for precise results

6. **Sidebar Controls (Same Layout as Working App):**
   - Search input box (same as cortex_search_app.py)
   - Result limit slider
   - Filter sections:
     * Price range slider
     * Year multi-select
     * Customer demographics
     * Product categories
   - Clear filters button

7. **Main Content (Same Structure):**
   - Search button (same styling)
   - Performance metrics (same format)
   - Results summary (same layout)
   - Visualizations tabs (same as cortex_search_app.py)
   - Detailed results table
   - Export options

🔧 **TROUBLESHOOTING:**
✅ Uses same imports as your working cortex_search_app.py
✅ Same Python API approach (snowflake.core.Root)
✅ Same function structure and error handling
✅ Same performance optimization patterns

📊 **Example Search Queries:**
- "premium electronics married customers"
- "California stores clothing"
- "luxury items female customers"
- "electronics accessories high price"

🎯 **Why This Should Work:**
- Follows EXACT same pattern as your working cortex_search_app.py
- Uses same imports and package dependencies
- Same API calls and error handling
- Only differences are the database/schema/service names and data fields

*/

-- Show final deployment status
SELECT 
    'TPCDS Cortex Search App' as application_name,
    'Following cortex_search_app.py Pattern' as approach,
    'Same structure, imports, and UI as working app' as benefits,
    'Upload tpcds_cortex_search_app.py with same packages' as next_action;

