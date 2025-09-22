# 🚀 Cortex Search Streamlit Application - Deployment Guide

This guide walks you through deploying and using the interactive Streamlit application that demonstrates Cortex Search with user-based entitlements and performance monitoring.

## 📋 Overview

The Streamlit application provides:

- **👤 User Selection**: Dropdown to select users and view their accessible transactions
- **🔍 Cortex Search Integration**: Real-time semantic search with entitlement filtering  
- **⚡ Performance Monitoring**: API response time tracking and metrics
- **📊 Interactive Analytics**: Rich visualizations and transaction summaries
- **📈 Real-time Data**: Live connection to Dynamic Tables with automatic refresh

## 🎯 Prerequisites

Before deploying, ensure you have completed:

1. ✅ Run the standalone Dynamic Tables setup (`11_standalone_dynamic_tables_setup.sql`)
2. ✅ Verify the Cortex Search service is working (`05_cortex_search_examples.sql`)
3. ✅ Test incremental updates (`12_test_standalone_incremental_updates.sql`)
4. ✅ Have appropriate permissions (SYSADMIN or custom role with required grants)

## 🚀 Deployment Steps

### Step 1: Run the Deployment Script

```sql
-- Execute the deployment script in Snowflake
-- This creates the Streamlit app, stage, and required functions
@14_deploy_streamlit_app.sql
```

### Step 2: Upload the Streamlit App File

Choose one of the following methods to upload `cortex_search_app.py`:

#### Option A: Snowflake Web UI (Recommended)
1. Navigate to **Data** > **Databases** > **CORTEX_SEARCH_ENTITLEMENT_DB** > **DYNAMIC_DEMO** > **Stages** > **streamlit_stage**
2. Click **"+ Files"** or **"Upload Files"**
3. Select and upload `cortex_search_app.py`
4. Verify the file appears in the stage

#### Option B: SnowSQL Command Line
```bash
snowsql -c <your_connection_name> -q "PUT file://cortex_search_app.py @CORTEX_SEARCH_ENTITLEMENT_DB.DYNAMIC_DEMO.streamlit_stage"
```

#### Option C: Python Snowpark
```python
from snowflake.snowpark import Session

# Your connection parameters
session = Session.builder.configs(<your_connection_params>).create()

# Upload file
session.file.put("cortex_search_app.py", "@CORTEX_SEARCH_ENTITLEMENT_DB.DYNAMIC_DEMO.streamlit_stage")
```

### Step 3: Verify Deployment

```sql
-- Verify the Streamlit app was created
SHOW STREAMLIT cortex_search_dashboard;

-- Check if file was uploaded successfully
LIST @CORTEX_SEARCH_ENTITLEMENT_DB.DYNAMIC_DEMO.streamlit_stage;

-- Test data access
SELECT COUNT(*) FROM CORTEX_SEARCH_ENTITLEMENT_DB.DYNAMIC_DEMO.user_region_access WHERE status = 'ACTIVE';
```

### Step 4: Access the Application

1. **Via Snowflake Web UI**:
   - Navigate to **Projects** > **Streamlit**
   - Find **cortex_search_dashboard** 
   - Click **"Open"** or the app name

2. **Via Direct URL** (if available):
   - Use the URL provided after deployment
   - Format: `https://<account>.snowflakecomputing.com/streamlit/<app_path>`

## 🎮 Using the Application

### Main Interface

The application features:

#### 🎛️ Sidebar Controls
- **👤 User Selection**: Dropdown with all active users
- **🔍 Search Options**: Optional semantic search queries
- **⚙️ Settings**: Result limits and auto-refresh options

#### 📊 Main Dashboard
- **⚡ Performance Metrics**: Response times and result counts
- **💰 Transaction Summary**: Financial analytics and summaries  
- **📈 Interactive Visualizations**: Charts and graphs
- **📋 Detailed Data Table**: Filterable transaction list

### Key Features

#### 1. User-Based Entitlements
- Select any user from the dropdown
- View only transactions they have access to based on region
- See user details (region, access level, status)

#### 2. Semantic Search
- Leave search blank for all accessible transactions
- Use natural language queries like:
  - "restaurant purchases"
  - "high value transactions"
  - "travel expenses"
  - "retail shopping"

#### 3. Performance Monitoring
- Real-time API response times
- Results count and data metrics
- Performance indicators (Fast/Slow)

#### 4. Interactive Analytics
- **Amount Distribution**: Histogram of transaction values
- **Regional Analysis**: Geographic breakdown with pie charts
- **Category Breakdown**: Transaction categories bar charts  
- **Timeline View**: Daily transaction trends

#### 5. Data Export
- Download results as CSV or JSON
- Timestamped filenames for tracking

### Example Usage Scenarios

#### Scenario 1: User Access Validation
1. Select user `USR_0050`
2. Leave search query empty
3. Click "Search Transactions"
4. Verify user only sees transactions from their region

#### Scenario 2: Semantic Search Testing
1. Select user `USR_0010` (Admin level)
2. Enter search query: "restaurant dining"
3. Compare results with query: "high value purchases"
4. Monitor response times for each query

#### Scenario 3: Regional Analysis
1. Select users from different regions
2. Compare accessible transaction volumes
3. Analyze regional spending patterns
4. Export data for further analysis

## 🔧 Troubleshooting

### Common Issues

#### 1. Application Won't Load
**Symptoms**: Streamlit app shows error or won't start

**Solutions**:
- ✅ Verify file uploaded to correct stage
- ✅ Check permissions (USAGE on database, schema, warehouse)
- ✅ Ensure warehouse is running and accessible
- ✅ Verify Python file syntax (no syntax errors)

#### 2. No Data Displayed
**Symptoms**: App loads but shows "No transactions found"

**Solutions**:
- ✅ Check Dynamic Table has data: `SELECT COUNT(*) FROM financial_transactions_enriched`
- ✅ Verify Cortex Search service is active: `DESCRIBE CORTEX SEARCH SERVICE financial_search_service`
- ✅ Ensure users have ACTIVE status in user_region_access table
- ✅ Refresh Dynamic Table: `CALL refresh_financial_dynamic_table()`

#### 3. Slow Performance
**Symptoms**: Long response times, app feels sluggish

**Solutions**:
- ✅ Use larger warehouse (M, L, XL instead of XS, S)
- ✅ Enable result caching in Snowflake
- ✅ Reduce result limits in sidebar
- ✅ Check Dynamic Table refresh frequency

#### 4. Permission Errors
**Symptoms**: "Access denied" or "Insufficient privileges" errors

**Solutions**:
```sql
-- Grant necessary permissions
GRANT USAGE ON DATABASE CORTEX_SEARCH_ENTITLEMENT_DB TO ROLE <your_role>;
GRANT USAGE ON SCHEMA DYNAMIC_DEMO TO ROLE <your_role>;
GRANT SELECT ON ALL TABLES IN SCHEMA DYNAMIC_DEMO TO ROLE <your_role>;
GRANT USAGE ON CORTEX SEARCH SERVICE financial_search_service TO ROLE <your_role>;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE <your_role>;
```

### Debugging Queries

```sql
-- Check Streamlit app status
SHOW STREAMLIT cortex_search_dashboard;

-- Verify file in stage
LIST @CORTEX_SEARCH_ENTITLEMENT_DB.DYNAMIC_DEMO.streamlit_stage;

-- Test Cortex Search manually
SELECT COUNT(*) FROM TABLE(
    CORTEX_SEARCH_DATA_SCAN(
        SERVICE_NAME => 'financial_search_service'
    )
) LIMIT 10;

-- Check Dynamic Table status
SELECT * FROM CORTEX_SEARCH_ENTITLEMENT_DB.DYNAMIC_DEMO.dynamic_table_monitor;

-- Test user entitlements
SELECT user_id, region_name, status 
FROM CORTEX_SEARCH_ENTITLEMENT_DB.DYNAMIC_DEMO.user_region_access 
WHERE status = 'ACTIVE' 
LIMIT 5;
```

## 🔄 Updates and Maintenance

### Updating the Application

1. **Code Changes**:
   - Modify `cortex_search_app.py`
   - Upload new version to stage (overwrites existing)
   - Streamlit automatically detects and reloads

2. **Data Refresh**:
   ```sql
   -- Refresh Dynamic Table data
   CALL CORTEX_SEARCH_ENTITLEMENT_DB.DYNAMIC_DEMO.refresh_financial_dynamic_table();
   
   -- Refresh dashboard data (includes all dependencies)
   CALL CORTEX_SEARCH_ENTITLEMENT_DB.DYNAMIC_DEMO.refresh_dashboard_data();
   ```

3. **Adding New Users**:
   ```sql
   -- Add new users (they'll appear in dropdown automatically)
   INSERT INTO CORTEX_SEARCH_ENTITLEMENT_DB.DYNAMIC_DEMO.user_region_access 
   (user_id, user_name, region_name, access_level, status)
   VALUES ('USR_NEW', 'New User', 'North America', 'STANDARD', 'ACTIVE');
   
   -- Refresh to update entitlements
   CALL CORTEX_SEARCH_ENTITLEMENT_DB.DYNAMIC_DEMO.refresh_financial_dynamic_table();
   ```

### Performance Optimization

- **Warehouse Sizing**: Use appropriate warehouse size for expected usage
- **Result Caching**: Enable Snowflake result caching for repeated queries
- **Data Limits**: Use reasonable limits for large result sets
- **Auto-refresh**: Use sparingly to avoid excessive compute costs

## 🎯 Advanced Usage

### Custom Searches

The semantic search supports sophisticated queries:

- **Category-based**: "restaurant spending", "travel expenses", "retail purchases"
- **Value-based**: "high value transactions", "large purchases", "small expenses"  
- **Time-based**: "recent transactions", "monthly expenses"
- **Combined**: "expensive restaurant meals", "large travel bookings"

### Analytics Insights

Use the visualizations to:

- **Identify spending patterns** across regions and categories
- **Monitor transaction volumes** and values over time
- **Compare user access levels** and their transaction visibility
- **Analyze regional differences** in spending behavior
- **Track performance metrics** for search optimization

## 🆘 Support

For issues or questions:

1. **Check this guide** for common solutions
2. **Review deployment logs** in Snowflake
3. **Test individual components** (Dynamic Tables, Cortex Search)
4. **Verify permissions** and data access
5. **Monitor resource usage** (warehouse, credits)

## ✅ Success Criteria

Your deployment is successful when:

- ✅ Streamlit app loads without errors
- ✅ User dropdown shows all active users  
- ✅ Search returns appropriate results for selected users
- ✅ Response times are reasonable (<2 seconds)
- ✅ Visualizations render correctly
- ✅ Data export functions work
- ✅ Performance metrics display accurately

---

**🎉 Congratulations!** You now have a fully functional Cortex Search Streamlit application with user-based entitlements, performance monitoring, and rich analytics capabilities!
