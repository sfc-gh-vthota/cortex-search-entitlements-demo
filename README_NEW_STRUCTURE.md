# Cortex Search Entitlements Demo with Incremental Pipeline

This project demonstrates a complete Cortex Search implementation with region-based entitlements and real-time incremental updates. It showcases **two different approaches** for automatically updating search indexes when data changes, ensuring users only see transactions they're entitled to access.

## 🎯 Two Incremental Update Approaches

### 🔄 **Approach 1: Streams & Tasks (Traditional)**
Event-driven architecture using Snowflake Streams, Tasks, and Stored Procedures

### ⚡ **Approach 2: Dynamic Tables (Modern - Recommended)**  
Declarative architecture using Snowflake Dynamic Tables with automatic refresh

## Architecture Overview

### Core Data Components:
1. **TRANSACTIONS** - Main transaction data (10,000 records across 10 regions)
2. **USER_REGION_MAPPING** - User-to-region mappings for access control (1,000 users)
3. **Cortex Search Service** - AI-powered search with automatic indexing

### Incremental Update Options:
- **Option A**: Streams + Tasks + Stored Procedures (Complex but flexible)
- **Option B**: Dynamic Tables (Simple and automatically optimized)

## Files Overview

### 01_create_transactions_table.sql
- Creates the `TRANSACTIONS` table with 10,000+ records
- Includes `REGION_USER_IDS` array for entitlement filtering
- Spreads data across 10 regions with realistic transaction patterns
- **Key Feature**: Array column automatically updated when user mappings change

**Transaction Fields:**
- Transaction ID, User ID, Date, Amount, Type, Description
- Region Name, Merchant, Category, Status
- **REGION_USER_IDS**: Array of all user IDs entitled to see this transaction

### 02_create_user_region_mapping.sql
- Creates `USER_REGION_MAPPING` table with 1,000 users
- Distributes users across 10 regions for comprehensive testing
- **Key Feature**: Changes here automatically trigger transaction table updates

### 03_create_cortex_search_service.sql
- Sets up Cortex Search service with 1-minute refresh lag
- Indexes transaction data for semantic search capabilities
- **Key Feature**: Automatically picks up changes from underlying table

### 04_create_incremental_pipeline.sql 🔄 **STREAMS & TASKS APPROACH**
- **Streams**: Capture all changes to both tables in real-time
- **Stored Procedures**: Process updates and maintain data consistency  
- **Automated Task**: Runs every minute to process changes
- **Logging System**: Full audit trail of all update activities
- **Key Feature**: When user mappings change, transaction entitlements update automatically

### 05_cortex_search_examples.sql
- Demonstrates entitlement-based search queries
- Shows how different users see different data sets
- Includes semantic search examples with region filtering

### 06_test_incremental_pipeline.sql
- Comprehensive test suite for the streams & tasks pipeline
- Demonstrates live updates and entitlement changes
- Performance and monitoring verification

### 08_create_dynamic_table_solution.sql ⚡ **DYNAMIC TABLES APPROACH** ⭐ **RECOMMENDED**
- **Dynamic Table**: Automatically maintains entitlement arrays
- **Auto-Refresh**: Updates when source data changes (1-minute TARGET_LAG)
- **Cortex Search Integration**: Direct indexing of Dynamic Table
- **Key Feature**: Zero maintenance - Snowflake handles everything automatically

### 09_test_dynamic_table_incremental.sql ⭐ **NEW**
- Comprehensive test suite for the Dynamic Tables approach
- Demonstrates automatic refresh and entitlement updates
- Performance comparison and monitoring

### 10_comparison_streams_vs_dynamic_tables.sql ⭐ **NEW**
- Side-by-side comparison of both approaches
- Performance analysis and cost comparison
- Architecture diagrams and recommendations

### 11_standalone_dynamic_tables_setup.sql 🆕 **STANDALONE SETUP**
- **Complete separate implementation** - doesn't disturb existing tables
- **New schema**: `DYNAMIC_DEMO` with different table names
- **5,000 transactions** + **200 users** across 10 regions
- **Dynamic Table + Cortex Search** in one complete script
- **Key Feature**: Completely isolated testing environment

### 12_test_standalone_incremental_updates.sql 🆕 **STANDALONE TESTING**
- Comprehensive test suite for the standalone setup
- Tests incremental updates without affecting original data
- Demonstrates Dynamic Tables automatic refresh
- Performance comparisons and monitoring

### 13_setup_summary_comparison.sql 🆕 **OVERVIEW**
- Shows what implementations you currently have
- Compares original vs standalone setups
- Provides usage recommendations and quick start commands

## How to Use

### 🚀 Quick Start - Choose Your Approach:

#### Option A: Streams & Tasks (Traditional)
```sql
-- 1. Create foundation
@01_create_transactions_table.sql      -- Base data (10K transactions)
@02_create_user_region_mapping.sql     -- User mappings (1K users)

-- 2. Set up Streams & Tasks pipeline  
@03_create_cortex_search_service.sql   -- Cortex Search service
@04_create_incremental_pipeline.sql    -- Streams + Tasks + Procedures

-- 3. Test and explore
@06_test_incremental_pipeline.sql      -- Test streams pipeline
@05_cortex_search_examples.sql         -- Explore search capabilities
```

#### Option B: Dynamic Tables (Modern - Recommended) ⭐
```sql
-- 1. Create foundation  
@01_create_transactions_table.sql      -- Base data (10K transactions)
@02_create_user_region_mapping.sql     -- User mappings (1K users)

-- 2. Set up Dynamic Tables solution
@08_create_dynamic_table_solution.sql  -- Dynamic Table + Cortex Search

-- 3. Test and explore
@09_test_dynamic_table_incremental.sql -- Test dynamic table updates
@05_cortex_search_examples.sql         -- Explore search capabilities (update service name)
```

#### Option C: Standalone Setup (Isolated Testing) 🆕 **RECOMMENDED FOR TESTING**
```sql
-- Complete standalone setup (doesn't disturb existing tables)
@11_standalone_dynamic_tables_setup.sql      -- Creates everything in separate schema
@12_test_standalone_incremental_updates.sql  -- Test incremental updates

-- Overview of all your setups
@13_setup_summary_comparison.sql             -- Shows what you have
```

#### Compare All Approaches:
```sql
@10_comparison_streams_vs_dynamic_tables.sql -- Side-by-side comparison
```

### 🏗️ Database Structure:
- **Database**: `CORTEX_SEARCH_ENTITLEMENT_DB`
- **Original Schema**: `TRANSACTIONS` (main implementation)
- **Standalone Schema**: `DYNAMIC_DEMO` (isolated testing)
- **Warehouse**: `COMPUTE_WH` (modify as needed)

## 🔍 Key Features Demonstrated

### Cortex Search with Entitlements
- **Semantic Search**: AI-powered natural language queries
- **Region-Based Security**: Users only see data they're entitled to access
- **Real-Time Updates**: Search index updates automatically when data changes

### Incremental Pipeline Options

#### 🔄 **Streams & Tasks (Traditional)**
- **Change Detection**: Streams capture all table modifications instantly
- **Automatic Processing**: Tasks run every minute to process changes
- **Entitlement Sync**: User mapping changes trigger transaction updates
- **Full Monitoring**: Complete audit trail and performance metrics

#### ⚡ **Dynamic Tables (Modern)**
- **Declarative**: Single SQL definition maintains entitlement arrays
- **Automatic Refresh**: TARGET_LAG ensures updates within 1 minute
- **Zero Maintenance**: Snowflake handles all optimization automatically
- **Built-in Monitoring**: Integrated observability and performance tuning

### Sample Queries Available
- **Entitlement Testing**: See how different users access different data
- **Semantic Search**: "Find restaurant purchases over $100"  
- **Regional Analysis**: Transaction patterns by region
- **Cross-Region Activity**: Users transacting outside home regions
- **High-Value Monitoring**: Track large transactions with region filtering

## 📊 Data Characteristics

### **Original Setup (TRANSACTIONS Schema):**
- **Scale**: 10,000 transactions + 1,000 users across 10 regions
- **Realistic Data**: Varied merchants, categories, amounts ($1-$5,000)
- **Time Range**: January 2023 to January 2025
- **Production Ready**: Full-scale implementation

### **Standalone Setup (DYNAMIC_DEMO Schema):**
- **Scale**: 5,000 transactions + 200 users across 10 regions  
- **Testing Focused**: Smaller dataset for faster testing
- **Time Range**: 2024 data for recent testing
- **Isolated**: Completely separate from original implementation

### **Common Features:**
- **Entitlement Arrays**: Each transaction knows which users can access it
- **Real-Time Updates**: Changes propagate within 1-2 minutes
- **10 Regions**: Consistent regional distribution across all setups

## ⚡ Performance Features

### **Both Approaches:**
- **Snowflake Optimized**: Uses native functions and capabilities
- **Micro-Partitioned**: Automatic optimization (no manual indexes needed)
- **Cortex AI**: Advanced semantic search capabilities
- **Incremental Only**: Only changed data is processed, not full refreshes

### **Streams & Tasks Specific:**
- **Event-Driven**: Responds immediately to data changes
- **Custom Logic**: Full control over processing workflow
- **Detailed Monitoring**: Custom logging and audit trails

### **Dynamic Tables Specific:**
- **Auto-Optimized**: Snowflake handles all performance tuning
- **Declarative**: Simple SQL-based transformations
- **Cost-Effective**: Only refreshes when source data actually changes
- **Built-in Monitoring**: Integrated with Snowflake's observability tools

## 🔧 Monitoring & Maintenance

### **Streams & Tasks Monitoring:**
- `stream_monitoring` - View pending changes
- `update_activity_summary` - Historical update statistics  
- `incremental_update_log` - Detailed operation audit trail
- Built-in error handling and retry logic
- Manual procedures for emergency operations

### **Dynamic Tables Monitoring:**
- `dynamic_table_status` - Current table status and refresh info
- `table_comparison` - Compare base tables vs dynamic table
- `entitlement_summary` - Entitlement distribution analysis
- Built-in Snowflake monitoring via Information Schema
- Zero-maintenance automatic optimization

## 🏆 **Recommendations**

### **For Learning and Testing: Standalone Setup** 🆕
- ✅ **Start here** - `@11_standalone_dynamic_tables_setup.sql`
- ✅ **No conflicts** - Completely isolated from existing work
- ✅ **Faster setup** - Smaller dataset, quicker testing
- ✅ **Perfect for experimentation** - Test Dynamic Tables safely

### **For Production: Dynamic Tables Approach**
- ✅ **Simpler architecture** - One Dynamic Table vs multiple components
- ✅ **Lower maintenance** - Snowflake manages everything automatically  
- ✅ **Better performance** - Auto-optimized by Snowflake
- ✅ **Cost effective** - Only refreshes when needed
- ✅ **Future-proof** - Built on Snowflake's latest technology

### **For Complex Logic: Streams & Tasks Approach**
- ✅ **Full control** - Custom business logic and error handling
- ✅ **Fine-grained processing** - Step-by-step workflow management
- ✅ **Integration ready** - Can connect to external systems
- ✅ **Advanced monitoring** - Custom logging and audit trails

### **Suggested Path:**
1. **Start**: Standalone setup (`@11_*`) for learning
2. **Compare**: Run comparison script (`@10_*`) to understand differences  
3. **Choose**: Pick Dynamic Tables or Streams/Tasks for your production needs
4. **Deploy**: Use original schema setup for production implementation
