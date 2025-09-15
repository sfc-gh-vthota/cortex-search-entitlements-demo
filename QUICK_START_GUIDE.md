# Quick Start Guide - Cortex Search Entitlements Demo

## 🚀 Quick Setup (10 minutes)

### 1. Generate Sample Data
```bash
cd cortex-search-entitlements-demo
python3 -m venv venv
source venv/bin/activate
pip install pandas numpy
python generate_sample_data.py
```
✅ Creates `credit_card_transactions.csv` with 5,000 sample transactions

### 2. Setup Snowflake (Run in order)
```sql
-- Execute these scripts in Snowflake in order:
@01_setup_database_and_table.sql      -- Creates database and table
@02_load_data.sql                      -- Loads CSV data (upload file to stage first)
@03_create_roles_and_access_policies.sql  -- Creates 15+ roles and access policies
@04_create_sample_users.sql           -- Creates 16 sample users
@05_create_entitlement_view.sql       -- Creates entitlement views with user arrays
@06_create_cortex_search_index.sql    -- Creates Cortex Search indexes
```

### 3. Test the Demo
```sql
@07_sample_queries_and_demo.sql       -- Run comprehensive demo examples
```

## 🎯 Key Demo Examples

### Executive Search (Sees Everything)
```sql
SELECT * FROM TABLE(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        TRANSACTION_SEARCH_SERVICE,
        'high value premium transaction',
        10
    )
)
WHERE ARRAY_CONTAINS('ceo_jane_smith'::VARIANT, AUTHORIZED_USERS);
```

### Regional Manager (Region-Limited)
```sql
SELECT * FROM TABLE(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        TRANSACTION_SEARCH_SERVICE,
        'restaurant declined payment',
        10
    )
)
WHERE ARRAY_CONTAINS('mgr_sarah_davis_us_east'::VARIANT, AUTHORIZED_USERS);
```

### External Partner (Public Data Only)
```sql
SELECT * FROM TABLE(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        TRANSACTION_SEARCH_SERVICE,
        'grocery store transaction',
        10
    )
)
WHERE ARRAY_CONTAINS('partner_vendor_alex_jones'::VARIANT, AUTHORIZED_USERS)
  AND SENSITIVITY_LEVEL = 'PUBLIC';
```

## 📊 Sample Users to Test With

| User | Role | Access Level | What They See |
|------|------|--------------|---------------|
| `ceo_jane_smith` | CEO | Global | All transactions |
| `mgr_sarah_davis_us_east` | Regional Manager | US East only | US East transactions |
| `fraud_analyst_james_taylor` | Fraud Analyst | High/Critical Risk | Risk-based filtering |
| `premium_specialist_john_clark` | Premium Specialist | Premium customers | Premium tier only |
| `partner_vendor_alex_jones` | External Partner | Public data | Public sensitivity only |

## 🔍 Quick Validation Queries

### Check Data Loading
```sql
-- Set context first
USE DATABASE CORTEX_SEARCH_ENTITLEMENTS_DB;
USE SCHEMA ENTITLEMENTS;
USE WAREHOUSE ENTITLEMENTS_WH;

SELECT COUNT(*) FROM CREDIT_CARD_TRANSACTIONS; -- Should be 5000
SELECT COUNT(*) FROM USER_ACCESS_MAPPING;      -- Should be 16
```

### Verify Entitlements
```sql
-- See how many transactions each user can access
SELECT 
    USERNAME,
    COUNT(CASE WHEN ARRAY_CONTAINS(USERNAME::VARIANT, AUTHORIZED_USERS) THEN 1 END) AS ACCESSIBLE_TRANSACTIONS
FROM USER_ACCESS_MAPPING u
CROSS JOIN CORTEX_SEARCH_TRANSACTIONS t
GROUP BY USERNAME
ORDER BY ACCESSIBLE_TRANSACTIONS DESC;
```

### Check Search Services
```sql
SHOW CORTEX SEARCH SERVICES;
```

## ⚡ Common Issues & Solutions

**Search returns no results:**
- Verify search service is refreshed: `SHOW CORTEX SEARCH SERVICES`
- Check user is in AUTHORIZED_USERS array
- Ensure proper role context: `USE ROLE <role_name>`

**Data not loading:**
- Set proper context: `USE DATABASE CORTEX_SEARCH_ENTITLEMENTS_DB; USE SCHEMA ENTITLEMENTS;`
- Verify CSV file uploaded to stage: `LIST @DEMO_STAGE`
- Check file format: `DESCRIBE FILE FORMAT CSV_FORMAT`

**Access policies not working:**
- Test individual policies with simple queries
- Verify role assignments: `SHOW GRANTS TO USER <username>`

## 🎨 Demo Flow Suggestions

1. **Start with Executive**: Show unrestricted access
2. **Switch to Regional Manager**: Demonstrate geographic filtering  
3. **Try Fraud Analyst**: Show risk-based filtering
4. **Test External Partner**: Demonstrate sensitivity filtering
5. **Compare Results**: Same query, different users, different results

## 📁 Files Overview

- `generate_sample_data.py` - Creates realistic transaction data
- `01_setup_database_and_table.sql` - Foundation setup  
- `02_load_data.sql` - Data loading
- `03_create_roles_and_access_policies.sql` - RBAC setup
- `04_create_sample_users.sql` - User creation
- `05_create_entitlement_view.sql` - Entitlement logic
- `06_create_cortex_search_index.sql` - Search setup
- `07_sample_queries_and_demo.sql` - Demo examples

## 🏃‍♂️ Next Steps

After running the demo:
- Explore different search queries
- Modify user permissions to see access changes
- Add new users or roles
- Extend with additional entitlement dimensions
- Monitor search usage patterns

**Total Setup Time: ~10-15 minutes**
**Demo Runtime: ~20-30 minutes for full walkthrough**
