# Cortex Search Fine-Grained Entitlements Demo

> **🚀 Quick Start Available!**  
> For a step-by-step guided tutorial, see [README_QUICKSTART.md](README_QUICKSTART.md) which follows Snowflake Labs format with estimated durations and structured steps.

## Overview

A comprehensive demonstration of implementing fine-grained access control and entitlements with Snowflake Cortex Search functionality using credit card transaction data.

This demo showcases how to build a sophisticated entitlements system where search results are dynamically filtered based on user roles, permissions, and organizational context. It demonstrates real-world scenarios where different users should only see transaction data they're authorized to access.

## 📋 Demo Components

| Component | Description | Files |
|-----------|-------------|-------|
| **Sample Data** | 5,000+ realistic credit card transactions | `generate_sample_data.py`, `credit_card_transactions.csv` |
| **Database Setup** | Snowflake tables, schemas, warehouses | `01_setup_database_and_table.sql` |
| **Data Loading** | CSV import and validation | `02_load_data.sql` |
| **RBAC System** | 15+ roles and row access policies | `03_create_roles_and_access_policies.sql` |
| **User Management** | 16 sample users with personas | `04_create_sample_users.sql` |
| **Entitlements** | User access arrays and views | `05_create_entitlement_view.sql` |
| **Search Services** | Cortex Search indexes | `06_create_cortex_search_index.sql` |
| **Demo Queries** | 12+ example scenarios | `07_sample_queries_and_demo.sql` |

## 🎯 Key Features

### Multi-Dimensional Access Control
- **Geographic**: Regional access (US_EAST, US_WEST, EUROPE, ASIA_PAC)
- **Customer Tiers**: Tier-based filtering (PREMIUM, GOLD, SILVER, STANDARD)
- **Sensitivity**: Data classification (RESTRICTED, CONFIDENTIAL, INTERNAL, PUBLIC)
- **Risk Levels**: Risk-based access (CRITICAL, HIGH, MEDIUM, LOW)
- **Departments**: Organizational units (FINANCE, FRAUD, COMPLIANCE, OPERATIONS, CUSTOMER_SERVICE)

### Sophisticated Role System
- **Executive Level**: Global access to all data
- **Regional Managers**: Geographic-based filtering
- **Department Staff**: Function-specific access
- **Specialists**: Customer tier or risk-based access
- **External Partners**: Public data only

### Production-Ready Architecture
- **Row Access Policies**: Three complementary policies for comprehensive control
- **User Entitlement Arrays**: Pre-calculated access lists for optimal search performance  
- **Multiple Search Services**: Optimized indexes for different use cases
- **Application Functions**: Ready-to-use search functions with embedded entitlements

## ⚡ Quick Setup (15 minutes)

> **Prerequisites**  
> - Snowflake account with Cortex Search enabled
> - ACCOUNTADMIN privileges
> - Python 3.8+ for data generation

### 1. Generate Data
```bash
cd cortex-search-entitlements-demo
python3 -m venv venv
source venv/bin/activate
pip install pandas numpy
python generate_sample_data.py
```

### 2. Setup Snowflake
Execute SQL scripts in order:
```sql
-- Run these in Snowsight in sequence:
@01_setup_database_and_table.sql      -- Creates database/schema/table
@02_load_data.sql                      -- Loads CSV data  
@03_create_roles_and_access_policies.sql  -- Creates RBAC system
@04_create_sample_users.sql           -- Creates sample users
@05_create_entitlement_view.sql       -- Creates entitlement logic
@06_create_cortex_search_index.sql    -- Creates search services
@07_sample_queries_and_demo.sql       -- Demo examples
```

> **⚠️ Important**  
> Upload `credit_card_transactions.csv` to the Snowflake stage before running script 02.

## 🔍 Demo Scenarios

### Executive Search (Global Access)
```sql
-- CEO sees all transactions
SELECT * FROM TABLE(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        TRANSACTION_SEARCH_SERVICE,
        'high value premium transaction',
        10
    )
)
WHERE ARRAY_CONTAINS('ceo_jane_smith'::VARIANT, AUTHORIZED_USERS);
```

### Regional Manager (Geographic Filtering) 
```sql
-- Manager sees only US East transactions
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
-- Partner sees only public sensitivity data
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

## 👥 Sample Users & Access Patterns

| User Persona | Role | Access Pattern | Visible Transactions |
|--------------|------|----------------|---------------------|
| **Jane Smith (CEO)** | Executive | Global access | ~4,500 (90%) |
| **Sarah Davis** | US East Manager | Regional + Department | ~1,200 (25%) |
| **James Taylor** | Fraud Analyst | Risk-based + Sensitivity | ~800 (16%) |
| **John Clark** | Premium Specialist | Customer tier + Sensitivity | ~600 (12%) |
| **Alex Jones** | External Partner | Public data only | ~1,000 (20%) |

## 🏗️ Technical Architecture

### Data Layer
```
CORTEX_SEARCH_ENTITLEMENTS_DB
├── ENTITLEMENTS (Schema)
    ├── CREDIT_CARD_TRANSACTIONS (Main table)
    ├── USER_ACCESS_MAPPING (User permissions)
    └── Row Access Policies (3 complementary policies)
```

### Entitlement Layer
```
Entitlement Views
├── TRANSACTION_USER_ACCESS (Access calculations)
├── TRANSACTIONS_WITH_ENTITLEMENTS (User arrays)
└── CORTEX_SEARCH_TRANSACTIONS (Search-optimized)
```

### Search Layer
```
Cortex Search Services
├── TRANSACTION_SEARCH_SERVICE (Main search)
├── HIGH_VALUE_TRANSACTION_SEARCH (High-value focus)  
└── FRAUD_RISK_TRANSACTION_SEARCH (Risk analysis)
```

## 📊 Validation & Verification

### Check Data Loading
```sql
SELECT COUNT(*) FROM CREDIT_CARD_TRANSACTIONS;  -- Should be 5000
SELECT COUNT(*) FROM USER_ACCESS_MAPPING;       -- Should be 16
SHOW CORTEX SEARCH SERVICES;                    -- Should show 3 services
```

### Verify Entitlements  
```sql
-- Compare access levels
SELECT 
    USERNAME, ACCESS_LEVEL,
    COUNT(CASE WHEN ARRAY_CONTAINS(USERNAME::VARIANT, AUTHORIZED_USERS) THEN 1 END) AS ACCESSIBLE_TRANSACTIONS
FROM USER_ACCESS_MAPPING u
CROSS JOIN CORTEX_SEARCH_TRANSACTIONS t  
GROUP BY USERNAME, ACCESS_LEVEL
ORDER BY ACCESSIBLE_TRANSACTIONS DESC;
```

### Test Search Services
```sql
-- Verify search service status
SHOW CORTEX SEARCH SERVICES;

-- Quick search test
SELECT COUNT(*) FROM TABLE(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        TRANSACTION_SEARCH_SERVICE,
        'credit card transaction',
        100
    )
);
```

## 🔧 Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|---------|
| Search returns no results | Service not refreshed | Wait for `SHOW CORTEX SEARCH SERVICES` to show RUNNING |
| Empty entitlement arrays | User mapping incomplete | Verify USER_ACCESS_MAPPING has all users |  
| Access denied errors | Missing role grants | Check role assignments and permissions |
| Data load failures | File format issues | Verify CSV format and stage upload |

### Debug Queries
```sql
-- Check user access for specific transaction
SELECT 
    USERNAME, HAS_ACCESS, 
    HAS_REGION_ACCESS, HAS_SENSITIVITY_ACCESS
FROM TRANSACTION_USER_ACCESS 
WHERE TRANSACTION_ID = 'TXN_EXAMPLE123'
ORDER BY HAS_ACCESS DESC;

-- View search service details
DESCRIBE CORTEX SEARCH SERVICE TRANSACTION_SEARCH_SERVICE;

-- Check row access policy status
SHOW ROW ACCESS POLICIES ON CREDIT_CARD_TRANSACTIONS;
```

## 🚀 Extension Ideas

### Advanced Features
- **Time-based Access**: Business hours or temporary access controls
- **Dynamic Risk Scoring**: Real-time fraud detection integration  
- **Data Masking**: Sensitive field obfuscation based on user clearance
- **ML Access Optimization**: Machine learning-driven access recommendations
- **Geofencing**: IP-based geographic access restrictions
- **Audit Workflows**: Approval processes for sensitive data access

### Integration Patterns
- **Streamlit Apps**: Interactive dashboards with embedded search
- **REST APIs**: Snowflake functions exposed as web services
- **Identity Integration**: SAML/OAuth integration with corporate directories
- **Real-time Alerting**: Unusual search pattern detection and notification
- **Mobile Applications**: Secure mobile search with biometric authentication

## 📚 Additional Resources

### Documentation
- [Snowflake Cortex Search Guide](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search)
- [Row Access Policies Documentation](https://docs.snowflake.com/en/user-guide/security-row-access-policies)  
- [Role-Based Access Control Best Practices](https://docs.snowflake.com/en/user-guide/security-access-control-overview)

### Related Demos
- [Cortex Search Tutorial](https://quickstarts.snowflake.com/guide/cortex_search_getting_started/)
- [Data Governance with Snowflake](https://quickstarts.snowflake.com/guide/data_governance_with_snowflake/)
- [Advanced Security Features](https://quickstarts.snowflake.com/guide/advanced_security_features/)

## 📄 License & Support

This demo is provided under MIT License for educational and demonstration purposes.

> **💡 Need Help?**  
> - Check the [troubleshooting section](#🔧-troubleshooting) above
> - Review validation queries for debugging
> - [Open an issue](https://github.com/sfc-gh-mbaron/mb_demos/issues) for questions or bugs

---

**Built with ❄️ by Snowflake Labs community**