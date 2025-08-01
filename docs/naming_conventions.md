# Naming Conventions - QR Payment Analytics Project

## Overview

This document outlines naming conventions for schemas, tables, views, and columns in our QR Payment Analytics Data Lakehouse following medallion architecture (Bronze-Silver-Gold layers).

## General Principles

- **Case**: Use `snake_case` with lowercase and underscores
- **Language**: English with Vietnamese business terms where appropriate
- **Descriptive**: Names should clearly indicate purpose and content
- **Consistent**: Maintain patterns across all layers
- **No Reserved Words**: Avoid SQL reserved keywords

## Schema Naming

**Pattern**: `<project>_<environment>.<layer>`

```sql
qrpayment_dev.bronze
qrpayment_dev.silver  
qrpayment_dev.gold
```

## Table Naming Conventions

### Bronze Layer (Raw Data)
**Pattern**: `<source_entity>` - preserve original names

```sql
qrpayment_dev.bronze.alltransaction_fact
qrpayment_dev.bronze.customer_lookup
```

### Silver Layer (Processed Data)  
**Pattern**: `<business_entity>_<type>`

```sql
qrpayment_dev.silver.alltransaction_fact
qrpayment_dev.silver.customer_lookup
qrpayment_dev.silver.terminal_lookup
qrpayment_dev.silver.merchant_lookup
qrpayment_dev.silver.issuer_lookup
```

### Gold Layer (Business Ready)
**Pattern**: `<prefix>_<business_entity>`

| Prefix | Purpose | Example |
|--------|---------|---------|
| `vw_` | Views for BI | `vw_alltransaction_fact` |
| `dim_` | Dimension tables | `dim_customer` |
| `fact_` | Fact tables | `fact_transactions` |
| `report_` | Report tables | `report_daily_summary` |

```sql
qrpayment_dev.gold.vw_alltransaction_fact
qrpayment_dev.gold.vw_customer_analytics
qrpayment_dev.gold.vw_fraud_detection
```

## Column Naming

### Key Columns
```sql
-- Primary/Foreign Keys
user_id           -- Customer identifier
terminal_id       -- Terminal identifier  
merchant_id       -- Merchant identifier
issuer_id         -- Card issuer identifier
```

### Business Columns (Vietnamese Terms)
```sql
-- Transaction Identifiers
ma_gd                    -- Transaction ID
ma_thanh_toan           -- Payment ID  
ma_don_hang             -- Order ID

-- Merchant Info
ten_merchant            -- Merchant name
ma_ten_terminal         -- Terminal code

-- Payment Data
so_tien_truoc_km        -- Amount before promotion
so_tien_sau_km          -- Amount after promotion
phuong_thuc_thanh_toan  -- Payment method
kenh_thanh_toan         -- Payment channel

-- Customer Info
so_dien_thoai           -- Phone number
so_tai_khoan            -- Account number
loai_the_tai_khoan      -- Account type

-- Status & Timing
trang_thai              -- Transaction status
thoi_gian_thanh_toan    -- Payment timestamp
```

### Technical Columns
```sql
ingestion_date          -- Load timestamp
dwh_load_date          -- System load date
dwh_created_date       -- Record creation
dwh_updated_date       -- Last update
```

## View Naming (Gold Layer)

**Pattern**: `vw_<business_purpose>` - for Power BI consumption

```sql
vw_alltransaction_fact     -- All transaction reporting
vw_customer_analytics      -- Customer analysis
vw_merchant_performance    -- Merchant metrics
vw_fraud_detection         -- Fraud monitoring
vw_payment_trends          -- Payment analysis
```

## Stored Procedures

**Pattern**: `sp_<action>_<layer>` or `load_<layer>`

```sql
load_bronze                -- Bronze layer loading
load_silver                -- Silver layer processing  
load_gold                  -- Gold layer transformation
sp_transform_silver_fact   -- Specific transformations
```

## Implementation Examples

### Table Structure
```sql
-- Bronze: External Parquet Tables
CREATE TABLE qrpayment_dev.bronze.customer_lookup
USING PARQUET
OPTIONS (path 'abfss://bronze@storage.dfs.core.windows.net/customer_lookup/');

-- Silver: Managed Delta Tables
CREATE TABLE qrpayment_dev.silver.alltransaction_fact AS
SELECT 
    CAST(ma_gd AS INT) AS ma_gd,
    ma_thanh_toan,
    ten_merchant,
    UserID AS user_id,
    current_timestamp() AS ingestion_date
FROM qrpayment_dev.bronze.alltransaction_fact;

-- Gold: Views for Power BI
CREATE VIEW qrpayment_dev.gold.vw_alltransaction_fact AS
SELECT ma_gd, ma_thanh_toan, ten_merchant, user_id
FROM OPENROWSET(
    BULK 'abfss://silver@storage.dfs.core.windows.net/alltransaction_fact/',
    FORMAT = 'DELTA'
);
```

### Key Relationships
```sql
-- Consistent foreign key naming
alltransaction_fact.user_id → customer_lookup.user_id
alltransaction_fact.terminal_id → terminal_lookup.terminal_id  
alltransaction_fact.merchant_id → merchant_lookup.merchant_id
```

## Data Quality Patterns

```sql
-- Handle 'nan' values consistently
CASE WHEN column_name = 'nan' THEN NULL ELSE column_name END

-- MERGE operations for data enrichment
MERGE INTO silver.alltransaction_fact 
USING silver.customer_lookup 
ON matching_conditions
```

## Best Practices

1. **Layer Consistency**: Use appropriate patterns for each medallion layer
2. **Business Alignment**: Preserve Vietnamese business terminology
3. **BI Optimization**: Gold layer views should be Power BI friendly  
4. **Data Lineage**: Maintain clear naming relationships between layers
5. **Tool Compatibility**: Ensure names work with Databricks, Synapse, Power BI
