# Naming Conventions - VNPAY QR Payment Analytics Project

## Overview

This document outlines naming conventions for schemas, tables, views, and columns in our QR Payment Analytics Data Lakehouse following medallion architecture (Bronze-Silver-Gold layers).

## General Principles

- **Case**: Use `snake_case` with lowercase and underscores
- **Language**: English with Vietnamese business terms preserved from source systems
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

**Alternative Pattern**: `<project>_<layer>_<engine>`

```sql
vnpay_silver_spark     -- For Spark-based processing
```

## Table Naming Conventions

### Bronze Layer (Raw Data)
**Pattern**: `<source_entity>` - preserve original names

```sql
qrpayment_dev.bronze.alltransaction_fact
```

### Silver Layer (Processed Data)  
**Pattern**: `<business_entity>lookup` or `<business_entity>_<type>`

```sql
qrpayment_dev.silver.alltransaction_fact
qrpayment_dev.silver.customerlookup
qrpayment_dev.silver.terminallookup
qrpayment_dev.silver.merchantlookup
qrpayment_dev.silver.mastermerchandlookup
qrpayment_dev.silver.issuerlookup
qrpayment_dev.silver.mcclookup
```

**Alternative Spark Schema**:
```sql
vnpay_silver_spark.alltransaction_fact
```

### Gold Layer (Business Ready)
**Pattern**: `<business_entity>_lookup` for dimension tables, `<business_entity>_<type>` for facts

**Tables:**
```sql
vnpay_gold_spark.alltransaction_fact
vnpay_gold_spark.issuer_lookup
vnpay_gold_spark.master_merchant_lookup
vnpay_gold_spark.mcc_lookup
vnpay_gold_spark.merchant_lookup
vnpay_gold_spark.terminal_lookup
vnpay_gold_spark.user_lookup
```

**Views (Future):**
```sql
vnpay_gold_spark.vw_alltransaction_fact
vnpay_gold_spark.vw_customer_analytics
vnpay_gold_spark.vw_merchant_performance
```

## Column Naming

### Key Columns
```sql
-- Primary/Foreign Keys
user_id                 -- Customer identifier
terminal_id            -- Terminal identifier  
merchant_id            -- Merchant identifier
issuer_id              -- Card issuer identifier
```

### Business Columns (Vietnamese Source Terms Preserved)
```sql
-- Customer Information
ten_kh_thanh_toan      -- Customer payment name
so_dien_thoai          -- Original phone number
so_dien_thoai_moi      -- Standardized phone number (Vietnamese format)
don_vi_tt              -- Payment unit
so_tai_khoan           -- Account number
loai_the_tai_khoan     -- Account card type

-- Transaction Data
ma_gd                  -- Transaction ID
ma_thanh_toan          -- Payment ID
thoi_gian_thanh_toan   -- Payment timestamp
so_tien_truoc_km       -- Amount before promotion
so_tien_sau_km         -- Amount after promotion
```

### Technical Columns
```sql
-- Audit and Control
created_date           -- Record creation timestamp
updated_date           -- Last update timestamp
ingestion_date         -- Data load timestamp
dwh_load_date          -- Data warehouse load date

-- Partitioning Columns
year                   -- Partition by year (string format)
month                  -- Partition by month (MM format string)
```

## File System Naming

### ADLS Gen2 Container Structure
```
abfss://bronze@vnpayproject.dfs.core.windows.net/
abfss://silver@vnpayproject.dfs.core.windows.net/
abfss://gold@vnpayproject.dfs.core.windows.net/
```

### Unity Catalog Paths
```
abfss://silver@vnpayproject.dfs.core.windows.net/__unitystorage/schemas/{schema-guid}/tables/{table-guid}
```

### Standard Paths
```
abfss://silver@vnpayproject.dfs.core.windows.net/alltransaction_fact
```

## Lookup Table Patterns

### Customer Dimension
```sql
CREATE TABLE vnpay_silver_spark.user_lookup (
    user_id INT,                    -- Surrogate key
    ten_kh_thanh_toan STRING,       -- Customer name
    so_dien_thoai STRING,           -- Original phone
    so_dien_thoai_moi STRING,       -- Standardized phone
    don_vi_tt STRING,               -- Payment unit
    so_tai_khoan STRING,            -- Account number
    loai_the_tai_khoan STRING,      -- Account type
    created_date TIMESTAMP,         -- Creation timestamp
    updated_date TIMESTAMP          -- Update timestamp
)
```

## Vietnamese Phone Number Standardization

### Update Pattern for Phone Numbers
```sql
-- MobiFone prefix conversions
WHEN SUBSTRING(so_dien_thoai, 1, 4) = '0120' THEN CONCAT('070', SUBSTRING(so_dien_thoai, 5))
WHEN SUBSTRING(so_dien_thoai, 1, 4) = '0121' THEN CONCAT('079', SUBSTRING(so_dien_thoai, 5))

-- VinaPhone prefix conversions  
WHEN SUBSTRING(so_dien_thoai, 1, 4) = '0123' THEN CONCAT('083', SUBSTRING(so_dien_thoai, 5))

-- Viettel prefix conversions
WHEN SUBSTRING(so_dien_thoai, 1, 4) = '0162' THEN CONCAT('032', SUBSTRING(so_dien_thoai, 5))
```

## Deduplication Patterns

### Customer Deduplication Logic
```sql
-- Composite key for uniqueness
PARTITION BY 
    COALESCE(so_dien_thoai, '_'),
    COALESCE(don_vi_tt, '_'),
    COALESCE(so_tai_khoan, '_')
```

## Data Quality Patterns

### Null Handling
```sql
-- Consistent null representation
COALESCE(column_name, '_')  -- For string comparisons
COALESCE(column_name, 0)    -- For numeric comparisons
```

### Surrogate Key Generation
```sql
-- Auto-incrementing surrogate keys
(SELECT COALESCE(MAX(user_id), 0) FROM target_table) + 
ROW_NUMBER() OVER (ORDER BY natural_key_columns)
```

## Best Practices

1. **Vietnamese Business Terms**: Preserve original Vietnamese column names from source systems
2. **Surrogate Keys**: Use auto-generated integer IDs for dimension tables
3. **Phone Standardization**: Apply Vietnamese telecom prefix conversions
4. **Partitioning Strategy**: Use year/month string partitions for time-based queries
5. **Deduplication**: Use composite natural keys for uniqueness validation
6. **ADLS Integration**: Follow abfss:// protocol for Data Lake Storage Gen 2
7. **Delta Lake**: Use managed Delta tables for Silver and Gold layers
8. **Unity Catalog**: Support both Unity Catalog and standard schema paths
