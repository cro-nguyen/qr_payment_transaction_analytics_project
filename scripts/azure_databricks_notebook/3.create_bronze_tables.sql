-- =====================================================================================
-- File: 3_create_bronze_tables.sql
-- Description: Create external tables in Bronze layer for raw data storage
-- Author: Hung Nguyen
-- Created: 2025-07-31
-- =====================================================================================

-- ## Overview
-- This script creates external Parquet tables in the Bronze layer that point to 
-- raw data files stored in Azure Data Lake Storage Gen2. These tables provide
-- structured access to raw QR payment transaction data and lookup tables.

-- ## Prerequisites
-- - Unity Catalog metastore configured
-- - External locations set up for bronze container
-- - Storage credentials configured for ADLS Gen2 access
-- - Raw data files uploaded to respective storage paths

-- =====================================================================================
-- BRONZE LAYER TABLE CREATION
-- =====================================================================================

-- ## Table: alltransaction_fact
-- **Purpose**: Main transaction fact table containing QR payment transaction records
-- **Source**: Raw transaction data from payment processing systems
-- **Format**: Parquet files
-- **Update Pattern**: Daily batch loads

DROP TABLE IF EXISTS qrpayment_dev.bronze.alltransaction_fact;

CREATE TABLE IF NOT EXISTS qrpayment_dev.bronze.alltransaction_fact 
USING PARQUET
OPTIONS (
  path 'abfss://bronze@qrpayment.dfs.core.windows.net/alltransactions_fact/'
)
COMMENT 'External table for raw QR payment transaction data';

-- =====================================================================================

-- ## Table: customerlookup  
-- **Purpose**: Customer reference data for user identification and demographics
-- **Source**: Customer management system exports
-- **Format**: Parquet files
-- **Update Pattern**: Weekly incremental updates

DROP TABLE IF EXISTS qrpayment_dev.bronze.customerlookup;

CREATE TABLE IF NOT EXISTS qrpayment_dev.bronze.customerlookup
USING PARQUET
OPTIONS (
  path 'abfss://bronze@qrpayment.dfs.core.windows.net/CustomerLookup_New/'
)
COMMENT 'External table for customer lookup and reference data';

-- =====================================================================================

-- ## Table: merchant_lookup
-- **Purpose**: Merchant reference data including business information and classifications
-- **Source**: Merchant onboarding and management systems  
-- **Format**: Parquet files
-- **Update Pattern**: Daily updates for active merchants

DROP TABLE IF EXISTS qrpayment_dev.bronze.merchant_lookup;

CREATE TABLE IF NOT EXISTS qrpayment_dev.bronze.merchant_lookup
USING PARQUET
OPTIONS (
  path 'abfss://bronze@qrpayment.dfs.core.windows.net/merchant_list/'
)
COMMENT 'External table for merchant lookup and business classification data';

-- =====================================================================================

-- ## Table: mcc_lookup
-- **Purpose**: Merchant Category Code (MCC) reference data for transaction classification
-- **Source**: Payment network MCC standards and internal classifications
-- **Format**: Parquet files  
-- **Update Pattern**: Monthly updates or as standards change

DROP TABLE IF EXISTS qrpayment_dev.bronze.mcc_lookup;

CREATE TABLE IF NOT EXISTS qrpayment_dev.bronze.mcc_lookup
USING PARQUET
OPTIONS (
  path 'abfss://bronze@qrpayment.dfs.core.windows.net/mcc_vnpay/'
)
COMMENT 'External table for Merchant Category Code lookup data';

-- =====================================================================================
-- VERIFICATION QUERIES
-- =====================================================================================

-- ## Data Validation
-- Use these queries to verify table creation and data accessibility

-- **Check table creation status**
SHOW TABLES IN qrpayment_dev.bronze;

-- **Verify table schemas and properties**
DESCRIBE EXTENDED qrpayment_dev.bronze.alltransaction_fact;
DESCRIBE EXTENDED qrpayment_dev.bronze.customerlookup;
DESCRIBE EXTENDED qrpayment_dev.bronze.merchant_lookup;
DESCRIBE EXTENDED qrpayment_dev.bronze.mcc_lookup;

-- **Test data accessibility (sample queries)**
-- SELECT COUNT(*) as transaction_count FROM qrpayment_dev.bronze.alltransaction_fact;
-- SELECT COUNT(*) as customer_count FROM qrpayment_dev.bronze.customerlookup;
-- SELECT COUNT(*) as merchant_count FROM qrpayment_dev.bronze.merchant_lookup;
-- SELECT COUNT(*) as mcc_count FROM qrpayment_dev.bronze.mcc_lookup;

-- **Check for data freshness**
-- SELECT MAX(ingestion_date) as latest_data FROM qrpayment_dev.bronze.alltransaction_fact;

-- =====================================================================================
-- TROUBLESHOOTING
-- =====================================================================================

-- ## Common Issues and Solutions

-- **Issue**: Table creation fails with permission errors
-- **Solution**: Verify storage credentials and external location configuration
-- SHOW STORAGE CREDENTIALS;
-- SHOW EXTERNAL LOCATIONS;

-- **Issue**: Cannot access data files  
-- **Solution**: Check file paths and container accessibility
-- %fs ls "abfss://bronze@qrpayment.dfs.core.windows.net/"

-- **Issue**: Schema inference fails
-- **Solution**: Ensure Parquet files are valid and accessible
-- **Manually specify schema if needed**

-- =====================================================================================
-- MAINTENANCE NOTES
-- =====================================================================================

-- ## Regular Maintenance Tasks
-- 1. Monitor table usage and performance
-- 2. Validate data quality after each load
-- 3. Update table comments when source systems change
-- 4. Review and optimize file organization in storage
-- 5. Check external location permissions periodically

-- ## Change Log
-- 2025-01-31: Initial creation of bronze layer tables
-- Future updates will be documented here

-- =====================================================================================
-- END OF SCRIPT
-- =====================================================================================
