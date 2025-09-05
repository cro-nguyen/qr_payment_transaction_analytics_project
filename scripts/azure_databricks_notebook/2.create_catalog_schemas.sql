-- =====================================================================================
-- File: 01_setup_catalog_schemas.sql
-- Description: Create Unity Catalog and schemas for medallion architecture
-- Created: 2025-07-31
-- =====================================================================================

-- ## Overview
-- Creates project catalog and schemas for Bronze, Silver, and Gold layers
-- Prerequisites: External locations must be configured first

-- =====================================================================================
-- CREATE PROJECT CATALOG
-- =====================================================================================

-- Create main project catalog (no managed location)
CREATE CATALOG IF NOT EXISTS qrpayment_dev
COMMENT 'QR Payment Analytics project catalog for development environment';

-- Set current catalog context
USE CATALOG qrpayment_dev;

-- =====================================================================================
-- CREATE SCHEMAS WITH MANAGED LOCATIONS
-- =====================================================================================

-- Bronze Schema - Raw Data Layer
CREATE SCHEMA IF NOT EXISTS bronze
  MANAGED LOCATION "abfss://bronze@projectstorage.dfs.core.windows.net/"
  COMMENT 'Bronze layer schema for raw data storage';

-- Silver Schema - Processed Data Layer  
CREATE SCHEMA IF NOT EXISTS silver
  MANAGED LOCATION "abfss://silver@projectstorage.dfs.core.windows.net/"
  COMMENT 'Silver layer schema for cleaned and processed data';

-- Gold Schema - Business Ready Data Layer
CREATE SCHEMA IF NOT EXISTS gold
  MANAGED LOCATION "abfss://gold@projectstorage.dfs.core.windows.net/"
  COMMENT 'Gold layer schema for business-ready analytical data';

-- =====================================================================================
-- VERIFICATION
-- =====================================================================================

-- List all schemas in catalog
SHOW SCHEMAS IN qrpayment_dev;

-- Describe schema details
DESCRIBE SCHEMA EXTENDED qrpayment_dev.bronze;
DESCRIBE SCHEMA EXTENDED qrpayment_dev.silver;
DESCRIBE SCHEMA EXTENDED qrpayment_dev.gold;

-- Verify current catalog context
SELECT current_catalog(), current_schema();

-- =====================================================================================
-- GRANT PERMISSIONS (Optional)
-- =====================================================================================

-- Grant permissions to data engineering team
-- GRANT USE CATALOG ON CATALOG qrpayment_dev TO `data-engineers`;
-- GRANT CREATE SCHEMA ON CATALOG qrpayment_dev TO `data-engineers`;
-- GRANT USE SCHEMA ON SCHEMA qrpayment_dev.bronze TO `data-engineers`;
-- GRANT USE SCHEMA ON SCHEMA qrpayment_dev.silver TO `data-engineers`;
-- GRANT USE SCHEMA ON SCHEMA qrpayment_dev.gold TO `business-analysts`;

-- =====================================================================================
