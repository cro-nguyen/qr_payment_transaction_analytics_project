-- =====================================================================================
-- File: 01_setup_external_locations.sql
-- Description: Configure external locations for Unity Catalog medallion architecture
-- Created: 2025-01-31
-- =====================================================================================

-- ## Overview
-- Creates external locations for Bronze, Silver, and Gold layers in Unity Catalog
-- Prerequisites: Storage credential 'databricks-project-storage-credential' must exist

-- =====================================================================================
-- VERIFY STORAGE CREDENTIAL
-- =====================================================================================

SHOW STORAGE CREDENTIALS;

-- =====================================================================================
-- CREATE EXTERNAL LOCATIONS
-- =====================================================================================

-- Bronze Layer - Raw Data
CREATE EXTERNAL LOCATION IF NOT EXISTS databricks_project_ext_bronze
  URL 'abfss://bronze@projectstorage.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL `databricks-project-storage-credential`)
  COMMENT 'Bronze layer - raw data storage';

-- Silver Layer - Processed Data  
CREATE EXTERNAL LOCATION IF NOT EXISTS databricks_project_ext_silver
  URL 'abfss://silver@projectstorage.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL `databricks-project-storage-credential`)
  COMMENT 'Silver layer - processed data storage';

-- Gold Layer - Business Ready Data
CREATE EXTERNAL LOCATION IF NOT EXISTS databricks_project_ext_gold
  URL 'abfss://gold@projectstorage.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL `databricks-project-storage-credential`)
  COMMENT 'Gold layer - business-ready data storage';

-- =====================================================================================
-- VERIFICATION
-- =====================================================================================

-- List all external locations
SHOW EXTERNAL LOCATIONS;

-- Describe each external location
DESC EXTERNAL LOCATION databricks_project_ext_bronze;
DESC EXTERNAL LOCATION databricks_project_ext_silver;
DESC EXTERNAL LOCATION databricks_project_ext_gold;

-- Test storage access (use in notebook)
-- %fs ls "abfss://bronze@projectstorage.dfs.core.windows.net/"
-- %fs ls "abfss://silver@projectstorage.dfs.core.windows.net/"
-- %fs ls "abfss://gold@projectstorage.dfs.core.windows.net/"

-- =====================================================================================
-- TROUBLESHOOTING
-- =====================================================================================

-- If storage credential missing:
-- CREATE STORAGE CREDENTIAL `databricks-project-storage-credential` USING AZURE_MANAGED_IDENTITY;

-- If permission denied:  
-- Verify Azure RBAC roles: Storage Blob Data Contributor/Reader on storage account

-- =====================================================================================
