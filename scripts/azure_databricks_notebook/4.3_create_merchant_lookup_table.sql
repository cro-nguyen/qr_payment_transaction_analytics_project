-- =====================================================================================
-- File: 4.3_create_merchant_lookup_table.sql
-- Description: Create and populate merchant lookup table in Silver layer
-- Created: 2025-01-31
-- =====================================================================================

-- ## Overview
-- Creates merchant lookup table with unique merchant identifiers derived from terminal data
-- Enriches merchant data with merchant names from bronze layer merchant reference

-- ## Prerequisites
-- - Silver schema created with managed location
-- - terminal_lookup table exists and populated in silver schema
-- - Bronze merchant_lookup table exists with merchant reference data

-- =====================================================================================
-- CREATE MERCHANT LOOKUP TABLE
-- =====================================================================================

-- ## Step 1: Create Merchant Lookup Table from Terminal Data
-- **Purpose**: Generate unique merchant records from distinct terminal combinations
-- **Logic**: Extract unique mid and ten_merchant combinations from terminal data

CREATE OR REPLACE TABLE qrpayment_dev.silver.merchant_lookup AS
SELECT 
    -- Generate sequential merchant_id for unique identification
    ROW_NUMBER() OVER (ORDER BY mid, ten_merchant) AS merchant_id,
    mid,
    ten_merchant
FROM (
    -- Get distinct merchant combinations from terminal lookup
    SELECT DISTINCT 
        mid,
        ten_merchant
    FROM qrpayment_dev.silver.terminal_lookup
    WHERE mid IS NOT NULL
      AND mid != 'nan'
      AND mid != ''
) unique_merchants;

-- Add table comment for documentation
COMMENT ON TABLE qrpayment_dev.silver.merchant_lookup IS 
'Merchant lookup table with unique identifiers derived from terminal data';

-- =====================================================================================
-- ENRICH MERCHANT DATA
-- =====================================================================================

-- ## Step 2: Add Merchant Name Column
-- **Purpose**: Add column for enriched merchant name from reference data

ALTER TABLE qrpayment_dev.silver.merchant_lookup
ADD COLUMN merchant_name STRING
COMMENT 'Enriched merchant name from bronze merchant reference data';

-- ## Step 3: Update Merchant Names from Bronze Reference Data
-- **Purpose**: Enrich merchant records with official merchant names
-- **Source**: Bronze layer merchant_lookup table

UPDATE qrpayment_dev.silver.merchant_lookup m
SET merchant_name = (
    SELECT t.merchant_name
    FROM qrpayment_dev.bronze.merchant_lookup t
    WHERE t.merchant_code = m.mid
    LIMIT 1
)
WHERE merchant_name IS NULL;

-- =====================================================================================
-- DATA VALIDATION
-- =====================================================================================

-- ## Step 4: Verify Merchant Lookup Table Population
-- **Purpose**: Validate record counts, enrichment results, and data integrity

-- Check total records and unique merchant_id count
SELECT 
    COUNT(*) AS total_merchants,
    COUNT(merchant_id) AS non_null_merchant_ids,
    COUNT(DISTINCT merchant_id) AS unique_merchant_ids,
    COUNT(DISTINCT mid) AS unique_mids,
    COUNT(merchant_name) AS enriched_merchant_names,
    COUNT(*) - COUNT(merchant_name) AS missing_merchant_names
FROM qrpayment_dev.silver.merchant_lookup;

-- Sample merchant data with enrichment results
SELECT 
    merchant_id,
    mid,
    ten_merchant,
    merchant_name,
    CASE 
        WHEN merchant_name IS NOT NULL THEN 'Enriched'
        ELSE 'Not Enriched'
    END AS enrichment_status
FROM qrpayment_dev.silver.merchant_lookup
ORDER BY merchant_id
LIMIT 10;

-- Check merchants without enrichment
SELECT 
    COUNT(*) AS merchants_without_names,
    COUNT(DISTINCT mid) AS unique_mids_without_names
FROM qrpayment_dev.silver.merchant_lookup
WHERE merchant_name IS NULL;

-- =====================================================================================
-- ADDITIONAL VALIDATION QUERIES
-- =====================================================================================

-- ## Optional Data Quality Checks

-- Check for duplicate merchant_ids (should be 0)
-- SELECT merchant_id, COUNT(*) as duplicate_count
-- FROM qrpayment_dev.silver.merchant_lookup
-- GROUP BY merchant_id
-- HAVING COUNT(*) > 1;

-- Check for duplicate mid values (should be unique)
-- SELECT mid, COUNT(*) as duplicate_count
-- FROM qrpayment_dev.silver.merchant_lookup
-- GROUP BY mid
-- HAVING COUNT(*) > 1;

-- Compare ten_merchant vs merchant_name for enriched records
-- SELECT 
--     mid,
--     ten_merchant,
--     merchant_name,
--     CASE 
--         WHEN ten_merchant = merchant_name THEN 'Match'
--         WHEN ten_merchant != merchant_name THEN 'Different'
--         ELSE 'Unknown'
--     END AS name_comparison
-- FROM qrpayment_dev.silver.merchant_lookup
-- WHERE merchant_name IS NOT NULL
-- LIMIT 15;

-- Check bronze merchant reference data availability
-- SELECT 
--     COUNT(*) AS total_bronze_merchants,
--     COUNT(DISTINCT merchant_code) AS unique_merchant_codes
-- FROM qrpayment_dev.bronze.merchant_lookup;

-- =====================================================================================
-- RELATIONSHIP VALIDATION
-- =====================================================================================

-- ## Step 5: Verify Terminal-Merchant Relationships
-- **Purpose**: Ensure proper relationships between terminal and merchant lookup tables

-- Check terminal count per merchant
SELECT 
    m.merchant_id,
    m.mid,
    m.ten_merchant,
    COUNT(t.terminal_id) AS terminal_count
FROM qrpayment_dev.silver.merchant_lookup m
LEFT JOIN qrpayment_dev.silver.terminal_lookup t ON m.mid = t.mid
GROUP BY m.merchant_id, m.mid, m.ten_merchant
ORDER BY terminal_count DESC
LIMIT 10;

-- Verify all terminals have corresponding merchants
-- SELECT 
--     COUNT(*) AS terminals_without_merchants
-- FROM qrpayproject_dev.silver.terminal_lookup t
-- WHERE NOT EXISTS (
--     SELECT 1 
--     FROM qrpayproject_dev.silver.merchant_lookup m 
--     WHERE m.mid = t.mid
-- );

-- =====================================================================================
-- TABLE STATISTICS
-- =====================================================================================

-- ## Final Statistics Summary
SELECT 
    'Merchant Summary' AS metric_category,
    COUNT(*) AS total_count,
    COUNT(DISTINCT mid) AS unique_mids,
    MIN(merchant_id) AS min_merchant_id,
    MAX(merchant_id) AS max_merchant_id,
    COUNT(merchant_name) * 100.0 / COUNT(*) AS enrichment_percentage
FROM qrpayment_dev.silver.merchant_lookup

UNION ALL

SELECT 
    'Terminal Relationship',
    COUNT(DISTINCT t.mid),
    COUNT(DISTINCT t.terminal_id),
    NULL,
    NULL,
    NULL
FROM qrpayment_dev.silver.terminal_lookup t
WHERE t.mid IS NOT NULL;

-- =====================================================================================
-- END OF SCRIPT
-- =====================================================================================
