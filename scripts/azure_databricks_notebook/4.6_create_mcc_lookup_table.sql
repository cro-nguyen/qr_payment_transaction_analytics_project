-- =====================================================================================
-- File: 4.6_create_mcc_lookup_table.sql
-- Description: Create MCC lookup table from bronze data and transaction codes
-- Created: 2025-07-31
-- =====================================================================================

-- ## Overview
-- Creates MCC (Merchant Category Code) lookup table by:
-- 1. Parsing MCC codes and names from bronze mcc_lookup
-- 2. Adding missing MCC codes from transaction data
-- 3. Enriching missing names using 4-digit prefix matching

-- =====================================================================================
-- CREATE MCC LOOKUP TABLE
-- =====================================================================================

-- Step 1: Create MCC table from bronze data
-- Parse mcc_original field: 'CODE-DESCRIPTION' format
CREATE OR REPLACE TABLE qrpayment_dev.silver.mcc_lookup AS
SELECT 
    SPLIT(mcc_original, '-')[0] AS mcc,
    SPLIT(mcc_original, '-')[1] AS mcc_name
FROM qrpayment_dev.bronze.mcc_lookup
WHERE mcc_original IS NOT NULL
  AND mcc_original != 'nan'
  AND LOCATE('-', mcc_original) > 0;

-- =====================================================================================
-- ADD MISSING MCC CODES FROM TRANSACTION DATA
-- =====================================================================================

-- Step 2: Create temporary view of distinct MCC codes from transactions
CREATE OR REPLACE TEMPORARY VIEW mcc_noi_dia AS
SELECT DISTINCT mcc_noi_dia
FROM qrpayment_dev.silver.alltransaction_fact
WHERE mcc_noi_dia IS NOT NULL 
  AND mcc_noi_dia != 'nan'
  AND mcc_noi_dia != ''
  AND mcc_noi_dia != '3'
ORDER BY mcc_noi_dia;

-- Step 3: Insert missing MCC codes from transaction data
INSERT INTO qrpayment_dev.silver.mcc_lookup (mcc, mcc_name)
SELECT 
    t.mcc_noi_dia AS mcc,
    NULL AS mcc_name
FROM mcc_noi_dia t
LEFT ANTI JOIN qrpayment_dev.silver.mcc_lookup m
    ON t.mcc_noi_dia = m.mcc;

-- =====================================================================================
-- ENRICH MISSING MCC NAMES
-- =====================================================================================

-- Step 4: Enrich missing MCC names using 4-digit prefix matching
-- Logic: Match first 4 digits of MCC codes to find category names
MERGE INTO qrpayment_dev.silver.mcc_lookup AS target
USING (
    SELECT DISTINCT
        LEFT(mcc, 4) as mcc_prefix,
        FIRST_VALUE(mcc_name) OVER (
            PARTITION BY LEFT(mcc, 4) 
            ORDER BY CASE WHEN mcc_name IS NOT NULL THEN 0 ELSE 1 END, mcc
        ) as source_mcc_name
    FROM qrpayment_dev.silver.mcc_lookup
    WHERE mcc_name IS NOT NULL
) AS source
ON LEFT(target.mcc, 4) = source.mcc_prefix
WHEN MATCHED AND target.mcc_name IS NULL THEN
    UPDATE SET target.mcc_name = source.source_mcc_name;

-- =====================================================================================
-- VALIDATION
-- =====================================================================================

-- Check record counts and enrichment results
SELECT 
    COUNT(*) AS total_mcc_codes,
    COUNT(mcc_name) AS codes_with_names,
    COUNT(*) - COUNT(mcc_name) AS codes_without_names,
    COUNT(mcc_name) * 100.0 / COUNT(*) AS enrichment_percentage
FROM qrpayment_dev.silver.mcc_lookup;

-- Sample MCC data
SELECT mcc, mcc_name
FROM qrpayment_dev.silver.mcc_lookup
WHERE mcc_name IS NOT NULL
ORDER BY mcc
LIMIT 10;

-- Check MCC codes without names
SELECT mcc, mcc_name
FROM qrpayment_dev.silver.mcc_lookup
WHERE mcc_name IS NULL
ORDER BY mcc
LIMIT 5;

-- Check transaction usage of MCC codes
SELECT 
    m.mcc,
    m.mcc_name,
    COUNT(*) AS transaction_count
FROM qrpayment_dev.silver.mcc_lookup m
INNER JOIN qrpayment_dev.silver.alltransaction_fact t ON m.mcc = t.mcc_noi_dia
GROUP BY m.mcc, m.mcc_name
ORDER BY transaction_count DESC
LIMIT 10;

-- =====================================================================================
