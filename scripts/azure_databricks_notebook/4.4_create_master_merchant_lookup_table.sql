-- =====================================================================================
-- File: 4.4_create_master_merchant_lookup_table.sql
-- Description: Create master merchant lookup table from terminal data
-- Created: 2025-07-31
-- =====================================================================================

-- ## Overview
-- Creates master merchant lookup table with unique master merchant codes
-- Source: Distinct master_mc values from terminal lookup data

-- =====================================================================================
-- CREATE MASTER MERCHANT LOOKUP TABLE
-- =====================================================================================

-- Create master merchant table from distinct terminal data
CREATE TABLE IF NOT EXISTS qrpayment_dev.silver.master_merchant_lookup AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY master_mc) AS master_merchant_id,
    master_mc
FROM (
    SELECT DISTINCT master_mc
    FROM qrpayment_dev.silver.terminal_lookup
    WHERE master_mc IS NOT NULL
      AND master_mc != 'nan'
      AND master_mc != ''
) unique_master_mc;

-- =====================================================================================
-- VALIDATION
-- =====================================================================================

-- Check record counts
SELECT 
    COUNT(*) AS total_master_merchants,
    COUNT(DISTINCT master_merchant_id) AS unique_master_merchant_ids,
    COUNT(DISTINCT master_mc) AS unique_master_codes
FROM qrpayment_dev.silver.master_merchant_lookup;

-- Sample data
SELECT master_merchant_id, master_mc
FROM qrpayment_dev.silver.master_merchant_lookup
ORDER BY master_merchant_id
LIMIT 10;

-- Check terminal relationships
SELECT 
    m.master_merchant_id,
    m.master_mc,
    COUNT(t.terminal_id) AS terminal_count
FROM qrpayment_dev.silver.master_merchant_lookup m
LEFT JOIN qrpayment_dev.silver.terminal_lookup t ON m.master_mc = t.master_mc
GROUP BY m.master_merchant_id, m.master_mc
ORDER BY terminal_count DESC
LIMIT 5;

-- =====================================================================================
