-- =====================================================================================
-- File: 4.5_create_issuer_lookup_table.sql
-- Description: Create issuer lookup table from transaction data
-- Created: 2025-07-31
-- =====================================================================================

-- ## Overview
-- Creates issuer lookup table with unique card issuer identifiers
-- Source: Distinct don_vi_tt (issuer) values from transaction fact data

-- =====================================================================================
-- CREATE ISSUER LOOKUP TABLE
-- =====================================================================================

-- Create issuer table from distinct transaction data
CREATE TABLE IF NOT EXISTS qrpayment_dev.silver.issuer_lookup AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY don_vi_tt) AS issuer_id,
    don_vi_tt AS issuer_name
FROM (
    SELECT DISTINCT don_vi_tt
    FROM qrpayment_dev.bronze.alltransaction_fact
    WHERE don_vi_tt IS NOT NULL
      AND don_vi_tt != 'nan'
      AND don_vi_tt != ''
) unique_issuer;

-- =====================================================================================
-- VALIDATION
-- =====================================================================================

-- Check record counts
SELECT 
    COUNT(*) AS total_issuers,
    COUNT(DISTINCT issuer_id) AS unique_issuer_ids,
    COUNT(DISTINCT issuer_name) AS unique_issuer_names
FROM qrpayment_dev.silver.issuer_lookup;

-- Sample data
SELECT issuer_id, issuer_name
FROM qrpayment_dev.silver.issuer_lookup
ORDER BY issuer_id
LIMIT 10;

-- Check transaction relationships
SELECT 
    i.issuer_id,
    i.issuer_name,
    COUNT(*) AS transaction_count
FROM qrpayment_dev.silver.issuer_lookup i
LEFT JOIN qrpayment_dev.silver.alltransaction_fact t ON i.issuer_name = t.don_vi_tt
GROUP BY i.issuer_id, i.issuer_name
ORDER BY transaction_count DESC
LIMIT 5;

-- =====================================================================================
