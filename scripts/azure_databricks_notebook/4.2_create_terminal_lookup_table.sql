-- =====================================================================================
-- File: 4.2_create_terminal_lookup_table.sql
-- Description: Create and populate terminal lookup table in Silver layer
-- Created: 2025-01-31
-- =====================================================================================

-- ## Overview
-- Creates terminal lookup table with unique terminal identifiers extracted from transaction data
-- Parses ma_ten_terminal field to extract MID, TID, and terminal name components

-- ## Prerequisites
-- - Silver schema created with managed location
-- - Bronze alltransaction_fact table exists and populated
-- - ma_ten_terminal field contains data in format: 'MID_TID - Terminal Name'

-- =====================================================================================
-- CREATE TERMINAL LOOKUP TABLE
-- =====================================================================================

-- ## Step 1: Create Terminal Lookup Table Structure
-- **Purpose**: Create managed Delta table for terminal reference data
-- **Features**: Unique terminal identifiers, parsed terminal components, audit columns

CREATE TABLE IF NOT EXISTS qrpayment_dev.silver.terminal_lookup (
    terminal_id INT,
    ten_merchant STRING,
    master_mc STRING,
    ma_ten_terminal STRING,
    mid STRING,
    tid STRING,
    terminal_name STRING,
    created_date TIMESTAMP DEFAULT current_timestamp(),
    updated_date TIMESTAMP DEFAULT current_timestamp()
) 
USING DELTA
COMMENT 'Terminal lookup table with unique identifiers and parsed terminal components';

-- =====================================================================================
-- TERMINAL LOOKUP TABLE POPULATION
-- =====================================================================================

-- ## Step 2: Insert New Terminal Records
-- **Purpose**: Add unique terminal combinations from transaction data with parsed components
-- **Logic**: Prevents duplicates and extracts MID, TID, and terminal name from ma_ten_terminal

WITH distinct_new_records AS (
    -- Get distinct terminal combinations from transaction data
    SELECT 
        ten_merchant,
        master_mc,
        ma_ten_terminal,
        -- Parse ma_ten_terminal field: 'MID_TID - Terminal Name'
        CASE 
            WHEN ma_ten_terminal IS NOT NULL AND LOCATE('_', ma_ten_terminal) > 0 THEN
                SUBSTRING(ma_ten_terminal, 1, LOCATE('_', ma_ten_terminal) - 1)
            ELSE NULL
        END AS mid,
        
        CASE 
            WHEN ma_ten_terminal IS NOT NULL 
                 AND LOCATE('_', ma_ten_terminal) > 0 
                 AND LOCATE(' - ', ma_ten_terminal) > 0 THEN
                SUBSTRING(ma_ten_terminal, 
                         LOCATE('_', ma_ten_terminal) + 1, 
                         LOCATE(' - ', ma_ten_terminal) - LOCATE('_', ma_ten_terminal) - 1)
            WHEN ma_ten_terminal IS NOT NULL 
                 AND LOCATE('_', ma_ten_terminal) > 0 
                 AND LOCATE(' - ', ma_ten_terminal) = 0 THEN
                SUBSTRING(ma_ten_terminal, LOCATE('_', ma_ten_terminal) + 1)
            ELSE NULL
        END AS tid,
        
        CASE 
            WHEN ma_ten_terminal IS NOT NULL AND LOCATE(' - ', ma_ten_terminal) > 0 THEN
                TRIM(SUBSTRING(ma_ten_terminal, LOCATE(' - ', ma_ten_terminal) + 3))
            ELSE NULL
        END AS terminal_name,
        
        -- Deduplicate within new records using ROW_NUMBER
        ROW_NUMBER() OVER (
            PARTITION BY 
                COALESCE(ten_merchant, '_'),
                COALESCE(master_mc, '_'),
                COALESCE(ma_ten_terminal, '_')
            ORDER BY ten_merchant, master_mc, ma_ten_terminal
        ) AS row_rank
    FROM qrpayment_dev.bronze.alltransaction_fact fact
    WHERE ma_ten_terminal IS NOT NULL
      AND ma_ten_terminal != 'nan'
      AND NOT EXISTS (
        -- Exclude combinations that already exist in terminal lookup
        SELECT 1
        FROM qrpayment_dev.silver.terminal_lookup lookup
        WHERE 
            COALESCE(fact.ten_merchant, '_') = COALESCE(lookup.ten_merchant, '_')
            AND COALESCE(fact.master_mc, '_') = COALESCE(lookup.master_mc, '_')
            AND COALESCE(fact.ma_ten_terminal, '_') = COALESCE(lookup.ma_ten_terminal, '_')
    )
)

-- Insert unique terminal records with auto-generated terminal_id
INSERT INTO qrpayment_dev.silver.terminal_lookup 
(
    terminal_id,
    ten_merchant,
    master_mc,
    ma_ten_terminal,
    mid,
    tid,
    terminal_name
)
SELECT 
    -- Generate sequential terminal_id starting from max existing + 1
    (SELECT COALESCE(MAX(terminal_id), 0) FROM qrpayment_dev.silver.terminal_lookup) + 
    ROW_NUMBER() OVER (ORDER BY ten_merchant, master_mc, ma_ten_terminal) AS terminal_id,
    ten_merchant,
    master_mc,
    ma_ten_terminal,
    mid,
    tid,
    terminal_name
FROM distinct_new_records
WHERE row_rank = 1  -- Only insert first occurrence of each unique combination
  AND ma_ten_terminal IS NOT NULL;

-- =====================================================================================
-- DATA VALIDATION
-- =====================================================================================

-- ## Step 3: Verify Terminal Lookup Table Population
-- **Purpose**: Validate record counts, parsing results, and data integrity

-- Check total records and unique terminal_id count
SELECT 
    COUNT(*) AS total_records,
    COUNT(terminal_id) AS non_null_terminal_ids,
    COUNT(DISTINCT terminal_id) AS unique_terminal_ids,
    COUNT(DISTINCT CONCAT(COALESCE(ten_merchant, '_'), '|', 
                         COALESCE(master_mc, '_'), '|', 
                         COALESCE(ma_ten_terminal, '_'))) AS unique_combinations
FROM qrpayment_dev.silver.terminal_lookup;

-- Check parsing results
SELECT 
    COUNT(*) AS total_terminals,
    COUNT(mid) AS parsed_mid_count,
    COUNT(tid) AS parsed_tid_count,
    COUNT(terminal_name) AS parsed_name_count,
    COUNT(*) - COUNT(mid) AS unparsed_mid_count
FROM qrpayment_dev.silver.terminal_lookup;

-- Sample parsed data
SELECT 
    ma_ten_terminal,
    mid,
    tid,
    terminal_name,
    ten_merchant
FROM qrpayment_dev.silver.terminal_lookup
WHERE ma_ten_terminal IS NOT NULL
LIMIT 10;

-- =====================================================================================
-- ADDITIONAL VALIDATION QUERIES
-- =====================================================================================

-- ## Optional Data Quality Checks

-- Check for duplicate terminal_ids (should be 0)
-- SELECT terminal_id, COUNT(*) as duplicate_count
-- FROM qrpayment_dev.silver.terminal_lookup
-- GROUP BY terminal_id
-- HAVING COUNT(*) > 1;

-- Check parsing edge cases
-- SELECT 
--     ma_ten_terminal,
--     CASE WHEN LOCATE('_', ma_ten_terminal) = 0 THEN 'No underscore' ELSE 'Has underscore' END as underscore_check,
--     CASE WHEN LOCATE(' - ', ma_ten_terminal) = 0 THEN 'No dash separator' ELSE 'Has dash separator' END as dash_check
-- FROM qrpayment_dev.silver.terminal_lookup
-- WHERE ma_ten_terminal IS NOT NULL
-- LIMIT 20;

-- Check terminals without proper parsing
-- SELECT 
--     ma_ten_terminal,
--     mid,
--     tid, 
--     terminal_name
-- FROM qrpayment_dev.silver.terminal_lookup
-- WHERE ma_ten_terminal IS NOT NULL
--   AND (mid IS NULL OR tid IS NULL)
-- LIMIT 10;

-- Verify sample parsing with known pattern
-- Sample: '000000000001_TEST31 - Test Terminal'
-- Expected: mid='000000000001', tid='TEST31', terminal_name='Test Terminal'

-- =====================================================================================
-- PARSING LOGIC EXPLANATION
-- =====================================================================================

-- ## ma_ten_terminal Parsing Logic
-- **Input Format**: 'MID_TID - Terminal Name'
-- **Example**: '000000000001_TEST31 - Test Terminal'
-- 
-- **Extraction Rules**:
-- 1. **MID**: Everything before first underscore '_'
-- 2. **TID**: Everything between underscore '_' and ' - ' (dash with spaces)
-- 3. **Terminal Name**: Everything after ' - ' (dash with spaces), trimmed
--
-- **Edge Cases Handled**:
-- - Missing underscore: mid=NULL, tid=NULL, terminal_name=NULL
-- - Missing dash separator: tid includes everything after underscore
-- - NULL or 'nan' values: Excluded from processing

-- =====================================================================================
-- END OF SCRIPT
-- =====================================================================================
