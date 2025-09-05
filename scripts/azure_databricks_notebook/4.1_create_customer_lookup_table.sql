-- =====================================================================================
-- File: 4.1_create_customer_lookup_table.sql
-- Description: Create and populate customer lookup table in Silver layer
-- Created: 2025-07-31
-- =====================================================================================

-- ## Overview
-- Creates customer lookup table with unique customer identifiers and phone number standardization
-- Prevents duplicate records and applies Vietnamese phone number format updates

-- ## Prerequisites
-- - Silver schema created with managed location
-- - alltransaction_fact table exists and populated with transaction data
-- - customerlookup table structure created (if not exists, create first)

-- =====================================================================================
-- CREATE CUSTOMER LOOKUP TABLE
-- =====================================================================================

-- ## Step 1: Create Customer Lookup Table Structure
-- **Purpose**: Create managed Delta table for customer reference data
-- **Features**: Unique customer identifiers, phone standardization, audit columns

CREATE TABLE IF NOT EXISTS qrpayment_dev.silver.customerlookup (
    user_id INT,
    ten_kh_thanh_toan STRING,
    so_dien_thoai STRING,
    so_dien_thoai_moi STRING,
    don_vi_tt STRING,
    so_tai_khoan STRING,
    loai_the_tai_khoan STRING,
    created_date TIMESTAMP DEFAULT current_timestamp(),
    updated_date TIMESTAMP DEFAULT current_timestamp()
) 
USING DELTA
COMMENT 'Customer lookup table with unique identifiers and standardized phone numbers';

-- =====================================================================================
-- CUSTOMER LOOKUP TABLE POPULATION
-- =====================================================================================

-- ## Step 2: Insert New Customer Records
-- **Purpose**: Add unique customer combinations from transaction data
-- **Logic**: Prevents duplicates both from existing data and within new inserts

WITH distinct_new_records AS (
    -- Get distinct customer combinations from transaction data
    SELECT 
        ten_kh_thanh_toan,
        so_dien_thoai,
        don_vi_tt,
        so_tai_khoan,
        loai_the_tai_khoan,
        -- Deduplicate within new records using ROW_NUMBER
        ROW_NUMBER() OVER (
            PARTITION BY 
                COALESCE(so_dien_thoai, '_'),
                COALESCE(don_vi_tt, '_'),
                COALESCE(so_tai_khoan, '_')
            ORDER BY ten_kh_thanh_toan
        ) AS row_rank
    FROM qrpayment_dev.bronze.alltransaction_fact fact
    WHERE NOT EXISTS (
        -- Exclude combinations that already exist in customer lookup
        SELECT 1
        FROM qrpayment_dev.silver.customerlookup lookup
        WHERE 
            COALESCE(fact.so_dien_thoai, '_') = COALESCE(lookup.so_dien_thoai, '_')
            AND COALESCE(fact.don_vi_tt, '_') = COALESCE(lookup.don_vi_tt, '_') 
            AND COALESCE(fact.so_tai_khoan, '_') = COALESCE(lookup.so_tai_khoan, '_')
    )
)

-- Insert unique customer records with auto-generated user_id
INSERT INTO qrpayment_dev.silver.customerlookup 
(
    user_id,
    ten_kh_thanh_toan,
    so_dien_thoai,
    don_vi_tt,
    so_tai_khoan,
    loai_the_tai_khoan
)
SELECT 
    -- Generate sequential user_id starting from max existing + 1
    (SELECT COALESCE(MAX(user_id), 0) FROM qrpayment_dev.silver.customerlookup) + 
    ROW_NUMBER() OVER (ORDER BY so_dien_thoai, don_vi_tt, so_tai_khoan) AS user_id,
    ten_kh_thanh_toan,
    so_dien_thoai,
    don_vi_tt,
    so_tai_khoan,
    loai_the_tai_khoan
FROM distinct_new_records
WHERE row_rank = 1;  -- Only insert first occurrence of each unique combination

-- =====================================================================================
-- PHONE NUMBER STANDARDIZATION
-- =====================================================================================

-- ## Step 3: Update Phone Numbers to Current Vietnamese Format
-- **Purpose**: Convert old Vietnamese mobile prefixes to current 11-digit format
-- **Business Rule**: Update outdated telecom prefixes per Vietnamese regulations

UPDATE qrpayment_dev.silver.customerlookup
SET so_dien_thoai_moi = 
    CASE 
        WHEN so_dien_thoai IS NULL THEN NULL
        
        -- MobiFone prefix conversions
        WHEN SUBSTRING(so_dien_thoai, 1, 4) = '0120' THEN CONCAT('070', SUBSTRING(so_dien_thoai, 5))
        WHEN SUBSTRING(so_dien_thoai, 1, 4) = '0121' THEN CONCAT('079', SUBSTRING(so_dien_thoai, 5))
        WHEN SUBSTRING(so_dien_thoai, 1, 4) = '0122' THEN CONCAT('077', SUBSTRING(so_dien_thoai, 5))
        WHEN SUBSTRING(so_dien_thoai, 1, 4) = '0126' THEN CONCAT('076', SUBSTRING(so_dien_thoai, 5))
        WHEN SUBSTRING(so_dien_thoai, 1, 4) = '0128' THEN CONCAT('078', SUBSTRING(so_dien_thoai, 5))
        
        -- VinaPhone prefix conversions
        WHEN SUBSTRING(so_dien_thoai, 1, 4) = '0123' THEN CONCAT('083', SUBSTRING(so_dien_thoai, 5))
        WHEN SUBSTRING(so_dien_thoai, 1, 4) = '0124' THEN CONCAT('084', SUBSTRING(so_dien_thoai, 5))
        WHEN SUBSTRING(so_dien_thoai, 1, 4) = '0125' THEN CONCAT('085', SUBSTRING(so_dien_thoai, 5))
        WHEN SUBSTRING(so_dien_thoai, 1, 4) = '0127' THEN CONCAT('081', SUBSTRING(so_dien_thoai, 5))
        WHEN SUBSTRING(so_dien_thoai, 1, 4) = '0129' THEN CONCAT('082', SUBSTRING(so_dien_thoai, 5))
        
        -- Viettel prefix conversions
        WHEN SUBSTRING(so_dien_thoai, 1, 4) = '0162' THEN CONCAT('032', SUBSTRING(so_dien_thoai, 5))
        WHEN SUBSTRING(so_dien_thoai, 1, 4) = '0163' THEN CONCAT('033', SUBSTRING(so_dien_thoai, 5))
        WHEN SUBSTRING(so_dien_thoai, 1, 4) = '0164' THEN CONCAT('034', SUBSTRING(so_dien_thoai, 5))
        WHEN SUBSTRING(so_dien_thoai, 1, 4) = '0165' THEN CONCAT('035', SUBSTRING(so_dien_thoai, 5))
        WHEN SUBSTRING(so_dien_thoai, 1, 4) = '0166' THEN CONCAT('036', SUBSTRING(so_dien_thoai, 5))
        WHEN SUBSTRING(so_dien_thoai, 1, 4) = '0167' THEN CONCAT('037', SUBSTRING(so_dien_thoai, 5))
        WHEN SUBSTRING(so_dien_thoai, 1, 4) = '0168' THEN CONCAT('038', SUBSTRING(so_dien_thoai, 5))
        WHEN SUBSTRING(so_dien_thoai, 1, 4) = '0169' THEN CONCAT('039', SUBSTRING(so_dien_thoai, 5))
        
        -- Vietnamobile prefix conversions
        WHEN SUBSTRING(so_dien_thoai, 1, 4) = '0186' THEN CONCAT('056', SUBSTRING(so_dien_thoai, 5))
        WHEN SUBSTRING(so_dien_thoai, 1, 4) = '0188' THEN CONCAT('058', SUBSTRING(so_dien_thoai, 5))
        
        -- Gmobile prefix conversions
        WHEN SUBSTRING(so_dien_thoai, 1, 4) = '0199' THEN CONCAT('059', SUBSTRING(so_dien_thoai, 5))
        
        -- Keep original if no conversion needed
        ELSE so_dien_thoai
    END
WHERE so_dien_thoai_moi IS NULL
  AND so_dien_thoai IS NOT NULL;

-- =====================================================================================
-- DATA VALIDATION
-- =====================================================================================

-- ## Step 4: Verify Customer Lookup Table Population
-- **Purpose**: Validate record counts and data integrity

-- Check total records and unique user_id count
SELECT 
    COUNT(*) AS total_records,
    COUNT(user_id) AS non_null_user_ids,
    COUNT(DISTINCT user_id) AS unique_user_ids,
    COUNT(DISTINCT CONCAT(COALESCE(so_dien_thoai, '_'), '|', 
                         COALESCE(don_vi_tt, '_'), '|', 
                         COALESCE(so_tai_khoan, '_'))) AS unique_combinations
FROM qrpayment_dev.silver.customerlookup;

-- =====================================================================================
-- ADDITIONAL VALIDATION QUERIES
-- =====================================================================================

-- ## Optional Data Quality Checks

-- Check for duplicate user_ids (should be 0)
-- SELECT user_id, COUNT(*) as duplicate_count
-- FROM qrpayment_dev.silver.customerlookup
-- GROUP BY user_id
-- HAVING COUNT(*) > 1;

-- Check phone number conversion results
-- SELECT 
--     COUNT(*) as total_phones,
--     COUNT(so_dien_thoai_moi) as converted_phones,
--     COUNT(*) - COUNT(so_dien_thoai_moi) as unconverted_phones
-- FROM qrpayment_dev.silver.customerlookup
-- WHERE so_dien_thoai IS NOT NULL;

-- Sample converted phone numbers
-- SELECT so_dien_thoai, so_dien_thoai_moi
-- FROM qrpayment_dev.silver.customerlookup
-- WHERE so_dien_thoai != so_dien_thoai_moi
-- LIMIT 10;

-- =====================================================================================
-- END OF SCRIPT
-- =====================================================================================
