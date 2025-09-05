-- =====================================================================================
-- File: 4.7_create_alltransaction_fact_table.sql
-- Description: Create and enrich transaction fact table in Silver layer
-- Created: 2025-07-31
-- =====================================================================================

-- ## Overview
-- Creates main transaction fact table with data transformations and foreign key relationships
-- Process: Create table → Clean data → Add foreign keys → Enrich lookup tables

-- ## Prerequisites
-- - All lookup tables created (customerlookup, terminal_lookup, issuer_lookup, etc.)
-- - Bronze alltransaction_fact table exists and populated

-- =====================================================================================
-- CREATE TRANSACTION FACT TABLE
-- =====================================================================================

-- Step 1: Create transaction fact table from bronze data
CREATE TABLE IF NOT EXISTS qrpayment_dev.silver.alltransaction_fact
AS
SELECT 
    CAST(ma_gd AS INT) AS ma_gd,
    ma_thanh_toan,
    ma_don_hang,
    so_hoa_don,
    ten_merchant,
    master_mc,
    ma_ten_terminal,
    don_vi_tt,
    kenh_thanh_toan,
    phuong_thuc_thanh_toan,
    ten_kh_thanh_toan,
    so_dien_thoai,
    so_tai_khoan,
    loai_the_tai_khoan,
    CAST(so_tien_truoc_km AS INT) AS so_tien_truoc_km,
    CAST(so_tien_sau_km AS INT) AS so_tien_sau_km,
    CAST(so_tien_truoc_km_ngoai_te AS INT) AS so_tien_truoc_km_ngoai_te,
    CAST(so_tien_sau_km_ngoai_te AS INT) AS so_tien_sau_km_ngoai_te,
    to_chuc_tai_tro_km,
    ty_gia,
    loai_ngoai_te,
    thoi_gian_thanh_toan,
    trang_thai,
    so_trace_phase_2,
    phase_2_code,
    so_trace_phase_3,
    phase_3_code,
    so_trace_phase_4,
    phase_4_code,
    ma_phe_duyet,
    mcc_noi_dia,
    mcc_quoc_te,
    so_tien_tip,
    hinh_thuc_the,
    UserID AS user_id,
    current_timestamp() AS ingestion_date
FROM qrpayment_dev.bronze.alltransaction_fact;

-- =====================================================================================
-- DATA QUALITY CLEANING
-- =====================================================================================

-- Step 2: Clean 'nan' values - convert to NULL
UPDATE qrpayment_dev.silver.alltransaction_fact
SET 
    ma_thanh_toan = CASE WHEN ma_thanh_toan = 'nan' THEN NULL ELSE ma_thanh_toan END,
    ma_don_hang = CASE WHEN ma_don_hang = 'nan' THEN NULL ELSE ma_don_hang END,
    so_hoa_don = CASE WHEN so_hoa_don = 'nan' THEN NULL ELSE so_hoa_don END,
    ten_merchant = CASE WHEN ten_merchant = 'nan' THEN NULL ELSE ten_merchant END,
    master_mc = CASE WHEN master_mc = 'nan' THEN NULL ELSE master_mc END,
    ma_ten_terminal = CASE WHEN ma_ten_terminal = 'nan' THEN NULL ELSE ma_ten_terminal END,
    don_vi_tt = CASE WHEN don_vi_tt = 'nan' THEN NULL ELSE don_vi_tt END,
    kenh_thanh_toan = CASE WHEN kenh_thanh_toan = 'nan' THEN NULL ELSE kenh_thanh_toan END,
    phuong_thuc_thanh_toan = CASE WHEN phuong_thuc_thanh_toan = 'nan' THEN NULL ELSE phuong_thuc_thanh_toan END,
    ten_kh_thanh_toan = CASE WHEN ten_kh_thanh_toan = 'nan' THEN NULL ELSE ten_kh_thanh_toan END,
    so_dien_thoai = CASE WHEN so_dien_thoai = 'nan' THEN NULL ELSE so_dien_thoai END,
    so_tai_khoan = CASE WHEN so_tai_khoan = 'nan' THEN NULL ELSE so_tai_khoan END,
    loai_the_tai_khoan = CASE WHEN loai_the_tai_khoan = 'nan' THEN NULL ELSE loai_the_tai_khoan END,
    to_chuc_tai_tro_km = CASE WHEN to_chuc_tai_tro_km = 'nan' THEN NULL ELSE to_chuc_tai_tro_km END,
    loai_ngoai_te = CASE WHEN loai_ngoai_te = 'nan' THEN NULL ELSE loai_ngoai_te END,
    trang_thai = CASE WHEN trang_thai = 'nan' THEN NULL ELSE trang_thai END,
    so_trace_phase_2 = CASE WHEN so_trace_phase_2 = 'nan' THEN NULL ELSE so_trace_phase_2 END,
    phase_2_code = CASE WHEN phase_2_code = 'nan' THEN NULL ELSE phase_2_code END,
    so_trace_phase_3 = CASE WHEN so_trace_phase_3 = 'nan' THEN NULL ELSE so_trace_phase_3 END,
    phase_3_code = CASE WHEN phase_3_code = 'nan' THEN NULL ELSE phase_3_code END,
    so_trace_phase_4 = CASE WHEN so_trace_phase_4 = 'nan' THEN NULL ELSE so_trace_phase_4 END,
    phase_4_code = CASE WHEN phase_4_code = 'nan' THEN NULL ELSE phase_4_code END,
    ma_phe_duyet = CASE WHEN ma_phe_duyet = 'nan' THEN NULL ELSE ma_phe_duyet END,
    mcc_noi_dia = CASE WHEN mcc_noi_dia = 'nan' THEN NULL ELSE mcc_noi_dia END,
    mcc_quoc_te = CASE WHEN mcc_quoc_te = 'nan' THEN NULL ELSE mcc_quoc_te END,
    hinh_thuc_the = CASE WHEN hinh_thuc_the = 'nan' THEN NULL ELSE hinh_thuc_the END;

-- =====================================================================================
-- ADD FOREIGN KEY COLUMNS
-- =====================================================================================

-- Step 3: Add foreign key columns for dimensional relationships
ALTER TABLE qrpayment_dev.silver.alltransaction_fact ADD COLUMN terminal_id INT;
ALTER TABLE qrpayment_dev.silver.alltransaction_fact ADD COLUMN issuer_id INT;
ALTER TABLE qrpayment_dev.silver.alltransaction_fact ADD COLUMN merchant_id INT;
ALTER TABLE qrpayment_dev.silver.alltransaction_fact ADD COLUMN master_merchant_id INT;

-- =====================================================================================
-- UPDATE FOREIGN KEY RELATIONSHIPS
-- =====================================================================================

-- Step 4: Update user_id from customer lookup
MERGE INTO qrpayment_dev.silver.alltransaction_fact AS t
USING (
    SELECT 
        MAX(user_id) as user_id,  -- Resolve duplicates
        COALESCE(so_dien_thoai, '_') as so_dien_thoai,
        COALESCE(don_vi_tt, '_') as don_vi_tt,
        COALESCE(so_tai_khoan, '_') as so_tai_khoan
    FROM qrpayment_dev.silver.customerlookup
    WHERE user_id IS NOT NULL
    GROUP BY 
        COALESCE(so_dien_thoai, '_'), 
        COALESCE(don_vi_tt, '_'), 
        COALESCE(so_tai_khoan, '_')
) AS c
ON CONCAT(COALESCE(t.so_dien_thoai, '_'), 
          COALESCE(t.don_vi_tt, '_'), 
          COALESCE(t.so_tai_khoan, '_')) = 
   CONCAT(c.so_dien_thoai, c.don_vi_tt, c.so_tai_khoan)
WHEN MATCHED THEN
    UPDATE SET user_id = c.user_id;

-- Step 5: Update terminal_id from terminal lookup
MERGE INTO qrpayment_dev.silver.alltransaction_fact AS target
USING (
    SELECT DISTINCT
        CONCAT(
            COALESCE(ten_merchant, ''), 
            '|', 
            COALESCE(ma_ten_terminal, ''), 
            '|', 
            COALESCE(master_mc, '')
        ) as lookup_key,
        terminal_id
    FROM qrpayment_dev.silver.terminal_lookup
    WHERE ten_merchant IS NOT NULL 
      AND ma_ten_terminal IS NOT NULL
) AS source
ON CONCAT(
    COALESCE(target.ten_merchant, ''), 
    '|', 
    COALESCE(target.ma_ten_terminal, ''), 
    '|', 
    COALESCE(target.master_mc, '')
) = source.lookup_key
WHEN MATCHED THEN
    UPDATE SET target.terminal_id = source.terminal_id;

-- Step 6: Update issuer_id from issuer lookup
UPDATE qrpayment_dev.silver.alltransaction_fact t
SET issuer_id = (
    SELECT m.issuer_id
    FROM qrpayment_dev.silver.issuer_lookup m
    WHERE m.issuer_name = t.don_vi_tt
    LIMIT 1
);

-- Step 7: Update merchant_id from terminal lookup
UPDATE qrpayment_dev.silver.alltransaction_fact t
SET merchant_id = (
    SELECT tl.merchant_id
    FROM qrpayment_dev.silver.terminal_lookup tl
    INNER JOIN qrpayment_dev.silver.merchant_lookup ml ON tl.mid = ml.mid
    WHERE tl.terminal_id = t.terminal_id
    LIMIT 1
);

-- Step 8: Update master_merchant_id from master merchant lookup
UPDATE qrpayment_dev.silver.alltransaction_fact t
SET master_merchant_id = (
    SELECT m.master_merchant_id
    FROM qrpayment_dev.silver.master_merchant_lookup m
    WHERE m.master_mc = t.master_mc
    LIMIT 1
);

-- =====================================================================================
-- ENRICH LOOKUP TABLES WITH TRANSACTION DATA
-- =====================================================================================

-- Step 9: Add MCC codes to merchant lookup table
ALTER TABLE qrpayment_dev.silver.merchant_lookup ADD COLUMN mcc_noi_dia STRING;
ALTER TABLE qrpayment_dev.silver.merchant_lookup ADD COLUMN mcc_quoc_te STRING;

-- Update merchant lookup with MCC codes from transactions
UPDATE qrpayment_dev.silver.merchant_lookup m
SET mcc_noi_dia = (
    SELECT t.mcc_noi_dia
    FROM qrpayment_dev.silver.alltransaction_fact t
    WHERE m.merchant_id = t.merchant_id
      AND t.mcc_noi_dia IS NOT NULL
    LIMIT 1
)
WHERE mcc_noi_dia IS NULL;

UPDATE qrpayment_dev.silver.merchant_lookup m
SET mcc_quoc_te = (
    SELECT t.mcc_quoc_te
    FROM qrpayment_dev.silver.alltransaction_fact t
    WHERE m.merchant_id = t.merchant_id
      AND t.mcc_quoc_te IS NOT NULL
    LIMIT 1
)
WHERE mcc_quoc_te IS NULL;

-- Step 10: Fill missing issuer data for customers
MERGE INTO qrpayment_dev.silver.alltransaction_fact AS target
USING (
    SELECT DISTINCT
        user_id,
        don_vi_tt
    FROM qrpayment_dev.silver.customerlookup
    WHERE user_id IS NOT NULL 
      AND don_vi_tt IS NOT NULL
) AS source
ON target.user_id = source.user_id
   AND target.issuer_id IS NULL
WHEN MATCHED THEN
    UPDATE SET target.don_vi_tt = source.don_vi_tt;

-- =====================================================================================
-- VALIDATION
-- =====================================================================================

-- Check transaction fact table population
SELECT 
    COUNT(*) AS total_transactions,
    COUNT(user_id) AS transactions_with_user_id,
    COUNT(terminal_id) AS transactions_with_terminal_id,
    COUNT(issuer_id) AS transactions_with_issuer_id,
    COUNT(merchant_id) AS transactions_with_merchant_id,
    COUNT(master_merchant_id) AS transactions_with_master_merchant_id
FROM qrpayment_dev.silver.alltransaction_fact;

-- Sample transaction data with foreign keys
SELECT 
    ma_gd,
    ten_merchant,
    user_id,
    terminal_id,
    issuer_id,
    merchant_id,
    master_merchant_id,
    so_tien_sau_km,
    trang_thai
FROM qrpayment_dev.silver.alltransaction_fact
LIMIT 5;

-- =====================================================================================
