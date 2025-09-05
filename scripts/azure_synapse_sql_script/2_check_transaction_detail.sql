SELECT TOP 100
    txn_id,
    tnx.master_merchant_id,
    mm.master_mc,
    tnx.merchant_id,
    m.ten_merchant,
    m.merchant_name,
    terminal_id,
    tnx.user_id,
    u.ten_kh_thanh_toan AS user_name,
    u.so_dien_thoai_moi AS phone,
    u.don_vi_tt AS issuer,
    u.so_tai_khoan AS account_number,
    issuer_id,
    payment_method,
    org_amount,
    final_amount,
    CAST(promotion_amount AS INT) AS promotion_amount,
    payment_time,
    payment_date,
    year,
    month,
    payment_status,
    domestic_mcc,
    int_mcc
FROM alltransaction_fact tnx
JOIN user_lookup u ON tnx.user_id = u.user_id
JOIN merchant_lookup m ON tnx.merchant_id = m.merchant_id
JOIN master_merchant_lookup mm ON tnx.master_merchant_id = mm.master_merchant_id
WHERE tnx.user_id = '9096241'