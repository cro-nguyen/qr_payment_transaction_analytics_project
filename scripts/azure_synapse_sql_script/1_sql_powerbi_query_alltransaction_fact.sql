USE vnpay_gold_spark;

SELECT TOP 100
    txn_id,
    master_merchant_id,
    merchant_id,
    terminal_id,
    user_id,
    issuer_id,
    user_name,
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
FROM alltransaction_fact
WHERE issuer_id NOT IN ('33', '37');