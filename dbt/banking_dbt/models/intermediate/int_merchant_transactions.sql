SELECT
    t.transaction_id,
    t.account_id,
    t.merchant_id,
    m.merchant_name,
    m.merchant_category,
    m.city AS merchant_city,
    m.state AS merchant_state,
    t.transaction_type,
    t.amount,
    t.currency,
    t.status,
    t.transaction_timestamp
FROM {{ ref('stg_transactions') }} AS t
INNER JOIN {{ ref('stg_merchants') }} AS m
    ON t.merchant_id = m.merchant_id
WHERE t.merchant_id IS NOT NULL