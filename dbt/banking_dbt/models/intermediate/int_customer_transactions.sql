SELECT
    t.transaction_id,
    t.account_id,
    a.customer_id,
    c.customer_segment,
    t.transaction_type,
    t.amount,
    t.currency,
    t.status,
    t.transaction_timestamp,
    t.merchant_id,
    t.description
FROM {{ ref('stg_transactions') }} AS t
INNER JOIN {{ ref('stg_accounts') }} AS a
    ON t.account_id = a.account_id
INNER JOIN {{ ref('stg_customers') }} AS c
    ON a.customer_id = c.customer_id