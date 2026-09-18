SELECT
    merchant_id,
    merchant_name,
    merchant_category,
    merchant_city,
    merchant_state,

    COUNT(*) AS transaction_count,

    SUM(amount) AS total_transaction_amount,

    AVG(amount) AS average_transaction_amount,

    SUM(
        CASE
            WHEN status = 'COMPLETED' THEN 1
            ELSE 0
        END
    ) AS completed_transactions,

    SUM(
        CASE
            WHEN status = 'FAILED' THEN 1
            ELSE 0
        END
    ) AS failed_transactions,

    SUM(
        CASE
            WHEN status = 'CANCELLED' THEN 1
            ELSE 0
        END
    ) AS cancelled_transactions

FROM {{ ref('int_merchant_transactions') }}

GROUP BY
    merchant_id,
    merchant_name,
    merchant_category,
    merchant_city,
    merchant_state