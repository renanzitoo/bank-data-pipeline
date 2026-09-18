SELECT
    CAST(transaction_timestamp AS DATE) AS transaction_date,
    transaction_type,
    status,

    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS average_amount,
    MIN(amount) AS minimum_amount,
    MAX(amount) AS maximum_amount

FROM {{ ref('stg_transactions') }}

GROUP BY
    transaction_date,
    transaction_type,
    status