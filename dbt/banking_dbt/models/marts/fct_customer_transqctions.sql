SELECT
    customer_id,
    customer_segment,

    COUNT(*) AS total_transactions,

    SUM(amount) AS total_transaction_amount,

    AVG(amount) AS average_transaction_amount,

    MIN(amount) AS minimum_transaction_amount,

    MAX(amount) AS maximum_transaction_amount,

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
            WHEN status = 'PENDING' THEN 1
            ELSE 0
        END
    ) AS pending_transactions,

    SUM(
        CASE
            WHEN status = 'CANCELLED' THEN 1
            ELSE 0
        END
    ) AS cancelled_transactions

FROM {{ ref('int_customer_transactions') }}

GROUP BY
    customer_id,
    customer_segment