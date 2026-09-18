WITH loan_metrics AS (

    SELECT
        customer_id,
        COUNT(DISTINCT loan_id) AS total_loans,
        SUM(principal_amount) AS total_loan_amount,

        SUM(
            CASE
                WHEN status = 'ACTIVE' THEN 1
                ELSE 0
            END
        ) AS active_loans,

        SUM(
            CASE
                WHEN status = 'DEFAULTED' THEN 1
                ELSE 0
            END
        ) AS defaulted_loans

    FROM {{ ref('stg_loans') }}

    GROUP BY customer_id

),

payment_metrics AS (

    SELECT
        l.customer_id,
        COUNT(p.payment_id) AS total_loan_payments,
        SUM(
            CASE
                WHEN p.status = 'PAID' THEN 1
                ELSE 0
            END
        ) AS paid_payments,
        SUM(
            CASE
                WHEN p.status = 'LATE' THEN 1
                ELSE 0
            END
        ) AS late_payments,
        SUM(
            CASE
                WHEN p.status = 'PAID' THEN p.amount
                ELSE 0
            END
        ) AS total_paid_amount
    FROM {{ ref('stg_loan_payments') }} AS p
    INNER JOIN {{ ref('stg_loans') }} AS l
        ON p.loan_id = l.loan_id
    GROUP BY l.customer_id

)

SELECT
    c.customer_id,
    c.customer_segment,

    COALESCE(l.total_loans, 0) AS total_loans,
    COALESCE(l.total_loan_amount, 0) AS total_loan_amount,
    COALESCE(l.active_loans, 0) AS active_loans,
    COALESCE(l.defaulted_loans, 0) AS defaulted_loans,

    COALESCE(p.total_loan_payments, 0) AS total_loan_payments,
    COALESCE(p.paid_payments, 0) AS paid_payments,
    COALESCE(p.late_payments, 0) AS late_payments,
    COALESCE(p.total_paid_amount, 0) AS total_paid_amount

FROM {{ ref('stg_customers') }} AS c

LEFT JOIN loan_metrics AS l
    ON c.customer_id = l.customer_id

LEFT JOIN payment_metrics AS p
    ON c.customer_id = p.customer_id