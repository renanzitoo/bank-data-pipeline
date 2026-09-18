SELECT
    loan_id,
    customer_id,
    loan_type,
    principal_amount,
    interest_rate,
    term_months,
    loan_status,

    COUNT(payment_id) AS total_payments,

    SUM(
        CASE
            WHEN payment_status = 'PAID' THEN 1
            ELSE 0
        END
    ) AS paid_payments,

    SUM(
        CASE
            WHEN payment_status = 'LATE' THEN 1
            ELSE 0
        END
    ) AS late_payments,

    SUM(
        CASE
            WHEN payment_status = 'PENDING' THEN 1
            ELSE 0
        END
    ) AS pending_payments,

    SUM(
        CASE
            WHEN payment_status = 'PAID' THEN payment_amount
            ELSE 0
        END
    ) AS total_paid_amount,

    SUM(
        CASE
            WHEN payment_status = 'LATE' THEN payment_amount
            ELSE 0
        END
    ) AS total_late_amount

FROM {{ ref('int_loan_payments') }}

GROUP BY
    loan_id,
    customer_id,
    loan_type,
    principal_amount,
    interest_rate,
    term_months,
    loan_status