WITH transaction_metrics AS (

    SELECT
        customer_id,
        customer_segment,

        COUNT(*) AS total_transactions,
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

),

account_metrics AS (

    SELECT
        customer_id,

        COUNT(*) AS total_accounts,

        SUM(
            CASE
                WHEN status = 'ACTIVE' THEN 1
                ELSE 0
            END
        ) AS active_accounts,

        SUM(
            CASE
                WHEN status = 'BLOCKED' THEN 1
                ELSE 0
            END
        ) AS blocked_accounts,

        SUM(
            CASE
                WHEN status = 'CLOSED' THEN 1
                ELSE 0
            END
        ) AS closed_accounts

    FROM {{ ref('stg_accounts') }}

    GROUP BY customer_id

),

card_metrics AS (

    SELECT
        a.customer_id,

        COUNT(c.card_id) AS total_cards,

        SUM(
            CASE
                WHEN c.card_type = 'CREDIT' THEN 1
                ELSE 0
            END
        ) AS credit_cards,

        SUM(
            CASE
                WHEN c.card_type = 'DEBIT' THEN 1
                ELSE 0
            END
        ) AS debit_cards

    FROM {{ ref('stg_cards') }} AS c

    INNER JOIN {{ ref('stg_accounts') }} AS a
        ON c.account_id = a.account_id

    GROUP BY a.customer_id

)

SELECT
    c.customer_id,
    c.customer_segment,

    COALESCE(a.total_accounts, 0) AS total_accounts,
    COALESCE(a.active_accounts, 0) AS active_accounts,
    COALESCE(a.blocked_accounts, 0) AS blocked_accounts,
    COALESCE(a.closed_accounts, 0) AS closed_accounts,

    COALESCE(t.total_transactions, 0) AS total_transactions,
    COALESCE(t.total_transaction_amount, 0) AS total_transaction_amount,
    COALESCE(t.average_transaction_amount, 0) AS average_transaction_amount,
    COALESCE(t.completed_transactions, 0) AS completed_transactions,
    COALESCE(t.failed_transactions, 0) AS failed_transactions,
    COALESCE(t.pending_transactions, 0) AS pending_transactions,
    COALESCE(t.cancelled_transactions, 0) AS cancelled_transactions,

    COALESCE(card.total_cards, 0) AS total_cards,
    COALESCE(card.credit_cards, 0) AS credit_cards,
    COALESCE(card.debit_cards, 0) AS debit_cards,

    COALESCE(l.total_loans, 0) AS total_loans,
    COALESCE(l.total_loan_amount, 0) AS total_loan_amount,
    COALESCE(l.active_loans, 0) AS active_loans,
    COALESCE(l.defaulted_loans, 0) AS defaulted_loans,
    COALESCE(l.total_loan_payments, 0) AS total_loan_payments,
    COALESCE(l.paid_payments, 0) AS paid_payments,
    COALESCE(l.late_payments, 0) AS late_payments,
    COALESCE(l.total_paid_amount, 0) AS total_paid_amount

FROM {{ ref('stg_customers') }} AS c

LEFT JOIN account_metrics AS a
    ON c.customer_id = a.customer_id

LEFT JOIN transaction_metrics AS t
    ON c.customer_id = t.customer_id

LEFT JOIN card_metrics AS card
    ON c.customer_id = card.customer_id

LEFT JOIN {{ ref('int_customer_loans') }} AS l
    ON c.customer_id = l.customer_id