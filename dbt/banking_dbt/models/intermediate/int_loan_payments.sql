SELECT
    p.payment_id,
    p.loan_id,
    l.customer_id,
    l.loan_type,
    l.principal_amount,
    l.interest_rate,
    l.term_months,
    l.status AS loan_status,
    p.payment_number,
    p.due_date,
    p.payment_date,
    p.amount AS payment_amount,
    p.status AS payment_status
FROM {{ ref('stg_loan_payments') }} AS p
INNER JOIN {{ ref('stg_loans') }} AS l
    ON p.loan_id = l.loan_id