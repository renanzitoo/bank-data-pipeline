SELECT
    loan_id,
    customer_id,
    loan_type,
    principal_amount,
    interest_rate,
    term_months,
    status,
    created_at
FROM read_parquet(
    '../../data/silver/loans/**/*.parquet',
    hive_partitioning = true
)