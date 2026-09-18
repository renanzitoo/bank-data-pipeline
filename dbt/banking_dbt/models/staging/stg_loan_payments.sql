SELECT
    payment_id,
    loan_id,
    payment_number,
    due_date,
    payment_date,
    amount,
    status
FROM read_parquet(
    '../../data/silver/loan_payments/**/*.parquet',
    hive_partitioning = true
)