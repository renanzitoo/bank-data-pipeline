SELECT
    account_id,
    customer_id,
    account_type,
    branch,
    account_number,
    status,
    opened_at,
    closed_at
FROM read_parquet(
    '../../data/silver/accounts/**/*.parquet',
    hive_partitioning = true
)