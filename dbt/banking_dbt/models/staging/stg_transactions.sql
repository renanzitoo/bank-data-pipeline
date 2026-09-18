SELECT
    transaction_id,
    account_id,
    transaction_type,
    amount,
    currency,
    status,
    transaction_timestamp,
    merchant_id,
    description
FROM read_parquet(
    '../../data/silver/transactions/**/*.parquet',
    hive_partitioning = true
)