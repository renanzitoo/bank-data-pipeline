SELECT
    card_id,
    account_id,
    card_type,
    brand,
    status,
    credit_limit,
    issued_at,
    expires_at
FROM read_parquet(
    's3://banking/silver/cards/**/*.parquet',
    hive_partitioning = true
)