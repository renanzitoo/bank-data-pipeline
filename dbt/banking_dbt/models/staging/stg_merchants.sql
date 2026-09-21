SELECT
    merchant_id,
    merchant_name,
    merchant_category,
    city,
    state,
    created_at
FROM read_parquet(
    's3://banking/silver/merchants/**/*.parquet',
    hive_partitioning = true
)