SELECT
  customer_id,
  first_name,
  last_name,
  birth_date,
  document,
  email,
  phone,
  city,
  state,
  customer_segment,
  created_at,
FROM read_parquet(
  's3://banking/silver/customers/**/*.parquet',
  hive_partitioning = true
)
