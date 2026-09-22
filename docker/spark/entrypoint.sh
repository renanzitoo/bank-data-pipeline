#!/bin/bash

exec /opt/spark/bin/spark-submit \
  --packages \
  org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,org.apache.hadoop:hadoop-aws:3.5.0 \
  --conf spark.jars.ivy=/opt/spark/.ivy2 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.iceberg.type=hadoop \
  --conf spark.sql.catalog.iceberg.warehouse=s3a://banking/iceberg \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key="${MINIO_ACCESS_KEY}" \
  --conf spark.hadoop.fs.s3a.secret.key="${MINIO_SECRET_KEY}" \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.endpoint.region=us-east-1 \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.driver.host=banking-spark \
  "$@"