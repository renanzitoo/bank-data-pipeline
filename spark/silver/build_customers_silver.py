from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, lower, lit, when


BRONZE_PATH = "s3a://banking/bronze/customers"
SILVER_PATH = "s3a://banking/silver/customers"
QUARANTINE_PATH = "data/quarantine/customers"


VALID_SEGMENTS = [
    "BASIC",
    "STANDARD",
    "PREMIUM",
]


import os

def create_spark_session():
    return (
        SparkSession.builder
        .appName("BankingAccountsSilver")
        .master("local[*]")
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            f"http://{os.getenv('MINIO_ENDPOINT', 'minio:9000')}",
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            os.getenv("MINIO_ACCESS_KEY", "banking"),
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            os.getenv("MINIO_SECRET_KEY", "banking_dev"),
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true",
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false",
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint.region",
            "us-east-1",
        )
        .getOrCreate()
    )


def transform_customers(df):
    return (
        df
        .withColumn("first_name", trim(col("first_name")))
        .withColumn("last_name", trim(col("last_name")))
        .withColumn("document", trim(col("document")))
        .withColumn("email", lower(trim(col("email"))))
        .withColumn("phone", trim(col("phone")))
        .withColumn("city", trim(col("city")))
        .withColumn("state", upper(trim(col("state"))))
        .withColumn(
            "customer_segment",
            upper(trim(col("customer_segment")))
        )
    )


def validate_customers(df):
    return (
        df
        .withColumn(
            "quality_error",
            when(
                col("customer_id").isNull(),
                lit("NULL_CUSTOMER_ID")
            )
            .when(
                col("first_name").isNull()
                | (trim(col("first_name")) == ""),
                lit("INVALID_FIRST_NAME")
            )
            .when(
                col("last_name").isNull()
                | (trim(col("last_name")) == ""),
                lit("INVALID_LAST_NAME")
            )
            .when(
                col("document").isNull()
                | (trim(col("document")) == ""),
                lit("INVALID_DOCUMENT")
            )
            .when(
                col("email").isNull()
                | (trim(col("email")) == ""),
                lit("INVALID_EMAIL")
            )
            .when(
                ~col("customer_segment").isin(VALID_SEGMENTS),
                lit("INVALID_CUSTOMER_SEGMENT")
            )
            .when(
                col("birth_date").isNull(),
                lit("NULL_BIRTH_DATE")
            )
            .when(
                col("birth_date") > col("created_at"),
                lit("INVALID_BIRTH_DATE")
            )
            .when(
                col("created_at").isNull(),
                lit("NULL_CREATED_AT")
            )
        )
    )


def main():
    spark = create_spark_session()

    print("Reading Bronze customers from MinIO...")
    customers = spark.read.parquet(BRONZE_PATH)

    print(f"Bronze records: {customers.count():,}")

    print("Transforming customers...")
    customers = transform_customers(customers)

    print("Validating data quality...")
    validated = validate_customers(customers)

    print("Checking duplicate customers...")

    duplicate_customers = (
        validated
        .groupBy("customer_id")
        .count()
        .filter(col("count") > 1)
        .select("customer_id")
    )

    validated = (
        validated
        .join(
            duplicate_customers.withColumn(
                "is_duplicate",
                lit(True)
            ),
            on="customer_id",
            how="left"
        )
        .withColumn(
            "quality_error",
            when(
                col("quality_error").isNotNull(),
                col("quality_error")
            )
            .when(
                col("is_duplicate") == True,
                lit("DUPLICATE_CUSTOMER_ID")
            )
        )
        .drop("is_duplicate")
    )

    valid = validated.filter(
        col("quality_error").isNull()
    )

    invalid = validated.filter(
        col("quality_error").isNotNull()
    )

    valid_count = valid.count()
    invalid_count = invalid.count()
    total_count = valid_count + invalid_count

    print()
    print("=" * 40)
    print("       CUSTOMER DATA QUALITY")
    print("=" * 40)
    print(f"Total records:       {total_count:,}")
    print(f"Valid records:       {valid_count:,}")
    print(f"Invalid records:     {invalid_count:,}")
    print()
    print("Errors:")

    if invalid_count == 0:
        print("  No quality errors found.")
    else:
        error_counts = (
            invalid
            .groupBy("quality_error")
            .count()
            .orderBy(col("count").desc())
        )

        for row in error_counts.collect():
            print(
                f"  {row['quality_error']:<30} "
                f"{row['count']:,}"
            )

    print("=" * 40)
    print()

    print("Writing Quarantine...")

    (
        invalid.write
        .mode("overwrite")
        .parquet(QUARANTINE_PATH)
    )

    print("Writing Silver...")

    (
        valid
        .drop("quality_error")
        .write
        .mode("overwrite")
        .parquet(SILVER_PATH)
    )

    print("Silver customers completed.")

    spark.stop()


if __name__ == "__main__":
    main()