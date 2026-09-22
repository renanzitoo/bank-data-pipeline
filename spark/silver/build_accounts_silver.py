from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, lit, when


BRONZE_PATH = "s3a://banking/bronze/accounts"
BRONZE_CUSTOMERS_PATH = "s3a://banking/bronze/customers"

SILVER_PATH = "s3a://banking/silver/accounts"
QUARANTINE_PATH = "data/quarantine/accounts"


VALID_ACCOUNT_TYPES = [
    "CHECKING",
    "SAVINGS",
]

VALID_STATUSES = [
    "ACTIVE",
    "BLOCKED",
    "CLOSED",
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

def transform_accounts(df):
    return (
        df
        .withColumn("account_type", upper(trim(col("account_type"))))
        .withColumn("status", upper(trim(col("status"))))
        .withColumn("branch", trim(col("branch")))
        .withColumn("account_number", trim(col("account_number")))
    )


def validate_accounts(df):
    return (
        df
        .withColumn(
            "quality_error",
            when(
                col("account_id").isNull(),
                lit("NULL_ACCOUNT_ID")
            )
            .when(
                col("customer_id").isNull(),
                lit("NULL_CUSTOMER_ID")
            )
            .when(
                col("branch").isNull() | (trim(col("branch")) == ""),
                lit("INVALID_BRANCH")
            )
            .when(
                col("account_number").isNull()
                | (trim(col("account_number")) == ""),
                lit("INVALID_ACCOUNT_NUMBER")
            )
            .when(
                ~col("account_type").isin(VALID_ACCOUNT_TYPES),
                lit("INVALID_ACCOUNT_TYPE")
            )
            .when(
                ~col("status").isin(VALID_STATUSES),
                lit("INVALID_STATUS")
            )
            .when(
                col("opened_at").isNull(),
                lit("NULL_OPENED_AT")
            )
            .when(
                (col("status") == "CLOSED")
                & col("closed_at").isNull(),
                lit("MISSING_CLOSED_AT")
            )
            .when(
                (col("status") != "CLOSED")
                & col("closed_at").isNotNull(),
                lit("UNEXPECTED_CLOSED_AT")
            )
        )
    )


def main():
    spark = create_spark_session()

    print("Reading Bronze accounts...")
    accounts = spark.read.parquet(BRONZE_PATH)

    print(f"Bronze records: {accounts.count():,}")

    print("Reading Bronze customers...")
    customers = spark.read.parquet(BRONZE_CUSTOMERS_PATH)

    print(f"Customer records: {customers.count():,}")

    print("Transforming accounts...")
    accounts = transform_accounts(accounts)

    print("Validating data quality...")
    validated = validate_accounts(accounts)

    print("Validating customer references...")

    customer_reference = (
        customers
        .select(
            col("customer_id").alias("valid_customer_id")
        )
        .dropDuplicates()
    )

    validated = (
        validated
        .join(
            customer_reference,
            validated.customer_id == col("valid_customer_id"),
            "left"
        )
        .withColumn(
            "quality_error",
            when(
                col("quality_error").isNotNull(),
                col("quality_error")
            )
            .when(
                col("valid_customer_id").isNull(),
                lit("INVALID_CUSTOMER_ID")
            )
        )
        .drop("valid_customer_id")
    )

    print("Checking duplicate accounts...")

    duplicate_accounts = (
        validated
        .groupBy("account_id")
        .count()
        .filter(col("count") > 1)
        .select("account_id")
    )

    validated = (
        validated
        .join(
            duplicate_accounts.withColumn(
                "is_duplicate",
                lit(True)
            ),
            on="account_id",
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
                lit("DUPLICATE_ACCOUNT_ID")
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
    print("       ACCOUNT DATA QUALITY")
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

    print("Silver accounts completed.")

    spark.stop()


if __name__ == "__main__":
    main()