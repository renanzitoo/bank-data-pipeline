from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    when,
    lit,
)
from pyspark import StorageLevel


BRONZE_PATH = "s3a://banking/bronze/transactions"
BRONZE_ACCOUNTS_PATH = "s3a://banking/bronze/accounts"

SILVER_PATH = "s3a://banking/silver/transactions"
QUARANTINE_PATH = "data/quarantine/transactions"


VALID_TRANSACTION_TYPES = [
    "PIX",
    "CARD_PURCHASE",
    "TRANSFER",
    "DEPOSIT",
    "WITHDRAWAL",
    "TED",
    "DOC",
]

VALID_STATUSES = [
    "COMPLETED",
    "PENDING",
    "FAILED",
    "CANCELLED",
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


def validate_transactions(df):

    validated = (
        df
        .withColumn(
            "quality_error",
            when(
                col("transaction_id").isNull(),
                lit("NULL_TRANSACTION_ID")
            )
            .when(
                col("account_id").isNull(),
                lit("NULL_ACCOUNT_ID")
            )
            .when(
                col("amount").isNull() | (col("amount") <= 0),
                lit("INVALID_AMOUNT")
            )
            .when(
                col("currency") != "BRL",
                lit("INVALID_CURRENCY")
            )
            .when(
                ~col("transaction_type").isin(
                    VALID_TRANSACTION_TYPES
                ),
                lit("INVALID_TRANSACTION_TYPE")
            )
            .when(
                ~col("status").isin(
                    VALID_STATUSES
                ),
                lit("INVALID_STATUS")
            )
            .when(
                (
                    col("transaction_type") == "CARD_PURCHASE"
                )
                & col("merchant_id").isNull(),
                lit("MISSING_MERCHANT_ID")
            )
        )
    )

    return validated


def transform_transactions(df):

    transformed = (
        df
        .withColumn(
            "transaction_type",
            upper(
                trim(
                    col("transaction_type")
                )
            ),
        )
        .withColumn(
            "status",
            upper(
                trim(
                    col("status")
                )
            ),
        )
        .withColumn(
            "currency",
            upper(
                trim(
                    col("currency")
                )
            ),
        )
        .withColumn(
            "description",
            trim(
                col("description")
            ),
        )
    )

    return transformed


def main():

    spark = create_spark_session()

    print("Reading Bronze transactions...")

    transactions = (
        spark.read
        .parquet(BRONZE_PATH)
    )

    print("Reading Bronze accounts...")

    accounts = (
        spark.read
        .parquet(BRONZE_ACCOUNTS_PATH)
        .select("account_id")
        .dropDuplicates()
    )

    print("Transforming transactions...")

    transactions = transform_transactions(
        transactions
    )

    print("Validating data quality...")

    validated = validate_transactions(
        transactions
    )

    # Liberamos a referência do DataFrame original
    transactions.unpersist(blocking=False)

    print("Validating account references...")

    account_reference = (
        accounts
        .select(
            col("account_id").alias(
                "valid_account_id"
            )
        )
    )

    validated = (
        validated
        .join(
            account_reference,
            validated.account_id
            == col("valid_account_id"),
            "left"
        )
        .withColumn(
            "quality_error",
            when(
                col("quality_error").isNotNull(),
                col("quality_error")
            )
            .when(
                col("valid_account_id").isNull(),
                lit("INVALID_ACCOUNT_ID")
            )
        )
        .drop("valid_account_id")
    )

    print("Checking duplicate transactions...")

    duplicate_transactions = (
        validated
        .groupBy("transaction_id")
        .count()
        .filter(
            col("count") > 1
        )
        .select("transaction_id")
    )

    validated = (
        validated
        .join(
            duplicate_transactions.withColumn(
                "is_duplicate",
                lit(True)
            ),
            on="transaction_id",
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
                lit("DUPLICATE_TRANSACTION_ID")
            )
        )
        .drop("is_duplicate")
    )

    # Persistimos em disco + memória.
    # Isso evita que o Spark refaça todos os joins/shuffles
    # toda vez que valid ou invalid for utilizado.
    validated = validated.persist(
        StorageLevel.MEMORY_AND_DISK
    )

    print("Generating Data Quality Report...")

    # Calculamos as quantidades através de uma única agregação.
    quality_counts = (
        validated
        .select(
            when(
                col("quality_error").isNull(),
                lit(1)
            )
            .otherwise(lit(0))
            .alias("valid"),

            when(
                col("quality_error").isNotNull(),
                lit(1)
            )
            .otherwise(lit(0))
            .alias("invalid")
        )
        .agg(
            {"valid": "sum", "invalid": "sum"}
        )
        .collect()[0]
    )

    valid_count = (
        quality_counts["sum(valid)"]
        or 0
    )

    invalid_count = (
        quality_counts["sum(invalid)"]
        or 0
    )

    total_count = (
        valid_count
        + invalid_count
    )

    print()
    print("=" * 40)
    print("       DATA QUALITY REPORT")
    print("=" * 40)

    print(
        f"Total records:       {total_count:,}"
    )

    print(
        f"Valid records:       {valid_count:,}"
    )

    print(
        f"Invalid records:     {invalid_count:,}"
    )

    print()

    print("Errors:")

    if invalid_count == 0:

        print(
            "  No quality errors found."
        )

    else:

        error_counts = (
            validated
            .filter(
                col("quality_error").isNotNull()
            )
            .groupBy("quality_error")
            .count()
            .orderBy(
                col("count").desc()
            )
        )

        for row in error_counts.collect():

            print(
                f"  {row['quality_error']:<30} "
                f"{row['count']:,}"
            )

    print("=" * 40)
    print()

    print("Writing Quarantine...")

    invalid = (
        validated
        .filter(
            col("quality_error").isNotNull()
        )
    )

    (
        invalid
        .write
        .mode("overwrite")
        .partitionBy(
            "year",
            "month"
        )
        .parquet(
            QUARANTINE_PATH
        )
    )

    print("Writing Silver...")

    valid = (
        validated
        .filter(
            col("quality_error").isNull()
        )
        .drop("quality_error")
    )

    (
        valid
        .write
        .mode("overwrite")
        .partitionBy(
            "year",
            "month"
        )
        .parquet(
            SILVER_PATH
        )
    )

    print()
    print(
        "Silver transactions completed."
    )

    validated.unpersist(
        blocking=True
    )

    spark.stop()


if __name__ == "__main__":
    main()