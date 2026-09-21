from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    when,
    lit
)


BRONZE_PATH = "s3a://banking/bronze/transactions"
BRONZE_ACCOUNTS_PATH = "s3a://banking/bronze/accounts"

SILVER_PATH = "data/silver/transactions"
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


def create_spark_session():
    return (
        SparkSession.builder
        .appName("BankingAccountsSilver")
        .master("local[*]")
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            "http://localhost:9000",
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            "banking",
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            "banking_dev",
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
                ~col("transaction_type").isin(VALID_TRANSACTION_TYPES),
                lit("INVALID_TRANSACTION_TYPE")
            )
            .when(
                ~col("status").isin(VALID_STATUSES),
                lit("INVALID_STATUS")
            )
            .when(
                (col("transaction_type") == "CARD_PURCHASE")
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
    transactions = spark.read.parquet(BRONZE_PATH)

    print("Reading Bronze accounts...")
    accounts = spark.read.parquet(BRONZE_ACCOUNTS_PATH)

    print(f"Account records: {accounts.count():,}")
    print(f"Bronze records: {transactions.count():,}")

    print("Transforming transactions...")
    transactions = transform_transactions(transactions)

    print("Validating data quality...")
    validated = validate_transactions(transactions)

    print("Validating account references...")

    account_reference = (
        accounts
        .select(
            col("account_id").alias("valid_account_id")
        )
        .dropDuplicates()
    )

    validated = (
        validated
        .join(
            account_reference,
            validated.account_id == col("valid_account_id"),
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

    valid = validated.filter(
        col("quality_error").isNull()
    )

    invalid = validated.filter(
        col("quality_error").isNotNull()
    )

    print("Generating Data Quality Report...")

    valid_count = valid.count()
    invalid_count = invalid.count()
    total_count = valid_count + invalid_count

    print()
    print("=" * 40)
    print("       DATA QUALITY REPORT")
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
        .partitionBy("year", "month")
        .parquet(QUARANTINE_PATH)
    )

    print("Writing Silver...")

    (
        valid
        .drop("quality_error")
        .write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(SILVER_PATH)
    )

    print("Silver transactions completed.")

    spark.stop()


if __name__ == "__main__":
    main()