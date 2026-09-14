from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, lit, when


BRONZE_PATH = "data/bronze/loan_payments"
BRONZE_LOANS_PATH = "data/bronze/loans"

SILVER_PATH = "data/silver/loan_payments"
QUARANTINE_PATH = "data/quarantine/loan_payments"


VALID_STATUSES = [
    "PAID",
    "PENDING",
    "LATE",
]


def create_spark_session():
    return (
        SparkSession.builder
        .appName("BankingLoanPaymentsSilver")
        .master("local[*]")
        .getOrCreate()
    )


def transform_loan_payments(df):
    return (
        df
        .withColumn("status", upper(trim(col("status"))))
    )


def validate_loan_payments(df):
    return (
        df
        .withColumn(
            "quality_error",
            when(
                col("payment_id").isNull(),
                lit("NULL_PAYMENT_ID")
            )
            .when(
                col("loan_id").isNull(),
                lit("NULL_LOAN_ID")
            )
            .when(
                col("payment_number").isNull()
                | (col("payment_number") <= 0),
                lit("INVALID_PAYMENT_NUMBER")
            )
            .when(
                col("due_date").isNull(),
                lit("NULL_DUE_DATE")
            )
            .when(
                col("amount").isNull()
                | (col("amount") <= 0),
                lit("INVALID_AMOUNT")
            )
            .when(
                ~col("status").isin(VALID_STATUSES),
                lit("INVALID_STATUS")
            )
            .when(
                (col("status") == "PAID")
                & col("payment_date").isNull(),
                lit("MISSING_PAYMENT_DATE")
            )
            .when(
                (col("status") == "PENDING")
                & col("payment_date").isNotNull(),
                lit("UNEXPECTED_PAYMENT_DATE")
            )
        )
    )


def main():
    spark = create_spark_session()

    print("Reading Bronze loan payments...")
    payments = spark.read.parquet(BRONZE_PATH)

    print(f"Bronze records: {payments.count():,}")

    print("Reading Bronze loans...")
    loans = spark.read.parquet(BRONZE_LOANS_PATH)

    print(f"Loan records: {loans.count():,}")

    print("Transforming loan payments...")
    payments = transform_loan_payments(payments)

    print("Validating data quality...")
    validated = validate_loan_payments(payments)

    print("Validating loan references...")

    loan_reference = (
        loans
        .select(
            col("loan_id").alias("valid_loan_id")
        )
        .dropDuplicates()
    )

    validated = (
        validated
        .join(
            loan_reference,
            validated.loan_id == col("valid_loan_id"),
            "left"
        )
        .withColumn(
            "quality_error",
            when(
                col("quality_error").isNotNull(),
                col("quality_error")
            )
            .when(
                col("valid_loan_id").isNull(),
                lit("INVALID_LOAN_ID")
            )
        )
        .drop("valid_loan_id")
    )

    print("Checking duplicate payments...")

    duplicate_payments = (
        validated
        .groupBy("payment_id")
        .count()
        .filter(col("count") > 1)
        .select("payment_id")
    )

    validated = (
        validated
        .join(
            duplicate_payments.withColumn(
                "is_duplicate",
                lit(True)
            ),
            on="payment_id",
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
                lit("DUPLICATE_PAYMENT_ID")
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
    print("    LOAN PAYMENT DATA QUALITY")
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
                f"  {row['quality_error']:<35} "
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

    print("Silver loan payments completed.")

    spark.stop()


if __name__ == "__main__":
    main()