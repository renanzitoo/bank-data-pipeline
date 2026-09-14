from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, lit, when


BRONZE_PATH = "data/bronze/loans"
BRONZE_CUSTOMERS_PATH = "data/bronze/customers"

SILVER_PATH = "data/silver/loans"
QUARANTINE_PATH = "data/quarantine/loans"


VALID_LOAN_TYPES = [
    "PERSONAL",
    "PAYROLL",
    "AUTO",
]

VALID_STATUSES = [
    "ACTIVE",
    "PAID",
    "DEFAULTED",
    "CANCELLED",
]


def create_spark_session():
    return (
        SparkSession.builder
        .appName("BankingLoansSilver")
        .master("local[*]")
        .getOrCreate()
    )


def transform_loans(df):
    return (
        df
        .withColumn("loan_type", upper(trim(col("loan_type"))))
        .withColumn("status", upper(trim(col("status"))))
    )


def validate_loans(df):
    return (
        df
        .withColumn(
            "quality_error",
            when(
                col("loan_id").isNull(),
                lit("NULL_LOAN_ID")
            )
            .when(
                col("customer_id").isNull(),
                lit("NULL_CUSTOMER_ID")
            )
            .when(
                col("principal_amount").isNull()
                | (col("principal_amount") <= 0),
                lit("INVALID_PRINCIPAL_AMOUNT")
            )
            .when(
                col("interest_rate").isNull()
                | (col("interest_rate") < 0),
                lit("INVALID_INTEREST_RATE")
            )
            .when(
                col("term_months").isNull()
                | (col("term_months") <= 0),
                lit("INVALID_TERM_MONTHS")
            )
            .when(
                ~col("loan_type").isin(VALID_LOAN_TYPES),
                lit("INVALID_LOAN_TYPE")
            )
            .when(
                ~col("status").isin(VALID_STATUSES),
                lit("INVALID_STATUS")
            )
            .when(
                col("created_at").isNull(),
                lit("NULL_CREATED_AT")
            )
        )
    )


def main():
    spark = create_spark_session()

    print("Reading Bronze loans...")
    loans = spark.read.parquet(BRONZE_PATH)

    print(f"Bronze records: {loans.count():,}")

    print("Reading Bronze customers...")
    customers = spark.read.parquet(BRONZE_CUSTOMERS_PATH)

    print(f"Customer records: {customers.count():,}")

    print("Transforming loans...")
    loans = transform_loans(loans)

    print("Validating data quality...")
    validated = validate_loans(loans)

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

    print("Checking duplicate loans...")

    duplicate_loans = (
        validated
        .groupBy("loan_id")
        .count()
        .filter(col("count") > 1)
        .select("loan_id")
    )

    validated = (
        validated
        .join(
            duplicate_loans.withColumn(
                "is_duplicate",
                lit(True)
            ),
            on="loan_id",
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
                lit("DUPLICATE_LOAN_ID")
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
    print("         LOAN DATA QUALITY")
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

    print("Silver loans completed.")

    spark.stop()


if __name__ == "__main__":
    main()