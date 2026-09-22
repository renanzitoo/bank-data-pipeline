from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    when,
    lit,
)
from pyspark import StorageLevel


# ============================================================
# PATHS
# ============================================================

BRONZE_PATH = "s3a://banking/bronze/loans"
BRONZE_CUSTOMERS_PATH = "s3a://banking/bronze/customers"

SILVER_PATH = "s3a://banking/silver/loans"
QUARANTINE_PATH = "data/quarantine/loans"


# ============================================================
# VALID VALUES
# ============================================================

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


# ============================================================
# SPARK
# ============================================================

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


# ============================================================
# TRANSFORMATION
# ============================================================

def transform_loans(df):

    transformed = (
        df

        .withColumn(
            "loan_type",
            upper(
                trim(
                    col("loan_type")
                )
            )
        )

        .withColumn(
            "status",
            upper(
                trim(
                    col("status")
                )
            )
        )
    )

    return transformed


# ============================================================
# DATA QUALITY
# ============================================================

def validate_loans(df):

    validated = (
        df

        # ----------------------------------------------------
        # NULL PRIMARY KEY
        # ----------------------------------------------------

        .withColumn(
            "quality_error",

            when(
                col("loan_id").isNull(),
                lit("NULL_LOAN_ID")
            )

            # ------------------------------------------------
            # NULL CUSTOMER
            # ------------------------------------------------

            .when(
                col("customer_id").isNull(),
                lit("NULL_CUSTOMER_ID")
            )

            # ------------------------------------------------
            # INVALID LOAN TYPE
            # ------------------------------------------------

            .when(
                ~col("loan_type").isin(
                    VALID_LOAN_TYPES
                ),
                lit("INVALID_LOAN_TYPE")
            )

            # ------------------------------------------------
            # INVALID PRINCIPAL
            # ------------------------------------------------

            .when(
                col("principal_amount").isNull()
                | (col("principal_amount") <= 0),
                lit("INVALID_PRINCIPAL_AMOUNT")
            )

            # ------------------------------------------------
            # INVALID INTEREST
            # ------------------------------------------------

            .when(
                col("interest_rate").isNull()
                | (col("interest_rate") < 0),
                lit("INVALID_INTEREST_RATE")
            )

            # ------------------------------------------------
            # INVALID TERM
            # ------------------------------------------------

            .when(
                col("term_months").isNull()
                | (col("term_months") <= 0),
                lit("INVALID_TERM_MONTHS")
            )

            # ------------------------------------------------
            # INVALID STATUS
            # ------------------------------------------------

            .when(
                ~col("status").isin(
                    VALID_STATUSES
                ),
                lit("INVALID_STATUS")
            )

            # ------------------------------------------------
            # CREATED AT
            # ------------------------------------------------

            .when(
                col("created_at").isNull(),
                lit("NULL_CREATED_AT")
            )
        )
    )

    return validated


# ============================================================
# MAIN
# ============================================================

def main():

    spark = create_spark_session()

    # ========================================================
    # READ BRONZE LOANS
    # ========================================================

    print("Reading Bronze loans...")

    loans = (
        spark.read
        .parquet(
            BRONZE_PATH
        )
    )

    # ========================================================
    # READ BRONZE CUSTOMERS
    # ========================================================

    print("Reading Bronze customers...")

    customers = (
        spark.read
        .parquet(
            BRONZE_CUSTOMERS_PATH
        )
        .select(
            "customer_id"
        )
        .dropDuplicates()
    )

    # ========================================================
    # TRANSFORM
    # ========================================================

    print("Transforming loans...")

    loans = transform_loans(
        loans
    )

    # ========================================================
    # BASIC DATA QUALITY
    # ========================================================

    print("Validating data quality...")

    validated = validate_loans(
        loans
    )

    # ========================================================
    # CUSTOMER FK
    # ========================================================

    print("Validating customer references...")

    customer_reference = (
        customers
        .select(
            col("customer_id")
            .alias(
                "valid_customer_id"
            )
        )
    )

    validated = (
        validated

        .join(
            customer_reference,

            validated.customer_id
            == col("valid_customer_id"),

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

                lit(
                    "INVALID_CUSTOMER_ID"
                )
            )
        )

        .drop(
            "valid_customer_id"
        )
    )

    # ========================================================
    # DUPLICATE LOAN ID
    # ========================================================

    print("Checking duplicate loans...")

    duplicate_loans = (
        validated

        .groupBy(
            "loan_id"
        )

        .count()

        .filter(
            col("count") > 1
        )

        .select(
            "loan_id"
        )
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

                lit(
                    "DUPLICATE_LOAN_ID"
                )
            )
        )

        .drop(
            "is_duplicate"
        )
    )

    # ========================================================
    # PERSIST
    # ========================================================

    validated = validated.persist(
        StorageLevel.MEMORY_AND_DISK
    )

    # ========================================================
    # DATA QUALITY REPORT
    # ========================================================

    print(
        "Generating Data Quality Report..."
    )

    quality_counts = (
        validated

        .select(

            when(
                col("quality_error").isNull(),
                lit(1)
            )
            .otherwise(
                lit(0)
            )
            .alias(
                "valid"
            ),

            when(
                col("quality_error").isNotNull(),
                lit(1)
            )
            .otherwise(
                lit(0)
            )
            .alias(
                "invalid"
            )
        )

        .agg(
            {
                "valid": "sum",
                "invalid": "sum"
            }
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

    # ========================================================
    # ERROR REPORT
    # ========================================================

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

            .groupBy(
                "quality_error"
            )

            .count()

            .orderBy(
                col("count").desc()
            )
        )

        for row in error_counts.collect():

            print(
                f"  {row['quality_error']:<30}"
                f"{row['count']:,}"
            )

    print("=" * 40)
    print()

    # ========================================================
    # QUARANTINE
    # ========================================================

    print(
        "Writing Quarantine..."
    )

    invalid = (
        validated
        .filter(
            col("quality_error").isNotNull()
        )
    )

    (
        invalid

        .write

        .mode(
            "overwrite"
        )

        .partitionBy(
            "year",
            "month"
        )

        .parquet(
            QUARANTINE_PATH
        )
    )

    # ========================================================
    # SILVER
    # ========================================================

    print(
        "Writing Silver..."
    )

    valid = (
        validated

        .filter(
            col("quality_error").isNull()
        )

        .drop(
            "quality_error"
        )
    )

    (
        valid

        .write

        .mode(
            "overwrite"
        )

        .partitionBy(
            "year",
            "month"
        )

        .parquet(
            SILVER_PATH
        )
    )

    # ========================================================
    # FINISH
    # ========================================================

    print()
    print(
        "Silver loans completed."
    )

    print(
        f"Silver path: {SILVER_PATH}"
    )

    print(
        f"Valid records: {valid_count:,}"
    )

    print(
        f"Invalid records: {invalid_count:,}"
    )

    validated.unpersist(
        blocking=True
    )

    spark.stop()


# ============================================================
# ENTRYPOINT
# ============================================================

if __name__ == "__main__":
    main()