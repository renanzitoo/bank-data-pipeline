from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, lit, when


BRONZE_PATH = "data/bronze/cards"
BRONZE_ACCOUNTS_PATH = "data/bronze/accounts"

SILVER_PATH = "data/silver/cards"
QUARANTINE_PATH = "data/quarantine/cards"


VALID_CARD_TYPES = [
    "DEBIT",
    "CREDIT",
]

VALID_STATUSES = [
    "ACTIVE",
    "BLOCKED",
    "EXPIRED",
    "CANCELLED",
]


def create_spark_session():
    return (
        SparkSession.builder
        .appName("BankingCardsSilver")
        .master("local[*]")
        .getOrCreate()
    )


def transform_cards(df):
    return (
        df
        .withColumn("card_type", upper(trim(col("card_type"))))
        .withColumn("brand", upper(trim(col("brand"))))
        .withColumn("status", upper(trim(col("status"))))
    )


def validate_cards(df):
    return (
        df
        .withColumn(
            "quality_error",
            when(
                col("card_id").isNull(),
                lit("NULL_CARD_ID")
            )
            .when(
                col("account_id").isNull(),
                lit("NULL_ACCOUNT_ID")
            )
            .when(
                col("brand").isNull() | (trim(col("brand")) == ""),
                lit("INVALID_BRAND")
            )
            .when(
                ~col("card_type").isin(VALID_CARD_TYPES),
                lit("INVALID_CARD_TYPE")
            )
            .when(
                ~col("status").isin(VALID_STATUSES),
                lit("INVALID_STATUS")
            )
            .when(
                col("issued_at").isNull(),
                lit("NULL_ISSUED_AT")
            )
            .when(
                col("expires_at").isNull(),
                lit("NULL_EXPIRES_AT")
            )
            .when(
                col("expires_at") <= col("issued_at"),
                lit("INVALID_EXPIRATION_DATE")
            )
            .when(
                (col("card_type") == "CREDIT")
                & col("credit_limit").isNull(),
                lit("MISSING_CREDIT_LIMIT")
            )
            .when(
                (col("card_type") == "DEBIT")
                & col("credit_limit").isNotNull(),
                lit("UNEXPECTED_CREDIT_LIMIT")
            )
        )
    )


def main():
    spark = create_spark_session()

    print("Reading Bronze cards...")
    cards = spark.read.parquet(BRONZE_PATH)

    print(f"Bronze records: {cards.count():,}")

    print("Reading Bronze accounts...")
    accounts = spark.read.parquet(BRONZE_ACCOUNTS_PATH)

    print(f"Account records: {accounts.count():,}")

    print("Transforming cards...")
    cards = transform_cards(cards)

    print("Validating data quality...")
    validated = validate_cards(cards)

    print("Validating account references...")

    account_reference = (
        accounts
        .select(
            "account_id",
            col("account_type").alias("valid_account_type")
        )
        .dropDuplicates(["account_id"])
    )

    validated = (
        validated
        .join(
            account_reference,
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
                col("valid_account_type").isNull(),
                lit("INVALID_ACCOUNT_ID")
            )
            .when(
                (col("valid_account_type") == "SAVINGS")
                & (col("card_type") != "DEBIT"),
                lit("INVALID_CARD_TYPE_FOR_SAVINGS")
            )
        )
        .drop("valid_account_type")
    )

    print("Checking duplicate cards...")

    duplicate_cards = (
        validated
        .groupBy("card_id")
        .count()
        .filter(col("count") > 1)
        .select("card_id")
    )

    validated = (
        validated
        .join(
            duplicate_cards.withColumn(
                "is_duplicate",
                lit(True)
            ),
            on="card_id",
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
                lit("DUPLICATE_CARD_ID")
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
    print("         CARD DATA QUALITY")
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

    print("Silver cards completed.")

    spark.stop()


if __name__ == "__main__":
    main()