from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    when,
    lit,
)


BRONZE_PATH = "data/bronze/merchants"
SILVER_PATH = "data/silver/merchants"
QUARANTINE_PATH = "data/quarantine/merchants"


VALID_MERCHANT_CATEGORIES = [
    "SUPERMARKET",
    "RESTAURANT",
    "GAS_STATION",
    "PHARMACY",
    "CLOTHING",
    "ELECTRONICS",
    "ENTERTAINMENT",
    "TRAVEL",
    "SERVICES",
    "OTHER",
]


VALID_STATES = [
    "AC",
    "AL",
    "AP",
    "AM",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MT",
    "MS",
    "MG",
    "PA",
    "PB",
    "PR",
    "PE",
    "PI",
    "RJ",
    "RN",
    "RS",
    "RO",
    "RR",
    "SC",
    "SP",
    "SE",
    "TO",
]


def create_spark_session():

    return (
        SparkSession.builder
        .appName("BankingSilverMerchants")
        .master("local[*]")
        .getOrCreate()
    )


def transform_merchants(df):

    transformed = (
        df
        .withColumn(
            "merchant_name",
            trim(col("merchant_name")),
        )
        .withColumn(
            "merchant_category",
            upper(
                trim(
                    col("merchant_category")
                )
            ),
        )
        .withColumn(
            "city",
            trim(col("city")),
        )
        .withColumn(
            "state",
            upper(
                trim(
                    col("state")
                )
            ),
        )
    )

    return transformed


def validate_merchants(df):

    validated = (
        df
        .withColumn(
            "quality_error",
            when(
                col("merchant_id").isNull(),
                lit("NULL_MERCHANT_ID"),
            )
            .when(
                col("merchant_name").isNull()
                | (trim(col("merchant_name")) == ""),
                lit("INVALID_MERCHANT_NAME"),
            )
            .when(
                ~col("merchant_category").isin(
                    VALID_MERCHANT_CATEGORIES
                ),
                lit("INVALID_MERCHANT_CATEGORY"),
            )
            .when(
                col("city").isNull()
                | (trim(col("city")) == ""),
                lit("INVALID_CITY"),
            )
            .when(
                ~col("state").isin(VALID_STATES),
                lit("INVALID_STATE"),
            )
            .when(
                col("created_at").isNull(),
                lit("NULL_CREATED_AT"),
            )
        )
    )

    return validated


def main():

    spark = create_spark_session()

    print("Reading Bronze merchants...")

    merchants = spark.read.parquet(
        BRONZE_PATH
    )

    print(
        f"Bronze records: {merchants.count():,}"
    )

    print("Transforming merchants...")

    merchants = transform_merchants(
        merchants
    )

    print("Validating data quality...")

    validated = validate_merchants(
        merchants
    )

    print("Checking duplicate merchants...")

    duplicate_merchants = (
        validated
        .groupBy("merchant_id")
        .count()
        .filter(
            col("count") > 1
        )
        .select("merchant_id")
    )

    validated = (
        validated
        .join(
            duplicate_merchants.withColumn(
                "is_duplicate",
                lit(True)
            ),
            on="merchant_id",
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
                lit("DUPLICATE_MERCHANT_ID")
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
            invalid
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

    (
        invalid.write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(
            QUARANTINE_PATH
        )
    )

    print("Writing Silver...")

    (
        valid
        .drop("quality_error")
        .write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(
            SILVER_PATH
        )
    )

    print(
        "Silver merchants completed."
    )

    spark.stop()


if __name__ == "__main__":
    main()