from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    when,
)


BRONZE_PATH = "data/bronze/transactions"
SILVER_PATH = "data/silver/transactions"


def create_spark_session():

    return (
        SparkSession.builder
        .appName("BankingSilver")
        .master("local[*]")
        .getOrCreate()
    )


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

    print(
        f"Bronze records: "
        f"{transactions.count():,}"
    )

    print("Transforming transactions...")

    silver = transform_transactions(
        transactions
    )

    print("Removing duplicated transactions...")

    silver = silver.dropDuplicates(
        ["transaction_id"]
    )

    print("Writing Silver...")

    (
        silver.write
        .mode("overwrite")
        .partitionBy(
            "year",
            "month",
        )
        .parquet(SILVER_PATH)
    )

    print(
        "Silver transactions completed."
    )

    spark.stop()


if __name__ == "__main__":
    main()