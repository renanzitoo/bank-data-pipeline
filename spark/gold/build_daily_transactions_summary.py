from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    max,
    min,
    sum,
    to_date,
)


SILVER_PATH = "data/silver/transactions"
GOLD_PATH = "data/gold/daily_transaction_summary"


def create_spark_session():

    return (
        SparkSession.builder
        .appName("BankingGoldDailyTransactionSummary")
        .master("local[*]")
        .getOrCreate()
    )


def build_daily_transaction_summary(df):

    result = (
        df
        .withColumn(
            "transaction_date",
            to_date(
                col("transaction_timestamp")
            )
        )
        .groupBy(
            "transaction_date",
            "transaction_type",
            "status",
        )
        .agg(
            count("*").alias(
                "transaction_count"
            ),
            sum("amount").alias(
                "total_amount"
            ),
            avg("amount").alias(
                "average_amount"
            ),
            min("amount").alias(
                "minimum_amount"
            ),
            max("amount").alias(
                "maximum_amount"
            ),
        )
        .orderBy(
            "transaction_date",
            "transaction_type",
            "status",
        )
    )

    return result


def main():

    spark = create_spark_session()

    print("=" * 70)
    print("BANKING DATA LAKEHOUSE - GOLD")
    print("Daily Transaction Summary")
    print("=" * 70)

    print()
    print("Reading Silver transactions...")

    transactions = spark.read.parquet(
        SILVER_PATH
    )

    print(
        f"Silver records: "
        f"{transactions.count():,}"
    )

    print()
    print("Building daily transaction summary...")

    gold = build_daily_transaction_summary(
        transactions
    )

    print()
    print("Gold schema:")

    gold.printSchema()

    print()
    print("Sample results:")

    gold.show(
        20,
        truncate=False
    )

    print()
    print("Writing Gold...")

    (
        gold.write
        .mode("overwrite")
        .partitionBy("transaction_date")
        .parquet(GOLD_PATH)
    )

    print()
    print("Gold daily transaction summary completed.")

    spark.stop()


if __name__ == "__main__":
    main()