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


SILVER_PATH = "s3a://banking/silver/transactions"
GOLD_PATH = "s3a://banking/gold/daily_transaction_summary"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("BankingGoldDailyTransactionSummary")
        .master("local[4]")
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            "http://localhost:9000"
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            "banking"
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            "banking_dev"
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true"
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false"
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint.region",
            "us-east-1"
        )
        .config(
            "spark.sql.shuffle.partitions",
            "100"
        )
        .config(
            "spark.default.parallelism",
            "100"
        )
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