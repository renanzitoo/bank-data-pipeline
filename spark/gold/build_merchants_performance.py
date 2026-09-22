from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    sum,
    when,
)


TRANSACTIONS_PATH = "s3a://banking/silver/transactions"
MERCHANTS_PATH = "s3a://banking/silver/merchants"

GOLD_PATH = "s3a://banking/gold/merchant_performance"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("BankingGoldDailyTransactionSummary")
        .master("local[4]")
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            "http://minio:9000"
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


def build_merchant_performance(
    transactions,
    merchants,
):

    print("Filtering merchant transactions...")

    merchant_transactions = (
        transactions
        .filter(
            col("merchant_id").isNotNull()
        )
    )

    print("Joining transactions with merchants...")

    enriched = (
        merchant_transactions
        .join(
            merchants.select(
                "merchant_id",
                "merchant_name",
                "merchant_category",
                "city",
                "state",
            ),
            on="merchant_id",
            how="inner",
        )
    )

    print("Aggregating merchant performance...")

    result = (
        enriched
        .groupBy(
            "merchant_id",
            "merchant_name",
            "merchant_category",
            "city",
            "state",
        )
        .agg(
            count("*").alias(
                "transaction_count"
            ),

            sum("amount").alias(
                "total_transaction_amount"
            ),

            avg("amount").alias(
                "average_transaction_amount"
            ),

            count(
                when(
                    col("status") == "COMPLETED",
                    True,
                )
            ).alias(
                "completed_transactions"
            ),

            count(
                when(
                    col("status") == "FAILED",
                    True,
                )
            ).alias(
                "failed_transactions"
            ),

            count(
                when(
                    col("status") == "PENDING",
                    True,
                )
            ).alias(
                "pending_transactions"
            ),

            countDistinct(
                "account_id"
            ).alias(
                "unique_accounts"
            ),
        )
    )

    return result


def main():

    spark = create_spark_session()

    print("=" * 70)
    print("BANKING DATA LAKEHOUSE - GOLD")
    print("Merchant Performance")
    print("=" * 70)

    print()
    print("Reading Silver transactions...")

    transactions = spark.read.parquet(
        TRANSACTIONS_PATH
    )

    print(
        f"Transactions: "
        f"{transactions.count():,}"
    )

    print()
    print("Reading Silver merchants...")

    merchants = spark.read.parquet(
        MERCHANTS_PATH
    )

    print(
        f"Merchants: "
        f"{merchants.count():,}"
    )

    print()

    gold = build_merchant_performance(
        transactions,
        merchants,
    )

    print()
    print("Gold schema:")

    gold.printSchema()

    print()
    print("Sample results:")

    gold.show(
        20,
        truncate=False,
    )

    print()
    print("Merchant metrics count:")

    print(
        f"Merchants with transactions: "
        f"{gold.count():,}"
    )

    print()
    print("Top merchants by transaction amount:")

    (
        gold
        .orderBy(
            col(
                "total_transaction_amount"
            ).desc()
        )
        .show(
            10,
            truncate=False,
        )
    )

    print()
    print("Writing Gold...")

    (
        gold.write
        .mode("overwrite")
        .parquet(
            GOLD_PATH
        )
    )

    print()
    print(
        "Gold merchant performance "
        "completed."
    )

    spark.stop()


if __name__ == "__main__":
    main()