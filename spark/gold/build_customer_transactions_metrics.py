from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    max,
    min,
    sum,
    when,
)


TRANSACTIONS_PATH = "s3a://banking/silver/transactions"
ACCOUNTS_PATH = "s3a://banking/silver/accounts"
CUSTOMERS_PATH = "s3a://banking/silver/customers"

GOLD_PATH = "s3a://banking/gold/customer_transaction_metrics"


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


def build_customer_transaction_metrics(
    transactions,
    accounts,
    customers,
):

    print("Joining transactions with accounts...")

    transaction_accounts = (
        transactions
        .join(
            accounts.select(
                "account_id",
                "customer_id",
            ),
            on="account_id",
            how="inner",
        )
    )

    print("Joining with customers...")

    customer_transactions = (
        transaction_accounts
        .join(
            customers.select(
                "customer_id",
                "customer_segment",
            ),
            on="customer_id",
            how="inner",
        )
    )

    print("Aggregating customer transaction metrics...")

    result = (
        customer_transactions
        .groupBy(
            "customer_id",
            "customer_segment",
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
                    col("transaction_type") == "PIX",
                    True,
                )
            ).alias(
                "pix_count"
            ),

            count(
                when(
                    col("transaction_type") == "CARD_PURCHASE",
                    True,
                )
            ).alias(
                "card_purchase_count"
            ),

            count(
                when(
                    col("transaction_type") == "TRANSFER",
                    True,
                )
            ).alias(
                "transfer_count"
            ),

            count(
                when(
                    col("transaction_type") == "DEPOSIT",
                    True,
                )
            ).alias(
                "deposit_count"
            ),

            count(
                when(
                    col("transaction_type") == "WITHDRAWAL",
                    True,
                )
            ).alias(
                "withdrawal_count"
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

            min(
                "transaction_timestamp"
            ).alias(
                "first_transaction_at"
            ),

            max(
                "transaction_timestamp"
            ).alias(
                "last_transaction_at"
            ),
        )
    )

    return result


def main():

    spark = create_spark_session()

    print("=" * 70)
    print("BANKING DATA LAKEHOUSE - GOLD")
    print("Customer Transaction Metrics")
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
    print("Reading Silver accounts...")

    accounts = spark.read.parquet(
        ACCOUNTS_PATH
    )

    print(
        f"Accounts: "
        f"{accounts.count():,}"
    )

    print()
    print("Reading Silver customers...")

    customers = spark.read.parquet(
        CUSTOMERS_PATH
    )

    print(
        f"Customers: "
        f"{customers.count():,}"
    )

    print()

    gold = build_customer_transaction_metrics(
        transactions,
        accounts,
        customers,
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
    print("Customer metrics count:")

    print(
        f"Customers with transactions: "
        f"{gold.count():,}"
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
        "Gold customer transaction "
        "metrics completed."
    )

    spark.stop()


if __name__ == "__main__":
    main()
    