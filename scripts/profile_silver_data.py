from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    max,
    min,
    sum,
    when,
)


SILVER_DIR = "s3a://banking/silver"


DATASETS = {
    "customers": {
        "path": f"{SILVER_DIR}/customers",
        "primary_key": "customer_id",
    },
    "accounts": {
        "path": f"{SILVER_DIR}/accounts",
        "primary_key": "account_id",
    },
    "merchants": {
        "path": f"{SILVER_DIR}/merchants",
        "primary_key": "merchant_id",
    },
    "transactions": {
        "path": f"{SILVER_DIR}/transactions",
        "primary_key": "transaction_id",
    },
    "cards": {
        "path": f"{SILVER_DIR}/cards",
        "primary_key": "card_id",
    },
    "loans": {
        "path": f"{SILVER_DIR}/loans",
        "primary_key": "loan_id",
    },
    "loan_payments": {
        "path": f"{SILVER_DIR}/loan_payments",
        "primary_key": "payment_id",
    },
}


def create_spark_session():
    return (
        SparkSession.builder
        .appName("BankingSilverQA")
        .master("local[*]")
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            "http://minio:9000",
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            "banking",
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            "banking_dev",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
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


def print_header(title):
    print()
    print("=" * 70)
    print(title)
    print("=" * 70)


def test_primary_key(df, primary_key):
    total = df.count()

    distinct_keys = (
        df
        .select(primary_key)
        .distinct()
        .count()
    )

    null_keys = (
        df
        .filter(col(primary_key).isNull())
        .count()
    )

    duplicate_records = total - distinct_keys

    print(f"Records:             {total:,}")
    print(f"Distinct PKs:        {distinct_keys:,}")
    print(f"Duplicate records:   {duplicate_records:,}")
    print(f"NULL PKs:            {null_keys:,}")

    return {
        "total": total,
        "distinct": distinct_keys,
        "duplicates": duplicate_records,
        "nulls": null_keys,
    }


def test_dataset(spark, name, config):
    print_header(f"DATASET: {name.upper()}")

    start = perf_counter()

    df = spark.read.parquet(
        config["path"]
    )

    results = test_primary_key(
        df,
        config["primary_key"],
    )

    elapsed = perf_counter() - start

    print(f"Execution time:      {elapsed:.2f}s")

    return df, results


def test_nulls(df, columns):
    print("NULL analysis:")

    expressions = [
        count(
            when(col(column).isNull(), 1)
        ).alias(column)
        for column in columns
    ]

    result = df.select(
        *expressions
    ).collect()[0]

    for column in columns:
        value = result[column]

        print(
            f"  {column:<25} {value:,}"
        )


def test_transactions(df):
    print_header("TRANSACTION ANALYSIS")

    print("Transaction types:")

    (
        df
        .groupBy("transaction_type")
        .count()
        .orderBy(col("count").desc())
        .show(truncate=False)
    )

    print("Transaction statuses:")

    (
        df
        .groupBy("status")
        .count()
        .orderBy(col("count").desc())
        .show(truncate=False)
    )

    print("Transaction statistics:")

    (
        df
        .select(
            count("*").alias("transactions"),
            sum("amount").alias("total_amount"),
            avg("amount").alias("average_amount"),
            min("amount").alias("minimum_amount"),
            max("amount").alias("maximum_amount"),
        )
        .show(truncate=False)
    )

    print("Transactions by currency:")

    (
        df
        .groupBy("currency")
        .count()
        .orderBy(col("count").desc())
        .show(truncate=False)
    )

    print("CARD_PURCHASE merchant validation:")

    invalid_card_transactions = (
        df
        .filter(
            (col("transaction_type") == "CARD_PURCHASE")
            & col("merchant_id").isNull()
        )
        .count()
    )

    print(
        f"  CARD_PURCHASE without merchant_id: "
        f"{invalid_card_transactions:,}"
    )


def test_cards(df):
    print_header("CARD ANALYSIS")

    print("Card types:")

    (
        df
        .groupBy("card_type")
        .count()
        .orderBy(col("count").desc())
        .show(truncate=False)
    )

    print("Card statuses:")

    (
        df
        .groupBy("status")
        .count()
        .orderBy(col("count").desc())
        .show(truncate=False)
    )

    print("Card brands:")

    (
        df
        .groupBy("brand")
        .count()
        .orderBy(col("count").desc())
        .show(truncate=False)
    )

    print("Credit limit rules:")

    invalid_credit = (
        df
        .filter(
            (
                (col("card_type") == "CREDIT")
                & col("credit_limit").isNull()
            )
            |
            (
                (col("card_type") == "DEBIT")
                & col("credit_limit").isNotNull()
            )
        )
        .count()
    )

    print(
        f"  Invalid credit limit rules: "
        f"{invalid_credit:,}"
    )


def test_loans(df):
    print_header("LOAN ANALYSIS")

    print("Loan types:")

    (
        df
        .groupBy("loan_type")
        .count()
        .orderBy(col("count").desc())
        .show(truncate=False)
    )

    print("Loan statuses:")

    (
        df
        .groupBy("status")
        .count()
        .orderBy(col("count").desc())
        .show(truncate=False)
    )

    print("Loan statistics:")

    (
        df
        .select(
            count("*").alias("loans"),
            sum("principal_amount").alias(
                "total_principal"
            ),
            avg("principal_amount").alias(
                "average_principal"
            ),
            avg("interest_rate").alias(
                "average_interest_rate"
            ),
        )
        .show(truncate=False)
    )


def test_loan_payments(df):
    print_header("LOAN PAYMENT ANALYSIS")

    print("Payment statuses:")

    (
        df
        .groupBy("status")
        .count()
        .orderBy(col("count").desc())
        .show(truncate=False)
    )

    print("Payment statistics:")

    (
        df
        .select(
            count("*").alias("payments"),
            sum("amount").alias("total_amount"),
            avg("amount").alias("average_amount"),
        )
        .show(truncate=False)
    )

    print("Payment date rules:")

    invalid_paid = (
        df
        .filter(
            (col("status") == "PAID")
            & col("payment_date").isNull()
        )
        .count()
    )

    invalid_pending = (
        df
        .filter(
            (col("status") == "PENDING")
            & col("payment_date").isNotNull()
        )
        .count()
    )

    print(
        f"  PAID without payment_date: "
        f"{invalid_paid:,}"
    )

    print(
        f"  PENDING with payment_date: "
        f"{invalid_pending:,}"
    )


def test_foreign_key(
    child_df,
    parent_df,
    child_key,
    parent_key,
    relationship_name,
):
    parent_keys = (
        parent_df
        .select(
            col(parent_key).alias("_parent_key")
        )
        .distinct()
    )

    invalid = (
        child_df
        .join(
            parent_keys,
            col(child_key) == col("_parent_key"),
            "left",
        )
        .filter(
            col("_parent_key").isNull()
        )
        .count()
    )

    print(
        f"  {relationship_name}: "
        f"{invalid:,} invalid references"
    )


def test_referential_integrity(datasets):
    print_header("REFERENTIAL INTEGRITY")

    test_foreign_key(
        datasets["accounts"],
        datasets["customers"],
        "customer_id",
        "customer_id",
        "accounts.customer_id -> customers.customer_id",
    )

    test_foreign_key(
        datasets["transactions"],
        datasets["accounts"],
        "account_id",
        "account_id",
        "transactions.account_id -> accounts.account_id",
    )

    test_foreign_key(
        datasets["transactions"].filter(
            col("merchant_id").isNotNull()
        ),
        datasets["merchants"],
        "merchant_id",
        "merchant_id",
        "transactions.merchant_id -> merchants.merchant_id",
    )

    test_foreign_key(
        datasets["cards"],
        datasets["accounts"],
        "account_id",
        "account_id",
        "cards.account_id -> accounts.account_id",
    )

    test_foreign_key(
        datasets["loans"],
        datasets["customers"],
        "customer_id",
        "customer_id",
        "loans.customer_id -> customers.customer_id",
    )

    test_foreign_key(
        datasets["loan_payments"],
        datasets["loans"],
        "loan_id",
        "loan_id",
        "loan_payments.loan_id -> loans.loan_id",
    )


def test_business_rules(datasets):
    print_header("BUSINESS RULES")

    cards = datasets["cards"]
    accounts = datasets["accounts"]

    savings_credit_cards = (
        cards
        .join(
            accounts.select(
                "account_id",
                "account_type",
            ),
            on="account_id",
            how="inner",
        )
        .filter(
            (col("account_type") == "SAVINGS")
            & (col("card_type") == "CREDIT")
        )
        .count()
    )

    print(
        f"  SAVINGS accounts with CREDIT card: "
        f"{savings_credit_cards:,}"
    )

    transactions = datasets["transactions"]

    invalid_transaction_types = (
        transactions
        .filter(
            ~col("transaction_type").isin(
                [
                    "PIX",
                    "CARD_PURCHASE",
                    "TRANSFER",
                    "DEPOSIT",
                    "WITHDRAWAL",
                    "TED",
                    "DOC",
                ]
            )
        )
        .count()
    )

    print(
        f"  Invalid transaction types: "
        f"{invalid_transaction_types:,}"
    )

    invalid_statuses = (
        transactions
        .filter(
            ~col("status").isin(
                [
                    "COMPLETED",
                    "PENDING",
                    "FAILED",
                    "CANCELLED",
                ]
            )
        )
        .count()
    )

    print(
        f"  Invalid transaction statuses: "
        f"{invalid_statuses:,}"
    )


def main():
    spark = create_spark_session()

    print_header(
        "BANKING DATA LAKEHOUSE - SILVER QA"
    )

    datasets = {}
    summary = {}

    for name, config in DATASETS.items():

        df, results = test_dataset(
            spark,
            name,
            config,
        )

        datasets[name] = df
        summary[name] = results

    test_referential_integrity(
        datasets
    )

    test_business_rules(
        datasets
    )

    test_transactions(
        datasets["transactions"]
    )

    test_cards(
        datasets["cards"]
    )

    test_loans(
        datasets["loans"]
    )

    test_loan_payments(
        datasets["loan_payments"]
    )

    print_header("SILVER QA SUMMARY")

    print(
        f"{'Dataset':<20}"
        f"{'Records':>15}"
        f"{'Duplicates':>15}"
        f"{'NULL PK':>15}"
    )

    print("-" * 65)

    for name, result in summary.items():

        print(
            f"{name:<20}"
            f"{result['total']:>15,}"
            f"{result['duplicates']:>15,}"
            f"{result['nulls']:>15,}"
        )

    print()
    print("=" * 70)
    print("SILVER QA COMPLETED")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()