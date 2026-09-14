from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    sum,
    when,
)


CUSTOMERS_PATH = "data/silver/customers"
ACCOUNTS_PATH = "data/silver/accounts"
TRANSACTIONS_PATH = "data/silver/transactions"
CARDS_PATH = "data/silver/cards"
LOANS_PATH = "data/silver/loans"
PAYMENTS_PATH = "data/silver/loan_payments"

GOLD_PATH = "data/gold/customer_360"


def create_spark_session():

    return (
        SparkSession.builder
        .appName("BankingGoldCustomer360")
        .master("local[*]")
        .getOrCreate()
    )


def build_account_metrics(accounts):

    print("Building account metrics...")

    return (
        accounts
        .groupBy("customer_id")
        .agg(
            count("*").alias(
                "total_accounts"
            ),

            count(
                when(
                    col("status") == "ACTIVE",
                    True,
                )
            ).alias(
                "active_accounts"
            ),
        )
    )


def build_transaction_metrics(
    transactions,
    accounts,
):

    print("Building transaction metrics...")

    transaction_customer = (
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

    return (
        transaction_customer
        .groupBy("customer_id")
        .agg(
            count("*").alias(
                "total_transactions"
            ),

            sum("amount").alias(
                "total_transaction_amount"
            ),

            avg("amount").alias(
                "average_transaction_amount"
            ),
        )
    )


def build_card_metrics(
    cards,
    accounts,
):

    print("Building card metrics...")

    card_customer = (
        cards
        .join(
            accounts.select(
                "account_id",
                "customer_id",
            ),
            on="account_id",
            how="inner",
        )
    )

    return (
        card_customer
        .groupBy("customer_id")
        .agg(
            count("*").alias(
                "total_cards"
            ),

            count(
                when(
                    col("card_type") == "CREDIT",
                    True,
                )
            ).alias(
                "credit_cards"
            ),
        )
    )


def build_loan_metrics(loans):

    print("Building loan metrics...")

    return (
        loans
        .groupBy("customer_id")
        .agg(
            count("*").alias(
                "total_loans"
            ),

            sum("principal_amount").alias(
                "total_loan_amount"
            ),

            count(
                when(
                    col("status") == "ACTIVE",
                    True,
                )
            ).alias(
                "active_loans"
            ),

            count(
                when(
                    col("status") == "DEFAULTED",
                    True,
                )
            ).alias(
                "defaulted_loans"
            ),
        )
    )


def build_payment_metrics(
    payments,
    loans,
):

    print("Building payment metrics...")

    payment_customer = (
        payments
        .join(
            loans.select(
                "loan_id",
                "customer_id",
            ),
            on="loan_id",
            how="inner",
        )
    )

    return (
        payment_customer
        .groupBy("customer_id")
        .agg(
            count("*").alias(
                "total_loan_payments"
            ),

            count(
                when(
                    col("status") == "PAID",
                    True,
                )
            ).alias(
                "paid_payments"
            ),

            count(
                when(
                    col("status") == "LATE",
                    True,
                )
            ).alias(
                "late_payments"
            ),

            sum(
                when(
                    col("status") == "PAID",
                    col("amount"),
                ).otherwise(0)
            ).alias(
                "total_paid_amount"
            ),
        )
    )


def build_customer_360(
    customers,
    account_metrics,
    transaction_metrics,
    card_metrics,
    loan_metrics,
    payment_metrics,
):

    print("Joining customer metrics...")

    result = (
        customers
        .select(
            "customer_id",
            "customer_segment",
        )

        .join(
            account_metrics,
            on="customer_id",
            how="left",
        )

        .join(
            transaction_metrics,
            on="customer_id",
            how="left",
        )

        .join(
            card_metrics,
            on="customer_id",
            how="left",
        )

        .join(
            loan_metrics,
            on="customer_id",
            how="left",
        )

        .join(
            payment_metrics,
            on="customer_id",
            how="left",
        )

        .fillna(
            {
                "total_accounts": 0,
                "active_accounts": 0,
                "total_transactions": 0,
                "total_transaction_amount": 0,
                "average_transaction_amount": 0,
                "total_cards": 0,
                "credit_cards": 0,
                "total_loans": 0,
                "total_loan_amount": 0,
                "active_loans": 0,
                "defaulted_loans": 0,
                "total_loan_payments": 0,
                "paid_payments": 0,
                "late_payments": 0,
                "total_paid_amount": 0,
            }
        )
    )

    return result


def main():

    spark = create_spark_session()

    print("=" * 70)
    print("BANKING DATA LAKEHOUSE - GOLD")
    print("Customer 360")
    print("=" * 70)

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
    print("Reading Silver accounts...")

    accounts = spark.read.parquet(
        ACCOUNTS_PATH
    )

    print(
        f"Accounts: "
        f"{accounts.count():,}"
    )

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
    print("Reading Silver cards...")

    cards = spark.read.parquet(
        CARDS_PATH
    )

    print(
        f"Cards: "
        f"{cards.count():,}"
    )

    print()
    print("Reading Silver loans...")

    loans = spark.read.parquet(
        LOANS_PATH
    )

    print(
        f"Loans: "
        f"{loans.count():,}"
    )

    print()
    print("Reading Silver loan payments...")

    payments = spark.read.parquet(
        PAYMENTS_PATH
    )

    print(
        f"Loan payments: "
        f"{payments.count():,}"
    )

    print()

    account_metrics = build_account_metrics(
        accounts
    )

    transaction_metrics = build_transaction_metrics(
        transactions,
        accounts,
    )

    card_metrics = build_card_metrics(
        cards,
        accounts,
    )

    loan_metrics = build_loan_metrics(
        loans
    )

    payment_metrics = build_payment_metrics(
        payments,
        loans,
    )

    print()
    print("Building Customer 360...")

    gold = build_customer_360(
        customers,
        account_metrics,
        transaction_metrics,
        card_metrics,
        loan_metrics,
        payment_metrics,
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
    print("Customer 360 count:")

    customer_count = gold.count()

    print(
        f"Customers: "
        f"{customer_count:,}"
    )

    print()
    print("Customer segments:")

    (
        gold
        .groupBy("customer_segment")
        .count()
        .orderBy(
            col("count").desc()
        )
        .show(truncate=False)
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
        "Gold Customer 360 completed."
    )

    spark.stop()


if __name__ == "__main__":
    main()