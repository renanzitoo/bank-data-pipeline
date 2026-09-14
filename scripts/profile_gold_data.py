from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum,
)


SILVER_DIR = Path("data/silver")
GOLD_DIR = Path("data/gold")


def create_spark_session():

    return (
        SparkSession.builder
        .appName("BankingGoldQA")
        .master("local[*]")
        .getOrCreate()
    )


def print_header(title):

    print()
    print("=" * 70)
    print(title)
    print("=" * 70)


def test_primary_key(
    df,
    primary_key,
    dataset_name,
):

    print_header(
        f"PRIMARY KEY: {dataset_name.upper()}"
    )

    total = df.count()

    distinct_keys = (
        df
        .select(primary_key)
        .distinct()
        .count()
    )

    null_keys = (
        df
        .filter(
            col(primary_key).isNull()
        )
        .count()
    )

    duplicates = total - distinct_keys

    print(
        f"Records:             {total:,}"
    )

    print(
        f"Distinct PKs:        {distinct_keys:,}"
    )

    print(
        f"Duplicate records:   {duplicates:,}"
    )

    print(
        f"NULL PKs:            {null_keys:,}"
    )

    return {
        "total": total,
        "distinct": distinct_keys,
        "duplicates": duplicates,
        "nulls": null_keys,
    }


def test_composite_key(
    df,
    columns,
    dataset_name,
):

    print_header(
        f"COMPOSITE KEY: {dataset_name.upper()}"
    )

    total = df.count()

    distinct_keys = (
        df
        .select(*columns)
        .distinct()
        .count()
    )

    duplicates = total - distinct_keys

    null_condition = col(columns[0]).isNull()

    for column in columns[1:]:
        null_condition = (
            null_condition
            | col(column).isNull()
        )

    null_keys = (
        df
        .filter(null_condition)
        .count()
    )

    print(
        f"Records:             {total:,}"
    )

    print(
        f"Distinct keys:       {distinct_keys:,}"
    )

    print(
        f"Duplicate records:   {duplicates:,}"
    )

    print(
        f"NULL key records:    {null_keys:,}"
    )

    return {
        "total": total,
        "distinct": distinct_keys,
        "duplicates": duplicates,
        "nulls": null_keys,
    }

def validate_daily_transaction_summary(
    silver_transactions,
    gold,
):

    print_header(
        "DAILY TRANSACTION SUMMARY QA"
    )

    silver_count = (
        silver_transactions.count()
    )

    gold_count = (
        gold
        .select(
            sum("transaction_count")
        )
        .collect()[0][0]
    )

    print(
        f"Silver transactions: {silver_count:,}"
    )

    print(
        f"Gold transactions:   {gold_count:,}"
    )

    if silver_count == gold_count:

        print(
            "PASS: Transaction count matches."
        )

    else:

        print(
            "FAIL: Transaction count mismatch."
        )

    invalid_amount = (
        gold
        .filter(
            col("total_amount") <= 0
        )
        .count()
    )

    print(
        f"Groups with invalid total amount: "
        f"{invalid_amount:,}"
    )


def validate_customer_transaction_metrics(
    silver_transactions,
    gold,
):

    print_header(
        "CUSTOMER TRANSACTION METRICS QA"
    )

    silver_count = (
        silver_transactions.count()
    )

    gold_count = (
        gold
        .select(
            sum("transaction_count")
        )
        .collect()[0][0]
    )

    print(
        f"Silver transactions: {silver_count:,}"
    )

    print(
        f"Gold transactions:   {gold_count:,}"
    )

    if silver_count == gold_count:

        print(
            "PASS: Transaction count matches."
        )

    else:

        print(
            "FAIL: Transaction count mismatch."
        )

    customer_count = gold.count()

    print(
        f"Customers with transactions: "
        f"{customer_count:,}"
    )

    negative_metrics = (
        gold
        .filter(
            (col("transaction_count") < 0)
            |
            (
                col("total_transaction_amount")
                < 0
            )
        )
        .count()
    )

    print(
        f"Customers with negative metrics: "
        f"{negative_metrics:,}"
    )


def validate_merchant_performance(
    silver_transactions,
    gold,
):

    print_header(
        "MERCHANT PERFORMANCE QA"
    )

    merchant_transactions = (
        silver_transactions
        .filter(
            col("merchant_id").isNotNull()
        )
    )

    silver_count = (
        merchant_transactions.count()
    )

    gold_count = (
        gold
        .select(
            sum("transaction_count")
        )
        .collect()[0][0]
    )

    print(
        f"Silver merchant transactions: "
        f"{silver_count:,}"
    )

    print(
        f"Gold merchant transactions:   "
        f"{gold_count:,}"
    )

    if silver_count == gold_count:

        print(
            "PASS: Merchant transaction "
            "count matches."
        )

    else:

        print(
            "FAIL: Merchant transaction "
            "count mismatch."
        )

    negative_metrics = (
        gold
        .filter(
            (col("transaction_count") < 0)
            |
            (
                col("total_transaction_amount")
                < 0
            )
        )
        .count()
    )

    print(
        f"Merchants with negative metrics: "
        f"{negative_metrics:,}"
    )


def validate_loan_portfolio(
    silver_loans,
    silver_payments,
    gold,
):

    print_header(
        "LOAN PORTFOLIO QA"
    )

    silver_loans_count = (
        silver_loans.count()
    )

    gold_loans_count = gold.count()

    print(
        f"Silver loans: {silver_loans_count:,}"
    )

    print(
        f"Gold loans:   {gold_loans_count:,}"
    )

    if silver_loans_count == gold_loans_count:

        print(
            "PASS: Loan count matches."
        )

    else:

        print(
            "FAIL: Loan count mismatch."
        )

    silver_payment_count = (
        silver_payments.count()
    )

    gold_payment_count = (
        gold
        .select(
            sum("total_payments")
        )
        .collect()[0][0]
    )

    print(
        f"Silver payments: {silver_payment_count:,}"
    )

    print(
        f"Gold payments:   {gold_payment_count:,}"
    )

    if silver_payment_count == gold_payment_count:

        print(
            "PASS: Payment count matches."
        )

    else:

        print(
            "FAIL: Payment count mismatch."
        )

    invalid_remaining = (
        gold
        .filter(
            col("remaining_amount") < 0
        )
        .count()
    )

    print(
        f"Loans with negative remaining amount: "
        f"{invalid_remaining:,}"
    )


def validate_customer_360(
    silver_customers,
    silver_accounts,
    silver_transactions,
    silver_cards,
    silver_loans,
    gold,
):

    print_header(
        "CUSTOMER 360 QA"
    )

    silver_customer_count = (
        silver_customers.count()
    )

    gold_customer_count = gold.count()

    print(
        f"Silver customers: "
        f"{silver_customer_count:,}"
    )

    print(
        f"Gold customers:   "
        f"{gold_customer_count:,}"
    )

    if (
        silver_customer_count
        == gold_customer_count
    ):

        print(
            "PASS: Customer count matches."
        )

    else:

        print(
            "FAIL: Customer count mismatch."
        )

    gold_accounts = (
        gold
        .select(
            sum("total_accounts")
        )
        .collect()[0][0]
    )

    silver_accounts_count = (
        silver_accounts.count()
    )

    print(
        f"Silver accounts: {silver_accounts_count:,}"
    )

    print(
        f"Gold accounts:   {gold_accounts:,}"
    )

    if silver_accounts_count == gold_accounts:

        print(
            "PASS: Account count matches."
        )

    else:

        print(
            "FAIL: Account count mismatch."
        )

    gold_transactions = (
        gold
        .select(
            sum("total_transactions")
        )
        .collect()[0][0]
    )

    silver_transactions_count = (
        silver_transactions.count()
    )

    print(
        f"Silver transactions: "
        f"{silver_transactions_count:,}"
    )

    print(
        f"Gold transactions:   "
        f"{gold_transactions:,}"
    )

    if (
        silver_transactions_count
        == gold_transactions
    ):

        print(
            "PASS: Transaction count matches."
        )

    else:

        print(
            "FAIL: Transaction count mismatch."
        )

    gold_cards = (
        gold
        .select(
            sum("total_cards")
        )
        .collect()[0][0]
    )

    silver_cards_count = (
        silver_cards.count()
    )

    print(
        f"Silver cards: {silver_cards_count:,}"
    )

    print(
        f"Gold cards:   {gold_cards:,}"
    )

    if silver_cards_count == gold_cards:

        print(
            "PASS: Card count matches."
        )

    else:

        print(
            "FAIL: Card count mismatch."
        )

    gold_loans = (
        gold
        .select(
            sum("total_loans")
        )
        .collect()[0][0]
    )

    silver_loans_count = (
        silver_loans.count()
    )

    print(
        f"Silver loans: {silver_loans_count:,}"
    )

    print(
        f"Gold loans:   {gold_loans:,}"
    )

    if silver_loans_count == gold_loans:

        print(
            "PASS: Loan count matches."
        )

    else:

        print(
            "FAIL: Loan count mismatch."
        )

    negative_metrics = (
        gold
        .filter(
            (col("total_accounts") < 0)
            |
            (col("total_transactions") < 0)
            |
            (col("total_cards") < 0)
            |
            (col("total_loans") < 0)
            |
            (
                col("total_loan_payments")
                < 0
            )
        )
        .count()
    )

    print(
        f"Customers with negative metrics: "
        f"{negative_metrics:,}"
    )


def main():

    spark = create_spark_session()

    print_header(
        "BANKING DATA LAKEHOUSE - GOLD QA"
    )

    print("Reading Silver datasets...")

    customers = spark.read.parquet(
        str(
            SILVER_DIR / "customers"
        )
    )

    accounts = spark.read.parquet(
        str(
            SILVER_DIR / "accounts"
        )
    )

    transactions = spark.read.parquet(
        str(
            SILVER_DIR / "transactions"
        )
    )

    cards = spark.read.parquet(
        str(
            SILVER_DIR / "cards"
        )
    )

    loans = spark.read.parquet(
        str(
            SILVER_DIR / "loans"
        )
    )

    payments = spark.read.parquet(
        str(
            SILVER_DIR / "loan_payments"
        )
    )

    print("Reading Gold datasets...")

    daily_transactions = spark.read.parquet(
        str(
            GOLD_DIR
            / "daily_transaction_summary"
        )
    )

    customer_transactions = spark.read.parquet(
        str(
            GOLD_DIR
            / "customer_transaction_metrics"
        )
    )

    merchant_performance = spark.read.parquet(
        str(
            GOLD_DIR
            / "merchant_performance"
        )
    )

    loan_portfolio = spark.read.parquet(
        str(
            GOLD_DIR
            / "loan_portfolio"
        )
    )

    customer_360 = spark.read.parquet(
        str(
            GOLD_DIR
            / "customer_360"
        )
    )

    # --------------------------------------------------
    # PRIMARY KEY VALIDATION
    # --------------------------------------------------

    test_composite_key(
        daily_transactions,
        [
            "transaction_date",
            "transaction_type",
            "status",
        ],
        "daily_transaction_summary",
    )

    test_primary_key(
        customer_transactions,
        "customer_id",
        "customer_transaction_metrics",
    )

    test_primary_key(
        merchant_performance,
        "merchant_id",
        "merchant_performance",
    )

    test_primary_key(
        loan_portfolio,
        "loan_id",
        "loan_portfolio",
    )

    test_primary_key(
        customer_360,
        "customer_id",
        "customer_360",
    )

    # --------------------------------------------------
    # GOLD VALIDATION
    # --------------------------------------------------

    validate_daily_transaction_summary(
        transactions,
        daily_transactions,
    )

    validate_customer_transaction_metrics(
        transactions,
        customer_transactions,
    )

    validate_merchant_performance(
        transactions,
        merchant_performance,
    )

    validate_loan_portfolio(
        loans,
        payments,
        loan_portfolio,
    )

    validate_customer_360(
        customers,
        accounts,
        transactions,
        cards,
        loans,
        customer_360,
    )

    print_header(
        "GOLD QA COMPLETED"
    )

    spark.stop()


if __name__ == "__main__":
    main()