from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum,
    when,
)


LOANS_PATH = "data/silver/loans"
PAYMENTS_PATH = "data/silver/loan_payments"

GOLD_PATH = "data/gold/loan_portfolio"


def create_spark_session():

    return (
        SparkSession.builder
        .appName("BankingGoldLoanPortfolio")
        .master("local[*]")
        .getOrCreate()
    )


def build_loan_portfolio(
    loans,
    payments,
):

    print("Aggregating loan payments...")

    payment_metrics = (
        payments
        .groupBy("loan_id")
        .agg(
            count("*").alias(
                "total_payments"
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
                    col("status") == "PENDING",
                    True,
                )
            ).alias(
                "pending_payments"
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

    print("Joining loans with payment metrics...")

    result = (
        loans
        .join(
            payment_metrics,
            on="loan_id",
            how="left",
        )
        .fillna(
            {
                "total_payments": 0,
                "paid_payments": 0,
                "pending_payments": 0,
                "late_payments": 0,
                "total_paid_amount": 0,
            }
        )
        .withColumn(
            "remaining_amount",
            col("principal_amount")
            - col("total_paid_amount")
        )
        .select(
            "loan_id",
            "customer_id",
            "loan_type",
            "principal_amount",
            "interest_rate",
            "term_months",
            col("status").alias(
                "loan_status"
            ),
            "created_at",
            "total_payments",
            "paid_payments",
            "pending_payments",
            "late_payments",
            "total_paid_amount",
            "remaining_amount",
        )
    )

    return result


def main():

    spark = create_spark_session()

    print("=" * 70)
    print("BANKING DATA LAKEHOUSE - GOLD")
    print("Loan Portfolio")
    print("=" * 70)

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

    gold = build_loan_portfolio(
        loans,
        payments,
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
    print("Loan portfolio count:")

    print(
        f"Loans in portfolio: "
        f"{gold.count():,}"
    )

    print()
    print("Portfolio by loan status:")

    (
        gold
        .groupBy("loan_status")
        .count()
        .orderBy(
            col("count").desc()
        )
        .show(
            truncate=False
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
        "Gold loan portfolio "
        "completed."
    )

    spark.stop()


if __name__ == "__main__":
    main()