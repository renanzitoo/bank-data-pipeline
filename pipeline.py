import subprocess
import sys


# ============================================================
# SPARK CONFIGURATION
# ============================================================

SPARK_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,"
    "org.apache.hadoop:hadoop-aws:3.5.0"
)

SPARK_SUBMIT_BASE = [
    "/opt/spark/bin/spark-submit",

    "--master",
    "local[4]",

    "--driver-memory",
    "4g",

    "--conf",
    "spark.sql.shuffle.partitions=200",

    "--conf",
    "spark.default.parallelism=200",

    "--conf",
    "spark.jars.ivy=/opt/spark/.ivy2",

    # --------------------------------------------------------
    # S3A / MINIO
    # --------------------------------------------------------

    "--conf",
    "spark.hadoop.fs.s3a.endpoint=http://minio:9000",

    "--conf",
    "spark.hadoop.fs.s3a.access.key=banking",

    "--conf",
    "spark.hadoop.fs.s3a.secret.key=banking_dev",

    "--conf",
    "spark.hadoop.fs.s3a.aws.credentials.provider="
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",

    "--conf",
    "spark.hadoop.fs.s3a.path.style.access=true",

    "--conf",
    "spark.hadoop.fs.s3a.connection.ssl.enabled=false",

    "--conf",
    "spark.hadoop.fs.s3a.endpoint.region=us-east-1",

    # --------------------------------------------------------
    # ICEBERG
    # --------------------------------------------------------

    "--conf",
    "spark.sql.extensions="
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",

    "--conf",
    "spark.sql.catalog.iceberg="
    "org.apache.iceberg.spark.SparkCatalog",

    "--conf",
    "spark.sql.catalog.iceberg.type=hadoop",

    "--conf",
    "spark.sql.catalog.iceberg.warehouse="
    "s3a://banking/iceberg",

    # Evita Derby / Hive metastore local
    "--conf",
    "spark.sql.catalogImplementation=in-memory",

    # --------------------------------------------------------
    # PACKAGES
    # --------------------------------------------------------

    "--packages",
    SPARK_PACKAGES,
]


def spark_submit(script):
    return [
        *SPARK_SUBMIT_BASE,
        script,
    ]


# ============================================================
# PIPELINE EXECUTION
# ============================================================

def run_step(name, command):
    print()
    print("=" * 70)
    print(f"STEP: {name}")
    print("=" * 70)

    result = subprocess.run(
        command,
        stdout=sys.stdout,
        stderr=sys.stderr,
    )

    if result.returncode != 0:
        print()
        print("=" * 70)
        print(f"ERROR: Step '{name}' failed.")
        print("=" * 70)

        sys.exit(result.returncode)

    print()
    print(f"STEP '{name}' completed successfully.")


# ============================================================
# MAIN
# ============================================================

def main():

    # ========================================================
    # BRONZE
    # ========================================================

    run_step(
        "Build Bronze",
        [
            sys.executable,
            "scripts/build_bronze.py",
        ],
    )

    # ========================================================
    # SILVER
    # ========================================================

    silver_jobs = [
        (
            "Customers Silver",
            "spark/silver/build_customers_silver.py",
        ),
        (
            "Accounts Silver",
            "spark/silver/build_accounts_silver.py",
        ),
        (
            "Merchants Silver",
            "spark/silver/build_merchants_silver.py",
        ),
        (
            "Transactions Silver",
            "spark/silver/build_transactions_silver.py",
        ),
        (
            "Cards Silver",
            "spark/silver/build_cards_silver.py",
        ),
        (
            "Loans Silver",
            "spark/silver/build_loans_silver.py",
        ),
        (
            "Loan Payments Silver",
            "spark/silver/build_loan_payments_silver.py",
        ),
    ]

    for name, script in silver_jobs:
        run_step(
            f"Build {name}",
            spark_submit(script),
        )

    # ========================================================
    # SILVER QA
    # ========================================================

    run_step(
        "Silver QA",
        spark_submit(
            "scripts/profile_silver_data.py"
        ),
    )

    # ========================================================
    # GOLD
    # ========================================================

    gold_jobs = [
        (
            "Daily Transaction Summary",
            "spark/gold/build_daily_transactions_summary.py",
        ),
        (
            "Customer Transaction Metrics",
            "spark/gold/build_customer_transactions_metrics.py",
        ),
        (
            "Merchant Performance",
            "spark/gold/build_merchants_performance.py",
        ),
        (
            "Loan Portfolio",
            "spark/gold/build_loan_portfolio.py",
        ),
        (
            "Customer 360",
            "spark/gold/build_customer_360.py",
        ),
    ]

    for name, script in gold_jobs:
        run_step(
            f"Build {name}",
            spark_submit(script),
        )

    # ========================================================
    # GOLD QA
    # ========================================================

    run_step(
        "Gold QA",
        spark_submit(
            "scripts/profile_gold_data.py"
        ),
    )

    # ========================================================
    # ICEBERG
    # ========================================================

    run_step(
        "Build Iceberg",
        spark_submit(
            "spark/iceberg/build_iceberg.py"
        ),
    )

    # ========================================================
    # COMPLETED
    # ========================================================

    print()
    print("=" * 70)
    print("PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 70)
    print()
    print("Bronze       : OK")
    print("Silver       : OK")
    print("Silver QA    : OK")
    print("Gold         : OK")
    print("Gold QA      : OK")
    print("Iceberg      : OK")
    print()


if __name__ == "__main__":
    main()