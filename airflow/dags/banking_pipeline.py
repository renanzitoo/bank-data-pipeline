from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


SPARK_CONTAINER = "banking-spark"


# ============================================================
# SPARK CONFIGURATION
# ============================================================

SPARK_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,"
    "org.apache.hadoop:hadoop-aws:3.5.0"
)


def spark_submit(script: str) -> str:
    return f"""
    /opt/spark/bin/spark-submit \
        --master local[1] \
        --driver-memory 3g \
        --conf spark.sql.shuffle.partitions=32 \
        --conf spark.default.parallelism=32 \
        --conf spark.jars.ivy=/opt/spark/.ivy2 \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=banking \
        --conf spark.hadoop.fs.s3a.secret.key=banking_dev \
        --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
        --conf spark.hadoop.fs.s3a.endpoint.region=us-east-1 \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.iceberg.type=hadoop \
        --conf spark.sql.catalog.iceberg.warehouse=s3a://banking/iceberg \
        --conf spark.sql.catalogImplementation=in-memory \
        --packages {SPARK_PACKAGES} \
        {script}
"""


def spark_exec(command: str) -> str:
    """
    Executa comandos dentro do container Spark.
    """

    return f"""
    set -e

    docker exec {SPARK_CONTAINER} bash -c '
        set -e
        {command}
    '
    """


with DAG(
    dag_id="banking_data_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=[
        "banking",
        "data-engineering",
        "spark",
        "iceberg",
    ],
    description="End-to-end Banking Data Lakehouse pipeline",
) as dag:

    # ============================================================
    # 1. DATA GENERATION
    # ============================================================

    generate_data = BashOperator(
        task_id="generate_data",
        bash_command=spark_exec(
            """
            cd /workspace

            python3 scripts/generation/generate_costumers.py
            python3 scripts/generation/generate_accounts.py
            python3 scripts/generation/generate_merchants.py
            python3 scripts/generation/generate_transactions.py
            python3 scripts/generation/generate_cards.py
            python3 scripts/generation/generate_loans.py
            python3 scripts/generation/generate_loan_payments.py
            """
        ),
    )

    # ============================================================
    # 2. BRONZE
    # ============================================================

    build_bronze = BashOperator(
        task_id="build_bronze",
        bash_command=spark_exec(
            """
            cd /workspace

            python3 scripts/build_bronze.py
            """
        ),
    )

    # ============================================================
    # 3. SILVER
    # ============================================================

    build_silver = BashOperator(
        task_id="build_silver",
        bash_command=spark_exec(
            f"""
            cd /workspace

            {spark_submit(
                "spark/silver/build_customers_silver.py"
            )}

            {spark_submit(
                "spark/silver/build_accounts_silver.py"
            )}

            {spark_submit(
                "spark/silver/build_merchants_silver.py"
            )}

            {spark_submit(
                "spark/silver/build_transactions_silver.py"
            )}

            {spark_submit(
                "spark/silver/build_cards_silver.py"
            )}

            {spark_submit(
                "spark/silver/build_loans_silver.py"
            )}

            {spark_submit(
                "spark/silver/build_loan_payments_silver.py"
            )}
            """
        ),
    )

    # ============================================================
    # 4. SILVER QA
    # ============================================================

    silver_qa = BashOperator(
        task_id="silver_qa",
        bash_command=spark_exec(
            f"""
            cd /workspace

            {spark_submit(
                "scripts/profile_silver_data.py"
            )}
            """
        ),
    )

  # ============================================================
# 5. GOLD
# ============================================================

    build_gold = BashOperator(
        task_id="build_gold",
        bash_command=spark_exec(
            f"""
            cd /workspace

            {spark_submit(
                "spark/gold/build_daily_transactions_summary.py"
            )}

            {spark_submit(
                "spark/gold/build_customer_transactions_metrics.py"
            )}

            {spark_submit(
                "spark/gold/build_merchants_performance.py"
            )}

            {spark_submit(
                "spark/gold/build_loan_portfolio.py"
            )}

            {spark_submit(
                "spark/gold/build_customer_360.py"
            )}
            """
        ),
    )

    # ============================================================
    # 6. GOLD QA
    # ============================================================

    gold_qa = BashOperator(
        task_id="gold_qa",
        bash_command=spark_exec(
            f"""
            cd /workspace

            {spark_submit(
                "scripts/profile_gold_data.py"
            )}
            """
        ),
    )

    # ============================================================
    # 7. ICEBERG
    # ============================================================

    build_iceberg = BashOperator(
        task_id="build_iceberg",
        bash_command=spark_exec(
            f"""
            cd /workspace

            {spark_submit(
                "spark/iceberg/build_iceberg.py"
            )}
            """
        ),
    )

    # ============================================================
    # PIPELINE DEPENDENCIES
    # ============================================================

    (
        generate_data
        >> build_bronze
        >> build_silver
        >> silver_qa
        >> build_gold
        >> gold_qa
        >> build_iceberg
    )   