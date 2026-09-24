import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import days


# ============================================================
# CONFIGURATION
# ============================================================

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "minio:9000",
)

MINIO_ACCESS_KEY = os.getenv(
    "MINIO_ACCESS_KEY",
    "banking",
)

MINIO_SECRET_KEY = os.getenv(
    "MINIO_SECRET_KEY",
    "banking_dev",
)

MINIO_BUCKET = os.getenv(
    "MINIO_BUCKET",
    "banking",
)

WAREHOUSE = f"s3a://{MINIO_BUCKET}/iceberg"

GOLD_BASE = f"s3a://{MINIO_BUCKET}/gold"

CATALOG = "iceberg"

NAMESPACE = "banking"

REPARTITION_COUNT = 32

TARGET_FILE_SIZE = 134217728  # 128 MB


# ============================================================
# TABLE CONFIGURATION
# ============================================================

TABLES = {

    "customer_360": {
        "source": f"{GOLD_BASE}/customer_360",
        "partition_column": None,
    },

    "daily_transaction_summary": {
        "source": f"{GOLD_BASE}/daily_transaction_summary",
        "partition_column": "transaction_date",
    },

    "customer_transaction_metrics": {
        "source": f"{GOLD_BASE}/customer_transaction_metrics",
        "partition_column": None,
    },

    "merchant_performance": {
        "source": f"{GOLD_BASE}/merchant_performance",
        "partition_column": None,
    },

    "loan_portfolio": {
        "source": f"{GOLD_BASE}/loan_portfolio",
        "partition_column": None,
    },
}


# ============================================================
# SPARK SESSION
# ============================================================

def create_spark_session():

    return (
        SparkSession.builder
        .appName("BankingIceberg")

        # ----------------------------------------------------
        # Iceberg
        # ----------------------------------------------------

        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )

        .config(
            "spark.sql.catalog.iceberg",
            "org.apache.iceberg.spark.SparkCatalog",
        )

        .config(
            "spark.sql.catalog.iceberg.type",
            "hadoop",
        )

        .config(
            "spark.sql.catalog.iceberg.warehouse",
            WAREHOUSE,
        )

        # ----------------------------------------------------
        # Spark execution
        # ----------------------------------------------------

        .config(
            "spark.sql.shuffle.partitions",
            "100",
        )

        .config(
            "spark.default.parallelism",
            "100",
        )

        # ----------------------------------------------------
        # Iceberg write settings
        # ----------------------------------------------------

        .config(
            "spark.sql.iceberg.advisory-partition-size",
            str(TARGET_FILE_SIZE),
        )

        .config(
            "spark.sql.iceberg.target-file-size-bytes",
            str(TARGET_FILE_SIZE),
        )

        # ----------------------------------------------------
        # MinIO / S3A
        # ----------------------------------------------------

        .config(
            "spark.hadoop.fs.s3a.endpoint",
            f"http://{MINIO_ENDPOINT}",
        )

        .config(
            "spark.hadoop.fs.s3a.access.key",
            MINIO_ACCESS_KEY,
        )

        .config(
            "spark.hadoop.fs.s3a.secret.key",
            MINIO_SECRET_KEY,
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

        # ----------------------------------------------------
        # Avoid Hive / Derby
        # ----------------------------------------------------

        .config(
            "spark.sql.catalogImplementation",
            "in-memory",
        )

        .getOrCreate()
    )


# ============================================================
# HELPERS
# ============================================================

def iceberg_table_name(table_name):

    return f"{CATALOG}.{NAMESPACE}.{table_name}"


def create_namespace(spark):

    print("\n" + "=" * 70)
    print("ICEBERG NAMESPACE")
    print("=" * 70)

    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS "
        f"{CATALOG}.{NAMESPACE}"
    )

    print(
        f"Namespace: {CATALOG}.{NAMESPACE}"
    )

    print("Status: READY")


def get_existing_tables(spark):

    rows = (
        spark.sql(
            f"SHOW TABLES IN {CATALOG}.{NAMESPACE}"
        )
        .collect()
    )

    return {
        row.tableName
        for row in rows
    }


def validate_source(df, table_name):

    count = df.count()

    print(
        f"Source records: {count:,}"
    )

    if count == 0:

        raise RuntimeError(
            f"Gold dataset is empty: {table_name}"
        )

    return count


# ============================================================
# SCHEMA VALIDATION
# ============================================================

def validate_schema(
    source_df,
    target_df,
    table_name,
):

    source_schema = source_df.schema
    target_schema = target_df.schema

    source_fields = {
        field.name: field.dataType
        for field in source_schema
    }

    target_fields = {
        field.name: field.dataType
        for field in target_schema
    }

    missing_in_target = (
        set(source_fields)
        - set(target_fields)
    )

    extra_in_target = (
        set(target_fields)
        - set(source_fields)
    )

    incompatible_types = []

    for column in (
        set(source_fields)
        & set(target_fields)
    ):

        if (
            source_fields[column]
            != target_fields[column]
        ):

            incompatible_types.append(
                (
                    column,
                    str(source_fields[column]),
                    str(target_fields[column]),
                )
            )

    if missing_in_target:

        raise RuntimeError(
            f"""
Schema validation failed for {table_name}.

Missing columns in Iceberg:
{sorted(missing_in_target)}
"""
        )

    if incompatible_types:

        raise RuntimeError(
            f"""
Schema validation failed for {table_name}.

Incompatible column types:
{incompatible_types}
"""
        )

    print(
        f"Schema validation OK: {table_name}"
    )

    if extra_in_target:

        print(
            "Warning: Iceberg contains additional "
            f"columns: {sorted(extra_in_target)}"
        )


# ============================================================
# CREATE TABLE
# ============================================================

def create_table(
    df,
    iceberg_table,
    table_config,
):

    print(
        f"Creating Iceberg table: "
        f"{iceberg_table}"
    )

    writer = (
        df.writeTo(iceberg_table)
        .using("iceberg")
    )

    partition_column = (
        table_config["partition_column"]
    )

    # --------------------------------------------------------
    # Temporal partitioning
    # --------------------------------------------------------

    if partition_column:

        print(
            f"Partitioning: days({partition_column})"
        )

        writer = writer.partitionedBy(
            days(partition_column)
        )

    else:

        print(
            "Partitioning: none"
        )

    # --------------------------------------------------------
    # Create
    # --------------------------------------------------------

    writer.create()

    print(
        "Table created successfully."
    )


# ============================================================
# PREPARE DATA FOR WRITE
# ============================================================

def prepare_dataframe(
    df,
    table_config,
):

    partition_column = (
        table_config["partition_column"]
    )

    # --------------------------------------------------------
    # Tables with temporal partitioning
    # --------------------------------------------------------

    if partition_column:

        print(
            f"Repartitioning by "
            f"{partition_column} "
            f"with {REPARTITION_COUNT} partitions..."
        )

        return df.repartition(
            REPARTITION_COUNT,
            partition_column,
        )

    # --------------------------------------------------------
    # Non-partitioned tables
    # --------------------------------------------------------

    print(
        f"Repartitioning into "
        f"{REPARTITION_COUNT} partitions..."
    )

    return df.repartition(
        REPARTITION_COUNT
    )


# ============================================================
# WRITE EXISTING TABLE
# ============================================================

def overwrite_existing_table(
    df,
    iceberg_table,
):

    print(
        "Iceberg table already exists."
    )

    print(
        "Writing with overwritePartitions()..."
    )

    (
        df.writeTo(iceberg_table)
        .overwritePartitions()
    )

    print(
        "Iceberg write completed."
    )


# ============================================================
# VALIDATE TARGET
# ============================================================

def validate_target(
    spark,
    source_df,
    source_count,
    iceberg_table,
    table_name,
):

    print(
        "Validating Iceberg target..."
    )

    target_df = spark.table(
        iceberg_table
    )

    # --------------------------------------------------------
    # Schema
    # --------------------------------------------------------

    validate_schema(
        source_df,
        target_df,
        table_name,
    )

    # --------------------------------------------------------
    # Count
    # --------------------------------------------------------

    target_count = (
        target_df.count()
    )

    print(
        f"Source records: "
        f"{source_count:,}"
    )

    print(
        f"Iceberg records: "
        f"{target_count:,}"
    )

    if source_count != target_count:

        raise RuntimeError(
            f"""
Record count validation failed.

Table: {table_name}

Source:
{source_count:,}

Iceberg:
{target_count:,}
"""
        )

    print(
        f"Record count validation OK: "
        f"{table_name}"
    )


# ============================================================
# SNAPSHOT INFORMATION
# ============================================================

def show_snapshots(
    spark,
    table_name,
):

    iceberg_table = iceberg_table_name(
        table_name
    )

    print("\n" + "-" * 70)
    print(
        f"SNAPSHOTS: {iceberg_table}"
    )
    print("-" * 70)

    try:

        snapshots = spark.sql(
            f"""
            SELECT
                committed_at,
                snapshot_id,
                operation,
                summary
            FROM {iceberg_table}.snapshots
            ORDER BY committed_at DESC
            LIMIT 5
            """
        )

        snapshots.show(
            truncate=False
        )

    except Exception as exc:

        print(
            "Could not retrieve snapshots:"
        )

        print(
            str(exc)
        )


# ============================================================
# BUILD TABLE
# ============================================================

def build_table(
    spark,
    table_name,
    table_config,
    existing_tables,
):

    iceberg_table = iceberg_table_name(
        table_name
    )

    gold_path = table_config["source"]

    print("\n" + "=" * 70)
    print(
        f"TABLE:  {iceberg_table}"
    )
    print(
        f"SOURCE: {gold_path}"
    )
    print("=" * 70)

    # --------------------------------------------------------
    # Read Gold
    # --------------------------------------------------------

    print(
        "Reading Gold dataset..."
    )

    source_df = (
        spark.read
        .parquet(gold_path)
    )

    # --------------------------------------------------------
    # Validate source
    # --------------------------------------------------------

    source_count = validate_source(
        source_df,
        table_name,
    )

    # --------------------------------------------------------
    # Prepare partitions
    # --------------------------------------------------------

    write_df = prepare_dataframe(
        source_df,
        table_config,
    )

    # --------------------------------------------------------
    # Create / Update
    # --------------------------------------------------------

    if table_name in existing_tables:

        print(
            "Mode: UPDATE"
        )

        overwrite_existing_table(
            write_df,
            iceberg_table,
        )

    else:

        print(
            "Mode: CREATE"
        )

        create_table(
            write_df,
            iceberg_table,
            table_config,
        )

    # --------------------------------------------------------
    # Validate
    # --------------------------------------------------------

    validate_target(
        spark,
        source_df,
        source_count,
        iceberg_table,
        table_name,
    )

    # --------------------------------------------------------
    # Snapshots
    # --------------------------------------------------------

    show_snapshots(
        spark,
        table_name,
    )


# ============================================================
# FINAL TABLE LIST
# ============================================================

def show_tables(spark):

    print("\n" + "=" * 70)
    print("ICEBERG TABLES")
    print("=" * 70)

    (
        spark.sql(
            f"SHOW TABLES IN "
            f"{CATALOG}.{NAMESPACE}"
        )
        .show(
            truncate=False
        )
    )


# ============================================================
# MAIN
# ============================================================

def main():

    spark = create_spark_session()

    try:

        print("\n")
        print("=" * 70)
        print("BANKING DATA LAKEHOUSE")
        print("ICEBERG BUILD")
        print("=" * 70)

        print(
            f"MinIO endpoint: "
            f"{MINIO_ENDPOINT}"
        )

        print(
            f"Bucket: "
            f"{MINIO_BUCKET}"
        )

        print(
            f"Warehouse: "
            f"{WAREHOUSE}"
        )

        print(
            f"Namespace: "
            f"{CATALOG}.{NAMESPACE}"
        )

        # ----------------------------------------------------
        # Namespace
        # ----------------------------------------------------

        create_namespace(
            spark
        )

        # ----------------------------------------------------
        # Existing tables
        # ----------------------------------------------------

        existing_tables = (
            get_existing_tables(
                spark
            )
        )

        print("\nExisting Iceberg tables:")

        if existing_tables:

            for table in sorted(
                existing_tables
            ):

                print(
                    f"  - {table}"
                )

        else:

            print(
                "  None"
            )

        # ----------------------------------------------------
        # Build all tables
        # ----------------------------------------------------

        for (
            table_name,
            table_config,
        ) in TABLES.items():

            build_table(
                spark,
                table_name,
                table_config,
                existing_tables,
            )

        # ----------------------------------------------------
        # Final table list
        # ----------------------------------------------------

        show_tables(
            spark
        )

        # ----------------------------------------------------
        # Success
        # ----------------------------------------------------

        print("\n" + "=" * 70)
        print(
            "ICEBERG BUILD COMPLETED SUCCESSFULLY"
        )
        print("=" * 70)

    except Exception as exc:

        print("\n" + "=" * 70)
        print(
            "ICEBERG BUILD FAILED"
        )
        print("=" * 70)

        print(
            str(exc)
        )

        sys.exit(1)

    finally:

        spark.stop()


# ============================================================
# ENTRYPOINT
# ============================================================

if __name__ == "__main__":

    main()