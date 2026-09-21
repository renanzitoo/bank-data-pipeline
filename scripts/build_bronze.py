from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as pafs
import pyarrow.parquet as pq


GENERATED_DIR = Path("data/generated")

MINIO_BUCKET = "banking"
BRONZE_PREFIX = "bronze"

BATCH_SIZE = 500_000


DATASETS = {
    "customers": "created_at",
    "accounts": "opened_at",
    "merchants": "created_at",
    "transactions": "transaction_timestamp",
    "cards": "issued_at",
    "loans": "created_at",
    "loan_payments": "due_date",
}


def create_minio_filesystem():
    return pafs.S3FileSystem(
        access_key="banking",
        secret_key="banking_dev",
        endpoint_override="localhost:9000",
        scheme="http",
    )


def normalize_timestamps(table):
    """
    Converts timestamp columns to microseconds
    for Spark/Parquet compatibility.
    """

    fields = []

    for field in table.schema:
        if pa.types.is_timestamp(field.type):
            fields.append(
                pa.field(
                    field.name,
                    pa.timestamp("us"),
                    nullable=field.nullable,
                )
            )
        else:
            fields.append(field)

    return table.cast(pa.schema(fields))


def build_partitioned_dataset(
    name: str,
    date_column: str,
    s3: pafs.S3FileSystem,
):
    source_path = GENERATED_DIR / f"{name}.parquet"

    destination = (
        f"{MINIO_BUCKET}/{BRONZE_PREFIX}/{name}"
    )

    print(f"\nProcessing {name}...")
    print(f"Source: {source_path}")
    print(f"Destination: s3://{destination}")

    parquet_file = pq.ParquetFile(source_path)

    total_rows = parquet_file.metadata.num_rows
    processed_rows = 0

    for batch in parquet_file.iter_batches(
        batch_size=BATCH_SIZE
    ):
        table = pa.Table.from_batches([batch])

        table = normalize_timestamps(table)

        date_index = table.schema.get_field_index(
            date_column
        )

        date_array = table.column(date_index)

        year_array = pc.year(date_array)
        month_array = pc.month(date_array)

        table = table.append_column(
            "year",
            year_array,
        )

        table = table.append_column(
            "month",
            month_array,
        )

        pandas_df = table.to_pandas()

        for (year, month), partition in pandas_df.groupby(
            ["year", "month"]
        ):
            partition_path = (
                f"{destination}/"
                f"year={year}/"
                f"month={month:02d}"
            )

            partition = partition.drop(
                columns=["year", "month"]
            )

            table_partition = pa.Table.from_pandas(
                partition,
                preserve_index=False,
            )

            table_partition = normalize_timestamps(
                table_partition
            )

            output_path = (
                f"{partition_path}/"
                f"part-{processed_rows}.parquet"
            )

            with s3.open_output_stream(
                output_path
            ) as output:
                pq.write_table(
                    table_partition,
                    output,
                    compression="snappy",
                )

        processed_rows += batch.num_rows

        print(
            f"  Progress: "
            f"{processed_rows:,}/{total_rows:,} "
            f"rows"
        )

    print(f"Completed: {name}")


def main():

    s3 = create_minio_filesystem()

    for name, date_column in DATASETS.items():
        build_partitioned_dataset(
            name,
            date_column,
            s3,
        )

    print(
        "\nBronze layer written to MinIO successfully."
    )


if __name__ == "__main__":
    main()