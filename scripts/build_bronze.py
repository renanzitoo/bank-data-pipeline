from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


GENERATED_DIR = Path("data/generated")
BRONZE_DIR = Path("data/bronze")


DATASETS = {
    "customers": "created_at",
    "accounts": "opened_at",
    "merchants": "created_at",
    "transactions": "transaction_timestamp",
    "cards": "issued_at",
    "loans": "created_at",
    "loan_payments": "due_date",
}


def build_partitioned_dataset(
    name: str,
    date_column: str,
):
    source_path = (
        GENERATED_DIR / f"{name}.parquet"
    )

    destination = (
        BRONZE_DIR / name
    )

    print(
        f"\nProcessing {name}..."
    )

    df = pd.read_parquet(
        source_path
    )

    df[date_column] = pd.to_datetime(
        df[date_column]
    )

    df["year"] = (
        df[date_column]
        .dt.year
    )

    df["month"] = (
        df[date_column]
        .dt.month
    )

    for (year, month), partition in (
        df.groupby(
            ["year", "month"]
        )
    ):

        partition_path = (
            destination
            / f"year={year}"
            / f"month={month:02d}"
        )

        partition_path.mkdir(
            parents=True,
            exist_ok=True,
        )

        output_path = (
            partition_path
            / "data.parquet"
        )

        partition = partition.drop(
            columns=["year", "month"]
        )

        table = pa.Table.from_pandas(
            partition,
            preserve_index=False,
        )

        table = table.cast(
            pa.schema([
                pa.field(
                    field.name,
                    pa.timestamp("us")
                )
                if pa.types.is_timestamp(field.type)
                else field
                for field in table.schema
            ])
        )

        pq.write_table(
            table,
            output_path,
            compression="snappy",
        )

    print(
        f"Completed: {name}"
    )


def main():

    for name, date_column in DATASETS.items():

        build_partitioned_dataset(
            name,
            date_column,
        )

    print(
        "\nBronze layer completed."
    )


if __name__ == "__main__":
    main()  