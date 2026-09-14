from pathlib import Path

import numpy as np
import pandas as pd


ACCOUNTS_PATH = Path("data/generated/accounts.parquet")
OUTPUT_DIR = Path("data/generated")

NUM_TRANSACTIONS = 5_000_000
CHUNK_SIZE = 500_000

np.random.seed(42)


TRANSACTION_TYPES = [
    "PIX",
    "CARD_PURCHASE",
    "TRANSFER",
    "DEPOSIT",
    "WITHDRAWAL",
    "TED",
    "DOC",
]

TRANSACTION_TYPE_PROBABILITIES = [
    0.45,
    0.30,
    0.10,
    0.07,
    0.05,
    0.02,
    0.01,
]


STATUSES = [
    "COMPLETED",
    "PENDING",
    "FAILED",
    "CANCELLED",
]

STATUS_PROBABILITIES = [
    0.94,
    0.03,
    0.02,
    0.01,
]


DESCRIPTIONS = {
    "PIX": "Pagamento via PIX",
    "CARD_PURCHASE": "Compra com cartão",
    "TRANSFER": "Transferência entre contas",
    "DEPOSIT": "Depósito em conta",
    "WITHDRAWAL": "Saque",
    "TED": "Transferência TED",
    "DOC": "Transferência DOC",
}


def generate_transaction_amounts(
    transaction_types: np.ndarray,
) -> np.ndarray:

    amounts = np.zeros(len(transaction_types))

    for transaction_type in np.unique(transaction_types):

        mask = transaction_types == transaction_type
        count = mask.sum()

        if transaction_type == "PIX":
            values = np.random.lognormal(
                mean=3.8,
                sigma=1.0,
                size=count,
            )

        elif transaction_type == "CARD_PURCHASE":
            values = np.random.lognormal(
                mean=3.5,
                sigma=0.8,
                size=count,
            )

        elif transaction_type == "TRANSFER":
            values = np.random.lognormal(
                mean=5.0,
                sigma=1.0,
                size=count,
            )

        elif transaction_type == "DEPOSIT":
            values = np.random.lognormal(
                mean=5.2,
                sigma=1.0,
                size=count,
            )

        elif transaction_type == "WITHDRAWAL":
            values = np.random.lognormal(
                mean=4.0,
                sigma=0.6,
                size=count,
            )

        elif transaction_type == "TED":
            values = np.random.lognormal(
                mean=6.0,
                sigma=0.8,
                size=count,
            )

        else:  # DOC
            values = np.random.lognormal(
                mean=5.5,
                sigma=0.8,
                size=count,
            )

        amounts[mask] = np.clip(
            values,
            1.00,
            100_000.00,
        )

    return np.round(amounts, 2)


def generate_timestamps(
    size: int,
) -> pd.DatetimeIndex:

    start = pd.Timestamp("2024-01-01")
    end = pd.Timestamp("2026-09-14")

    total_seconds = int(
        (end - start).total_seconds()
    )

    random_seconds = np.random.randint(
        0,
        total_seconds,
        size=size,
    )

    timestamps = (
        start
        + pd.to_timedelta(
            random_seconds,
            unit="s",
        )
    )

    return timestamps


def generate_chunk(
    accounts: pd.DataFrame,
    start_id: int,
    size: int,
) -> pd.DataFrame:

    account_ids = accounts["account_id"].to_numpy()

    selected_accounts = np.random.choice(
        account_ids,
        size=size,
    )

    transaction_types = np.random.choice(
        TRANSACTION_TYPES,
        size=size,
        p=TRANSACTION_TYPE_PROBABILITIES,
    )

    statuses = np.random.choice(
        STATUSES,
        size=size,
        p=STATUS_PROBABILITIES,
    )

    amounts = generate_transaction_amounts(
        transaction_types
    )

    timestamps = generate_timestamps(size)

    merchant_ids = np.full(
        size,
        None,
        dtype=object,
    )

    card_mask = (
        transaction_types
        == "CARD_PURCHASE"
    )

    merchant_ids[card_mask] = np.random.randint(
        1,
        10_001,
        size=card_mask.sum(),
    )

    descriptions = np.array(
        [
            DESCRIPTIONS[transaction_type]
            for transaction_type in transaction_types
        ]
    )

    transaction_ids = np.arange(
        start_id,
        start_id + size,
    )

    return pd.DataFrame(
        {
            "transaction_id": transaction_ids,
            "account_id": selected_accounts,
            "transaction_type": transaction_types,
            "amount": amounts,
            "currency": "BRL",
            "status": statuses,
            "transaction_timestamp": timestamps,
            "merchant_id": merchant_ids,
            "description": descriptions,
        }
    )


def main():

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    print("Reading accounts...")

    accounts = pd.read_parquet(
        ACCOUNTS_PATH,
        columns=["account_id"],
    )

    print(
        f"Accounts loaded: {len(accounts):,}"
    )

    output_path = (
        OUTPUT_DIR / "transactions.parquet"
    )

    if output_path.exists():
        output_path.unlink()

    remaining = NUM_TRANSACTIONS
    current_id = 1
    chunk_number = 1

    print(
        f"Generating {NUM_TRANSACTIONS:,} transactions..."
    )

    import pyarrow as pa
    import pyarrow.parquet as pq

    writer = None

    try:

        while remaining > 0:

            current_chunk_size = min(
                CHUNK_SIZE,
                remaining,
            )

            print(
                f"Chunk {chunk_number}: "
                f"{current_chunk_size:,} rows"
            )

            chunk = generate_chunk(
                accounts=accounts,
                start_id=current_id,
                size=current_chunk_size,
            )

            table = pa.Table.from_pandas(
                chunk,
                preserve_index=False,
            )

            if writer is None:

                writer = pq.ParquetWriter(
                    output_path,
                    table.schema,
                )

            writer.write_table(table)

            current_id += current_chunk_size
            remaining -= current_chunk_size
            chunk_number += 1

    finally:

        if writer is not None:
            writer.close()

    print()
    print(
        f"File generated: {output_path}"
    )


if __name__ == "__main__":
    main()
