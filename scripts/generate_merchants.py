from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker


OUTPUT_DIR = Path("data/generated")

NUM_MERCHANTS = 10_000

fake = Faker("pt_BR")

Faker.seed(42)
np.random.seed(42)


CATEGORIES = [
    "SUPERMARKET",
    "RESTAURANT",
    "GAS_STATION",
    "PHARMACY",
    "CLOTHING",
    "ELECTRONICS",
    "ENTERTAINMENT",
    "TRAVEL",
    "SERVICES",
    "OTHER",
]

CATEGORY_PROBABILITIES = [
    0.18,
    0.18,
    0.12,
    0.12,
    0.10,
    0.08,
    0.06,
    0.05,
    0.07,
    0.04,
]


def generate_merchants(
    num_merchants: int,
) -> pd.DataFrame:

    merchants = []

    states = [
        "SP",
        "MG",
        "RJ",
        "PR",
        "RS",
        "SC",
        "BA",
        "GO",
        "PE",
        "CE",
    ]

    for merchant_id in range(
        1,
        num_merchants + 1,
    ):

        category = np.random.choice(
            CATEGORIES,
            p=CATEGORY_PROBABILITIES,
        )

        state = np.random.choice(
            states
        )

        merchants.append(
            {
                "merchant_id": merchant_id,
                "merchant_name": fake.company(),
                "merchant_category": category,
                "city": fake.city(),
                "state": state,
                "created_at": fake.date_time_between(
                    start_date="-5y",
                    end_date="now",
                ),
            }
        )

    return pd.DataFrame(merchants)


def main():

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    print(
        f"Generating {NUM_MERCHANTS:,} merchants..."
    )

    df = generate_merchants(
        NUM_MERCHANTS
    )

    output_path = (
        OUTPUT_DIR / "merchants.parquet"
    )

    df.to_parquet(
        output_path,
        index=False,
    )

    print(
        f"File generated: {output_path}"
    )

    print(
        f"Rows: {len(df):,}"
    )

    print(
        f"Columns: {len(df.columns)}"
    )


if __name__ == "__main__":
    main()