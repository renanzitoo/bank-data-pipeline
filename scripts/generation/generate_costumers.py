from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker


# Configurações
NUM_CUSTOMERS = 100_000
OUTPUT_DIR = Path("data/generated")

fake = Faker("pt_BR")
Faker.seed(42)
np.random.seed(42)


def generate_customers(num_customers: int) -> pd.DataFrame:
    customers = []

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

    segments = ["BASIC", "STANDARD", "PREMIUM"]
    segment_probabilities = [0.50, 0.35, 0.15]

    for customer_id in range(1, num_customers + 1):
        birth_date = fake.date_of_birth(
            minimum_age=18,
            maximum_age=80,
        )

        created_at = fake.date_time_between(
            start_date="-5y",
            end_date="now",
        )

        state = np.random.choice(states)

        segment = np.random.choice(
            segments,
            p=segment_probabilities,
        )

        customers.append(
            {
                "customer_id": customer_id,
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "birth_date": birth_date,
                "document": fake.cpf(),
                "email": fake.email(),
                "phone": fake.phone_number(),
                "city": fake.city(),
                "state": state,
                "customer_segment": segment,
                "created_at": created_at,
            }
        )

    return pd.DataFrame(customers)


def main():
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    print(f"Generating {NUM_CUSTOMERS:,} customers...")

    df = generate_customers(NUM_CUSTOMERS)

    output_path = OUTPUT_DIR / "customers.parquet"

    df.to_parquet(
        output_path,
        index=False,
    )

    print(f"File generated: {output_path}")
    print(f"Rows: {len(df):,}")
    print(f"Columns: {len(df.columns)}")


if __name__ == "__main__":
    main()