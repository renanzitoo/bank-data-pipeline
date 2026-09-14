from pathlib import Path

import numpy as np
import pandas as pd


CUSTOMERS_PATH = Path("data/generated/customers.parquet")
OUTPUT_DIR = Path("data/generated")

np.random.seed(42)


def generate_accounts(customers: pd.DataFrame) -> pd.DataFrame:
    accounts = []

    account_id = 1

    for _, customer in customers.iterrows():

        # Todo cliente possui pelo menos uma conta
        num_accounts = np.random.choice(
            [1, 2],
            p=[0.50, 0.50],
        )

        for _ in range(num_accounts):

            account_type = np.random.choice(
                ["CHECKING", "SAVINGS"],
                p=[0.70, 0.30],
            )

            status = np.random.choice(
                ["ACTIVE", "BLOCKED", "CLOSED"],
                p=[0.90, 0.05, 0.05],
            )

            opened_at = pd.Timestamp(
                customer["created_at"]
            ) + pd.Timedelta(
                days=np.random.randint(0, 90)
            )

            closed_at = None

            if status == "CLOSED":
                closed_at = opened_at + pd.Timedelta(
                    days=np.random.randint(30, 1000)
                )

            accounts.append(
                {
                    "account_id": account_id,
                    "customer_id": customer["customer_id"],
                    "account_type": account_type,
                    "branch": f"{np.random.randint(1, 1000):04d}",
                    "account_number": f"{np.random.randint(1, 100000000):08d}",
                    "status": status,
                    "opened_at": opened_at,
                    "closed_at": closed_at,
                }
            )

            account_id += 1

    return pd.DataFrame(accounts)


def main():

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    print("Reading customers...")

    customers = pd.read_parquet(
        CUSTOMERS_PATH
    )

    print(
        f"Customers loaded: {len(customers):,}"
    )

    print("Generating accounts...")

    accounts = generate_accounts(
        customers
    )

    output_path = (
        OUTPUT_DIR / "accounts.parquet"
    )

    accounts.to_parquet(
        output_path,
        index=False,
    )

    print(
        f"File generated: {output_path}"
    )

    print(
        f"Accounts generated: {len(accounts):,}"
    )


if __name__ == "__main__":
    main()