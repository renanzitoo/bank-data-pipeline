from pathlib import Path

import numpy as np
import pandas as pd


ACCOUNTS_PATH = Path("data/generated/accounts.parquet")
OUTPUT_DIR = Path("data/generated")

NUM_CARDS = 120_000

np.random.seed(42)


CARD_TYPES = [
    "DEBIT",
    "CREDIT",
]

CARD_TYPE_PROBABILITIES = [
    0.80,
    0.20,
]

BRANDS = [
    "VISA",
    "MASTERCARD",
]

STATUSES = [
    "ACTIVE",
    "BLOCKED",
    "EXPIRED",
    "CANCELLED",
]

STATUS_PROBABILITIES = [
    0.90,
    0.04,
    0.03,
    0.03,
]


def generate_cards(
    accounts: pd.DataFrame,
) -> pd.DataFrame:

    # Apenas contas que podem possuir cartões
    eligible_accounts = accounts[
        accounts["status"] == "ACTIVE"
    ].copy()

    account_ids = (
        eligible_accounts["account_id"]
        .to_numpy()
    )

    account_types = (
        eligible_accounts
        .set_index("account_id")["account_type"]
    )

    cards = []

    selected_accounts = np.random.choice(
        account_ids,
        size=NUM_CARDS,
        replace=True,
    )

    for card_id, account_id in enumerate(
        selected_accounts,
        start=1,
    ):

        account_type = account_types[
            account_id
        ]

        # Conta SAVINGS só pode ter débito
        if account_type == "SAVINGS":

            card_type = "DEBIT"

        else:

            card_type = np.random.choice(
                CARD_TYPES,
                p=CARD_TYPE_PROBABILITIES,
            )

        brand = np.random.choice(
            BRANDS
        )

        status = np.random.choice(
            STATUSES,
            p=STATUS_PROBABILITIES,
        )

        issued_at = pd.Timestamp(
            "2024-01-01"
        ) + pd.Timedelta(
            days=np.random.randint(
                0,
                900,
            )
        )

        expires_at = issued_at + pd.DateOffset(
            years=np.random.randint(
                3,
                6,
            )
        )

        credit_limit = None

        if card_type == "CREDIT":

            credit_limit = np.random.choice(
                [
                    1000,
                    2000,
                    3000,
                    5000,
                    7500,
                    10000,
                    15000,
                    25000,
                ]
            )

        cards.append(
            {
                "card_id": card_id,
                "account_id": account_id,
                "card_type": card_type,
                "brand": brand,
                "status": status,
                "credit_limit": credit_limit,
                "issued_at": issued_at,
                "expires_at": expires_at,
            }
        )

    return pd.DataFrame(cards)


def main():

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    print("Reading accounts...")

    accounts = pd.read_parquet(
        ACCOUNTS_PATH,
    )

    print(
        f"Accounts loaded: {len(accounts):,}"
    )

    print("Generating cards...")

    cards = generate_cards(
        accounts
    )

    output_path = (
        OUTPUT_DIR / "cards.parquet"
    )

    cards.to_parquet(
        output_path,
        index=False,
    )

    print(
        f"File generated: {output_path}"
    )

    print(
        f"Cards generated: {len(cards):,}"
    )


if __name__ == "__main__":
    main()