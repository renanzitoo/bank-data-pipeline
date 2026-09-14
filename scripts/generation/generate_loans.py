from pathlib import Path

import numpy as np
import pandas as pd


CUSTOMERS_PATH = Path(
    "data/generated/customers.parquet"
)

OUTPUT_DIR = Path(
    "data/generated"
)

NUM_LOANS = 30_000

np.random.seed(42)


LOAN_TYPES = [
    "PERSONAL",
    "PAYROLL",
    "AUTO",
]

LOAN_TYPE_PROBABILITIES = [
    0.60,
    0.25,
    0.15,
]


def generate_loans(
    customers: pd.DataFrame,
) -> pd.DataFrame:

    selected_customers = np.random.choice(
        customers["customer_id"].to_numpy(),
        size=NUM_LOANS,
        replace=True,
    )

    customer_segments = (
        customers
        .set_index("customer_id")[
            "customer_segment"
        ]
    )

    loans = []

    for loan_id, customer_id in enumerate(
        selected_customers,
        start=1,
    ):

        segment = customer_segments[
            customer_id
        ]

        loan_type = np.random.choice(
            LOAN_TYPES,
            p=LOAN_TYPE_PROBABILITIES,
        )

        if loan_type == "PERSONAL":

            principal_amount = np.random.uniform(
                1_000,
                30_000,
            )

            interest_rate = np.random.uniform(
                1.5,
                5.0,
            )

            term_months = np.random.choice(
                [6, 12, 18, 24, 36]
            )

        elif loan_type == "PAYROLL":

            principal_amount = np.random.uniform(
                2_000,
                50_000,
            )

            interest_rate = np.random.uniform(
                1.0,
                3.0,
            )

            term_months = np.random.choice(
                [12, 18, 24, 36, 48]
            )

        else:

            principal_amount = np.random.uniform(
                20_000,
                150_000,
            )

            interest_rate = np.random.uniform(
                1.2,
                3.5,
            )

            term_months = np.random.choice(
                [24, 36, 48, 60]
            )

        # Clientes PREMIUM possuem menor
        # probabilidade de inadimplência.
        if segment == "PREMIUM":

            status_probabilities = [
                0.60,
                0.34,
                0.02,
                0.04,
            ]

        elif segment == "STANDARD":

            status_probabilities = [
                0.55,
                0.30,
                0.08,
                0.07,
            ]

        else:

            status_probabilities = [
                0.50,
                0.25,
                0.14,
                0.11,
            ]

        status = np.random.choice(
            [
                "ACTIVE",
                "PAID",
                "DEFAULTED",
                "CANCELLED",
            ],
            p=status_probabilities,
        )

        created_at = pd.Timestamp(
            "2023-01-01"
        ) + pd.Timedelta(
            days=np.random.randint(
                0,
                1_300,
            )
        )

        loans.append(
            {
                "loan_id": loan_id,
                "customer_id": customer_id,
                "loan_type": loan_type,
                "principal_amount": round(
                    principal_amount,
                    2,
                ),
                "interest_rate": round(
                    interest_rate,
                    2,
                ),
                "term_months": term_months,
                "status": status,
                "created_at": created_at,
            }
        )

    return pd.DataFrame(loans)


def main():

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    print("Reading customers...")

    customers = pd.read_parquet(
        CUSTOMERS_PATH,
        columns=[
            "customer_id",
            "customer_segment",
        ],
    )

    print(
        f"Customers loaded: "
        f"{len(customers):,}"
    )

    print("Generating loans...")

    loans = generate_loans(
        customers
    )

    output_path = (
        OUTPUT_DIR / "loans.parquet"
    )

    loans.to_parquet(
        output_path,
        index=False,
    )

    print(
        f"File generated: {output_path}"
    )

    print(
        f"Loans generated: "
        f"{len(loans):,}"
    )


if __name__ == "__main__":
    main()