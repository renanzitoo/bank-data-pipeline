from pathlib import Path

import numpy as np
import pandas as pd


LOANS_PATH = Path(
    "data/generated/loans.parquet"
)

OUTPUT_DIR = Path(
    "data/generated"
)

np.random.seed(42)


def generate_loan_payments(
    loans: pd.DataFrame,
) -> pd.DataFrame:

    payments = []

    payment_id = 1

    for _, loan in loans.iterrows():

        loan_id = loan["loan_id"]

        principal = loan[
            "principal_amount"
        ]

        term_months = loan[
            "term_months"
        ]

        created_at = loan[
            "created_at"
        ]

        # Cálculo simplificado da parcela.
        monthly_interest = (
            loan["interest_rate"] / 100
        )

        installment = (
            principal
            * (
                1 + monthly_interest
            )
            / term_months
        )

        for payment_number in range(
            1,
            term_months + 1,
        ):

            due_date = (
                created_at
                + pd.DateOffset(
                    months=payment_number
                )
            )

            status = "PENDING"
            payment_date = None

            if loan["status"] == "PAID":

                status = "PAID"

                payment_date = (
                    due_date
                    - pd.Timedelta(
                        days=np.random.randint(
                            0,
                            5,
                        )
                    )
                )

            elif loan["status"] == "DEFAULTED":

                random_value = np.random.random()

                if random_value < 0.60:

                    status = "LATE"

                else:

                    status = "PAID"

                    payment_date = (
                        due_date
                        + pd.Timedelta(
                            days=np.random.randint(
                                1,
                                30,
                            )
                        )
                    )

            elif loan["status"] == "CANCELLED":

                status = "PENDING"

            else:

                # ACTIVE
                if due_date < pd.Timestamp.now():

                    if np.random.random() < 0.85:

                        status = "PAID"

                        payment_date = (
                            due_date
                            + pd.Timedelta(
                                days=np.random.randint(
                                    -3,
                                    5,
                                )
                            )
                        )

                    else:

                        status = "LATE"

            payments.append(
                {
                    "payment_id": payment_id,
                    "loan_id": loan_id,
                    "payment_number": payment_number,
                    "due_date": due_date,
                    "payment_date": payment_date,
                    "amount": round(
                        installment,
                        2,
                    ),
                    "status": status,
                }
            )

            payment_id += 1

    return pd.DataFrame(payments)


def main():

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    print("Reading loans...")

    loans = pd.read_parquet(
        LOANS_PATH,
    )

    print(
        f"Loans loaded: {len(loans):,}"
    )

    print(
        "Generating loan payments..."
    )

    payments = generate_loan_payments(
        loans
    )

    output_path = (
        OUTPUT_DIR
        / "loan_payments.parquet"
    )

    payments.to_parquet(
        output_path,
        index=False,
    )

    print(
        f"File generated: {output_path}"
    )

    print(
        f"Payments generated: "
        f"{len(payments):,}"
    )


if __name__ == "__main__":
    main()