import pandas as pd

loans = pd.read_parquet(
    "data/generated/loans.parquet"
)

payments = pd.read_parquet(
    "data/generated/loan_payments.parquet"
)

invalid = payments[
    ~payments["loan_id"].isin(
        loans["loan_id"]
    )
]

print(
    f"Invalid loan IDs: {len(invalid)}"
)

payment_counts = (
    payments
    .groupby("loan_id")
    .size()
    .rename("generated_payments")
)

validation = loans[
    [
        "loan_id",
        "term_months",
    ]
].merge(
    payment_counts,
    on="loan_id",
    how="left",
)

validation["generated_payments"] = (
    validation["generated_payments"]
    .fillna(0)
)

invalid = validation[
    validation["term_months"]
    != validation["generated_payments"]
]

print(
    f"Invalid payment schedules: "
    f"{len(invalid)}"
)