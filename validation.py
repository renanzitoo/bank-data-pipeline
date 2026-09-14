import pandas as pd

df = pd.read_parquet(
    "data/generated/merchants.parquet"
)

transactions = pd.read_parquet(
    "data/generated/transactions.parquet",
    columns=[
        "transaction_type",
        "merchant_id",
    ],
)

merchants = pd.read_parquet(
    "data/generated/merchants.parquet",
    columns=["merchant_id"],
)

card_transactions = transactions[
    transactions["transaction_type"]
    == "CARD_PURCHASE"
]

print(
    len(card_transactions)
)

print(
    card_transactions["merchant_id"]
    .isin(merchants["merchant_id"])
    .all()
)