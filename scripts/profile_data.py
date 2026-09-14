from pathlib import Path

import pandas as pd


DATA_DIR = Path("data/generated")


def load_data():

    print("Loading datasets...\n")

    datasets = {
        "customers": pd.read_parquet(
            DATA_DIR / "customers.parquet"
        ),
        "accounts": pd.read_parquet(
            DATA_DIR / "accounts.parquet"
        ),
        "merchants": pd.read_parquet(
            DATA_DIR / "merchants.parquet"
        ),
        "transactions": pd.read_parquet(
            DATA_DIR / "transactions.parquet"
        ),
        "cards": pd.read_parquet(
            DATA_DIR / "cards.parquet"
        ),
        "loans": pd.read_parquet(
            DATA_DIR / "loans.parquet"
        ),
        "loan_payments": pd.read_parquet(
            DATA_DIR / "loan_payments.parquet"
        ),
    }

    return datasets


def profile_dataset(
    name: str,
    df: pd.DataFrame,
):

    print("=" * 60)
    print(f"DATASET: {name.upper()}")
    print("=" * 60)

    print(
        f"Rows: {len(df):,}"
    )

    print(
        f"Columns: {len(df.columns)}"
    )

    print("\nColumns:")

    for column in df.columns:

        print(
            f"  {column:<25} "
            f"{str(df[column].dtype):<15}"
        )

    print("\nNull values:")

    nulls = df.isnull().sum()

    for column, count in nulls.items():

        if count > 0:

            percentage = (
                count / len(df)
            ) * 100

            print(
                f"  {column:<25} "
                f"{count:>10,} "
                f"({percentage:.2f}%)"
            )

    print(
        f"\nDuplicated rows: "
        f"{df.duplicated().sum():,}"
    )

    print()


def check_primary_key(
    name: str,
    df: pd.DataFrame,
    column: str,
):

    duplicates = (
        df[column]
        .duplicated()
        .sum()
    )

    nulls = (
        df[column]
        .isnull()
        .sum()
    )

    print(
        f"{name}.{column}"
    )

    print(
        f"  Nulls: {nulls:,}"
    )

    print(
        f"  Duplicates: {duplicates:,}"
    )

    if (
        nulls == 0
        and duplicates == 0
    ):

        print(
            "  Status: PASS"
        )

    else:

        print(
            "  Status: FAIL"
        )

    print()


def check_foreign_key(
    child_name: str,
    child_df: pd.DataFrame,
    child_column: str,
    parent_name: str,
    parent_df: pd.DataFrame,
    parent_column: str,
):

    invalid = child_df[
        ~child_df[child_column].isin(
            parent_df[parent_column]
        )
    ]

    print(
        f"{child_name}.{child_column}"
    )

    print(
        f"  Parent: "
        f"{parent_name}.{parent_column}"
    )

    print(
        f"  Invalid references: "
        f"{len(invalid):,}"
    )

    if len(invalid) == 0:

        print(
            "  Status: PASS"
        )

    else:

        print(
            "  Status: FAIL"
        )

    print()


def profile_transactions(
    transactions: pd.DataFrame,
):

    print("=" * 60)
    print("TRANSACTION ANALYSIS")
    print("=" * 60)

    print("\nTransaction types:")

    print(
        transactions[
            "transaction_type"
        ].value_counts()
    )

    print("\nTransaction status:")

    print(
        transactions[
            "status"
        ].value_counts()
    )

    print("\nAmount statistics:")

    print(
        transactions[
            "amount"
        ].describe()
    )

    print("\nTotal transaction volume:")

    print(
        f"R$ "
        f"{transactions['amount'].sum():,.2f}"
    )

    print()


def profile_loans(
    loans: pd.DataFrame,
):

    print("=" * 60)
    print("LOAN ANALYSIS")
    print("=" * 60)

    print("\nLoan types:")

    print(
        loans[
            "loan_type"
        ].value_counts()
    )

    print("\nLoan status:")

    print(
        loans[
            "status"
        ].value_counts()
    )

    print("\nPrincipal statistics:")

    print(
        loans[
            "principal_amount"
        ].describe()
    )

    print(
        "\nTotal loan portfolio:"
    )

    print(
        f"R$ "
        f"{loans['principal_amount'].sum():,.2f}"
    )

    print()
    
def check_card_merchant_relationship(
    transactions: pd.DataFrame,
    merchants: pd.DataFrame,
):

    card_transactions = transactions[
        transactions["transaction_type"]
        == "CARD_PURCHASE"
    ]

    invalid_nulls = card_transactions[
        card_transactions["merchant_id"].isnull()
    ]

    invalid_references = card_transactions[
        ~card_transactions["merchant_id"].isin(
            merchants["merchant_id"]
        )
    ]

    print("=" * 60)
    print("CARD / MERCHANT VALIDATION")
    print("=" * 60)

    print(
        f"Card transactions: "
        f"{len(card_transactions):,}"
    )

    print(
        f"Missing merchant IDs: "
        f"{len(invalid_nulls):,}"
    )

    print(
        f"Invalid merchant IDs: "
        f"{len(invalid_references):,}"
    )

    if (
        len(invalid_nulls) == 0
        and len(invalid_references) == 0
    ):

        print("Status: PASS")

    else:

        print("Status: FAIL")

    print()
    
def analyze_transaction_outliers(
    transactions: pd.DataFrame,
):

    print("=" * 60)
    print("TRANSACTION OUTLIERS")
    print("=" * 60)

    q1 = transactions[
        "amount"
    ].quantile(0.25)

    q3 = transactions[
        "amount"
    ].quantile(0.75)

    iqr = q3 - q1

    upper_limit = q3 + (
        1.5 * iqr
    )

    outliers = transactions[
        transactions["amount"]
        > upper_limit
    ]

    print(
        f"Q1: R$ {q1:,.2f}"
    )

    print(
        f"Q3: R$ {q3:,.2f}"
    )

    print(
        f"Upper limit: "
        f"R$ {upper_limit:,.2f}"
    )

    print(
        f"Potential outliers: "
        f"{len(outliers):,}"
    )

    print()


def main():

    datasets = load_data()

    for name, df in datasets.items():

        profile_dataset(
            name,
            df,
        )

    print("=" * 60)
    print("PRIMARY KEY VALIDATION")
    print("=" * 60)

    check_primary_key(
        "customers",
        datasets["customers"],
        "customer_id",
    )

    check_primary_key(
        "accounts",
        datasets["accounts"],
        "account_id",
    )

    check_primary_key(
        "merchants",
        datasets["merchants"],
        "merchant_id",
    )

    check_primary_key(
        "transactions",
        datasets["transactions"],
        "transaction_id",
    )

    check_primary_key(
        "cards",
        datasets["cards"],
        "card_id",
    )

    check_primary_key(
        "loans",
        datasets["loans"],
        "loan_id",
    )

    check_primary_key(
        "loan_payments",
        datasets["loan_payments"],
        "payment_id",
    )

    print("=" * 60)
    print("FOREIGN KEY VALIDATION")
    print("=" * 60)

    check_foreign_key(
        "accounts",
        datasets["accounts"],
        "customer_id",
        "customers",
        datasets["customers"],
        "customer_id",
    )

    check_foreign_key(
        "transactions",
        datasets["transactions"],
        "account_id",
        "accounts",
        datasets["accounts"],
        "account_id",
    )

    check_foreign_key(
        "cards",
        datasets["cards"],
        "account_id",
        "accounts",
        datasets["accounts"],
        "account_id",
    )

    check_foreign_key(
        "transactions",
        datasets["transactions"],
        "merchant_id",
        "merchants",
        datasets["merchants"],
        "merchant_id",
    )

    check_foreign_key(
        "loans",
        datasets["loans"],
        "customer_id",
        "customers",
        datasets["customers"],
        "customer_id",
    )

    check_foreign_key(
        "loan_payments",
        datasets["loan_payments"],
        "loan_id",
        "loans",
        datasets["loans"],
        "loan_id",
    )

    profile_transactions(
        datasets["transactions"]
    )

    profile_loans(
        datasets["loans"]
    )
    
    check_card_merchant_relationship(
    datasets["transactions"],
    datasets["merchants"],
    )
    
    analyze_transaction_outliers(
    datasets["transactions"]
    )


if __name__ == "__main__":
    main()