import subprocess
import sys


def run_step(name, command):
    print()
    print("=" * 60)
    print(f"STEP: {name}")
    print("=" * 60)

    result = subprocess.run(
        command,
        stdout=sys.stdout,
        stderr=sys.stderr,
    )

    if result.returncode != 0:
        print()
        print(f"ERROR: Step '{name}' failed.")
        sys.exit(result.returncode)

    print()
    print(f"STEP '{name}' completed successfully.")


def main():

    run_step(
        "Build Bronze",
        [sys.executable, "scripts/build_bronze.py"]
    )

    run_step(
        "Build Customers Silver",
        [
            "spark-submit",
            "spark/silver/build_customers_silver.py"
        ]
    )

    run_step(
        "Build Accounts Silver",
        [
            "spark-submit",
            "spark/silver/build_accounts_silver.py"
        ]
    )

    run_step(
        "Build Transactions Silver",
        [
            "spark-submit",
            "spark/silver/build_transactions_silver.py"
        ]
    )

    run_step(
        "Build Cards Silver",
        [
            "spark-submit",
            "spark/silver/build_cards_silver.py"
        ]
    )

    run_step(
        "Build Loans Silver",
        [
            "spark-submit",
            "spark/silver/build_loans_silver.py"
        ]
    )

    run_step(
        "Build Loan Payments Silver",
        [
            "spark-submit",
            "spark/silver/build_loan_payments_silver.py"
        ]
    )

    print()
    print("=" * 60)
    print("PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 60)


if __name__ == "__main__":
    main()