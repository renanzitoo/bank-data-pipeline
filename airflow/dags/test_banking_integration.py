from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="test_banking_integration",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["banking", "test"],
) as dag:

    test_environment = BashOperator(
        task_id="test_environment",
        bash_command="""
        echo "========================================"
        echo "BANKING DATA LAKEHOUSE"
        echo "AIRFLOW INTEGRATION TEST"
        echo "========================================"

        echo ""
        echo "Hostname:"
        hostname

        echo ""
        echo "Airflow version:"
        airflow version

        echo ""
        echo "Python:"
        python --version

        echo ""
        echo "Test completed successfully."
        """,
    )