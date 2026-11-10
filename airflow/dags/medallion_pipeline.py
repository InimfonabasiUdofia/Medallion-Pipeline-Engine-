
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

ENGINE_PATH = "/opt/airflow/project/enginexz"

with DAG(
    dag_id="medallion_data_pipeline",
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
    tags=["medallion"],
) as dag:

    bronze = BashOperator(
        task_id="bronze",
        bash_command=f"python {ENGINE_PATH}/bronze_builder.py",
    )

    silver = BashOperator(
        task_id="silver",
        bash_command=f"python {ENGINE_PATH}/silver_builder.py",
    )

    gold = BashOperator(
        task_id="gold",
        bash_command=f"python {ENGINE_PATH}/gold_builder.py",
    )

    bronze >> silver >> gold
