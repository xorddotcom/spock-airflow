from datetime import datetime

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

@dag(
    dag_id='tokens_dag',
    schedule_interval="@daily",
    catchup=True,
    start_date=datetime(2023, 1, 1),
)
def tokens_dag():
    _start = EmptyOperator(task_id="start")
    _finish = EmptyOperator(task_id="finish", trigger_rule="none_failed")

    _start >> _finish