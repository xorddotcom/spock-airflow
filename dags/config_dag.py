from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

from include.common.utils.gcp_connection import add_gcp_connection

@dag(dag_id='config',
     schedule=None,
     start_date=datetime(2023,11,1),
     catchup=False)
def configuration_dag():
    _start = EmptyOperator(task_id="start")

    _finish = EmptyOperator(task_id="finish", trigger_rule="none_failed")

    _create_GCP_connection = PythonOperator(
        task_id='create_gcp_connection',
        python_callable=add_gcp_connection,
        provide_context=True,
    )

    chain(_start, _create_GCP_connection, _finish)

configuration_dag()