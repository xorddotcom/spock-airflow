from datetime import datetime, timedelta
from include.common.utils.gcp_connection import add_gcp_connection

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

start_date = datetime.now() + timedelta(seconds=1)

@dag(
    dag_id='configurator',
    schedule='@once',
    start_date=start_date,
    catchup=True
)
def configurator():
    _start = EmptyOperator(task_id="start")

    _finish = EmptyOperator(task_id="finish", trigger_rule="none_failed")

    _create_GCP_connection = PythonOperator(
        task_id='create_gcp_connection',
        python_callable=add_gcp_connection,
        provide_context=True,
    )

    _start >> _create_GCP_connection >> _finish

configurator()