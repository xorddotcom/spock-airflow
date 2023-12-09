from datetime import datetime

from include.common.constants.index import PROTOCOLS
from include.common.utils.bigquery import load_metadata, check_syncing_state

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator


@dag(
    dag_id='operator',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False
)
def operator():
    _start = EmptyOperator(task_id="start")
    _finish = EmptyOperator(task_id="finish", trigger_rule="none_failed")

    for protocol_id in PROTOCOLS:
        
        _load_metadata = load_metadata(protocol_id)
        
        _check_syncing_state = check_syncing_state(protocol_id)
        
        _run_dag = TriggerDagRunOperator(
            task_id=f"run_{protocol_id}",
            trigger_dag_id=protocol_id,
        )
        
        _start >> _load_metadata >> _check_syncing_state 
        _check_syncing_state >> [_run_dag, _finish]
        _run_dag >> _finish

operator()
