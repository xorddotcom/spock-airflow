from datetime import datetime

from include.common.constants.index import PROTOCOLS
from include.common.utils.operator_helpers.load_metadata import load_metadata
from include.common.utils.operator_helpers.handle_execution import handle_execution

from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator

@dag(
    dag_id='operator',
    schedule_interval="@daily",
    catchup=True,
    start_date=datetime(2023, 1, 1),
)
def operator():
    _start = EmptyOperator(task_id="start")
    _finish = EmptyOperator(task_id="finish", trigger_rule="none_failed")

    for protocol_id in PROTOCOLS:
        with TaskGroup(group_id=f'{protocol_id}') as task_group:
            
            _load_metadata = load_metadata(protocol_id=protocol_id)
            
            _handle_execution = handle_execution(protocol_id=protocol_id)
            
            _load_metadata >> _handle_execution
        
        _start >> task_group >> _finish

operator()
