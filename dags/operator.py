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
    catchup=False,
    start_date=datetime(2023, 1, 1),
)
def operator():
    _start = EmptyOperator(task_id="start")
    _finish = EmptyOperator(task_id="finish", trigger_rule="none_failed")

    protocol_list = PROTOCOLS.copy()  
    
    black_list = []  
    white_list = []  

    # If white_list is not empty: Set protocol_list to white_list.
    # If black_list is not empty: filter protocol_list to exclude those protocols.
    # If both are empty: protocol_list remains unchanged.
    protocol_list = white_list if white_list else [protocol for protocol in protocol_list if protocol not in black_list]

    for protocol_id in protocol_list:
        with TaskGroup(group_id=f'{protocol_id}') as task_group:
            
            _load_metadata = load_metadata(protocol_id=protocol_id)
            
            _handle_execution = handle_execution(protocol_id=protocol_id)
            
            _load_metadata >> _handle_execution
        
        _start >> task_group >> _finish

operator()
