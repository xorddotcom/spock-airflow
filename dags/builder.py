from datetime import datetime, timedelta

from include.common.constants.index import PROTOCOLS
from include.common.utils.builder_helpers.generate_dag import generate_dag

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

start_date = datetime.now() + timedelta(seconds=1)

@dag(
    dag_id='builder',
    schedule='@once',
    catchup=True,
    start_date=start_date,
)
def builder():
    _start = EmptyOperator(task_id="start")
    _finish = EmptyOperator(task_id="finish", trigger_rule="none_failed")
    
    template_src = 'dags/position_dags/template_position_dag.py'
    
    for protocol_id in PROTOCOLS:
        template_dest = f'dags/position_dags/{protocol_id}.py'
        
        _generate_dag = generate_dag(
            dag_name=protocol_id,
            template={
                'src': template_src,
                'dest': template_dest,
                'params': [['template_position_dag'],[protocol_id]],
            } 
        )
        
        _start >> _generate_dag >> _finish
        
builder()