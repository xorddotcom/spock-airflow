from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

def check(dataset_id):
    has_backlog = True  
    
    if has_backlog:
        return f'{dataset_id}.handle_execution.run'
    else:
        return f'{dataset_id}.handle_execution.finish'
    
def handle_execution(dataset_id, **kwargs):
    
    with TaskGroup(group_id=f'handle_execution') as task_group:

        _check = BranchPythonOperator(
            task_id='check',
            python_callable=check,
            provide_context=True,
            op_kwargs={'dataset_id': dataset_id},
            **kwargs
        ) 
        
        _run = TriggerDagRunOperator(
            task_id="run",
            trigger_dag_id=dataset_id,
            **kwargs
        )
        
        _finish = EmptyOperator(
            task_id='finish',
            trigger_rule="none_failed",
            **kwargs
        )
        
        _check >> [_run, _finish]
        _run >> _finish
        
    return task_group
