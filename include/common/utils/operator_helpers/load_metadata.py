from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

def fetch(dataset_id):
    return

def add(dataset_id):
    return

def check(dataset_id):
    has_metadata = True  
    
    if has_metadata:
        return f'{dataset_id}.load_metadata.add'
    else:
        return f'{dataset_id}.load_metadata.finish'
    
def load_metadata(dataset_id, **kwargs):
    
    with TaskGroup(group_id='load_metadata') as task_group:
       
        _fetch = PythonOperator(
            task_id='fetch',
            python_callable=fetch,
            provide_context=True,
            op_kwargs={'dataset_id': dataset_id},
            **kwargs
        )

        _check = BranchPythonOperator(
            task_id='check',
            python_callable=check,
            provide_context=True,
            op_kwargs={'dataset_id': dataset_id},
            **kwargs
        )

        _add = PythonOperator(
            task_id='add',
            python_callable=add,
            provide_context=True,
            op_kwargs={'dataset_id': dataset_id},
            **kwargs
        )
        
        _finish = EmptyOperator(
            task_id='finish',
            trigger_rule="none_failed",
            **kwargs
        )

        _fetch >> _check
        _check >> [_add, _finish]
        _add >> _finish

    return task_group
   