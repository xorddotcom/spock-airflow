from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

# Placeholder function
def fetch(protocol_id):
    return

# Placeholder function
def add(protocol_id):
    return

# Function to check if metadata exists
def check(protocol_id):
    has_metadata = True  # Replace with your logic to check metadata existence
    
    if has_metadata:
        return f'{protocol_id}.load_metadata.add'
    else:
        return f'{protocol_id}.load_metadata.finish'

# Function to create a task group for metadata loading
def load_metadata(protocol_id, **kwargs):
    
    # Create a TaskGroup named 'load_metadata'
    with TaskGroup(group_id='load_metadata') as task_group:
       
        # Fetch metadata for the protocol
        _fetch = PythonOperator(
            task_id='fetch',
            python_callable=fetch,
            provide_context=True,
            op_kwargs={'protocol_id': protocol_id},
            **kwargs
        )

        # Check if metadata exists
        _check = BranchPythonOperator(
            task_id='check',
            python_callable=check,
            provide_context=True,
            op_kwargs={'protocol_id': protocol_id},
            **kwargs
        )

        # Add metadata to the protocol
        _add = PythonOperator(
            task_id='add',
            python_callable=add,
            provide_context=True,
            op_kwargs={'protocol_id': protocol_id},
            **kwargs
        )
        
        # Dummy operator to indicate finishing the task group
        _finish = EmptyOperator(
            task_id='finish',
            trigger_rule="none_failed",
            **kwargs
        )

        # Define task dependencies within the task group
        _fetch >> _check
        _check >> [_add, _finish]
        _add >> _finish

    return task_group  # Return the created task group
