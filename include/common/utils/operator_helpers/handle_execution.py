from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

# Function to check if protocol is in syncing state
def check(protocol_id):
    is_syncing = True  # Replace with your logic to check syncing state
    
    if is_syncing:
        return f'{protocol_id}.handle_execution.run'
    else:
        return f'{protocol_id}.handle_execution.finish'

# Function to handle protocol execution
def handle_execution(protocol_id, **kwargs):
    
    # Create a TaskGroup named 'handle_execution'
    with TaskGroup(group_id=f'handle_execution') as task_group:

        # Check if the protocol is in syncing state
        _check = BranchPythonOperator(
            task_id='check',
            python_callable=check,
            provide_context=True,
            op_kwargs={'protocol_id': protocol_id},
            **kwargs
        ) 
        
        # Trigger a DAG run for the protocol
        _run = TriggerDagRunOperator(
            task_id="run",
            trigger_dag_id=protocol_id,
            conf={
                "last_block_timestamp": '2021-05-04 19:27:00'
            }
        )
        
        # Dummy operator to indicate finishing the task group
        _finish = EmptyOperator(
            task_id='finish',
            trigger_rule="none_failed",
            **kwargs
        )
        
        # Define task dependencies within the task group
        _check >> [_run, _finish]
        _run >> _finish
        
    return task_group  # Return the created task group
