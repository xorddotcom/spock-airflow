from include.common.utils.xcom import pull_from_xcom

from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

# Function to check if protocol is in syncing state
def check(protocol_id, **kwargs):
    fetched_metadata = pull_from_xcom(key='protocol_metadata',
                        task_ids=f'{protocol_id}.load_metadata.check',
                        **kwargs)
    
    is_syncing = fetched_metadata["syncing_status"]
    
    if is_syncing:
        return f'{protocol_id}.handle_execution.finish'
    else:
        return f'{protocol_id}.handle_execution.run'
    
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

        last_block_timestamp = f"'{{{{ ti.xcom_pull(task_ids='{protocol_id}.load_metadata.check', key='protocol_metadata')['last_block_timestamp'] }}}}'"
        
        # # Trigger a DAG run for the protocol
        _run = TriggerDagRunOperator(
            task_id="run",
            trigger_dag_id=protocol_id,
            conf={
                "last_block_timestamp": last_block_timestamp
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
