from datetime import datetime
from airflow.operators.python_operator import BranchPythonOperator

def compare_dates(last_block_timestamp):
    last_block_timestamp = datetime.strptime(last_block_timestamp, '%Y-%m-%d %H:%M:%S%z')
    current_timestamp = datetime.now()
    
    difference = abs((current_timestamp - last_block_timestamp).days)
   
    if difference >= 1:
        return 'run_again'
    else:
        return 'update_syncing_status'
    
    
def check_historical_backlog(last_block_timestamp, **kwargs):
    return BranchPythonOperator(
        task_id=f'check_historical_backlog',
        python_callable=compare_dates,
        provide_context=True,
        op_kwargs={'last_block_timestamp': last_block_timestamp},
        **kwargs
    ) 
   
     
