from datetime import datetime, timezone
from airflow.operators.python_operator import BranchPythonOperator


def compare_dates(last_block_timestamp, run_once, options):
    last_block_timestamp = datetime.strptime(last_block_timestamp, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
    current_timestamp = datetime.now(timezone.utc)
    
    difference = abs((current_timestamp - last_block_timestamp).days)
   
    if difference >= 1 and not run_once:
        return options[0]
    else:
        return options[1]
    
    
def check_historical_backlog(last_block_timestamp, run_once, options, **kwargs):
    return BranchPythonOperator(
        task_id=f'check_historical_backlog',
        python_callable=compare_dates,
        provide_context=True,
        op_kwargs={
            'last_block_timestamp': last_block_timestamp,
            'run_once': run_once,
            'options': options
        },
        **kwargs
    ) 
   
     
