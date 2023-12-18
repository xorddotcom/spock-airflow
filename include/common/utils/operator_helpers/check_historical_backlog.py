from datetime import datetime, timezone
from airflow.operators.python_operator import BranchPythonOperator


def compare_dates(last_block_timestamp, options):
    last_block_timestamp = datetime.strptime(last_block_timestamp, '%Y-%m-%d %H:%M:%S%z')
    current_timestamp = datetime.utcnow().replace(tzinfo=timezone.utc)
    
    difference = abs((current_timestamp - last_block_timestamp).days)
   
    if difference >= 1:
        return options[0]
    else:
        return options[1]
    
    
def check_historical_backlog(last_block_timestamp, options, **kwargs):
    return BranchPythonOperator(
        task_id=f'check_historical_backlog',
        python_callable=compare_dates,
        provide_context=True,
        op_kwargs={
            'last_block_timestamp': last_block_timestamp,
            'options': options
        },
        **kwargs
    ) 
   
     
