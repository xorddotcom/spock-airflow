from airflow.operators.python_operator import BranchPythonOperator

def check_protocol_backlog(dataset_id):
    has_backlog = False  
    
    if has_backlog:
        return dataset_id
    else:
        return 'finish'
    
def check_historical_backlog(dataset_id, **kwargs):
    return BranchPythonOperator(
        task_id=f'check_historical_backlog_{dataset_id}',
        python_callable=check_protocol_backlog,
        provide_context=True,
        op_kwargs={'dataset_id': dataset_id},
        **kwargs
    ) 
   
     
