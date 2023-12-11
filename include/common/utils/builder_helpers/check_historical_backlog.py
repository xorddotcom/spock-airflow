from airflow.operators.python_operator import BranchPythonOperator

def check_protocol_backlog(protocol_id):
    has_backlog = False  
    
    if has_backlog:
        return protocol_id
    else:
        return 'finish'
    
def check_historical_backlog(protocol_id, **kwargs):
    return BranchPythonOperator(
        task_id=f'check_historical_backlog_{protocol_id}',
        python_callable=check_protocol_backlog,
        provide_context=True,
        op_kwargs={'protocol_id': protocol_id},
        **kwargs
    ) 
   
     
