from airflow.operators.python_operator import PythonOperator

def update(protocol_id):
    return
   
def update_metadata(protocol_id, **kwargs):
    return PythonOperator(
        task_id='update_metadata',
        python_callable=update,
        provide_context=True,
        op_kwargs={'protocol_id': protocol_id},
        **kwargs
    )  


