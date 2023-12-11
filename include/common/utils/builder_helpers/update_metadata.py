from airflow.operators.python_operator import PythonOperator

def update(dataset_id):
    return
   
def update_metadata(dataset_id, **kwargs):
    return PythonOperator(
        task_id='update_metadata',
        python_callable=update,
        provide_context=True,
        op_kwargs={'dataset_id': dataset_id},
        **kwargs
    )  


