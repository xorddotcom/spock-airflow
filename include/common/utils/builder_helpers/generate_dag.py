import os

from airflow.operators.python import PythonOperator

def generate(template):
    # print("DAGs folder permissions:", oct(os.stat('/usr/local/airflow/dags/position_dags').st_mode & 0o777))

    with open(template['src'], 'r') as file:
        dag = file.read()
    
    new_dag = template
    param_keys = template['params'][0]
    param_values = template['params'][1]
    
    for index, param in enumerate(param_keys):
        new_dag = dag.replace(param, param_values[index])

    with open(template['dest'], '+w') as file:
        file.write(new_dag)        
        
        
def generate_dag(dag_name, template):
    return PythonOperator(
        task_id=f"generate_{dag_name}_dag",
        python_callable=generate,
        provide_context=True,
        op_kwargs={
            'template': template
        },
    )