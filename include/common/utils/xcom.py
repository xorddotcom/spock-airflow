def push_to_xcom(key, data, **kwargs):
    ti = kwargs['ti']
    ti.xcom_push(key=key, value=data)

def pull_from_xcom(key, task_ids, **kwargs):
    ti = kwargs['ti']
    return ti.xcom_pull(key=key, task_ids=task_ids)

