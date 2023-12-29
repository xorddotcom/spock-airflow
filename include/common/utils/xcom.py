from typing import Any

def push_to_xcom(key, data, **kwargs):
    ti = kwargs['ti']
    ti.xcom_push(key=key, value=data)

def pull_from_xcom(key, task_ids, **kwargs):
    ti = kwargs['ti']
    return ti.xcom_pull(key=key, task_ids=task_ids)

def pull_from_xcom_jinja(task_ids, key, dict_key=None):
    nested = '' if dict_key == None else f"['{dict_key}']"
    return f"{{{{ ti.xcom_pull(task_ids='{task_ids}', key='{key}'){nested} }}}}"