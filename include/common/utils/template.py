import os

from jinja2 import Template

def render_template(template, **kwrags):
    jinja_template = Template(template)
    return jinja_template.render(**kwrags)

def read_sql_template_file(file_path):
    with open(file_path) as file_handle:
        content = file_handle.read()
        return content
    
def get_sql_template(file_path, **kwrags):
    template = read_sql_template_file(file_path)
    return render_template(template, **kwrags)

COMMON_SQL_DIR = '/usr/local/airflow/include/common/sql'
def get_common_sql_template(file_name, **kwrags):
    file_path = os.path.join(COMMON_SQL_DIR, f"{file_name}.sql")
    return get_sql_template(file_path, **kwrags)