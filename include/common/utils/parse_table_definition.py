import json
import os

from include.common.constants.index import PROJECT_ID, PROTOCOLS_PATH
from include.common.utils.file_helpers import load_json_file
from include.common.utils.template import get_common_sql_template, get_sql_template

def decode_parser(dataset_id, file_path):
    parser_data = load_json_file(file_path)

    parser_abi = parser_data['parser']['abi']
    parser_type = parser_data['parser']['type']  #log / trace
    parser_name = f"{parser_abi['name'].lower()}_{parser_type}"

    formatted_abi = json.dumps(parser_abi)
    parser_schema = ', '.join([ f"{column['name']} {column['type']}" for column in parser_data['table']['schema']])

    if(parser_type == 'log'):
        return get_common_sql_template(
                file_name='parse_logs_udf',
                project_id=PROJECT_ID,
                dataset_id=dataset_id,
                udf_name=parser_name,
                abi=formatted_abi,
                struct_fields=parser_schema
            )
    else:
        #TODO need to create trace udf sql
        return get_common_sql_template(
                file_name='parse_traces_udf',
                project_id=PROJECT_ID,
                dataset_id=dataset_id,
                udf_name=parser_name,
                abi=formatted_abi,
                struct_fields=parser_schema
            )

def generate_parser_udfs_sql(protocol_id):
    parser_directory = os.path.join(PROTOCOLS_PATH, protocol_id, 'parse')
    dataset_id = f"p_{protocol_id}"
    sql = ''

    for filename in os.listdir(parser_directory):
        parser_file_path = os.path.join(parser_directory,filename)
        sql += decode_parser(dataset_id=dataset_id,file_path=parser_file_path)

    return sql

def generate_custom_udfs_sql(protocol_id):
    sql_directory = os.path.join(PROTOCOLS_PATH, protocol_id, 'sql')
    dataset_id = f"p_{protocol_id}"
    sql = ''

    if os.path.exists(sql_directory) and os.path.isdir(sql_directory):
        for filename in os.listdir(sql_directory):
            sql_file_path = os.path.join(sql_directory, filename)
            sql += get_sql_template(file_path=sql_file_path,
                project_id=PROJECT_ID,
                dataset_id=dataset_id
                )
        
    return sql

def generate_udfs_sql(protocol_id):
    standard_parser_udfs = generate_parser_udfs_sql(protocol_id)
    custom_udfs = generate_custom_udfs_sql(protocol_id)
    return standard_parser_udfs + custom_udfs