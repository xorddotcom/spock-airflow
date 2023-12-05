import json
import os

from include.common.constants.index import PROJECT_ID, PROTOCOL_POSITIONS_PATH
from include.common.utils.file_helpers import load_json_file

def generate_parsers_udf_sql(project):
    parser_directory = os.path.join(PROTOCOL_POSITIONS_PATH, project, 'parser')
    dataset_id = f"p_{project}"
    sql = ''

    for filename in os.listdir(parser_directory):
        parser_file_path = os.path.join(parser_directory,filename)
        sql += decode_parser(dataset_id=dataset_id,file_path=parser_file_path)

    return sql

def decode_parser(dataset_id, file_path):
    parser_data = load_json_file(file_path)

    parser_abi = parser_data['parser']['abi']
    parser_type = parser_data['parser']['type']  #log / trace
    parser_name = f"{parser_abi['name'].lower()}_{parser_type}"

    formatted_abi = json.dumps(parser_abi)
    parser_schema = ', '.join([ f"{column['name']} {column['type']}" for column in parser_data['table']['schema']])

    if(parser_type == 'log'):
        return decode_log_udf(dataset_id=dataset_id, abi=formatted_abi, schema=parser_schema, name=parser_name)
    else:
        return decode_trace_udf(dataset_id=dataset_id, abi=formatted_abi, schema=parser_schema, name=parser_name)

def decode_log_udf(dataset_id, abi, schema, name):
    return  f"""
        CREATE OR REPLACE FUNCTION 
        `{PROJECT_ID}.{dataset_id}.decode_{name}` (log_data STRING, topics ARRAY<STRING>)
        RETURNS STRUCT<{schema}>
        LANGUAGE js
        OPTIONS (library=["gs://blockchain-etl-bigquery/ethers.js"]) AS
        '''
        var abi = [{abi}]
        var interface_instance = new ethers.utils.Interface(abi);
        try {{
           var parsedLog = interface_instance.parseLog({{topics: topics, data: log_data}});
        }}
        catch (e) {{
            return null;
        }}

        var transformParams = function(params, abiInputs) {{
            var result = {{}};
            if (params && params.length >= abiInputs.length) {{
                for (var i = 0; i < abiInputs.length; i++) {{
                    var paramName = abiInputs[i].name;
                    var paramValue = params[i];
                    if (abiInputs[i].type === 'address' && typeof paramValue === 'string') {{
                        // For consistency all addresses are lower-cased.
                        paramValue = paramValue.toLowerCase();
                    }}
                    if (ethers.utils.Interface.isIndexed(paramValue)) {{
                        paramValue = paramValue.hash;
                    }}
                    if (abiInputs[i].type === 'tuple' && 'components' in abiInputs[i]) {{
                        paramValue = transformParams(paramValue, abiInputs[i].components)
                    }}
                    result[paramName] = paramValue;
                }}
            }}
            return result;
        }};

        var result = transformParams(parsedLog.values, abi[0].inputs);
        return result;
        ''';
    """

def decode_trace_udf(dataset_id, abi, schema, name):
    return  ""