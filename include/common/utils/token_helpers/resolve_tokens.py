from typing import List, TypedDict, Optional

from include.common.utils.token_helpers.multi_erc20 import generate_calls, execute_multi_erc20

class Token(TypedDict):
    address:str
    name:Optional[str]
    symbol:Optional[str]
    decimals:Optional[int]

class Erc20_Result(TypedDict):
    metadata: Token
    success: bool

class Erc20_Batch(TypedDict):
    resolved_tokens: List[Erc20_Result]
    failed_addresses: List[str]

class Formatted_Token(TypedDict):
    address:str
    name:str
    symbol:str
    decimals:int
    type:str


def getErc20s(addresses:List[str]) -> List[Erc20_Result]:
    calls = [call for address in addresses for call in generate_calls(address=address, fragments=['name', 'symbol', 'decimals'])]
    multicall_result = execute_multi_erc20(calls)
    tokens = {}

    for call_result in multicall_result:
        call, output, success = call_result["call"], call_result["output"], call_result["success"]
        token = tokens.get(call.address, {"metadata": {"address": call.address}, "success": True})

        if not success and call.fragment != 'decimals':
            token["success"] = False
        else:
            token["metadata"][call.fragment] = output

        tokens[call.address] = token
        
    return list(tokens.values())
   
def reslove_erc20_batch(addresses:List[str], failed_addresses:List[str]) -> List[Erc20_Result]:
    try:
        return getErc20s(addresses)
    except Exception:
        failed_addresses.extend(addresses)

def resolve_erc20_batches(addresses:List[str], batch_size:int = 50) -> Erc20_Batch:
    failed_addresses:List[str] = []
    resolved_tokens:List[Erc20_Result] = []

    batches = [addresses[i:i + batch_size] for i in range(0, len(addresses), batch_size)]
    for batch in batches:
        tokens = reslove_erc20_batch(batch, failed_addresses)
        resolved_tokens.extend(tokens)
    
    return {"resolved_tokens":resolved_tokens, "failed_addresses":failed_addresses}

def reolsve_erc20s(addresses:List[str], batch_size:int = 50) -> List[Formatted_Token]:
    first_result = resolve_erc20_batches(addresses, batch_size)
    failed_addresses, resolved_tokens = first_result['failed_addresses'], first_result['resolved_tokens']

    if len(failed_addresses) > 0:
        second_result = resolve_erc20_batches(failed_addresses, 1)
        failed_addresses.extend(second_result['failed_addresses'])
        resolved_tokens.extend(second_result['resolved_tokens'])

    failed_tokens = [{"metadata":{"address":token}, "success":False} for token in failed_addresses]
    resolved_tokens.extend(failed_tokens)

    formatted_tokens:List[Formatted_Token] = []

    for token in resolved_tokens:
        metadata, success = token['metadata'], token['success']
        _token = {
            "address": metadata["address"],
            "name": metadata.get('name','UNKNOWN NAME'),
            "symbol": metadata.get('symbol','UNKNOWN SYMBOL'),
            "decimals": metadata.get('decimals',0),
            "type": 'erc20' if metadata.get('decimals',None) != None else 'erc721' if success else 'unknown'
        }

        formatted_tokens.append(_token)

    return formatted_tokens