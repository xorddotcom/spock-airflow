from include.common.utils.web3.abis.erc20 import erc20_abi
from include.common.utils.web3.abis.erc20_bytes import erc20_bytes_abi
from include.common.utils.web3.multicall.call import Call, CallResult
from include.common.utils.web3.constants import FAILED_TXN_DATA
from include.common.utils.web3.provider import Web3Node
from include.common.utils.web3.multicall.multicall import Multicall

from typing import List, Tuple, Any

def decode_call(erc20_call:Call, data:str) -> Tuple[Any]:
    try:
        return erc20_call.decode(data)
    except Exception as e:
        if erc20_call.fragment in ['name', 'symbol']:
            return Call(abi=erc20_bytes_abi, address=erc20_call.address, fragment=erc20_call.fragment).decode(data)
        else:
            raise e

def generate_calls(address:str, fragments:List[str]) -> List[Call]:
    return [Call(abi=erc20_abi, address=address, fragment=fragment) for fragment in fragments]

def execute_multi_erc20(network:Web3Node, calls:List[Call]) -> List[CallResult]:
    multicall = Multicall(network)
    result = multicall.raw_execute(calls=calls)
    formatted_result = []

    for return_data, call in zip(result[1], calls):
        decoded_data = multicall.convert_result(decode_call(call, return_data[2])) if return_data[2] != FAILED_TXN_DATA else None
        bytes_free_data = bytes.decode(decoded_data).rstrip('\x00') if call.fragment in ['name', 'symbol'] and decoded_data and isinstance(decoded_data, bytes) else decoded_data
        formatted_result.append({
            "call": call,
            "output": bytes_free_data,
            "success": return_data[2] != FAILED_TXN_DATA and return_data[0],
        })
    return formatted_result