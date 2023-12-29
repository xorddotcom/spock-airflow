from include.common.utils.web3.constants import MAX_GAS_LIMIT_WEI

from web3 import Web3
from eth_abi import abi
from typing import List, Optional, Tuple, Any, Union, NewType, TypedDict


CallInput = NewType('CallInput', List[Union[str, int]])

class CallStruct(TypedDict):
    target: str
    callData: str

class Call:
    def __init__(self, abi:object, address:str, fragment:str, call_input:Optional[CallInput] = None) -> None:
        self.abi = abi
        self.address = address
        self.fragment = fragment
        self.call_input = call_input
        self.contract = Web3().eth.contract(address=Web3.to_checksum_address(address), abi=abi)

    def encode(self) -> CallStruct:
        return {
            "target": Web3.to_checksum_address(self.address),
            "callData": self.contract.encodeABI(fn_name=self.fragment, args=self.call_input),
            "gasLimit": int(MAX_GAS_LIMIT_WEI)
        }

    def __get_output_types(self) -> List[str]:
        output_types = []
        for entry in self.abi:
            entry_name = entry.get("name", '')
            if entry_name == self.fragment and entry["type"] == "function":
                outputs = entry.get("outputs", [])
                output_types = [output["type"] for output in outputs]
        
        if len(output_types) > 0:
            return output_types
        else:
            raise Exception('Invalid fragment')
        
    
    def decode(self, data:str) -> Tuple[Any]:
        return abi.decode(self.__get_output_types() ,data)
    

class CallResult(TypedDict):
    call: Call
    output: Any
    success: bool


