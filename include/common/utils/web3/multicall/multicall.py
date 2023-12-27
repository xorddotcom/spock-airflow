from typing import List, Optional, Tuple

from include.common.utils.web3.abis.multicall import Multicall_abi
from include.common.utils.web3.constants import MULTICALL_ADDRESS, FAILED_TXN_DATA
from include.common.utils.web3.provider import Web3Node
from include.common.utils.web3.multicall.call import Call
from include.common.utils.web3.multicall.types import CallResult, CallInput

class Multicall:
    def __init__(self, network:Web3Node):
        self.__network = network
    
    def __connect(self):
        return self.__network.provider.eth.contract(address=MULTICALL_ADDRESS[self.__network.chain_id],abi=Multicall_abi)
    
    def convert_result(self, result):
        if isinstance(result, Tuple) and len(result) > 0:
            return result[0]
        else:
            return result

    def raw_execute(self, calls:List[Call], block_number:Optional[int] = None):
        contract = self.__connect()
        encoded_calls = [call.encode() for call in calls]
        contract_call = contract.functions.multicall(encoded_calls)
        return contract_call.call(block_identifier=block_number) if block_number else contract_call.call()
    
    def execute(self, calls:List[Call], block_number:Optional[int] = None) -> List[CallResult]:
        result =  self.raw_execute(calls, block_number)
        return [
              {
                "call": call,
                "output": self.convert_result(call.decode(return_data[2])) if return_data[2] != FAILED_TXN_DATA else None,
                "success": return_data[2] != FAILED_TXN_DATA and return_data[0],
              }
              for return_data, call in zip(result[1], calls)
           ]
    
    def single_contract_multiple_data(self, abi:object, address:str, fragment:str, call_input:Optional[CallInput], block_number:Optional[int] = None):
        calls = [Call(abi=abi, address=address, fragment=fragment, call_input=[call]) for call in call_input]
        return self.execute(calls=calls, block_number=block_number)
    
    def multiple_contract_multiple_data(self, abi:object, address:List[str], fragment:str, call_input:Optional[CallInput], block_number:Optional[int] = None):
        calls = [Call(abi=abi, address=_address, fragment=fragment, call_input=[call]) for call, _address in zip(call_input, address)]
        return self.execute(calls=calls, block_number=block_number)
    
    def multiple_contract_single_data(self, abi:object, address:List[str], fragment:str, call_input:Optional[CallInput] = None, block_number:Optional[int] = None):
        calls = [Call(abi=abi, address=_address, fragment=fragment, call_input=call_input) for _address in address]
        return self.execute(calls=calls, block_number=block_number)
  
    def single_call(self, abi:object, address:str, fragment:str, call_input:Optional[CallInput]=None, block_number:Optional[int]=None,):
        calls = [Call(abi=abi, address=address, fragment=fragment, call_input=call_input)]
        return self.execute(calls=calls, block_number=block_number)[0]
    










## Example usage
#single call
# Multicall(Network.Ethereum).single_call(abi=erc20_bytes_abi, address='0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2', fragment='name')

#single_contract_multiple_data
# Multicall(Network.Ethereum).single_contract_multiple_data(abi=abi, address='0x37c997b35c619c21323f3518b9357914e8b99525', fragment='balanceOf', call_input=['0xAfA13aa8F1b1d89454369c28b0CE1811961A7907','0x4B5C684f4F82d698f9E6752c22a5FeC099d431A6'])

#multiple_contract_multiple_data
# Multicall(Network.Ethereum).multiple_contract_multiple_data(abi=abi, address=['0x37c997b35c619c21323f3518b9357914e8b99525','0x1f9840a85d5af5bf1d1762f925bdaddc4201f984'], fragment='balanceOf', call_input=['0xAfA13aa8F1b1d89454369c28b0CE1811961A7907','0x4B5C684f4F82d698f9E6752c22a5FeC099d431A6'])

#multiple_contract_single_data
# Multicall(Network.Ethereum).multiple_contract_single_data(abi=abi, address=['0x37c997b35c619c21323f3518b9357914e8b99525','0x1f9840a85d5af5bf1d1762f925bdaddc4201f984'], fragment='name')
