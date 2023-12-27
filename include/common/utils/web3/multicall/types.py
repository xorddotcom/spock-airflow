from typing import List, Union, NewType, TypedDict, Any

from include.common.utils.web3.multicall.call import Call

CallInput = NewType('CallInput', List[Union[str, int]])

class CallStruct(TypedDict):
    target: str
    callData: str

class CallResult(TypedDict):
    call: Call
    output: Any
    success: bool