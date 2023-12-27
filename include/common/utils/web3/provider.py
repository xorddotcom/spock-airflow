from web3 import Web3
from eth_typing import BlockNumber

class Web3Node:
    def __init__(self, chain_id:int, rpc:str) -> None:
        self.chain_id = chain_id
        self.rpc = rpc
        self.provider = Web3(Web3.HTTPProvider(rpc))

    def current_block(self) -> BlockNumber:
        return self.provider.eth.block_number
    
class Network:
    Ethereum = Web3Node(1, "https://rpc.ankr.com/eth")
