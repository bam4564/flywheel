from web3 import Web3
from etherscan import Etherscan


URL_INFURA = 'https://mainnet.infura.io/v3/856c3834f317452a82e25bb06e04de18'
w3 = Web3(Web3.HTTPProvider(URL_INFURA))
hmmmm = 'VR7YA9DRDB4Y15B5N3WU9E7PSJ9RWPCP5S'
etherscan = Etherscan(hmmmm)


def get_verified_abi(address): 
    address = Web3.toChecksumAddress(address) 
    abi = etherscan.get_contract_abi(address) 
    return abi 


def verified_contract(address): 
    address = Web3.toChecksumAddress(address) 
    abi = get_verified_abi(address)
    contract = w3.eth.contract(address, abi=abi)
    return contract 


def erc20_read_contract(token_address, fn_names, fn_args=None, fn_kwargs=None):
    contract = verified_contract(token_address) 
    results = {}
    fn_args = fn_args or {}
    fn_kwargs = fn_kwargs or {}
    for name in fn_names: 
        args = fn_args.get(name, []) 
        kwargs = fn_kwargs.get(name, {})
        fn = getattr(contract.functions, name)
        results[name] = fn(*args, **kwargs).call()
    return results 