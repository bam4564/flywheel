import pandas as pd 

from web3 import Web3
from web3.contract import Contract
from etherscan import Etherscan


URL_INFURA = 'https://mainnet.infura.io/v3/856c3834f317452a82e25bb06e04de18'
w3 = Web3(Web3.HTTPProvider(URL_INFURA))
hmmmm = 'VR7YA9DRDB4Y15B5N3WU9E7PSJ9RWPCP5S'
etherscan = Etherscan(hmmmm)


def get_verified_abi(address): 
    address = Web3.toChecksumAddress(address) 
    abi = etherscan.get_contract_abi(address) 
    return abi 


def verified_contract(address, return_address_impl=False) -> Contract: 
    address = Web3.toChecksumAddress(address) 
    abi = get_verified_abi(address)
    contract = w3.eth.contract(address, abi=abi)
    is_eip_1967_proxy = len(contract.find_functions_by_name('implementation')) > 0
    if is_eip_1967_proxy: 
        impl_addr = Web3.toHex(w3.eth.get_storage_at(
            address, "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"
        ))
        address_impl = impl_addr.replace('0x000000000000000000000000', '0x')
        abi_impl = etherscan.get_contract_abi(address_impl)
        contract = w3.eth.contract(address, abi=abi_impl)
    else: 
        address_impl = address
        
    if return_address_impl:
        return contract, address_impl
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


def get_total_supply_of_erc20(token_address): 
    contract = verified_contract(token_address) 
    balance = contract.functions.totalSupply().call()
    decimals = contract.functions.decimals().call()
    return balance / 10**decimals 


def get_account_balance_of_erc20(account, token_address): 
    account = Web3.toChecksumAddress(account) 
    contract = verified_contract(token_address) 
    balance = contract.functions.balanceOf(account).call()
    decimals = contract.functions.decimals().call()
    return balance / 10**decimals 


def get_erc20_balance_for_all_accounts(token_address, fromBlock):
    # Note: Will not work for rebase tokens that don't emit transfer events      
    contract = verified_contract(token_address)
    transfers = contract.events.Transfer.getLogs(fromBlock=fromBlock)
    decimals = contract.functions.decimals().call()
    for to_key, from_key, value_key in [['to', 'from', 'value'], ['_to', '_from', '_value']]: 
        try: 
            df_transfers = pd.DataFrame([
                {
                    'to': t.args[to_key], 
                    'from': t.args[from_key], 
                    'value': t.args[value_key] / 10**decimals
                } 
                for t in transfers
            ])
            break 
        except: 
            pass 
    df_in = df_transfers.groupby('to')['value'].sum().reset_index().rename(columns={'to': 'account'})
    df_out = df_transfers.groupby('from')['value'].sum().reset_index().rename(columns={'from': 'account'})
    df_out.value *= -1
    df = pd.concat([df_in, df_out]).groupby('account').value.sum().reset_index()
    df = df.loc[(df.account != '0x0000000000000000000000000000000000000000') & (df.value != 0)]
    df = df.sort_values('value').reset_index(drop=True) 
    return df