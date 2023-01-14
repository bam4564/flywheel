from box import Box
from palettable.tableau import Tableau_20
from palettable.mycarta import Cube1_4, Cube1_8

# Color palettes 
colors_24 = Tableau_20.hex_colors + Cube1_4.hex_colors
colors_28 = Tableau_20.hex_colors + Cube1_8.hex_colors

# addresses of contracts / tokens 
addresses = Box({
    "token": {
        # By convention, keep token names lower case 
        "frax": '0x853d955aCEf822Db058eb8505911ED77F175b99e'.lower(), 
        "fxs": '0x3432B6A60D23Ca0dFCa7761B7ab56459D9C964D0'.lower(), 
        'crvfrax': '0x3175df0976dfa876431c2e9ee6bc45b65d3473cc'.lower(), 
        '3crv': '0x6c3f90f043a72fa612cbac8115ee7e52bde6e490'.lower(), 
        'frxeth': '0x5E8422345238F34275888049021821E8E08CAa1f'.lower(), 
        'frxethcrv': '0xf43211935c781d5ca1a41d2041f397b8a7366c7a'.lower(), # lp token for the frxETH:ETH curve pool 
        'cvxfrxethcrv': '0xC07e540DbFecCF7431EA2478Eb28A03918c1C30E'.lower(), # convex deposit token received when depositing frxethcrv (curve system)
        'stkcvxfrxethcrv-frax': '0x4659d5fF63A1E1EDD6D5DD9CC315e063c95947d0'.lower(), # frax's tokenized convex deposit for frxETHCRV         
    }, 
    "contract": {
        "curve": {
            # Contracts 
            'frxethcrv_gauge_deposit': '0x2932a86df44fe8d2a706d8e9c5d51c24883423f5'.lower(), # gauge for the frxETH:ETH curve pool
            # Pools 
            "fraxbp": '0xdcef968d416a41cdac0ed8702fac8128a64241a2'.lower(), 
            "frxeth-eth": '0xa1f8a6807c402e4a15ef4eba36528a3fed24e577'.lower(), 
        }, 
        "convex": {
            # facilitates deposits / withdrawls of curve lp tokens 
            # https://docs.convexfinance.com/convexfinanceintegration/booster
            # Example flow for call to `depositAll`:  
            # https://dashboard.tenderly.co/tx/mainnet/0xe375262081544c5a69acad953a60596167a7dce4589e3edb5682b42e3b0db034
            # 1. Gets balance of user held curve lp token (ex: crvCRVETH)
            # 2. Transfers user balance of lp token to curve voter proxy
            # 3. Calls `deposit` on the staking contract (curve voter proxy), which already holds the 
            #   user's lp tokens. This leads to the lp tokens being deposited into a curve gauge in 
            #   exchange for curve gauge deposit tokens (sent back to curve voter proxy). 
            # 4.A. If stake is False
            #   - Booster mints convex deposit wrapper (cvxcrvCRVETH) to the user.
            #   - The net end state in this scenario is as follows 
            #       - User sends X units of crvCRVETH to curve gauge contract. 
            #       - Convex voter proxy receives X units of crvCRVETH-gauge from curve gauge contract. 
            #       - User receives X units of cvxcrvCRVETH. 
            # 4.B. If stake is True 
            #   - Booster mints convex deposit wrapper (cvxcrvCRVETH) to itself.
            #   - Booster approves rewards contract (BaseRewardsPool) to use tokens it just minted. 
            #   - Booster calls `stakeFor` on the rewards contract. This sends the convex deposit 
            #       receipts to the base rewards pool. 
            #   - The net end state in this scenario is as follows 
            #       - User sends X units of crvCRVETH to curve gauge contract. 
            #       - Convex voter proxy receives X units of crvCRVETH-gauge from curve gauge contract. 
            #       - BaseRewardsPool receives X units of cvxcrvCRVETH. 
            # The difference between scenarios 4.A and 4.B is where the cvxcrvCRVETH ends up.
            "booster": '0xF403C135812408BFbE8713b5A23a04b3D48AAE31'.lower(),    
            # Main reward contract for convex pools. 
            # This is where convex deposit wrappers for deposited lp tokens live. User's who deposit into this 
            # contract do not receive an erc20 token in exchange, balances are accounted for using an internal mapping. 
            # https://docs.convexfinance.com/convexfinanceintegration/baserewardpool
            "base_reward_pool_cvxfrxethcrv": "0xbD5445402B0a287cbC77cb67B2a52e2FC635dce4".lower(),  
            # This contract holds the actual gauge deposit tokens for curve lp tokens. Since it holds the gauge deposit tokens, 
            # this contract holds convex's voting power on curve. 
            "curve_voter_proxy": "0x989aeb4d175e16225e39e87d0d97a3360524ad80".lower(),                                                                           
        }, 
        "frax": {
            "farm_frxeth": '0xa537d64881b84faffb9Ae43c951EEbF368b71cdA'.lower(), # Frax farming contract where users can deposit lp tokens from frxETH:ETH curve pool
        }, 
        "votium": {
            "voter_proxy": '0xde1E6A7ED0ad3F61D531a8a78E83CcDdbd6E0c49'.lower(), 
        }
    }
})

# Infura endpoint. TODO: Put this in an env file 
url_infura = 'https://mainnet.infura.io/v3/856c3834f317452a82e25bb06e04de18'

# snapshot graphql api 
url_snapshot = 'https://hub.snapshot.org/graphql'
snapshot_api_max_records_per_request = 1000 
snapshot_api_max_skip = 5000

# url to various subgraphs 
url_subgraphs = Box({
    "votium": {
        "bribes": 'https://api.thegraph.com/subgraphs/name/convex-community/votium-bribes',
    },
    "convex": {
        "curve_dao": 'https://api.thegraph.com/subgraphs/name/convex-community/curve-dao', 
        "curve_pools": 'https://api.thegraph.com/subgraphs/name/convex-community/curve-pools', 
        "curve_vol_mainnet": 'https://api.thegraph.com/subgraphs/name/convex-community/volume-mainnet' 
    }, 
    "frax": {
        "fraxlend": "https://api.thegraph.com/subgraphs/name/frax-finance-data/fraxlend-subgraph---mainnet"
    }
})


