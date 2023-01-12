from pprint import PrettyPrinter
from datetime import timedelta

from subgrounds import Subgrounds

from subgrounds import Subgrounds
from prefect import task
import pandas as pd 
import numpy as np 
import missingno as miss

from .utils import (
    ddf, 
    first_row, 
    cg_get_market_history, 
    df_cols_camel_to_snake, 
    df_cols_change_prefix, 
    df_sort_cols, 
    remove_prefix, 
    recursive_index_merge, 
    zip_dfs, 
    query_attrs, 
    compare_sets, 
    compare_cols, 
)
from .constants import addresses, url_subgraphs

pp = PrettyPrinter().pprint

ADDRESS_POOLS_IGNORE = [
    '0xd3301b7caa76f932816a6fc7ef0b673238e217ad'.lower(), # This is the BENTFRAXBP-f that was deprecated 
    '0xd9f907f7F84CbB0Af85C7829922fd692339147f9'.lower(), # honestly not sure what the fuck is going on here 
]

sg = Subgrounds()

sg_curve_pools = sg.load_subgraph(url_subgraphs.convex.curve_pools) 
sg_curve_vol = sg.load_subgraph(url_subgraphs.convex.curve_vol_mainnet)
sg_votium = sg.load_subgraph(url_subgraphs.votium.bribes) 


@task(persist_result=True, cache_key_fn=lambda *args: 'pools10', cache_expiration=timedelta(days=1)) 
def query_curve_mpools_with_gauge(): 
    # Get pool data for all fraxbp metapools + fraxbp (one row per combination of pool and coin) 
    # ----------------------------------------------------------------------
    attrs = [
        'name',
        'gauge',
        'token', # convex receipt token. TODO: How does this work? 
        'lpToken', 
        'swap', # pool contract address 
        'coins',
        'assetType',
    ]
    queries = [
        sg_curve_pools.Query.pools(first=1000, where={
            'coins_contains': [addresses.token.crvfrax], 'id_not_in': ADDRESS_POOLS_IGNORE
        }), 
        sg_curve_pools.Query.pools(where={'swap': addresses.contract.fraxbp}),
    ]
    dfs = []
    for q in queries: 
        df = query_attrs(sg, q, attrs)
        df.pools_assetType = df.pools_assetType.apply(lambda v: {0: "stable", 4: "non_stable"}[int(v)])
        df = df_cols_camel_to_snake(df) 
        df = df_cols_change_prefix(df, "pools", "pool") 
        df = df.rename(columns={
            'pool_token': 'pool_cvx_token',
            'pool_swap': 'pool_address',
            'pool_coins': 'pool_coin_address',
            'pool_asset_type': 'pool_type', 
        })
        dfs.append(df)
    df = pd.concat(dfs)
    df['pool_symbol'] = df['pool_name'].apply(lambda name: name.split(":")[-1].strip())
    return df

@task(persist_result=True, cache_key_fn=lambda *args: 'pool_snaps_raw19', cache_expiration=timedelta(days=1)) 
def query_curve_pool_snapshots(): 
    # Get curve pool historical data for all fraxbp metapools + fraxbp 
    # ----------------------------------------------------------------------
    attrs_pool_snaps = [
        'pool.id', 'pool.name', 'pool.address', 'pool.symbol', 'pool.lpToken', 
        'pool.poolType', 'pool.coins', 'pool.coinNames', 'pool.coinDecimals', 
        'timestamp', 'reserves', 'reservesUSD', 'tvl', 'lpPriceUSD'
    ]
    query_pool_snaps = [
        # FraxBP     
        sg_curve_vol.Query.dailyPoolSnapshots(first=100000, where={'pool': addresses.contract.fraxbp}), 
        # FraxBP metapools 
        sg_curve_vol.Query.dailyPoolSnapshots(
            first=100000, 
            where={
                "pool_": {
                    'coins_contains': [addresses.token.crvfrax], 
                    'cumulativeVolumeUSD_gt': 10000, # There are some test pools and pools that were incorrectly created that we filter out by only getting pools with more than 0 volume 
                    'id_not_in': ADDRESS_POOLS_IGNORE
                }
            }
        )
    ]
    dfs_concat = []
    for q in query_pool_snaps: 
        dfs = query_attrs(sg, q, attrs_pool_snaps) 
        df = recursive_index_merge(dfs)
        dfs_concat.append(df)
        
    df = pd.concat(dfs_concat)
    df = df_cols_change_prefix(df, "dailyPoolSnapshots_pool_", "pool_")
    df = remove_prefix(df, "dailyPoolSnapshots_")
    df = df_cols_camel_to_snake(df)
    df['date'] = pd.to_datetime(pd.to_datetime(df.timestamp, unit='s').dt.date) 
    df = df.rename(columns={
        'pool_coins': 'pool_coin_address',
        'pool_coin_names': 'pool_coin_name', 
        'pool_pool_type': 'pool_type', 
    })
    df['reserves'] /= 10**df.pool_coin_decimals # normalize reserves using coin decimals 
    df['reserves_coin_price_usd'] = df.reserves_usd / df.reserves.replace({0:1}) # spot price for pool coin 
    df['pool_type'] = df.pool_type.apply(
        lambda t: {'CRYPTO_FACTORY': 'non_stable', 'STABLE_FACTORY': 'stable'}.get(t, t).lower()
    )
    df = df.drop(columns=['timestamp', 'pool_id'])
    return df

@task(persist_result=True, cache_key_fn=lambda *args: 'pool_snaps_vol_raw27', cache_expiration=timedelta(days=1)) 
def query_curve_pool_vol_snapshots(): 
    # Get pool volumne snapshots for all curve metapools + FraxBP 
    daily_period = 60 * 60 * 24 # get snapshots at a daily cadence 
    attrs_swap_snaps = ['pool.id', 'timestamp', 'volumeUSD']
    query_swap_snaps = [
        # FraxBP     
        sg_curve_vol.Query.swapVolumeSnapshots(
            first=100000, 
            where={'pool': addresses.contract.fraxbp, "period": daily_period}
        ), 
        # FraxBP metapools 
        sg_curve_vol.Query.swapVolumeSnapshots(
            first=100000, 
            where={
                "pool_": {
                    'coins_contains': [addresses.token.crvfrax], 
                    'cumulativeVolumeUSD_gt': 0, # There are some test pools and pools that were incorrectly created that we filter out by only getting pools with more than 0 volume 
                    'id_not_in': ADDRESS_POOLS_IGNORE
                }, 
                "period": daily_period
            }
        )
    ]
    df = pd.concat([query_attrs(sg, q, attrs_swap_snaps) for q in query_swap_snaps])
    df = df_cols_change_prefix(df, "swapVolumeSnapshots_pool_", "pool_")
    df = df_cols_change_prefix(df, "swapVolumeSnapshots_", "snap_")
    df = df_cols_camel_to_snake(df)
    df['date'] = pd.to_datetime(pd.to_datetime(df.snap_timestamp, unit='s').dt.date) 
    df = df.rename(columns={'pool_id': 'pool_address'}).drop(columns="snap_timestamp")
    return df

@task(persist_result=True, cache_key_fn=lambda *args: 'convex_yield1', cache_expiration=timedelta(days=1))
def query_convex_yield(df_pools): 
    attrs = [
        'timestamp', 
        'baseApr', 
        'crvApr',
        'cvxApr',
        'extraRewardsApr',
        'poolid.swap',
    ]
    pool_addrs = df_pools.loc[~df_pools.pool_gauge.isna()].pool_address.unique().tolist()
    q = sg_curve_pools.Query.dailyPoolSnapshots(first=100000, where={"poolid_": {"swap_in": pool_addrs}})
    df = query_attrs(sg, q, attrs) 
    df = remove_prefix(df, "dailyPoolSnapshots_")
    df = df_cols_camel_to_snake(df)
    assert set(df.poolid_swap.unique()) == set(pool_addrs)
    df['date'] = pd.to_datetime(pd.to_datetime(df.timestamp, unit='s').dt.date) 
    df['total_apr'] = df.base_apr + df.crv_apr + df.cvx_apr + df.extra_rewards_apr
    df = df.rename(columns={"poolid_swap": "pool_address"})
    df = df.sort_values("date").reset_index(drop=True)  
    return df 

@task(persist_result=True, cache_key_fn=lambda *args: 'metapool_asset_eco_vol9', cache_expiration=timedelta(days=1)) 
def query_metapool_paired_asset_global_volume(cg, token_addr_map): 
    # Get ecosystem wide (eth + other chains) volume for assets paired against crvFRAX in metapools 
    return (
        cg_get_market_history(cg, token_addr_map, include_price=False, include_market_cap=False)
        .rename(columns={'token_name': 'pool_coin_name', 'total_volumes': 'mpool_paired_asset_vol_usd'})
        .drop(columns="timestamp")
    ) 

@task 
def compute_pool_dfs(df_mpools_gauge, df_pool_snaps):
    # One row per combination of pool and coin within that pool. 
    df_coins = (
        df_pool_snaps[['pool_address', 'pool_coin_address']]
        .drop_duplicates()
        .merge(
            df_pool_snaps[['pool_address', 'pool_coin_name', 'pool_coin_address', 'pool_coin_decimals']], 
            how='left', 
            on=['pool_address', 'pool_coin_address'], 
            validate="1:m"
        )
        .drop_duplicates()
        .reset_index(drop=True)
    ) 
    # One row per pool 
    df_pools = (
        df_pool_snaps[['pool_address', 'pool_lp_token', 'pool_symbol', 'pool_name', 'pool_type']]
        .drop_duplicates()
        .merge(
            df_mpools_gauge[['pool_address', 'pool_cvx_token', 'pool_gauge']].drop_duplicates(), 
            how='left', 
            on="pool_address", 
            validate="1:1"
        )
        .reset_index(drop=True)
    )
    df_pools['pool_fraxbp_metapool'] = df_pools.pool_address.apply(lambda a: a != addresses.contract.fraxbp)
    df_pools['pool_fraxbp'] = ~df_pools.pool_fraxbp_metapool
    # One row per combination of pool and coin within that pool 
    df_pool_coin = df_pools.merge(df_coins, how='left', on="pool_address").reset_index(drop=True)
    return [df.sort_values("pool_address").reset_index(drop=True) for df in (df_pools, df_coins, df_pool_coin)]

@task 
def compute_metapool_snaps(df_pools, df_pool_snaps, df_reserves): 
    mpool_addrs = df_pools.loc[df_pools.pool_fraxbp_metapool == True].pool_address.unique()
    df_mpool_snaps = df_pool_snaps.loc[df_pool_snaps.pool_address.isin(mpool_addrs)]
    # supply over crvFRAX in each metapool 
    df_mpool_supply_crvfrax = (
        df_reserves.loc[
            (df_reserves.pool_address != addresses.contract.fraxbp) & 
            (df_reserves.pool_coin_address == addresses.token.crvfrax)
        ]
        [['date', 'pool_address', 'reserves']]
        .rename(columns={'reserves': 'crvfrax_in_mpool'})
    )
    # total supply of crvFRAX over time 
    df_total_supply_crvfrax = (
        df_pool_snaps.loc[df_pool_snaps.pool_address == addresses.contract.fraxbp]
        [['date', 'snap_lp_supply']]
        .rename(columns={'snap_lp_supply': 'crvfrax_total'})
    ) 
    df_mpool_snaps = (
        df_mpool_snaps
        .merge(df_mpool_supply_crvfrax, how='left', on=['date', 'pool_address'])
        .merge(df_total_supply_crvfrax, how='left', on=['date'])
    ) 
    assert (not df_mpool_snaps.crvfrax_in_mpool.isna().any()) and (not df_mpool_snaps.crvfrax_total.isna().any())    
    df_mpool_snaps['crvfrax_in_all_mpools'] = df_mpool_snaps.groupby('date')['crvfrax_in_mpool'].transform("sum")
    assert not ((df_mpool_snaps.crvfrax_in_mpool != 0) & (df_mpool_snaps.crvfrax_in_all_mpools == 0)).any()
    df_mpool_snaps['crvfrax_share_mpools'] = df_mpool_snaps.crvfrax_in_mpool / df_mpool_snaps.crvfrax_in_all_mpools.replace({0:1}) # assertion above enables this
    df_mpool_snaps['crvfrax_share_fraxbp'] = df_mpool_snaps.crvfrax_in_mpool / df_mpool_snaps.crvfrax_total
    return df_mpool_snaps 

@task 
def remove_inactive_pools(df_pools, df_coins, df_pool_coin, df_pool_snaps, df_pool_vol_snaps):
    last_snapshot = (
        df_pool_snaps
        .groupby(["pool_address", "pool_name", "pool_coin_name"])
        ['reserves_usd'].last().reset_index()
    )
    inactive_pools = last_snapshot.loc[last_snapshot.reserves_usd < 1][["pool_name", "pool_address"]].drop_duplicates()
    if len(inactive_pools): 
        remove_pools = inactive_pools.pool_name.unique().tolist()
        print(f"Removing pools {', '.join(remove_pools)}")
        df_pools = df_pools.loc[~df_pools.pool_address.isin(remove_pools)]
        df_coins = df_coins.loc[~df_coins.pool_address.isin(remove_pools)]
        df_pool_snaps = df_pool_snaps.loc[~df_pool_snaps.pool_address.isin(remove_pools)]
        df_pool_coin = df_pool_coin.loc[~df_pool_coin.pool_address.isin(remove_pools)]
        df_pool_vol_snaps = df_pool_vol_snaps.loc[~df_pool_vol_snaps.pool_address.isin(remove_pools)]
    return df_pools, df_pool_coin, df_pool_snaps, df_pool_vol_snaps

