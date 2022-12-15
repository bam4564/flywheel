from pprint import PrettyPrinter

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
    remove_prefix, 
    remove_prefixes, 
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


@task(persist_result=True, cache_key_fn=lambda *args: 'pools9') 
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
        df = (
            df.rename(columns={
                'pool_token': 'pool_cvx_token',
                'pool_swap': 'pool_address',
                'pool_coins': 'pool_coin_address',
                'pool_asset_type': 'pool_type', 
            })
        ) 
        dfs.append(df)
    df = pd.concat(dfs)
    df['pool_symbol'] = df['pool_name'].apply(lambda name: name.split(":")[-1].strip())
    return df

@task(persist_result=True, cache_key_fn=lambda *args: 'pool_snaps_raw19') 
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
                    'cumulativeVolumeUSD_gt': 1, # There are some test pools and pools that were incorrectly created that we filter out by only getting pools with more than 0 volume 
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

@task(persist_result=True, cache_key_fn=lambda *args: 'pool_snaps_vol_raw27') 
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

@task(persist_result=True, cache_key_fn=lambda *args: 'metapool_asset_eco_vol8') 
def query_metapool_asset_ecosystem_volume(cg, token_addr_map): 
    # Get ecosystem wide (eth + other chains) volume for assets paired against crvFRAX in metapools 
    return (
        cg_get_market_history(cg, token_addr_map, include_price=False, include_market_cap=False)
        .rename(columns={'token_name': 'pool_coin_name', 'total_volumes': 'mpool_asset_eco_tvl_usd'})
        .drop(columns="timestamp")
    ) 

@task 
def compute_pool_dfs(df_pools, df_pool_snaps):
    join_cols = ['pool_address', 'pool_coin_address']
    col_diff = compare_cols(df_pools, df_pool_snaps)
    cols_shared = col_diff.intersection
    # Thought both dataframes share these columns, they have different values as they come from different subgraphs. 
    # Thus, we don't include these as join columns because it would cause failure. 
    assert cols_shared.difference(join_cols) == set(['pool_lp_token', 'pool_symbol', 'pool_name', 'pool_type'])
    assert not set(
        df_pools.pool_address.unique()).difference(df_pool_snaps.pool_address.unique()
    )
    # We join gauge specifc onto our filtered pool snapshots (one row per pool coin combination) 
    df_pool_coins = (
        df_pool_snaps
        .drop_duplicates(subset=join_cols)
        .merge(df_pools[join_cols + ['pool_gauge', 'pool_cvx_token']], how='left', on=join_cols)
    )
    df_pool_coins['pool_fraxbp_metapool'] = df_pool_coins.pool_address.apply(lambda a: a != addresses.contract.fraxbp)
    df_pool_coins['pool_fraxbp'] = ~df_pool_coins.pool_fraxbp_metapool
    coin_cols = [
        'pool_coin_name',
        'pool_coin_address',
        'pool_coin_decimals',
    ]
    # Compute our finalized version of the pools an pool_coins tables 
    df_pool_coins = df_pool_coins[[
        'pool_address',
        'pool_name',
        'pool_symbol',
        'pool_lp_token',
        'pool_type',
        *coin_cols, 
        'pool_fraxbp_metapool',
        'pool_fraxbp',
        'pool_gauge',
        'pool_cvx_token',
    ]]
    df_pools = df_pool_coins.drop(columns=coin_cols).drop_duplicates()
    return (
        df_pools.sort_values('pool_address').reset_index(drop=True), 
        df_pool_coins.sort_values('pool_address').reset_index(drop=True),
    )

@task 
def compute_curve_pool_reserves(df_pool_snaps): 
    # Converts pool snapshots (containing both pool level and pool-coin level metrics) into a dataframe 
    # containing only the reserves for each pool 
    df_reserves = df_pool_snaps[[
        'date',
        'pool_address',
        'pool_coin_address',
        'pool_coin_name',
        'reserves',
        'reserves_usd',
        'reserves_coin_price_usd',
    ]].drop_duplicates()
    assert all(
        len(gdf) == 1 for _, gdf in df_reserves.groupby(['date', 'pool_address', 'pool_coin_address'])
    )
    return df_reserves

@task
def join_curve_pool_vol(df_pool_snaps, df_pool_vol_snaps): 
    # Join in daily volume data for each of the curve pools   
    df = df_pool_snaps.merge(df_pool_vol_snaps, how='left', on=['pool_address', 'date'])
    df.snap_volume_usd = df.snap_volume_usd.fillna(0)
    return df 

@task 
def remove_inactive_pools(df): 
    # Remove inactive pools
    last_snapshot = df.groupby(["pool_address", "pool_name", "pool_coin_name"])['snapshot_reserves_usd'].last().reset_index()
    inactive_pools = last_snapshot.loc[last_snapshot.snapshot_reserves_usd < 1]
    if len(inactive_pools): 
        inactive_pools = inactive_pools[['pool_address', 'pool_name']].drop_duplicates()
        for p in inactive_pools.to_dict(orient="records"):
            peak_tvl = df.loc[df.pool_address == p['pool_address']]['snapshot_reserves_usd'].max()
            print(f"Removing data for inactive pool {p['pool_name']} with peak tvl {peak_tvl}.")
    inactive_addrs = inactive_pools.pool_address.unique()
    df = df.loc[~df.pool_address.isin(inactive_addrs)]
    return df 

@task 
def process_pool_snaps(df): 
    df = df.rename(columns={'tvl': 'snap_tvl_usd', 'lp_price_usd': 'snap_lp_price_usd', 'snap_volume_usd': 'snap_vol_usd'})
    assert not ((df.snap_tvl_usd == 0) ^ (df.snap_lp_price_usd == 0)).any()
    df['snap_lp_supply'] = (df.snap_tvl_usd / df.snap_lp_price_usd).fillna(0)    
    df['snap_liq_util'] = df.snap_vol_usd / df.snap_tvl_usd
    df.loc[df.snap_tvl_usd == 0, 'snap_liq_util'] = 0
    df.snap_liq_util = df.snap_liq_util.replace({np.inf: 0}) # TODO: Is this necessary? 
    assert not any(df.snap_liq_util.isna())
    return df[[
        "date", 
        "pool_address", 
        "snap_vol_usd",
        "snap_tvl_usd",
        "snap_liq_util",
        "snap_lp_supply",
        "snap_lp_price_usd",
    ]]

@task 
def process_metapool_snaps(df_pools, df_pool_snaps, df_reserves): 
    mpool_addrs = df_pools.loc[df_pools.pool_fraxbp_metapool == True].pool_address.unique()
    df_mpool_snaps = df_pool_snaps.loc[df_pool_snaps.pool_address.isin(mpool_addrs)]
    # supply over crvFRAX in each metapool overtime 
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
def remove_inactive_pools(df_pools, df_pool_coins, df_pool_snaps, df_pool_vol_snaps):
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
        df_pool_snaps = df_pool_snaps.loc[~df_pool_snaps.pool_address.isin(remove_pools)]
        df_pool_coins = df_pool_coins.loc[~df_pool_coins.pool_address.isin(remove_pools)]
        df_pool_vol_snaps = df_pool_vol_snaps.loc[~df_pool_vol_snaps.pool_address.isin(remove_pools)]
    return df_pools, df_pool_coins, df_pool_snaps, df_pool_vol_snaps

@task
def process_pool_data(df): 
    """Get daily snapshots of tvl for all of the metapools. 
    """
    # Add in column for total supply of crvFRAX over time 
    df = df.merge(
        df.loc[df.pool_address == addresses.contract.fraxbp][['snapshot_timestamp', 'snapshot_lp_supply']]
            .drop_duplicates() # must happen since there is one row per coin in pool 
            .rename(columns={'snapshot_lp_supply': 'snapshot_crvFRAX_supply'}), 
        how='left', 
        on='snapshot_timestamp'
    )
    assert not df.snapshot_crvFRAX_supply.isna().any()
    
    # Compute / Process columns representing metrics measured in relation to fraxBP 
    df_pool_bp_lp = (
        df.loc[(df.pool_is_metapool == True) & (df.pool_coin_name == 'crvFRAX')]
        [['pool_address', 'snapshot_timestamp', 'snapshot_reserves', 'pool_coin_decimals', 'snapshot_crvFRAX_supply']]
    )
    # number of lp tokens for base pool deposited in metapool 
    df_pool_bp_lp['snapshot_bp_lp_metapool'] = df_pool_bp_lp.snapshot_reserves / 10**df_pool_bp_lp.pool_coin_decimals 
    # fraction of number of lp tokens for base pool deposited in metapool to total supply of lp tokens in base pool 
    df_pool_bp_lp['snapshot_bp_lp_metapool_share'] = df_pool_bp_lp.snapshot_bp_lp_metapool / df_pool_bp_lp.snapshot_crvFRAX_supply
    # number of lp tokens for base pool desposited across all metapools 
    df_pool_bp_lp['snapshot_bp_lp_all_metapools'] = df_pool_bp_lp.groupby('snapshot_timestamp')['snapshot_bp_lp_metapool'].transform("sum")
    # fraction of number of lp tokens for base pool deposited in metapool to total number of lp tokens for base pool deposited across all metapools 
    df_pool_bp_lp['snapshot_bp_lp_all_metapools_share'] = 0
    mask = df_pool_bp_lp.snapshot_bp_lp_all_metapools != 0
    df_pool_bp_lp.loc[mask, 'snapshot_bp_lp_all_metapools_share'] = (
        df_pool_bp_lp.loc[mask, 'snapshot_bp_lp_metapool'] / df_pool_bp_lp.loc[mask, 'snapshot_bp_lp_all_metapools']
    )
    df = df.merge(
        df_pool_bp_lp[[
            'pool_address', 'snapshot_timestamp', 'snapshot_bp_lp_metapool', 'snapshot_bp_lp_metapool_share', 
            'snapshot_bp_lp_all_metapools', 'snapshot_bp_lp_all_metapools_share'
        ]], 
        how='left', on=['pool_address', 'snapshot_timestamp']
    )    
    assert set(df.loc[df.snapshot_bp_lp_metapool.isna()].pool_address.unique()) == set([addresses.contract.fraxbp])
            
    return df_sort_cols(df, ['pool', 'snapshot']) 
