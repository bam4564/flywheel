import pandas as pd 
import numpy as np 

from prefect import flow
from pycoingecko import CoinGeckoAPI
from subgrounds import Subgrounds
from sqlalchemy import create_engine

from flywheel_util.constants import addresses, url_subgraphs
from flywheel_util.tasks.general import df_to_sql
from flywheel_util.tasks.fraxbp import (
    query_curve_pool_snapshots, 
    query_curve_mpools_with_gauge, 
    query_curve_pool_vol_snapshots, 
    compute_pool_dfs, 
    remove_inactive_pools, 
    query_convex_yield, 
    compute_metapool_snaps, 
    query_metapool_paired_asset_global_volume, 
)

engine = create_engine("sqlite+pysqlite:///votium_bribes.db", echo=False, future=True)

sg = Subgrounds()
sg_curve_pools = sg.load_subgraph(url_subgraphs.convex.curve_pools) 
sg_curve_vol = sg.load_subgraph(url_subgraphs.convex.curve_vol_mainnet)
sg_votium = sg.load_subgraph(url_subgraphs.votium.bribes) 

cg = CoinGeckoAPI()


@flow(cache_result_in_memory=False)
def flow_fraxbp_metapool_data():
    """ Retrieves pool level data for all pools associated with the FraxBP 
    1) The FraxBP itself. 
    2) Metapools paired with the FraxBP. 
    """
    # metadata for metapools with curve gauges 
    df_mpools_gauge = query_curve_mpools_with_gauge.submit()
    # daily snapshots for all pools (metapools + fraxbp) 
    df_pool_snaps = query_curve_pool_snapshots.submit()
    # swap volume for all pools (metapools + fraxbp) 
    df_pool_vol_snaps = query_curve_pool_vol_snapshots.submit()
            
    # df_pools is an id table for pools - primary key (pool_address) 
    # df_coins is an id table for coins in pools - primary key (pool_address, pool_coin_address) 
    # df_pool_coin joins together all info from df_pools and df_coins
    df_pools, df_coins, df_pool_coin = compute_pool_dfs(df_mpools_gauge, df_pool_snaps)    

    # Remove inactive pools 
    df_pools, df_pool_coin, df_pool_snaps, df_pool_vol_snaps = remove_inactive_pools(
        df_pools, df_coins, df_pool_coin, df_pool_snaps, df_pool_vol_snaps
    )
    
    # Gets curve / convex yields for metapools that have a convex staking contract and FraxBP 
    df_convex_yield = query_convex_yield(df_pools)
    
    # Contains daily snapshot data of pool reserves - primary key (date, pool_address, pool_coin_address)
    df_reserves = df_pool_snaps[[
        'date',
        'pool_address',
        'pool_coin_address',
        'reserves',
        'reserves_usd',
        'reserves_coin_price_usd',
    ]].drop_duplicates()
    assert all(
        len(gdf) == 1 for _, gdf in df_reserves.groupby(['date', 'pool_address', 'pool_coin_address'])
    )
    
    # Contains daily snapshot data for pool level metrics - primary key (date, pool_address) 
    df_pool_snaps = (
        df_pool_snaps[['date', 'pool_address', 'lp_price_usd', 'tvl']]
        .rename(columns={
            'tvl': 'snap_tvl_usd', 
            'lp_price_usd': 'snap_lp_price_usd', 
        })
        # drop duplicates since there was one row per pool and coin pre-filtering
        .drop_duplicates()     
        # Joins in pool swap volume 
        .merge(
            df_pool_vol_snaps.rename(columns={'snap_volume_usd': 'snap_vol_usd'}), 
            how='left', 
            on=['date', 'pool_address']
        )
        # Joins in convex yield, only exists for a subset of pools.  
        .merge(df_convex_yield, on=["date", "pool_address"])
    )
    df_pool_snaps.snap_vol_usd = df_pool_snaps.snap_vol_usd.fillna(0)
    assert not ((df_pool_snaps.snap_tvl_usd == 0) ^ (df_pool_snaps.snap_lp_price_usd == 0)).any()
    df_pool_snaps['snap_lp_supply'] = (df_pool_snaps.snap_tvl_usd / df_pool_snaps.snap_lp_price_usd).fillna(0)    
    df_pool_snaps['snap_liq_util'] = df_pool_snaps.snap_vol_usd / df_pool_snaps.snap_tvl_usd
    df_pool_snaps.loc[df_pool_snaps.snap_tvl_usd == 0, 'snap_liq_util'] = 0
    df_pool_snaps.snap_liq_util = df_pool_snaps.snap_liq_util.replace({np.inf: 0}) # TODO: Is this necessary? 
    assert not any(df_pool_snaps.snap_liq_util.isna())
    
    # Same as df_pool_snaps but filtered to only include metapools, then augmented with metapool specific metrics
    df_mpool_snaps = compute_metapool_snaps(df_pools, df_pool_snaps, df_reserves)

    # Cointains global tvl of assets paired against crvFRAX in metapools - primary key (date, pool_coin_address) 
    df_mpool_paired_tokens = (
        df_pool_coin
        .loc[df_pool_coin.pool_coin_address != addresses.token.crvfrax]
        [['pool_coin_name', 'pool_coin_address']]
    )
    token_addr_map = {name: addr for name, addr in df_mpool_paired_tokens.itertuples(index=False)}
    df_mpool_paired_asset_vol = query_metapool_paired_asset_global_volume(cg, token_addr_map)
    df_mpool_paired_asset_vol['pool_coin_address'] = df_mpool_paired_asset_vol.pool_coin_name.apply(lambda name: token_addr_map[name])
    
    df_mpool_snaps = (
        df_mpool_snaps
        # Join the address of the coin that is not crvFRAX onto each mpool snapshot 
        .merge(
            df_coins.loc[df_coins.pool_coin_address != addresses.token.crvfrax]
                [['pool_address', 'pool_coin_address']]
                .rename(columns={'pool_coin_address': 'metapool_asset_address'}), 
            how='left', on=['pool_address']
        ) 
        # For each metapool snapshot, we join in the paired metapool asset and it's global tvl 
        .merge(
            df_mpool_paired_asset_vol
                .rename(columns={'pool_coin_address': 'metapool_asset_address'})
                [['mpool_paired_asset_vol_usd', 'date', 'metapool_asset_address']], 
            how='left', on=['date', 'metapool_asset_address'], 
        )
    )
    df_mpool_snaps.mpool_paired_asset_vol_usd = df_mpool_snaps.mpool_paired_asset_vol_usd.fillna(0)

    # Validation 
    num_metapools = len(df_mpool_snaps.pool_address.unique())
    num_metapools_gauge = len(df_pools.loc[~df_pools.pool_gauge.isna() & (df_pools.pool_fraxbp_metapool == True)])
    print(f"Discovered {num_metapools} metapools.")
    print(f"Number of pools with gauges: {num_metapools_gauge}")    
        
    for table_name, table_df in [
        ('pools', df_pools), 
        ('coins', df_coins), 
        ('pool_coin', df_pool_coin), 
        ('pool_snaps', df_pool_snaps),  
        ('mpool_snaps', df_mpool_snaps), 
        ('reserves', df_reserves), 
    ]:
        df_to_sql.submit(engine, table_df, table_name)
        
    return df_pools, df_coins, df_pool_coin, df_pool_snaps, df_mpool_snaps, df_reserves