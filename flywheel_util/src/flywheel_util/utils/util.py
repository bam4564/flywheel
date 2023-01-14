
import asyncio 
import re 

from collections import deque, namedtuple 
from itertools import chain, product 

from typing import List 

from subgrounds.pagination import ShallowStrategy
from IPython.display import HTML, display
from concurrent.futures import ThreadPoolExecutor
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

import pandas as pd 

import re 
from typing import List 

import pandas as pd 
from subgrounds.pagination import ShallowStrategy
from IPython.display import HTML, display


def ddf(df):
    display(HTML(df.to_html()))

def first_row(df): 
    ddf(df.head(1))
    
def remove_prefix(df: pd.DataFrame, prefix: str):
    # Remove a prefix from all columns 
    col_map = {}
    for i, c in enumerate(df.columns): 
        if c.startswith(prefix): 
            col_map[c] = c[len(prefix):]
    df = df.rename(columns=col_map) 
    return df 

def remove_prefixes(df: pd.DataFrame, prefixes: List[str]):
    for p in prefixes: 
        df = remove_prefix(df, p)
    return df 

def compare_sets(a, b): 
    SetDiff = namedtuple('SetDiff', 'intersection left right')
    a = set(a) 
    b = set(b) 
    i = a.intersection(b)
    l = a.difference(b)
    r = b.difference(a)
    return SetDiff(i, l, r) 

def compare_cols(dfa, dfb): 
    return compare_sets(dfa.columns, dfb.columns)

def zip_dfs(dfs, col_names): 
    data = dfs[0]
    for i, c in enumerate(col_names): 
        data = data.merge(dfs[i+1][[c]], how='left', left_index=True, right_index=True)
    return data

def recursive_index_merge(dfs):
    assert all(len(dfs[0]) == len(dfs[i]) for i in range(1, len(dfs)))
    dfs = deque(dfs) 
    df = dfs.popleft()
    while dfs: 
        df_right = dfs.popleft()
        # Drop shared cols from one of the dataframe, as they are the same in both dfs. 
        drop_cols = list(set(df.columns).intersection(df_right.columns))
        df_right = df_right.drop(columns=drop_cols)
        df = df.merge(df_right, how='left', left_index=True, right_index=True)
    return df 

# OLD IMPLEMENTATION 
# def recursive_index_merge(dfs):
#     drop_right_suffix = '__drop_right_suffix'
#     assert all(len(dfs[0]) == len(dfs[i]) for i in range(1, len(dfs)))
#     dfs = deque(dfs) 
#     df = dfs.popleft()
#     while dfs: 
#         df_right = dfs.popleft()
#         cols = list(set(df.columns).intersection(df_right.columns))
#         df = df.merge(df_right, how='left', left_index=True, right_index=True, suffixes=(None, drop_right_suffix))
#         drop_cols = [col for col in df.columns if col.endswith(drop_right_suffix)]
#         df = df.drop(columns=drop_cols)
#     return df 

def query_attrs(sg, query, attrs):
    qattrs = []
    for a in attrs: 
        if '.' in a: 
            # nested
            v = None 
            for p in a.split('.'): 
                v = getattr(query, p) if v is None else getattr(v, p) 
            qattrs.append(v) 
        else: 
            # non-nested 
            qattrs.append(getattr(query, a)) 
    return sg.query_df(qattrs, pagination_strategy=ShallowStrategy) 

def camel_to_snake(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()

def df_cols_camel_to_snake(df):
    col_map = {c: camel_to_snake(c) for c in df.columns}
    df = df.rename(columns=col_map) 
    return df 

def df_cols_change_prefix(df, prefix_cur, prefix_new):
    col_map = {}
    for c in df.columns: 
        if c.startswith(prefix_cur): 
            col_map[c] = prefix_new + c[len(prefix_cur):]
    df = df.rename(columns=col_map) 
    return df 

async def graphql_execute(
    url, 
    query, 
    variable_values=None, 
    paginate=False, 
    page_size=1000, 
    page_size_variable='page_size', 
    page_offset_variable='page_offset',
    verbose=False, 
    batch_size=3000, 
    poll_secs=2, 
    query_name=None, 
):
    # Batch size must be a multiple of page size 
    assert batch_size % page_size == 0
    variable_values = variable_values or {}
    transport = AIOHTTPTransport(url=url)
    gquery = gql(query)
    if query_name is None: 
        query_name = gquery.to_dict()['definitions'][0]['name']['value'].lower()
    # Using `async with` on the client will start a connection on the transport
    # and provide a `session` variable to execute queries on this connection
    async with Client(transport=transport, fetch_schema_from_transport=True) as session:
        if not paginate:
            res = await session.execute(gquery, variable_values=variable_values)
            records = res[query_name]
            if verbose: 
                print(f"Query returned {len(records)} records with page size {page_size}.")
            return records 
        else: 
            results = []
            finished = False 
            i = 0
            n_pages = int(batch_size / page_size)
            
            async def _request(page_num):
                # Requests the page_num-th page of data 
                res = await session.execute(
                    gquery, variable_values={
                        **variable_values, 
                        page_offset_variable: page_size * page_num, 
                        page_size_variable: page_size
                    }
                )
                records = res[query_name]
                if verbose: 
                    print(f"-- Page {page_num} returned {len(records)} records with page size {page_size}.")
                return records
            
            while not finished: 
                if i != 0: 
                    await asyncio.sleep(poll_secs)
                # Queue a sequence of batch_size / page_size requests, each requesting page_size records 
                """
                Paging example: 
                ---------------
                page_size = 150
                page_start = 1
                page_end = 3 
                0: 0-149
                1: 150-299
                2: 300-449
                3: 450-599
                """
                page_start = i 
                page_end = i + n_pages - 1 
                record_start = page_start * page_size
                record_end = (page_end + 1) * page_size - 1
                if verbose: 
                    print(f"Requesting page range {page_start} - {page_end} / Record Range [{record_start}, {record_end}]")
                futures = [_request(j) for j in range(page_start, page_end + 1)]
                list_records = await asyncio.gather(*futures)
                finished = any(len(records) == 0 for records in list_records)
                results.extend(list_records) 
                i = page_end + 1 
            return list(chain(*results))

def cg_get_market_history(
    cg, 
    token_name_addr_map, 
    ndays=720, 
    include_price=True, 
    include_market_cap=True, 
    include_volume=True,
):
    """Get price, market cap, and volume data for a set of tokens (lookup performed by deployment address on ethereum)
    
    Data is returned in long form.
    """
    cg_coins = cg.get_coins_list(include_platform=True)
    eth_addr_to_cg_id = {c['platforms']['ethereum'].lower(): c['id'] for c in cg_coins if 'ethereum' in c['platforms']}
    name_cg_id_map = {}
    for tname, taddr in token_name_addr_map.items(): 
        if taddr.lower() in eth_addr_to_cg_id: 
            name_cg_id_map[tname] = eth_addr_to_cg_id[taddr]
        else: 
            print(f"Missing data for {tname}")
    keys = list(filter(lambda v: v is not None, [
        'prices' if include_price else None, 
        'market_caps' if include_market_cap else None, 
        'total_volumes' if include_volume else None
    ]))
    
    def get_token_data(token_name, token_id): 
        token_data = cg.get_coin_market_chart_by_id(token_id, 'usd', ndays)
        tdfs = [pd.DataFrame(token_data[k], columns=['timestamp', k]).set_index('timestamp') for k in keys]
        tdf = pd.concat(tdfs, axis=1).reset_index()
        tdf.timestamp = pd.to_datetime(tdf.timestamp, unit="ms")
        tdf['token_name'] = token_name 
        return tdf 
        
    with ThreadPoolExecutor(max_workers=10) as executor: 
        futures = [executor.submit(get_token_data, tname, tid) for tname, tid in name_cg_id_map.items()]
        df = pd.concat([f.result() for f in futures])
        
    # Coingecko includes two records for current date. A snapshot at the beginning of the day and a snapshot 
    # for the most recent timestamp. We remove the most current timestamp snapshot so all of our timestamps 
    # fall exactly on days. 
    df['date'] = pd.to_datetime(df.timestamp.dt.date)
    df['rank'] = df.groupby(['token_name', 'date'])['timestamp'].cumcount()
    df = df.loc[df['rank'] == 0] 
    df = df.drop(columns='rank')
            
    return df 

def df_sort_cols(df, prefix_groups):
    # Ensure there are no columns that begin with two separate prefixes specified by user 
    if any(a.startswith(b) for a, b in product(prefix_groups, prefix_groups) if a != b):
        raise ValueError('Invalid prefix groups')
    
    # Compute sort rankings between groups 
    prefix_groups_sorted = list(sorted(prefix_groups))
    prefix_group_sort_indices = {v: prefix_groups_sorted.index(v) for v in prefix_groups}
    
    # Compute precendence scalar for all cols 
    time_cols = df.select_dtypes(include=['datetime', 'timedelta', 'datetimetz']).columns.values.tolist()
    bool_cols = df.select_dtypes(include=['bool']).columns.values.tolist()
    cat_cols = df.select_dtypes(include=['category']).columns.values.tolist()
    obj_cols = df.select_dtypes(include=['object']).columns.values.tolist()
    numeric_cols = df.select_dtypes(include=['number']).columns.values.tolist()
    obj_cols = [
        c for c in obj_cols 
        if c not in time_cols 
        and c not in cat_cols 
        and c not in numeric_cols
        and c not in bool_cols 
    ] 
    rem_cols = list(set(df.columns).difference(set(time_cols + cat_cols + numeric_cols + obj_cols + bool_cols)))
    precedences = {
        **{c: 0 for c in time_cols}, 
        **{c: 1 for c in bool_cols}, 
        **{c: 2 for c in cat_cols}, 
        **{c: 3 for c in obj_cols}, 
        **{c: 4 for c in rem_cols}, 
        **{c: 5 for c in numeric_cols}, 
    }

    # Perform sort ranking within groups 
    cols_new = []
    for prefix, sort_index in sorted(prefix_group_sort_indices.items(), key=lambda l: l[1]): 
        pcols = list(sorted([c for c in df.columns if c.startswith(prefix)]))
        pcols.sort(key=lambda c: (precedences[c], c))
        cols_new.extend(pcols)
        
    # Retain columns not included in prefix groups 
    rem_cols = [c for c in df.columns if c not in cols_new] 
    cols_new.extend(rem_cols) 
                            
    return df[cols_new]