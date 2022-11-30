import re 
from typing import List 

import pandas as pd 
from subgrounds.pagination import ShallowStrategy
from IPython.display import HTML, display


def ddf(df):
    display(HTML(df.to_html()))
    
    
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