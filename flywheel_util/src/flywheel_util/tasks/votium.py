import logging 
import concurrent 
import re 
import requests 
from datetime import timedelta 

from web3 import Web3

from subgrounds import Subgrounds
from prefect import task
from prefect.tasks import task_input_hash

import pandas as pd 
import missingno as miss
from pycoingecko import CoinGeckoAPI

from flywheel_util.constants import (
    addresses, 
    url_snapshot, 
    url_subgraphs, 
    url_infura, 
    snapshot_api_max_records_per_request, 
)
from flywheel_util.tasks.general import df_to_sql
from flywheel_util.utils.util import (
    ddf, 
    first_row, 
    cg_get_market_history, 
    df_cols_camel_to_snake, 
    df_cols_change_prefix, 
    df_sort_cols, 
    graphql_execute, 
    query_attrs,  
)
from flywheel_util.utils.w3 import (
    verified_contract
)

cg = CoinGeckoAPI()

sg = Subgrounds()
sg_votium = sg.load_subgraph(url_subgraphs.votium.bribes) 

w3 = Web3(Web3.HTTPProvider(url_infura))


@task(persist_result=True, cache_key_fn=lambda *args: "snapshot_proposals4", cache_expiration=timedelta(days=1))
async def query_snapshot_proposals(): 
    """Get all snapshot proposals corresponding to convex gauge weight votes. 
    
    Within each gauge weight vote, holders of vlCVX collectively determine how convex should distribute it's 
    veCRV in subsequent curve gauge votes. 
    
    - Votes are pulled from the snapshot graphql endpoint. 
    """
    proposal_attrs = ['id', 'title', 'choices', 'start', 'end', 'state', 'votes', 'state']
    proposals = await graphql_execute(
        url_snapshot, 
        '''
        query Proposals {
          proposals(
            first: <first>,
            where: {space: "cvx.eth"},
            orderBy: "created",
            orderDirection: asc
          ) {
            <proposal_attrs>
          }
        }
        '''
        .replace('<proposal_attrs>', '\n'.join(proposal_attrs))
        .replace('<first>', str(snapshot_api_max_records_per_request))
    )
    # Number of rounds here should match number of rounds on llama airforce 
    # https://llama.airforce/#/bribes/rounds/votium/cvx-crv/
    df_proposals = pd.DataFrame(proposals)
    df_proposals.title = df_proposals.title.str.lower()
    df_proposals = df_proposals.loc[~df_proposals.title.str.startswith("(test)") & df_proposals.title.str.contains("gauge weight")]
    df_proposals.start = pd.to_datetime(df_proposals.start, unit='s').dt.date
    df_proposals.end = pd.to_datetime(df_proposals.end, unit='s').dt.date 
    df_proposals = df_proposals.sort_values('start').reset_index(drop=True)
    # On votium, the proposal id is keccak256 hashed. So we need to perform this operation on the data pulled from snapshot 
    df_proposals['id_keccak256'] = df_proposals.id.apply(
        lambda _id: Web3.keccak(text=_id).hex() if not _id.startswith('0x') else Web3.keccak(hexstr=_id).hex()
    )
    # Validate that this query pulls in all data by ensuring that the number of votium voting rounds we get from this query 
    # matches the expected number of voting rounds, which we can compute using simple timedelta logic, the date of the 
    # first vote, and the current date. 
    dmin = pd.Timestamp('2021-09-16') # day of the first votium snapshot 
    assert pd.Timestamp(df_proposals.start.min()) == dmin
    actual_dates = df_proposals.sort_values('start')['start']
    expected_dates = pd.Series([d.date() for d in pd.date_range(pd.Timestamp('2021-09-16'), pd.Timestamp.now(), freq='14D')])
    pd.testing.assert_series_equal(actual_dates.iloc[:len(expected_dates)], expected_dates, check_names=False)
    # Sometimes, snapshot proposals for a week are posted the night before the week begins. This allows some lee-way 
    assert abs(len(actual_dates) - len(expected_dates)) <= 1
    df_proposals = df_proposals.rename(columns={
        'id': 'proposal_id', 
        'title': 'proposal_title', 
        'choices': 'proposal_choices', 
        'start': 'proposal_start', 
        'end': 'proposal_end', 
        'id_keccak256': 'proposal_id_keccak256', 
        'votes': 'vote_count', 
    })
    df_proposals = df_proposals.sort_values('proposal_start').reset_index(drop=True).reset_index()
    df_proposals['proposal_round'] = df_proposals['index'] + 1 
    df_proposals = df_proposals.drop(columns=['index']) 
    return df_proposals 
    
@task(persist_result=True, cache_key_fn=lambda context, pid: f"snapshot_proposal_votes2_{pid}", cache_expiration=timedelta(days=1), tags=['network_request'])
async def blocking_query_snapshot_votes_for_proposal(pid: str): 
    return await graphql_execute(
        url_snapshot, 
        '''
            query Votes($page_size: Int!, $page_offset: Int!, $pid: String!) {
              votes (
                first: $page_size
                skip: $page_offset
                where: { proposal: $pid }
              ) {
                proposal { id }
                id
                voter
                created
                choice
                vp
                vp_state
              }
            }
        ''', 
        paginate=True, 
        page_size=snapshot_api_max_records_per_request,
        # Snapshot keeps changing api specs :( now they only allow 3 concurrent requests to be made to the API. 
        batch_size=snapshot_api_max_records_per_request * 3, 
        variable_values={'pid': pid}, 
        verbose=True, 
    )
    
@task
async def process_snapshot_votes(votes, verbose=True): 
    """Get all votes for all convex gauge weight snapshot proposals. 
        
    - Votes are pulled from the snapshot graphql endpoint. 
    """
    vote_records = []
    for v in votes: 
        for index, amount in v['choice'].items(): 
            r = {
                **v, 
                'choice_index': int(index) - 1, # convert 1 to 0 based indexing for join with other data later 
                'choice_weight': amount, 
                'is_votium': v['voter'].lower() == addresses.contract.votium.voter_proxy
            }
            del r['choice'] 
            vote_records.append(r)
    df_votes = pd.DataFrame(vote_records)
    df_votes['proposal_id'] = df_votes.proposal.apply(lambda v: v['id']) 
    # So choice weights are really weird on snapshot. The best way to normalize is to find all votes 
    # by a voter in a proposal, sum the weights to get a total, then use this to find the fraction of 
    # the users total voting power (vp) allocated to the choices they voted for. 
    df_votes['choice_weight_total'] = df_votes.groupby(['proposal_id', 'voter'])['choice_weight'].transform('sum')
    df_votes['choice_percent'] = df_votes.choice_weight / df_votes.choice_weight_total * 100
    df_votes['choice_vp'] = (df_votes.choice_percent / 100) * df_votes.vp 
    df_votes = df_votes.drop(columns=['proposal', 'choice_weight_total', 'choice_weight']) 
    df_votes = df_votes.rename(columns={'created': 'vote_created', 'vp': 'total_vp', 'id': 'vote_id'})
    return df_votes

@task(persist_result=True, cache_key_fn=lambda *args: "gauge_results6", cache_expiration=timedelta(days=1))
def query_gauge_info(): 
    # Most recent gauge information. We will map the name of all the current gauges to the names 
    # within the snapshot proposal, so we can determine which choices in the proposals correspond 
    # to which gauges. 
    gauge_data = requests.get('https://api.curve.fi/api/getAllGauges').json()
    df_gauges = pd.DataFrame([
        {
          'gauge_name': d['name'].lower(), 
          'gauge_short_name': d['shortName'].lower(), 
          'gauge_address': d['gauge']
        } 
        for _, d in gauge_data['data'].items() 
    ])
    return df_gauges 

@task(persist_result=True, cache_key_fn=lambda *args: 'votium_epoches2', cache_expiration=timedelta(days=1))
def query_votium_epoches(): 
    # Get all votium voting epochs. Once we have validated that this set of epochs matches our set of snapshot proposals, 
    # we need to merge this data with our snapshot proposal data. 
    query_epoches = sg_votium.Query.epoches(first=1000, orderBy="initiatedAt", where={"bribeCount_gt": 0})
    df_epoches = query_attrs(sg, query_epoches, ['id', 'initiatedAt', 'deadline'])
    df_epoches = df_cols_change_prefix(df_epoches, 'epoches', 'epoch')
    df_epoches = df_cols_camel_to_snake(df_epoches) 
    df_epoches['epoch_start_date'] = pd.to_datetime(df_epoches.epoch_initiated_at, unit="s").dt.date
    df_epoches['epoch_end_date'] = pd.to_datetime(df_epoches.epoch_deadline, unit="s").dt.date 
    return df_epoches 

@task(persist_result=True, cache_key_fn=lambda *args: 'votium_bribes2', cache_expiration=timedelta(days=1))
def query_votium_bribes(epoch_ids): 
    # https://github.com/convex-community/convex-subgraph/blob/main/subgraphs/votium/src/mapping.ts
    # Addresses associated with the frax protocol used for votium bribes 
    # TODO: Frax controls some subset of the TVL in it's liquidity pools. Need to be cognizant of this because it leads 
    #       to a rebate the lowers the cost of bribing. 
    # TODO: Frax's vlCVX is not custodied in the investor custodian wallet. 
    token_map = {addresses.token.fxs: 'FXS', addresses.token.frax: 'FRAX'}
    query_bribes = sg_votium.Query.bribes(first=100000, where={
        "epoch_in": epoch_ids, "token_in": [addresses.token.fxs, addresses.token.frax]
    })
    df_bribes = query_attrs(sg, query_bribes, ['id', 'amount', 'token', 'choiceIndex', 'epoch.id'])
    df_bribes = df_cols_change_prefix(df_bribes, "bribes", "bribe")
    df_bribes = df_cols_camel_to_snake(df_bribes)
    df_bribes['bribe_token_name'] = df_bribes.bribe_token.apply(lambda addr: token_map[addr])
    df_bribes['bribe_tx_hash'] = df_bribes.bribe_id.apply(lambda _id: _id.split('-')[0])
    df_bribes.bribe_amount /= 1e18 # both frax and fxs have 18 decimals
    df_bribes = df_bribes.drop(columns=['bribe_id'])
    return df_bribes 

@task(persist_result=True, cache_key_fn=lambda *args: 'bribe_asset_prices2', cache_expiration=timedelta(days=1))
def query_bribe_asset_prices():
    token_addr_map = {'frax': addresses.token.frax, 'fxs': addresses.token.fxs}
    df_prices = cg_get_market_history(cg, token_addr_map, include_volume=False, include_market_cap=False)
    df_prices = (
        df_prices
        .drop(columns='date')
        .pivot(index="timestamp", columns="token_name", values="prices")
        .reset_index().rename(columns={'frax': 'price_frax', 'fxs': 'price_fxs'})
    )
    return df_prices 

@task(persist_result=True, cache_key_fn=lambda *args: "votium_reclaims2", cache_expiration=timedelta(days=1)) 
def query_votium_claims():
    # Query claim events from the votium multi-merkle stash 
    address_votium_multi_merkle_stash = '0x378ba9b73309be80bf4c2c027aad799766a7ed5a'
    contract = verified_contract(address_votium_multi_merkle_stash)
    addrs = {
        '0xb1748c79709f4ba2dd82834b8c82d4a505003f27': "investor custodian", 
        '0x7038c406e7e2c9f81571557190d26704bb39b8f3': "utility contract maker", 
    }
    events_claimed = []
    for account in addrs.keys(): 
        filter_claims = contract.events.Claimed.createFilter(
            fromBlock=13320169, argument_filters={"account": account}
        )
        _events_claimed = filter_claims.get_all_entries()
        print(f"Found {len(_events_claimed)} `Claimed` events on votium multi merkle stash for account {account}") 
        events_claimed.extend(_events_claimed)
    return events_claimed 

@task 
def process_events_claimed(events_claimed):
    df_claims = pd.DataFrame([
        {'token': e.args.token, 'account': e.args.account, 'amount': e.args.amount, "block_number": e.blockNumber} 
        for e in events_claimed
    ])
    df_claims = df_claims.loc[df_claims.token != '0xa693B19d2931d498c5B318dF961919BB4aee87a5'] # UST lol 
    # abi for DAI but works for all these erc20's since we're just calling symbol on the contract. TUSD is a proxy for some reason 
    # so if we try to auto-pull the abi using etherscan we get the wrong one.
    abi = '[{"inputs":[{"internalType":"uint256","name":"chainId_","type":"uint256"}],"payable":false,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"src","type":"address"},{"indexed":true,"internalType":"address","name":"guy","type":"address"},{"indexed":false,"internalType":"uint256","name":"wad","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":true,"inputs":[{"indexed":true,"internalType":"bytes4","name":"sig","type":"bytes4"},{"indexed":true,"internalType":"address","name":"usr","type":"address"},{"indexed":true,"internalType":"bytes32","name":"arg1","type":"bytes32"},{"indexed":true,"internalType":"bytes32","name":"arg2","type":"bytes32"},{"indexed":false,"internalType":"bytes","name":"data","type":"bytes"}],"name":"LogNote","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"src","type":"address"},{"indexed":true,"internalType":"address","name":"dst","type":"address"},{"indexed":false,"internalType":"uint256","name":"wad","type":"uint256"}],"name":"Transfer","type":"event"},{"constant":true,"inputs":[],"name":"DOMAIN_SEPARATOR","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"PERMIT_TYPEHASH","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"}],"name":"allowance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"usr","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"approve","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"usr","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"burn","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"guy","type":"address"}],"name":"deny","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"usr","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"mint","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"src","type":"address"},{"internalType":"address","name":"dst","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"move","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"nonces","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"holder","type":"address"},{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"nonce","type":"uint256"},{"internalType":"uint256","name":"expiry","type":"uint256"},{"internalType":"bool","name":"allowed","type":"bool"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"permit","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"usr","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"pull","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"usr","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"push","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"guy","type":"address"}],"name":"rely","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"dst","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"src","type":"address"},{"internalType":"address","name":"dst","type":"address"},{"internalType":"uint256","name":"wad","type":"uint256"}],"name":"transferFrom","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"version","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"wards","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"}]'
    token_symbol_map = {}
    token_decimal_map = {}
    for a in df_claims.token.unique(): 
        contract = w3.eth.contract(a, abi=abi)
        token_symbol_map[a] = contract.functions.symbol().call()
        token_decimal_map[a] = contract.functions.decimals().call()
    df_claims['token_name'] = df_claims.token.apply(lambda a: token_symbol_map[a])
    df_claims['token_decimals'] = df_claims.token.apply(lambda a: token_decimal_map[a])
    df_claims['amount'] = df_claims.apply(lambda row: row['amount'] / 10** row['token_decimals'], axis=1) 
    # Get the timestamp of a block 
    block_timestamp_map = {bnum: w3.eth.get_block(int(bnum))['timestamp'] for bnum in df_claims.block_number.unique()}    
    df_claims['timestamp'] = df_claims.block_number.apply(lambda block_num: block_timestamp_map[block_num])
    df_claims.timestamp = pd.to_datetime(df_claims.timestamp, unit='s')  
    return df_claims 

@task 
def validate_proposals_vs_epoches(df_proposals, df_epoches): 
    """We pull epoches from the votium subgraph, and snapshot proposals from the snapshot api. 
    
    We need to ensure that we have a one to one mapping between these two entites. 
    Additionally, we remove one erroneous proposal from our set of epoches 
    """
    # Validate that the epoch dates for votium bribes match the proposal data we pulled from snapshot. `
    epoch_dates = df_epoches.epoch_start_date.unique().tolist()
    proposal_dates = df_proposals.proposal_start.unique().tolist()
    d_exclude = pd.Timestamp('2021-11-08').date()
    assert d_exclude in epoch_dates and not d_exclude in proposal_dates
    epoch_dates.remove(d_exclude)
    assert set(epoch_dates) == set(proposal_dates)
    df_epoches = df_epoches.loc[df_epoches.epoch_start_date != d_exclude].reset_index(drop=True)
    return df_epoches

@task 
def process_df_epoches(df): 
    df = (
        df.sort_values('epoch_start_date').reset_index(drop=True).reset_index().rename(columns={'index': 'proposal_round'})
    )
    df.proposal_round += 1
    df = df.drop(columns=['epoch_initiated_at', 'epoch_deadline'])
    df.epoch_start_date = pd.to_datetime(df.epoch_start_date)
    df.epoch_end_date = pd.to_datetime(df.epoch_end_date)
    return df 

@task(cache_key_fn=task_input_hash)
def label_bribes(df_bribes): 
    # Label bribes from known frax associated addresses 
    def get_tx_from(tx_hash): 
        # Determine the "from" address for the transaction that contained the bribe 
        return w3.eth.get_transaction(tx_hash)['from'] 

    # compute mapping of tx_hash to the from address of the transaction 
    tx_hashes = df_bribes.bribe_tx_hash.unique()
    tx_from_map = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {tx_hash: executor.submit(get_tx_from, tx_hash) for tx_hash in tx_hashes}
        for tx_hash, future in futures.items(): 
            tx_from_map[tx_hash] = future.result()

    frax_bribe_addresses = [
        # ('comptroller', '0xb1748c79709f4ba2dd82834b8c82d4a505003f27'),
        # ('cvx locker amo', '0x7038c406e7e2c9f81571557190d26704bb39b8f3'),
        ('investor custodian', '0x5180db0237291A6449DdA9ed33aD90a38787621c'),
        ('frax1.eth', '0x234D953a9404Bf9DbC3b526271d440cD2870bCd2'),
    ]
    address_labels = {addr.lower(): name for name, addr in frax_bribe_addresses}

    df_bribes['bribe_from'] = df_bribes['bribe_tx_hash'].apply(lambda tx_hash: tx_from_map[tx_hash])
    df_bribes['briber_label'] = df_bribes['bribe_from'].apply(lambda a: address_labels.get(a.lower(), 'unknown'))

    label_counts = df_bribes.briber_label.value_counts(dropna=False)
    print("Label count for addresses submitting bribes") 
    print(label_counts)

    return df_bribes 
        
@task 
def join_bribes_choices_epoches_prices(df_bribes, df_choices, df_epoches, df_prices): 
    df = (
        # Aggregate bribes by unique combination of proposal, briber, bribe token, bribe choice index 
        df_bribes.groupby([
            'bribe_epoch_id', 'bribe_choice_index', 'bribe_token', 'bribe_token_name', 'bribe_from', 'briber_label'
        ])
        ['bribe_amount'].sum().reset_index()
    )
    plen = len(df)
    df = (
        df
        # Join in epoch, so we know the time period of voting (for visualizing a time axis) 
        .merge(
            df_epoches[['proposal_round', 'epoch_id', 'epoch_start_date', 'epoch_end_date']].rename(columns={'epoch_id': 'bribe_epoch_id'}), 
            how='left', 
            on='bribe_epoch_id', 
            validate='m:1'
        )
        # Join in choice information, so we know what was bribed for 
        .merge(
            df_choices[['proposal_round', 'choice', 'choice_index']].rename(columns={'choice_index': 'bribe_choice_index', 'choice': 'bribe_choice'}), 
            how='left',
            on=['proposal_round', 'bribe_choice_index'],
            validate='m:1',
        )
    )
    assert plen == len(df)

    # Append price data for FRAX and FXS so we can denominate bribes in $
    # For pricing, we use the price of the assets on the day the bribing period ends. 
    # If the bribing period is still active, then we use the most recent price. 
    df['price_timestamp'] = df.epoch_end_date
    df.loc[df.price_timestamp >= df_prices.timestamp.max(), 'price_timestamp'] = df_prices.timestamp.max()
    df = df.sort_values('price_timestamp').reset_index(drop=True)
    df = pd.merge_asof(df, df_prices, left_on="price_timestamp", right_on="timestamp")
    df['bribe_amount_usd'] = None 
    mask_fxs = df.bribe_token_name == 'FXS'
    mask_frax = df.bribe_token_name == 'FRAX'
    df.loc[mask_fxs, 'bribe_amount_usd'] = df.loc[mask_fxs].bribe_amount * df.loc[mask_fxs].price_fxs
    df.loc[mask_frax, 'bribe_amount_usd'] = df.loc[mask_frax].bribe_amount * df.loc[mask_frax].price_frax
    df = df.drop(columns=['price_timestamp', 'price_frax', 'price_fxs', 'timestamp'])

    df = df_sort_cols(df, ['proposal', 'bribe', 'choice', 'epoch']) 
    
    return df 
    
@task 
def standardize_choices(df_votium_frax, df_gauges): 
    # Goal: For each choice in the snapshot proposal (i.e. a curve pool gauge), we want to pair the 
    #       choice name taken from the snapshot API with the address of the curve pool that choice 
    #       corresponds to. 
            
    # Remove irrelevant gauges  
    remove_choices = [
        "arbitrum-f-4pool", # 4pool never launched 
        "tusd", # Only bribed for in 1 round. Not really sure what to do about this one 
        "ypool", # Single tiny bribe in round 13, not sure what this is.  
    ]
    df_votium_frax = df_votium_frax.loc[~df_votium_frax.bribe_choice.isin(remove_choices)]
    
    # We get the current set of gauges from the curve API. These names are what appear in the snapshot proposal 
    # This will get the most recent set of names but the structure of the names has changed over time. More work 
    # needs to be done to account for historical differences in naming pools 
    canonical_choices = {
        l[0]: l[1] for l in [
            ['frax', 'frax+3crv (0xd632…)'], 
            ['d3pool', 'frax+fei+alusd (0xbaaa…)'], 
            ['fpifrax', 'frax+fpi (0xf861…)'], 
            ['2pool-frax', 'frax+usdc (0xdcef…)'], 
            ['fraxbpsusd', 'susd+fraxbp (0xe3c1…)'], 
            ['fraxbplusd', 'lusd+fraxbp (0x497c…)'], 
            ['fraxbpbusd', 'busd+fraxbp (0x8fdb…)'], 
            ['fraxbpape', 'apeusd+fraxbp (0x04b7…)'], 
            ['fraxbpalusd', 'alusd+fraxbp (0xb30d…)'], 
            ['fraxbpusdd', 'usdd+fraxbp (0x4606…)'], 
            ['fraxbptusd', 'tusd+fraxbp (0x33ba…)'], 
            ['fraxbpgusd', 'gusd+fraxbp (0x4e43…)'], 
        ]
    }
    
    def preprocess_choice(choice):
        choice = choice.lower()
        # Some older voting rounds used crvfrax while newer ones use fraxbp 
        choice = choice.replace('crvfrax', 'fraxbp')
        # Some older voting rounds prefixed factory pools with f- while newer rounds do not
        if choice.startswith("f-"): 
            choice = choice[2:]
        # Some older voting rounds showed addresses in form (0x...ab123)
        # whereas newer rounds use the form (0x...) without trailing values. 
        # Here, we remove trailing values if they exist 
        m = re.search('.*\\u2026([^\)]*)\)$', choice)
        if m: 
            choice = choice.replace(m.group(1), '') 
        if choice in canonical_choices: 
            return canonical_choices[choice] 
        return choice 
    
    df_votium_frax.bribe_choice = df_votium_frax.bribe_choice.apply(preprocess_choice)
    
    # Ensure that the same canonical choice doesn't have two different choice indices in a given round 
    numeric_cols = ['bribe_amount', 'bribe_amount_usd']
    group_cols = [c for c in df_votium_frax.columns if c not in numeric_cols and c not in ['bribe_choice_index']]
    assert all(len(gdf) == 1 for _, gdf in df_votium_frax.groupby(group_cols))
    # Ensure that all choices across all rounds were mapped to a canonical choice 
    assert not set(df_votium_frax.bribe_choice.unique()).difference(set(df_gauges.gauge_short_name.unique()))
    
    df = df_votium_frax.merge(df_gauges, how='left', left_on='bribe_choice', right_on='gauge_short_name')
    assert all(~df.gauge_short_name.isna()) 
    
    return df 

@task
def proposals_to_choices(df_proposals): 
    # Expand the proposals table so that we have one row per combination of a proposal and a choice within that proposal. 
    df_choices = df_proposals.explode('proposal_choices').reset_index().rename(columns={'proposal_choices': 'choice'})
    df_choices['choice_index'] = df_choices.groupby('proposal_id').cumcount()
    df_choices = df_choices[['choice', 'choice_index', 'proposal_round', 'proposal_title', 'proposal_id', 'proposal_id_keccak256']]
    df_choices['choice'] = df_choices.choice.str.lower()
    return df_choices 
