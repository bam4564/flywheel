import asyncio 
from itertools import chain 

from prefect import flow 

from flywheel_util.utils.util import ddf 
from flywheel_util.tasks.votium import (
    query_snapshot_proposals, 
    blocking_query_snapshot_votes_for_proposal, 
    process_snapshot_votes, 
    query_gauge_info, 
    query_votium_epoches, 
    query_votium_bribes, 
    query_bribe_asset_prices, 
    query_votium_claims, 
    process_events_claimed, 
    validate_proposals_vs_epoches, 
    process_df_epoches, 
    label_bribes, 
    join_bribes_choices_epoches_prices, 
    standardize_choices, 
    proposals_to_choices, 
)

@flow(cache_result_in_memory=False)
async def flow_votium_votes(): 
    # COINGECKO DATA 
    # ----------------------------------------------------------------
    df_prices = query_bribe_asset_prices.submit()

    # CURVE DATA 
    # ----------------------------------------------------------------
    df_gauge_info = query_gauge_info.submit()
    
    # SNAPSHOT DATA 
    # ----------------------------------------------------------------
    df_proposals = await query_snapshot_proposals()
    df_choices = proposals_to_choices(df_proposals)
    print(f"Number of votium snapshot proposals: {len(df_proposals)}")
    proposal_ids = df_proposals.proposal_id.unique().tolist()
    # Query for data for each proposal independently. The concurrency here is limited 
    # due to constraints of the snapshot api 
    # results = [blocking_query_snapshot_votes_for_proposal(pid) for pid in proposal_ids]
    results = [blocking_query_snapshot_votes_for_proposal(pid) for pid in proposal_ids]
    results = await asyncio.gather(*results)
    # Map the proposal id to the count of votes in that proposal 
    for i in range(len(results)): 
        pid = proposal_ids[i]
        # pending votes should not count yet 
        results[i] = [r for r in results[i] if r['vp_state'] == 'final']
        computed_vote_count = len(results[i])
        actual_vote_count = df_proposals.loc[df_proposals.proposal_id == pid].vote_count.values[0]
        assert actual_vote_count == computed_vote_count, "Actual and computed vote counts did not match" 
    
    print("Here is the count of unique votes per each convex gauge weight snapshot proposal") 
    ddf(df_proposals[['proposal_round', 'vote_count', 'proposal_id']].sort_values('proposal_round').reset_index(drop=True))

    # 
    votes = list(chain(*results)) 
    df_votes = await process_snapshot_votes(votes) # convex gauge votes 
    
    # Join votes with the choice metadata from the snapshot proposal 
    df_votes = df_votes.merge(df_choices, how='left', on=['proposal_id', 'choice_index'], validate="m:1")
    assert not df_votes.proposal_title.isna().any() # ensures each vote was matched with a proposal 
        
    # VOTIUM DATA 
    # ----------------------------------------------------------------
    df_epoches = query_votium_epoches() 
    df_epoches = validate_proposals_vs_epoches(df_proposals, df_epoches)
    df_epoches = process_df_epoches(df_epoches) # must happen after validation to remove erroneous proposal 
    epoch_ids = df_epoches.epoch_id.unique().tolist()
    df_bribes = query_votium_bribes(epoch_ids)
    df_bribes = label_bribes(df_bribes)
    
    events_claimed = query_votium_claims()
    df_claims = process_events_claimed(events_claimed) 

    # Data Joins / Processing 
    # ----------------------------------------------------------------
    # Takes each bribe in fxs or frax and joins the name of the choice voted for, voting round, and prices for reward tokens 
    df_votium_frax = join_bribes_choices_epoches_prices(df_bribes, df_choices, df_epoches, df_prices)
    # Ensures that historical choices map to choices in most recent proposal 
    df_votium_frax = standardize_choices(df_votium_frax, df_gauge_info) 
    
    return df_proposals, df_bribes, df_choices, df_votes, df_epoches, df_prices.result(), df_votium_frax, df_gauge_info.result(), df_claims
