from prefect import flow, task 
import pandas as pd 
from utils import graphql_execute

URL_SNAPSHOT = 'https://hub.snapshot.org/graphql'
VOTIUM_VOTER = '0xde1E6A7ED0ad3F61D531a8a78E83CcDdbd6E0c49'.lower()


@task(persist_result=True, cache_key_fn=lambda *args: "snapshot_votes5")
async def query_snapshot_votes(proposal_ids, verbose=True): 
    """Get all votes for all convex gauge weight snapshot proposals. 
        
    - Votes are pulled from the snapshot graphql endpoint. 
    """
    votes = await graphql_execute(
        URL_SNAPSHOT, 
        '''
            query Votes($page_size: Int!, $page_offset: Int!, $proposal_ids: [String]!) {
              votes (
                first: $page_size
                skip: $page_offset
                where: { proposal_in: $proposal_ids }
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
        page_size=1000,
        variable_values={'proposal_ids': proposal_ids}, 
        verbose=True, 
    )
    vote_records = []
    for v in votes: 
        for index, amount in v['choice'].items(): 
            # TODO: data validation step / what does vp_state on this entity mean? Sometimes it's pending but still seems to be counted in votes. 
            # assert v['vp_state'] == 'final'
            r = {
                **v, 
                'choice_index': int(index) - 1, # convert 1 to 0 based indexing for join with other data later 
                'choice_weight': amount, 
                'is_votium': v['voter'].lower() == VOTIUM_VOTER
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
    df_votes = df_votes.drop(columns=['proposal', 'vp_state', 'choice_weight_total', 'choice_weight']) 
    df_votes = df_votes.rename(columns={'created': 'vote_created', 'vp': 'total_vp', 'id': 'vote_id'})
    return df_votes


@flow 
def temp(): 
    return query_snapshot_votes()