from prefect import flow, task

@task(persist_result=True, cache_key_fn=lambda *args: "one")
async def one(): 
    return 5

@task(persist_result=True, cache_key_fn=lambda *args: "two")
async def two(): 
    return 2

@flow(cache_result_in_memory=True)
async def flow_test():
    a = await one()
    b = await two()
    return a + b 
