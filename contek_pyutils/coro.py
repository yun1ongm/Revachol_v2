import asyncio


async def exec_coroutines(coroutines):
    fs = [asyncio.create_task(t) for t in coroutines]
    return [await fut for fut in fs]
