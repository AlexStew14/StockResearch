import aiohttp
import asyncio
from datetime import date
from api_info import api_key
import time

# async def pull_daily_change_and_volume_csv(tickers):
#     async with aiohttp.ClientSession() as session:
#         for ticker in tickers:
#             url = f"https://fmpcloud.io/api/v3/historical-price-full/{ticker}?datatype=csv&apikey={api_key}"

#             async with session.get(url) as resp:
#                 resp_text = await resp.text()
#                 with open(f'./data/daily/{ticker}_daily_{date.today().strftime("%d_%m_%y")}.csv', 'w', newline='') as file:
#                     file.write(resp_text)


async def _get_quarterly_ratios(session, url):
    async with session.get(url) as resp:
        return await resp.text()
        
async def _schedule_quarterly_ratios(tickers):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for ticker in tickers:
            url = f"https://fmpcloud.io/api/v3/ratios/{ticker}?datatype=csv&period=quarter&apikey={api_key}"
            tasks.append(asyncio.ensure_future(_get_quarterly_ratios(session, url)))

        all_ratios = await asyncio.gather(*tasks)
        for ticker, ratios in zip(tickers, all_ratios):
            with open(f'./data/ratios/{ticker}_quarterly_ratios_{date.today().strftime("%d_%m_%y")}.csv', 'w', newline='') as file:
                file.write(ratios)

def pull_quarterly_financial_ratios(tickers):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    batch_size = 15
    if len(tickers) > batch_size:
        num_jobs = len(tickers) // batch_size
        for i in range(num_jobs):
            tickers_batch = tickers[i*batch_size:(i*batch_size)+batch_size]
            asyncio.run(_schedule_quarterly_ratios(tickers_batch))
            print(f'Got batch: {i}/{num_jobs}, now sleeping.')
            time.sleep(3)

    
    