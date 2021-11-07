import aiohttp
import asyncio
from datetime import date
from api_info import api_key
import time


class FMPCloudInterface:
    def __init__(self, tickers) -> None:
        self.__tickers = tickers
        self.__BATCH_SIZE = 15
        self.__SLEEP_DURATION = 3
        self.__NUM_JOBS = len(self.__tickers) // self.__BATCH_SIZE
        self.__DATE_STR = date.today().strftime("%d_%m_%y")

    async def __fetch_data(self, session, url):
        async with session.get(url) as resp:
            return await resp.text()


    async def __schedule_fetching(self, tickers_batch, api_type):        
        async with aiohttp.ClientSession() as session:
            tasks = []
            for ticker in tickers_batch:
                
                if api_type == 'daily':
                    url = f"https://fmpcloud.io/api/v3/historical-price-full/{ticker}?datatype=csv&apikey={api_key}"
                elif api_type == 'ratios':
                    url = f"https://fmpcloud.io/api/v3/ratios/{ticker}?datatype=csv&period=quarter&apikey={api_key}"
                else:
                    url = f"https://fmpcloud.io/api/v3/historical-price-full/{ticker}?datatype=csv&apikey={api_key}"

                tasks.append(asyncio.ensure_future(self.__fetch_data(session, url)))
            
            results = await asyncio.gather(*tasks)
            for ticker, result in zip(tickers_batch, results):
                file_name = f'./data/{api_type}/{ticker}_{self.__DATE_STR}.csv'
                with open(file_name, 'w', newline='') as file:
                    file.write(result)


    def pull_data(self, api_type='daily', verbose=True):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        for job_idx in range(self.__NUM_JOBS):
            offset = job_idx * self.__BATCH_SIZE
            tickers_batch = self.__tickers[offset:offset + self.__BATCH_SIZE]
            asyncio.run(self.__schedule_fetching(tickers_batch, api_type))
            if verbose:
                print(f"job: {job_idx}/{self.__NUM_JOBS}")
            
            time.sleep(self.__SLEEP_DURATION)        


    
    