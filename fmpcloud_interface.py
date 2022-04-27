import aiohttp
import asyncio
from datetime import date
from api_info import api_key
import time
import pandas as pd
import json
import os

CURRENT_DATE = time.strftime("%Y-%m-%d")


def preprocess_fmpcloud_stock_data(resp_text):    
    if resp_text is None or "historical" not in json.loads(resp_text):
        return None
        
    stock_df = pd.DataFrame.from_records(json.loads(resp_text)['historical'])
    features = ['date', 'open', 'high', 'low', 'close', 'volume']
    # check if features in stock_df
    if not all(feature in stock_df.columns for feature in features):
        return None
    return stock_df[features][::-1]


async def _get_response(session, url, ticker, api_type, write_to_file):
    async with session.get(url) as resp:        
        resp_text = await resp.text()
        if "Error Message" in resp_text or "Too Many Requests" in resp_text:
            await asyncio.sleep(.2)
            return await _get_response(session,url,ticker,api_type)
        
        if api_type == 'daily':            
            stock_df = preprocess_fmpcloud_stock_data(resp_text)
            if stock_df is not None and write_to_file:
                file_name = f'./data/{api_type}/{ticker}_{CURRENT_DATE}.csv'
                stock_df.to_csv(file_name, index=False)

            if not write_to_file:
                return stock_df
        else:
            if write_to_file:
                file_name = f'./data/{api_type}/{ticker}_{CURRENT_DATE}.csv'
                with open(file_name, 'w', newline='') as file:
                    file.write(resp_text)

        return True
            

async def __schedule_fetching(ticker_list, url_list, api_type, write_to_file):        
    async with aiohttp.ClientSession() as session:
        tasks = []
        for ticker, url in zip(ticker_list, url_list):
            tasks.append(asyncio.ensure_future(_get_response(session, url, ticker, api_type, write_to_file)))         
        
        return await asyncio.gather(*tasks)


def download_daily_data(ticker_list, write_to_file=True):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    url_list = [f"https://fmpcloud.io/api/v3/historical-price-full/{ticker}?apikey={api_key}" for ticker in ticker_list]
    return asyncio.run(__schedule_fetching(ticker_list, url_list, 'daily', write_to_file))


if __name__ == "__main__":
    ticker_df = pd.read_csv("data/tickers/all_tradable_symbols.csv", encoding='latin1')
    # Only get symbols where exchangeShortName is either NYSE or NASDAQ
    ticker_list = ticker_df[ticker_df['exchangeShortName'].isin(['NYSE', 'NASDAQ'])].symbol.tolist()    
    ticker_set = set(ticker_list)
    # Get all files in data/daily
    file_list = os.listdir('./data/daily')
    # Get all tickers in file_list
    ticker_list = [file.split('_')[0] for file in file_list if file.endswith('.csv')]
    # Get all tickers in ticker_set that are not in ticker_list
    ticker_list = list(ticker_set - set(ticker_list))
    download_daily_data(ticker_list)


    


    
    