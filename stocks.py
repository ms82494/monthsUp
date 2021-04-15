#!/usr/bin/env python3
import os
import time
from datetime import datetime
import csv
import asyncio
import aiohttp
from urllib.parse import urlencode
from logconfig import *
import pandas as pd 

logger = logging.getLogger("stocks")
TIINGO_TOKEN = os.environ['TIINGO_TOKEN']
BASEURL = 'https://api.tiingo.com/tiingo/daily/'
MAX_SIM_DOWNLOADS = 6
LOGPATH = "./logs/"
TICKERFILE = "./stocks.csv"
STOCKPATH = "./stocks/"
URL_PARAMS = urlencode({
    'token': TIINGO_TOKEN,
    'startDate': '2011-02-28',
    'resampleFreq': 'monthly',
    'format': 'csv',
    'columns': 'date,open,high,low,close,volume,adjOpen,adjHigh,adjLow,adjClose,adjVolume'})


async def download(ticker, session, semaphore, chunk_size=1<<15):
    async with semaphore: # limit number of concurrent downloads
        filename = STOCKPATH + ticker + '.csv'
        url = BASEURL + ticker + '/prices?' + URL_PARAMS
        start = time.perf_counter() # starting download
        response = await session.get(url)
        with open(filename, 'wb') as file:
            while True: # save file
                chunk = await response.content.read(chunk_size)
                if not chunk:
                    break
                file.write(chunk)
        end = time.perf_counter() # download complete
        elapsed = end - start
        size = int(response.headers['Content-Length'])
        speed = size/(1000*elapsed)
    return (ticker, response.status, size, speed, elapsed)


def get_tickers():
    df = pd.read_csv(TICKERFILE)
    return list(df.symbol.values)


def write_log(results):
    datestr = datetime.now().strftime("%Y-%m-%d")
    filename = os.path.join(LOGPATH, f'log_{datestr}.csv')
    with open(filename, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        headers = ['symbol', 'status (HTTP code)', 'size (kB)', 'speed (kB/s)',
         'time']
        writer.writerow(headers)
        for result in results:
            writer.writerow(result)
    return filename


async def dl_histories():
    tickers = get_tickers()
    start_time = time.time()
    loop = asyncio.get_running_loop()
    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(MAX_SIM_DOWNLOADS)
        results = await asyncio.gather(*[download(ticker, session, semaphore) for ticker in tickers])
    resultsfile = write_log(results)
    msg = ( f'Attempted {len(results)} stock history downloads.\n'
            f'Elapsed time: {time.time() - start_time:.2f}s\n'
            f'Results written to "{resultsfile}".')
    logger.info(msg)


asyncio.run(dl_histories()) 
