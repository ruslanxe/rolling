from binance import AsyncClient, BinanceSocketManager
import pandas as pd
import csv
import requests
import asyncio
from datetime import datetime, date, time
from binance.enums import *



async def getAllData(bsm):
    #subscribe to websockets BTCUSDT, BNBBTC, ETHUSDT
    async with bsm.multiplex_socket(['btcusdt@kline_1m', 'bnbbtc@kline_1m', 'ethusdt@kline_1m']) as stream:
        while True:
            res = await stream.recv() #here we store all incoming data
            #formatting incoming data and making dataframe of whole stream data
            data  = res['data']
            data_kline = data['k']
            df1 = pd.DataFrame(data['k'], index=[data_kline['s']])
            df2 = df1[['s', 'o', 'c', 'h', 'l']] #since this line we get only prices data and symbol:'s', open price:'o',close price:'c', highest price:'h', lowest price:'l'

            #store dataframe to csv, basically log file for whole stream data - just in case.
            df2.to_csv('sample2.csv', mode='a', header=True)
            #reading from file could be replace with df2=, in case we don't need raw data from streams anymore
            df3 = pd.read_csv('sample2.csv', sep=',', delim_whitespace=False, engine='python')
            df3 = df3.loc[df3['s'] == 'BNBBTC']
            df4 = df3.loc[df3['s'] == 'BTCUSDT']
            df5 = df3.loc[df3['s'] == 'ETHUSDT']
            #after sorting tickers data to different dataframes we cut non-numeric data (symbol column) since we don't need it in this case
            df3= df3.drop('s', 1)
            cols = ['o', 'c', 'h', 'l']
            df3[cols] = df3[cols].apply(pd.to_numeric, errors='coerce', axis=1) #converting values of columns to float
            df3 = df3.rolling(10).mean() #rolling mean for BNBBTC's all prices values
            df3.to_csv('BNBBTC_roll.csv', mode='w', header=True)#store df consisted of rolling mean values to logfile
            df4 = df4.drop('s', 1)
            df4[cols] = df4[cols].apply(pd.to_numeric, errors='coerce', axis=1)
            df4 = df4.rolling(10).mean()

            df4.to_csv('BTCUSDT_roll.csv', mode='w', header=True)
            df5 = df5.drop('s', 1)
            df5[cols] = df5[cols].apply(pd.to_numeric, errors='coerce', axis=1)
            df5 = df5.rolling(10).mean()

            df5.to_csv('ETHUSDT_roll.csv', mode='w', header=True)
#NOTE: For rolling mean window = 10, script's need time to gather enough data for calculating rolling mean
#NOTE2: DO NOT EDIT sample2.scv file
#NOTE3: df exacutive block could be refactored for versality purpose (LINE17-LINE72)

async def main():
    client = await AsyncClient.create()
    bsm = BinanceSocketManager(client)
    await asyncio.gather(getAllData(bsm))
if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())