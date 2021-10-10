from binance.client import Client as binanceClient
from utils.utility import get_mysql_connection
import numpy as np
import pandas as pd
from collections import namedtuple

class binance_api():

    def __init__(self,conf):

        binance               = conf["binance"]
        self.binanceClient    = binanceClient(binance["api"], binance["secret"])
        self.conf             = conf
        self.timestampTupple  = namedtuple("timestampRange",
                                           ['startTime','endTime'])
            
    def process_tickers(self,startTimestamp,endTimestamp,tickers=['eth','btc']):
        #Get binance pairs
        for ticker in tickers:
            df = self.get_price_data(ticker, startTimestamp, endTimestamp)
            df["ticker"] = ticker
            self.push_binance_to_server(df)
    
    def push_binance_to_server(self,df):
        with get_mysql_connection(self.conf) as con:
            con.execute("drop table if exists temp_binance;")
            con.execute("CREATE TABLE temp_binance like oracle")
            df.to_sql('temp_binance',con,if_exists='append',index=False)
            con.execute("insert ignore into oracle (select * from temp_binance);")
            con.execute("DROP TABLE temp_binance;")
            
    def get_price_data(self,ticker,startTimestamp,endTimestamp):

        ticker = ticker.upper()
        
        #startPeriod in timestamp in milli seconds
        columns = ['openTime',
                   'open','high','low','close',
                   'volume',
                   'closeTime',
                   'QuoteAssetVolume',
                   'tradeCount','buyVolBase','buyvolTerm','ignore']
                        
        pair = ticker + 'USDT'

        timestampList=list()

        for start in range(startTimestamp,endTimestamp+1,60*500):
            end= min(start+60*500,endTimestamp)
            timestampList.append(self.timestampTupple(start+1,end))
        
        for timestampRange in timestampList:
            
            data = self.binanceClient.get_historical_klines(symbol=pair, 
                                                            interval=binanceClient.KLINE_INTERVAL_1MINUTE, 
                                                            start_str=int(timestampRange.startTime*1000),
                                                            end_str=int(timestampRange.endTime*1000))
            
            if len(data)>0:
            
                if not 'df' in vars():
                    df   = pd.DataFrame(np.array(data).T,columns).T
                else:
                    df = pd.concat([df,pd.DataFrame(np.array(data).T,columns).T],axis=0)
                
        df[["openTime","volume","open"]] = df[["openTime","volume","open"]].applymap(float)
        df["timestamp"]  = df["openTime"].divide(1000).astype(int)
        df["rate"] = df["open"]            

        df.reset_index(inplace=True,drop=False)
        df = df[["timestamp","rate"] ]
        return df
            
#%%    
if __name__ == '__main__':
    self = binance_api(conf)
    # df = oracle.gatherData('sTESLA',1610924400,1611010800)