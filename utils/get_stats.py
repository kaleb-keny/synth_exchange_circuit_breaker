from utils.utility import get_mysql_connection
from tabulate import tabulate
import seaborn as sns
import pandas as pd
import numpy as np

class stats():
    
    def __init__(self,conf,link):
        self.conf          = conf
        self.linkContracts = link
        self.timespanList  = [10,20,40,60] #in minutes
    
    
    def launch_stats(self,ticker,startTimestamp,endTimestamp):
        #Calculate stats, importing relevant data
        df = self.compute_stats(ticker, startTimestamp, endTimestamp)
        #visualize them
        self.visualize_results(df)
        #print out results
        self.print_stats(df)
        
    def import_data(self,ticker,startTimestamp,endTimestamp):
        
        #get oracle data
        sql = f'''select 
                        timestamp, 
                        rate as offChainPrice 
                    from 
                        oracle 
                    where 
                        ticker = '{ticker}' 
                    and 
                        timestamp between {startTimestamp} and {endTimestamp} 
                    order by 
                        timestamp asc;'''
        with get_mysql_connection(self.conf) as con:
            oracleDF = pd.read_sql(sql,con)
        
        #get link data
        address = self.linkContracts['s'+ticker.upper()]
        sql = f'''select 
                    timestamp, 
                    rate/1e8 as onChainPrice 
                  from 
                      answer_update 
                  where 
                      address = '{address}' 
                  and 
                      timestamp between {startTimestamp} and {endTimestamp} 
                  order by 
                      timestamp asc;'''

        with get_mysql_connection(self.conf) as con:
            linkDF = pd.read_sql(sql,con)
                    
        return oracleDF, linkDF
    
    def get_returns(self,df):
        
        #compute returns
        df[["offChainPriceLog"]] = df[["offChainPrice"]].apply(np.log)        
        df["offChainReturns"] = df["offChainPriceLog"].diff(1)
                
        #drop na's
        df.dropna(inplace=True)

        return df
    
    def compute_stats(self,ticker,startTimestamp,endTimestamp):

        #get data
        oracleDF, linkDF = self.import_data(ticker=ticker,
                                            startTimestamp=startTimestamp, 
                                            endTimestamp=endTimestamp)
        
        #cleanse it
        oracleDF = self.get_returns(oracleDF)

        
        endStamp = oracleDF.timestamp.iloc[-1]
        
        #iterate on timespans
        for timespan in self.timespanList:
            
            startStamp = oracleDF.timestamp.iloc[0]
            
            #iterate on all timestamps
            while startStamp < endStamp:
            
                #get a copy of dataframe and compute stats
                answerCount     = linkDF.query(f"timestamp >= {startStamp } and timestamp < {startStamp+timespan*60}").shape[0]
                vol             = oracleDF.query(f"timestamp >= {startStamp } and timestamp < {startStamp+timespan*60}")["offChainReturns"].apply(np.square).sum()*1e4
                startStamp      = startStamp +timespan*60
                        
                #save results
                result = {'answerCount':answerCount,
                          'vol':vol,
                          'timespan':timespan,
                          'startStamp':startStamp,
                          'endStamp':startStamp+timespan*60}
            
                if not 'resultDF' in vars():
                    resultDF   = pd.DataFrame(result,index=[0])
                else:
                    resultDF = pd.concat([resultDF,pd.DataFrame(result,index=[0])])
            
        return resultDF    

    def print_stats(self,df):
        
        #print average number of answers as a function of volatility
        volLevels = ['avg','elevated','extreme']

        q1 = df.groupby(by=['timespan'])["vol"].quantile(0.50)
        q2 = df.groupby(by=['timespan'])["vol"].quantile(0.75)
        q3 = df.groupby(by=['timespan'])["vol"].quantile(0.95)
        
        volDF = pd.concat([q1,q2,q3],axis=1)
        volDF.columns = volLevels
        
        
        for volLevel in volLevels:
            for timespan in self.timespanList:
                #grab the vol
                vol = volDF.loc[timespan,volLevel]
                #find average number of answers above a certain vol threshold
                answerCount = df.query(f'timespan=={timespan} and vol>{vol}')["answerCount"].mean()
                resultDict = {'timespan':timespan,
                              'vol':volLevel,
                              'number of answers':answerCount}
                
                if not 'resultDF' in vars():
                    resultDF   = pd.DataFrame(resultDict,index=[0])
                else:
                    resultDF = pd.concat([resultDF,pd.DataFrame(resultDict,index=[0])])
                    
        #print out the output
        resultDF = pd.pivot_table(data=resultDF,
                                  values='number of answers',
                                  columns='vol',
                                  index='timespan',
                                  aggfunc=max)
        
        resultDF = resultDF[["avg","elevated","extreme"]]
        
        print(tabulate(resultDF, headers = 'keys', tablefmt = 'psql'))
                
                    
    def visualize_results(self,df):
        
        #bin the data into n categories
        df["bin"] = pd.qcut(df["vol"],
                            q=50)
        df["binnedVol"] = df["bin"].apply(lambda x: x.right)

        #get average number of answers per bin per timespan
        df = df.groupby(by=['timespan','binnedVol'])['answerCount'].mean()
        df.fillna(0,inplace=True)
        df = df.reset_index()
        
        df.rename(columns={'timespan':'timespan in minutes',
                           'answerCount':'average # of price pushes',
                           'binnedVol':'vol'},inplace=True)

        plot = sns.relplot(data=df,
                           x="vol",
                           y="average # of price pushes",
                           col='timespan in minutes',
                           palette="deep",
                           hue='timespan in minutes',
                           kind='scatter',
                           col_wrap=2,
                           legend=False)
        
        plot.savefig(r'output/plot.png', dpi=400)
                    
    
#%%
if __name__ == '__main__':
    self = stats(conf, link)
        
    