from utils.utility import get_mysql_connection, hex_to_int, get_block_number_from_timestamp
import pandas as pd
import itertools
from utils.gather_topics_http import http_client
from collections import namedtuple

class gather_link_prices(http_client):
    
    def __init__(self,conf,link):

        self.tuple = namedtuple('topic', ['function',
                                          'initialBlock',
                                          'endingBlock',
                                          'initialBlockHex',
                                          'endingBlockHex',
                                          'topic',
                                          'address'])
        self.linkContracts = link

        super().__init__(conf)

    def run_topics_gathering(self,startTimestamp, endingTimestamp):
        
        missingTuples = self.get_missing_tuples(startTimestamp,endingTimestamp)        
        
        tuples, result = self.gather_on_chain_topics(missingTuples=missingTuples)
        
        df = pd.DataFrame(list(itertools.chain(*result)))
                        
        #Dump Answers to Server 
        self.process_answer_update(df)
            
    
    def get_missing_tuples(self,startTimestamp,endingTimestamp):

        initialBlock = get_block_number_from_timestamp(self.conf,startTimestamp)
        endingBlock  = get_block_number_from_timestamp(self.conf,endingTimestamp)
        
        missingTuples = list()
        #function
        #functionHex
        #intialBlock
        #endingBlock
        #initialBlockHex
        #endingBlockHex
        
        for ticker, address in self.linkContracts.items():
                    
            missingTuples.append(self.tuple(function='answer',
                                            topic='0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f',
                                            initialBlock=initialBlock,
                                            endingBlock=endingBlock,
                                            initialBlockHex=hex(initialBlock),
                                            endingBlockHex=hex(endingBlock),
                                            address=address
                                            ))
        
        df = pd.DataFrame(missingTuples)

        return list(df.itertuples())

    def process_answer_update(self,df):
        
        #Gets Schema and Contract Addresses (relevant to the topic)
        schema       = self.get_schema('answer_update')

        if len(df)>0:
            #Applies Map
            df[["logIndex","blockNumber","transactionIndex"]] = df[["logIndex","blockNumber","transactionIndex"]].applymap(hex_to_int)

            #Gets the data (the unique part)
            df["roundId"]       = df.topics.str[2].apply(hex_to_int)
            df["rate"]          = df.topics.str[1].apply(hex_to_int).astype(str)
            df["timestamp"]     = df.data.apply(hex_to_int)

            #Schema adjustment
            df = df[schema].copy()

            #Save it
            self.push_answers_to_server(df=df, tbName='answer_update')


    def get_schema(self,tbName):

        with get_mysql_connection(self.conf) as con:
            #Get Schema
            sql=\
            f'''
            SELECT *
            from
                {tbName}
            limit 0
            '''
            schema = pd.read_sql(sql,con).columns.to_list()
            
            return schema 

    def push_answers_to_server(self,df,tbName):
        #Save the SQL
        with get_mysql_connection(self.conf) as con:
            con.execute(f"drop table if exists temp_{tbName};")
            con.execute(f"CREATE TABLE temp_{tbName} like {tbName}")
            df.to_sql(f'temp_{tbName}',con,if_exists='append',index=False)
            con.execute(f"insert ignore into {tbName} (select * from temp_{tbName});")
            con.execute(f"DROP TABLE temp_{tbName};")

#%%Test
if __name__=='__main__':
    self=gather_topics(conf)
    # topicsGatheringClass.runTopicGathering()