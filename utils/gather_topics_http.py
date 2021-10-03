import json
from funcy import chunks as chunks
import asyncio
import nest_asyncio
nest_asyncio.apply()
from aiohttp import ClientSession
from web3.providers.base import JSONBaseProvider
import pandas as pd

class http_client():

    def __init__(self,conf):

        #Infura Nodes Setup
        self.conf             = conf
        self.ethNodeAddress   = conf["rpcAddress"]['mainnet']
        self.asynchBatches    = 3
        
    
    def gather_on_chain_topics(self,
                               missingTuples,
                               runCounter=1):

        if runCounter == 50:
            raise NotImplementedError(f"After {runCounter} runs failed to gather all receipts, manual intervention needed.")

        #First run in recursive loop
        if runCounter == 1:
            #Tupples is in the form of :
            #function
            #functionHex
            #intialBlock
            #endingBlock
            #initialBlockHex
            #endingBlockHex
            self.topicList = list()

        #Runs on all missing Blocks
        self.run_batch_iteration(missingTuples=missingTuples)

        #Check for missing TX Hashes
        missingTuples = self.find_missing_tuples()

        #Reruns Recursively in case we have missing transactions
        if len(missingTuples)>0:
            return self.gather_topics(missingTuples=missingTuples,
                                      runCounter=runCounter+1)

        #After the last missing tupples is run, topics list will
        #contail only 1 list of tupples and 1 list of topics (appending undone)
        finalTupples, finalResponse = self.topicList[0]
        finalTupples  =  [tupple             for tupple, response in zip(finalTupples,finalResponse) if "result" in response]
        finalResponse =  [response["result"] for response in finalResponse if "result" in response]
        return [finalTupples, finalResponse]

        
    def run_batch_iteration(self,
                          missingTuples):

        for tuppleList in  chunks(self.asynchBatches,missingTuples):
            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(self.run(tuppleList))
            loop.run_until_complete(future)
            self.topicList.append([tuppleList,future.result()])

    def find_missing_tuples(self):

        missingTuples=list()
        tupplesToSplit=list()#tupples that need splitting
        tupplesToRedo =list()#tupples that need to be redone because of limit on asynch batch calls
        tupplesCovered=list()#tupples that have results (i.e. done correctly)
        responsesCovered=list()#list of responses

        for iteration in self.topicList:
            tupples, responses = iteration
            #First find responses where we have errors
            #1 Type of error could be due to the tupple band being too large (i.e. more than 10k results)
            #or query taking too much time... in these case we need to split after performing calibration
            #https://infura.io/docs/ethereum/json-rpc/eth_getLogs

            tupplesToSplit.extend([tupple for tupple, response in zip(tupples,responses)
                          if "error" in response.keys() and
                                          ("query returned more than 10000 results" in response["error"]["message"] or
                                           "query timeout exceeded" in response["error"]["message"])])

            tupplesToRedo.extend([tupple for tupple, response in zip(tupples,responses)
                          if "error" in response.keys() and
                                          "project ID request rate exceeded" in response["error"]["message"]])

            tupplesCovered.extend([tupple for tupple, response in zip(tupples,responses)\
                                               if "result" in response.keys()\
                                                   and len(response["result"]) > 0 ])
            responsesCovered.extend([response for tupple, response in zip(tupples,responses)\
                                               if "result" in response.keys()\
                                                   and len(response["result"]) > 0 ])

            tupplesUnknownError = [[tupple, response] for tupple, response in zip(tupples,responses)
                          if "error" in response.keys() and  not
                                          ("query returned more than 10000 results" in response["error"]["message"] or
                                           "query timeout exceeded" in response["error"]["message"] or
                                           "project ID request rate exceeded" in response["error"]["message"])]
            if len(tupplesUnknownError)>0:
                print(f"New type of error found in the tupples, manual intervention required: {tupplesUnknownError}!")
                

        #Kills all data except captured in Receipts, in order to increase the speed of the
        #model. Note that without the deletion, the model still works fine
        #but it'll take more time as it'll also iterate on previous splits
        self.topicList=list()
        self.topicList.append([tupplesCovered,responsesCovered])

        #Generates Missing Tupples that need to be gathered in the next run
        #after doing some calibration
        if len(tupplesToSplit)>0:
            tupplesToSplit = self.splitTupples(tupples=tupplesToSplit)

        #Then does the union on split and redo tupples
        missingTuples = list(set(tupplesToSplit).union(set(tupplesToRedo)))

        return missingTuples

    def split_tuples(self,tupples):
        #Splits tupples in half - simple approach
        #less error prone, but slower

        newSplitTupples=list()

        #Splits Tupples in Half
        for tupple in tupples:
            function = tupple[0]
            topic    = tupple[1]
            initial  = tupple[2]
            mid      = (tupple[2]+tupple[3])//2
            end      = tupple[3]

            if mid == initial:
                raise NotImplementedError(f"Weird situation where tupples to split can't be split {tupple}")

            newSplitTupples.extend([(function,
                                     topic,
                                     initial,mid,
                                     hex(initial),hex(mid))])
            newSplitTupples.extend([(function,
                                     topic,
                                     mid+1,end,
                                     hex(mid+1),hex(end))])

        df= pd.DataFrame(newSplitTupples,columns=["function","topic","initialBlock","endingBlock","initialBlockHex","endingBlockHex"])

        tuppleList = list(df.itertuples(index=False,name="eth_getLogs"))

        return tuppleList

    # asynchronous JSON RPC API request
    async def async_make_request(self,session, url ,dataDump):
        async with session.post(url,
                                data=dataDump,
                                headers={'Content-Type': 'application/json'}) as response:
            content = await response.read()

        return json.loads(content)

    async def run(self,tupples):

        tasks = []
        method='eth_getLogs'
        base_provider = JSONBaseProvider()

        async with ClientSession() as session:

            for tupple in tupples:
                params=[{"fromBlock":tupple.initialBlockHex,
                         "toBlock":tupple.endingBlockHex,
                         'topics':[tupple.topic],
                         'address':tupple.address}]


                dataDump = base_provider.encode_rpc_request(method, params)

                task = asyncio.ensure_future(self.async_make_request(session=session,
                                                                     url=self.ethNodeAddress,
                                                                     dataDump=dataDump))
                tasks.append(task)
            responses = await asyncio.gather(*tasks)
        return responses

#%%
if __name__ == '__main__':
    topicBot = httpClient(w3, conf, topicsConf,'kovan')
    topicBot.gatherTopics()
