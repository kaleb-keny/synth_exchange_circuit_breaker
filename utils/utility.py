from sqlalchemy import create_engine
import requests
import yaml

def parse_config(path):
    with open(path, 'r') as stream:
        return  yaml.load(stream, Loader=yaml.FullLoader)

def hex_to_int(x):
    return int(x, 16)
                
def get_mysql_connection(conf):
    sqlConf =conf['mysql']
    engine_string = 'mysql+pymysql://{0}:{1}@{2}/{3}'.format(sqlConf["user"],sqlConf["password"],sqlConf["host"],sqlConf["database"])
    engine = create_engine(engine_string)
    con = engine.connect()
    return con

def get_block_number_from_timestamp(conf,targetTimestamp):    
    http = f'https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp={targetTimestamp}&closest=before&apikey={conf["etherscan"]}'
    result = requests.get(http)
    return int(result.json()["result"])
