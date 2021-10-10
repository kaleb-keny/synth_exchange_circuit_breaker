from sqlalchemy import create_engine
from utils.utility import get_mysql_connection
import pandas as pd

class db_manager():
    def __init__(self, conf):
        
        self.conf           = conf
        self.dbName         = conf["mysql"]["database"]
        
    def initialize_db(self):
                        
        self.create_db()
        self.generate_missing_tables()
                        
        
    def create_db(self):
        sql_conf = self.conf.get('mysql')

        user     = sql_conf["user"]
        password = sql_conf["password"]
        host     = sql_conf["host"]
        db       = sql_conf["database"]

        #Check if Database is there, deletes it
        try:
            engine_string = f'mysql+pymysql://{user}:{password}@{host}'
            engine = create_engine(engine_string)
            drop_db_sql = f'DROP DATABASE IF EXISTS {db};'
        except Exception as e:
            print(e)
            engine_string = f"mysql+pymysql://{user}:{password}@{host}/{db}"
            engine = create_engine(engine_string)

        with engine.connect() as con:
            con.execute(drop_db_sql)
            con.execute(f"CREATE DATABASE {db};")
            con.execute(f"USE {db};")


    def generate_missing_tables(self):

        with get_mysql_connection(self.conf) as con:

            availableTablesList = pd.read_sql("show tables;",con).iloc[:,0].to_list()

            if not "answer_update" in availableTablesList:
                sql=\
                '''
                CREATE table answer_update
                (
                transactionHash CHAR(66),
                address CHAR(42),
                logIndex INT unsigned,
                blockNumber INT UNSIGNED,
                transactionIndex INT UNSIGNED,
                roundId DECIMAL(65,0),
                timestamp INT(11) UNSIGNED,
                rate DECIMAL(65,0),
                CONSTRAINT pk_rates PRIMARY KEY (transactionHash,logIndex),
                INDEX (blockNumber,transactionIndex,address));
                '''
                con.execute(sql)

            if not "oracle" in availableTablesList:
                sql=\
                '''
                CREATE table oracle
                (                
                timestamp INT unsigned,
                rate DECIMAL(65,0),
                ticker CHAR(12),
                CONSTRAINT bin PRIMARY KEY (ticker,timestamp),
                INDEX (ticker,timestamp));
                '''
                con.execute(sql)

    def import_all(self,tbList=['oracle_eth','oracle_btc']):

        for tbName in tbList:
            df = pd.read_csv(r"input\{}.csv".format(tbName))
            df.drop(columns=[df.columns[0]],inplace=True)
            df["ticker"] = tbName[-3:]
            with get_mysql_connection(self.conf) as con:
                con.execute("DROP TABLE IF EXISTS temp;")
                df.to_sql("temp",index=False,con=con)
                con.execute("INSERT IGNORE INTO oracle (select * from temp);")
                con.execute("DROP TABLE IF EXISTS temp;")
            print(f"Table {tbName} has been imported into sql server.")
        

#%%
if __name__=='__main__':
    db = db_manager(conf)        
    db.initialize_db()