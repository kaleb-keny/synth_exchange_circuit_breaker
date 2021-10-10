import argparse
from utils.gather_oracle_data import binance_api
from argparse import RawTextHelpFormatter
from utils.get_stats import stats
from utils.db_manager import db_manager
from utils.gather_on_chain_data import gather_link_prices
from utils.utility import parse_config

conf       = parse_config(r"config/conf.yaml")
link       = parse_config(r"config/link.yaml")

#%%Arg Parse
if __name__ == '__main__':

    description = \
    '''
    
    - init: for creating the database
        python main.py -r init


    - import: for importing hf 1sec data
            python main.py -r import 

    - data: for updating the data from binance (1min intervals)
            python main.py -r data --t 1618781516 1618781616

    - compute stats
            python main.py -r compute --t 1618781516 1618781716
            
    '''
    
    parser = argparse.ArgumentParser(description=description,formatter_class=RawTextHelpFormatter)

    parser.add_argument("-r",
                        "-run",
                        type=str,
                        required=True,
                        choices=['init','import','data',"stats"],
                        help='''Enter one of the following: init, import, data, stats''')

    parser.add_argument("--t",
                        type=int,
                        nargs='+',
                        required=False,
                        help="enter the relevant timestamp")

    parser.add_argument("--u",
                        type=str,
                        required=False,
                        choices=['eth','btc'],
                        help="enter the relevant underlying, such as eth or btc")

    args = parser.parse_args()
            
    if args.r == 'init':
                
        db = db_manager(conf=conf)                    
        db.initialize_db()
        
        print("db initialized")
        
    elif args.r == 'import':
        db = db_manager(conf=conf)               
        db.import_all()
                
    elif args.r == 'data':
        
        assert len(args.t)==2, "Need to include start and end time"
        assert args.t[0]<args.t[1], "start timestamp needs to be < end timestamp"
                
        link = gather_link_prices(conf,link)
        link.run_topics_gathering(startTimestamp=args.t[0],
                                  endingTimestamp=args.t[1])
        print("link data gathered susccessfully")

        binance = binance_api(conf)
        binance.process_tickers(startTimestamp=args.t[0], 
                                endTimestamp=args.t[1])
        print("binance data gathered susccessfully")


    elif args.r == 'stats':
        assert args.u in ['eth','btc'], "ticker need to be added"
        assert len(args.t)==2, "Need to include start and end time"
        assert args.t[0]<args.t[1], "start timestamp needs to be < end timestamp"
        stats = stats(conf,link)
        stats.launch_stats(ticker=args.u, 
                           startTimestamp=args.t[0], 
                           endTimestamp=args.t[1])
                
    
    else:
        print("Doing Nothing!")