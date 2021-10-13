# Synth Exchange Circuit Breaker Calibration
 
The repo contains the tools necessary to obtain statistics on chainlink price pushes, which help calibrate the synthetix atomic exchange circuit breaker.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. 

### Prerequisites

The code needs miniconda, as all packages were installed and tested on conda v4.9.2. Installation of miniconda can be done by running the following:

```
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
chmod +x Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh -p $HOME/miniconda3
```

The user would also need a mysql server to hook on to, on a linux based system the mysql server can be installed with the following:

```
sudo apt-get install mysql-server
```


### Installing Enviroment

Refer to [conda docs](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html).

The enviroment files are available under env folder. Enviroment setup can be done with the one of the below actions:

```
conda env create --name snx_alpha --file=environments_ubuntu.yml
```

### Adding API Keys to config/conf.yaml

The user needs etherscan key, binance api keys and a mainnet rpc.

```
---
rpcAddress: 
    mainnet:  ''

mysql:
    user:  "root"
    password:  ''
    host:  "localhost"
    database:  "breaker_calibration"
    raise_on_warnings:  True

etherscan:  ''

binance:
    api: ''
    secret: ''

```

### Creating a db

A database can be created by running the following

```
python main.py -r init
```

### Gathering Data

All data from binance and on-chain prices from chainlink can be gathered  between 2 timestamps can be captured and stored in the db with the following

```
python main.py -r data --t 1618781516 1618781616
```

### Volatility Stats 

Stats that both visualize and diplay the distribution of price pushes across different price volatilties between 2 timestamps can be performed using

```
python main.py -r stats --t 1618781516 1618781716 --u eth
```

### Waiting Time Stats 

Stats on the number of circuit breaks and the average amount of waiting time can be computed with 

```
python main.py -r waiting --t 1618781516 1618781716 --u eth
```



## Authors

* **Kaleb Keny**

## License

Free-for-all :)