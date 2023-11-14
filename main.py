import requests
import pandas as pd
from configparser import ConfigParser
import sqlalchemy as sa

config = ConfigParser()
config.read("config/config.ini")

config_db = config['DataBase']
host = config_db['host']
port = config_db['port']
dbname = config_db['dbname']
username = config_db['username']
password = config_db['password']

config_api = config['api']
secret_key = config_api["secret_key"]

def conn_string():
    return f'postgresql://{username}:{password}@{host}:{port}/{dbname}'

def create(url):
    engine = sa.create_engine(url) 
    conn = engine.connect() ## se conecta pero no hace nada. 
    conn.execute(f"""
            DROP TABLE IF EXISTS palombopabloe_coderhouse.api;
            CREATE TABLE palombopabloe_coderhouse.api (
                id INT identity(0, 1) PRIMARY KEY,
                open_price FLOAT,
                high_price FLOAT,
                low_price FLOAT,
                close_price FLOAT,
                volume FLOAT,
                adj_high FLOAT,
                adj_low FLOAT,
                adj_close FLOAT,
                adj_open FLOAT,
                adj_volume FLOAT,
                split_factor FLOAT,
                dividend FLOAT,
                symbol VARCHAR(10),
                exchange VARCHAR(10),
                date TIMESTAMP
            );
        """
    )
    conn.close()

def rename_columns(df):
    columns = ["open", "high", "low", "close"]
    for name in columns:
        df.rename(columns={name: name+"_price"}, inplace=True)

def add(url, data):
    engine = sa.create_engine(url)
    conn = engine.connect()
    
    data.to_sql(name="api",
    con=conn,
    schema="palombopabloe_coderhouse",
    if_exists="append",
    method="multi",
    index=False, ##ya tenemos el id que se genera
    )



if __name__ == "__main__":
    url = conn_string()

    params = {
    'access_key': secret_key
    } ##necesita que se llame access key

    api_result = requests.get('http://api.marketstack.com/v1/tickers/aapl/eod/latest', params)
    api_response = api_result.json()
    df = pd.DataFrame([api_response]) ##van corchetes, para lista

    rename_columns(df)
    add(url, df)

