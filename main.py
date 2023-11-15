import requests
import pandas as pd
from configparser import ConfigParser
import sqlalchemy as sa

config = ConfigParser()  ##se configura para parsear el archivo config
config.read("config/config.ini")  #se instancia la lectura 

config_db = config['DataBase']  ##traen los datos referidos a la base y debajo lo mismo respecto a la contraseña de la api
host = config_db['host']
port = config_db['port']
dbname = config_db['dbname']
username = config_db['username']
password = config_db['password']

config_api = config['api']
secret_key = config_api["secret_key"]

def conn_string():
    return f'postgresql://{username}:{password}@{host}:{port}/{dbname}' ##se anexan las variables para conformar la URL

def create(url): #función que se conecta a la base de datos y crea la tabla. Toma como parámetro la URL
    engine = sa.create_engine(url)  se usa la librería de SQLALCHEMY para sentenciar la URL como motor para conectar
    conn = engine.connect() ##se usa el motor para conectar
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
    ) ##ya conectado, ejecutas la query que, en caso de no estar creada, crea la tabla.
    conn.close() ##luego, se cierra la conexión.

def rename_columns(df): #incorporación respecto al tp anterior, se cambian las columnas para incorporar una etapa de ETL. 
    columns = ["open", "high", "low", "close"]
    for name in columns:
        df.rename(columns={name: name+"_price"}, inplace=True)

def add(url, data): #función que incluye dos parámetros, URL y data. Ambas se declaran después. 
    engine = sa.create_engine(url) #se crea la variable que corre el motor de la base con el URL declarado. 
    conn = engine.connect() #se crea otra variable que representa al conexión de la variable anterior. 
    
    data.to_sql(name="api", #incorpora los datos bajo la función to_sql, que requiere se le indique el nombre de la tabla, la variable que representa la conexión, el esquema. 
    con=conn,
    schema="palombopabloe_coderhouse",
    if_exists="append", #si existe una tabla, que haga un append
    method="multi",
    index=False, ##indica que no genere un índice. 
    )



if __name__ == "__main__": # el main representa lo primero que se ejecuta
    url = conn_string() #se indica que la variable URL toma la URL que surge de la función conn_String

    params = {
    'access_key': secret_key #se invoca el parámetro del archivo confing.
    } 

    api_result = requests.get('http://api.marketstack.com/v1/tickers/aapl/eod/latest', params)  #indica que la variable denominada resultado surge de ejecutar la API
    api_response = api_result.json()  #luego lo ejecutado se pasa a json
    df = pd.DataFrame([api_response]) ##van corchetes, para lista #luego se pasa a un dataframe

    rename_columns(df) #se invoca la función que cambia las columnas
    add(url, df) #se invoca la función que toma los dos parámetros, el primero es la conexión a la base y crea la tabla, el segundo incorpora el dataframe

