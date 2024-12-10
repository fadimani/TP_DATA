import json
from datetime import datetime, date

import duckdb
import pandas as pd

pd.set_option('display.max_columns', 9)

today_date = datetime.now().strftime("%Y-%m-%d")
PARIS_CITY_CODE = 1
NANTES_CITY_CODE = 2
TOULOUSE_CITY_CODE = 3

def create_consolidate_tables():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    with open("data/sql_statements/create_consolidate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            print('table created')
            con.execute(statement)

def get_INSEE_code(city_name):
    duckdb_path = "data/duckdb/mobility_analysis.duckdb"
    connection = duckdb.connect(duckdb_path)
    insee = connection.execute(f"SELECT ID FROM CONSOLIDATE_CITY WHERE  NAME = '{city_name.capitalize() }'    ").fetchdf()
    insee = insee["ID"][0]
    return insee

def add_city_data(json_file, city_code,):
    with open(f"data/raw_data/{today_date}/{json_file}", encoding='UTF-8') as fd:
        city_data = json.load(fd)

    city_name = city_data[0]["contract_name"]
    insee_code = get_INSEE_code(city_name)

    city_raw_data_df = pd.json_normalize(city_data)
    city_raw_data_df["id"] = city_raw_data_df["number"].apply(lambda x: f"{city_code}-{x}")
    city_raw_data_df['status'] = city_raw_data_df['status'].apply(lambda x: 'OUI' if x == 'OPEN' else 'NON')
    city_raw_data_df["created_date"] = date.today()
    city_raw_data_df["code_insee_commune"] = insee_code
    city_station_data_df = city_raw_data_df[[
        "id",
        "number",
        "name",
        "contract_name",
        "code_insee_commune",
        "address",
        "position.lon",
        "position.lat",
        "status",
        "created_date",
        "bike_stands"
    ]]

    city_station_data_df.rename(columns={
        "number": "code",
        "name": "name",
        "position.lon": "longitude",
        "position.lat": "latitude",
        "status": "status",
        "contract_name": "city_name",
        "code_insee_commune": "city_code",
        "bike_stands": "capacity"
    }, inplace=True)
    return city_station_data_df

def consolidate_station_data(add_info=True):
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)

    # Consolidation logic for Paris Bicycle data
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)
    
    paris_raw_data_df = pd.json_normalize(data)
    paris_raw_data_df["id"] = paris_raw_data_df["stationcode"].apply(lambda x: f"{PARIS_CITY_CODE}-{x}")
    paris_raw_data_df["address"] = None
    paris_raw_data_df["created_date"] = date.today()

    paris_station_data_df = paris_raw_data_df[[
        "id",
        "stationcode",
        "name",
        "nom_arrondissement_communes",
        "code_insee_commune",
        "address",
        "coordonnees_geo.lon",
        "coordonnees_geo.lat",
        "is_installed",
        "created_date",
        "capacity"
    ]]

    paris_station_data_df.rename(columns={
        "stationcode": "code",
        "name": "name",
        "coordonnees_geo.lon": "longitude",
        "coordonnees_geo.lat": "latitude",
        "is_installed": "status",
        "nom_arrondissement_communes": "city_name",
        "code_insee_commune": "city_code"
    }, inplace=True)
    if add_info:
        nantes_station_data_df = add_city_data("nantes_realtime_bicycle_data.json", NANTES_CITY_CODE )
        toulouse_station_data_df = add_city_data("toulouse_realtime_bicycle_data.json", TOULOUSE_CITY_CODE)
        all_data = pd.concat([paris_station_data_df, nantes_station_data_df, toulouse_station_data_df])
    else:
        all_data = paris_station_data_df

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM all_data;")

def consolidate_city_data():

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)

    with open(f"data/raw_data/{today_date}/commune_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)

    city_data_df = raw_data_df[[
        "code",
        "nom",
        "population"
    ]]
    city_data_df.rename(columns={
        "code": "id",
        "nom": "name",
        "population": "nb_inhabitants"
    }, inplace=True)
    city_data_df.drop_duplicates(inplace = True)

    city_data_df["created_date"] = date.today()

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM city_data_df;")

def add_station_statement_data(json_file, city_code):
    with open(f"data/raw_data/{today_date}/{json_file}") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)
    raw_data_df["station_id"] = raw_data_df["number"].apply(lambda x: f"{city_code}-{x}")
    raw_data_df["created_date"] = date.today()
    station_statement_data_df = raw_data_df[[
        "station_id",
        "available_bike_stands",
        "available_bikes",
        "last_update",
        "created_date"
    ]]

    station_statement_data_df.rename(columns={
        "available_bike_stands": "bicycle_docks_available",
        "available_bikes": "bicycle_available",
        "last_update": "last_statement_date",
    }, inplace=True)
    return station_statement_data_df

def consolidate_station_statement_data(add_info=True):
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}

    # Consolidate station statement data for Paris
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    paris_raw_data_df = pd.json_normalize(data)
    paris_raw_data_df["station_id"] = paris_raw_data_df["stationcode"].apply(lambda x: f"{PARIS_CITY_CODE}-{x}")
    paris_raw_data_df["created_date"] = date.today()
    paris_station_statement_data_df = paris_raw_data_df[[
        "station_id",
        "numdocksavailable",
        "numbikesavailable",
        "duedate",
        "created_date"
    ]]
    
    paris_station_statement_data_df.rename(columns={
        "numdocksavailable": "bicycle_docks_available",
        "numbikesavailable": "bicycle_available",
        "duedate": "last_statement_date",
    }, inplace=True)

    if add_info:
        nantes_station_statement_data_df = add_station_statement_data("nantes_realtime_bicycle_data.json", NANTES_CITY_CODE)
        toulouse_station_statement_data_df = add_station_statement_data("toulouse_realtime_bicycle_data.json", TOULOUSE_CITY_CODE)
        all_data = pd.concat([paris_station_statement_data_df, nantes_station_statement_data_df, toulouse_station_statement_data_df])
    else:
        all_data = paris_station_statement_data_df


    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM all_data;")
