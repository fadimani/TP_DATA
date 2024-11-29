import os
from datetime import datetime

import duckdb
import requests
import json

def get_paris_realtime_bicycle_data():
    url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/json"
    response = requests.request("GET", url)
    serialize_data(response.text, "paris_realtime_bicycle_data.json")

def serialize_data(raw_json: str, file_name: str):

    today_date = datetime.now().strftime("%Y-%m-%d")
    
    if not os.path.exists(f"data/raw_data/{today_date}"):
        os.makedirs(f"data/raw_data/{today_date}")
    
    with open(f"data/raw_data/{today_date}/{file_name}", "w",encoding='utf-8') as fd:
        fd.write(raw_json)

def get_commune_data():
    url = "https://geo.api.gouv.fr/communes"
    response = requests.get(url)
    response.encoding = 'utf-8'
    serialize_data(response.text, "commune_data.json")


def get_nantes_realtime_bicycle_data():
    url = "https://data.nantesmetropole.fr/api/explore/v2.1/catalog/datasets/244400404_stations-velos-libre-service-nantes-metropole-disponibilites/records?limit=100"
    response = requests.request("GET", url)
    json_data = response.json()
    table_of_records = json_data.get("results", [])
    serialize_table_of_records(table_of_records, "nantes_realtime_bicycle_data.json")

def get_toulouse_realtime_bicycle_data():
    url = "https://data.toulouse-metropole.fr/api/explore/v2.1/catalog/datasets/api-velo-toulouse-temps-reel/records?limit=100"
    response = requests.request("GET", url)
    json_data = response.json()
    table_of_records = json_data.get("results", [])
    serialize_table_of_records(table_of_records, "toulouse_realtime_bicycle_data.json")


def serialize_table_of_records(table_of_records, file_name):
    today_date = datetime.now().strftime("%Y-%m-%d")

    if not os.path.exists(f"data/raw_data/{today_date}"):
        os.makedirs(f"data/raw_data/{today_date}")

    with open(f"data/raw_data/{today_date}/{file_name}", 'w', encoding='utf-8') as file:
        json.dump(table_of_records, file, ensure_ascii=False, indent=4)


def empty_duckdb_database():
    con = duckdb.connect("data/duckdb/mobility_analysis.duckdb")
    con.execute("DROP TABLE IF EXISTS FACT_STATION_STATEMENT")
    con.execute("DROP TABLE IF EXISTS DIM_CITY")
    con.execute("DROP TABLE IF EXISTS DIM_STATION")
    con.execute("DROP TABLE IF EXISTS CONSOLIDATE_CITY")
    con.execute("DROP TABLE IF EXISTS CONSOLIDATE_STATION")
    con.execute("DROP TABLE IF EXISTS CONSOLIDATE_STATION_STATEMENT")
    print("All tables have been deleted.")
