import pandas as pd
import os

DATA_IN = os.getenv("DATA_IN", "/data/in")

def extract_velib_stations() -> pd.DataFrame:
    path = os.path.join(DATA_IN, "velib-emplacement-des-stations.csv")
    return pd.read_csv(path, sep=";", encoding="utf-8-sig")

def extract_velib_disponibilite() -> pd.DataFrame:
    path = os.path.join(DATA_IN, "velib-disponibilite-en-temps-reel.csv")
    return pd.read_csv(path, sep=";", encoding="utf-8-sig")

def extract_evenements() -> pd.DataFrame:
    path = os.path.join(DATA_IN, "que-faire-a-paris.csv")
    return pd.read_csv(path, sep=";", encoding="utf-8-sig")

def extract_amenagements() -> pd.DataFrame:
    path = os.path.join(DATA_IN, "amenagements-cyclables.csv")
    return pd.read_csv(path, sep=";", encoding="utf-8-sig")