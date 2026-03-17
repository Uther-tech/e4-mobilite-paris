
"""
DAG : Rafraîchit les données Vélib depuis l'API Paris Open Data
Fréquence : toutes les heures
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import os

default_args = {
    "owner": "e4",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def _fetch_and_update():
    URL = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/records"
    params = {"limit": 100, "offset": 0}
    all_records = []

    while True:
        r = requests.get(URL, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        records = data.get("results", [])
        if not records:
            break
        all_records.extend(records)
        if len(records) < 100:
            break
        params["offset"] += 100

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "mobilite_paris"),
        user=os.getenv("POSTGRES_USER", "e4_user"),
        password=os.getenv("POSTGRES_PASSWORD", "e4_password_2026"),
    )
    cur = conn.cursor()

    cur.execute("TRUNCATE TABLE velib_disponibilite;")

    for rec in all_records:
        cur.execute("""
            INSERT INTO velib_disponibilite
                (station_id, nom_station, nb_velos_disponibles,
                 velos_mecaniques, velos_electriques,
                 nb_bornettes_libres, actualisation_donnee)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
        """, (
            rec.get("stationcode"),
            rec.get("name"),
            rec.get("numbikesavailable", 0),
            rec.get("mechanical", 0),
            rec.get("ebike", 0),
            rec.get("numdocksavailable", 0),
            rec.get("duedate"),
        ))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ {len(all_records)} stations Vélib mises à jour")

with DAG(
    dag_id="velib_refresh_horaire",
    description="Rafraîchit la disponibilité Vélib toutes les heures",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["velib", "refresh"],
) as dag:

    refresh = PythonOperator(
        task_id="fetch_and_update_velib",
        python_callable=_fetch_and_update,
    )