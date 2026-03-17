

##DAG : Vérifie la qualité des données en base
##Fréquence : quotidienne à 7h

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import os

default_args = {
    "owner": "e4",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def _check_velib(**context):
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "mobilite_paris"),
        user=os.getenv("POSTGRES_USER", "e4_user"),
        password=os.getenv("POSTGRES_PASSWORD", "e4_password_2026"),
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM velib_stations;")
    n = cur.fetchone()[0]
    cur.close(); conn.close()
    print(f"✅ velib_stations : {n} lignes")
    if n == 0:
        raise ValueError("❌ Table velib_stations vide !")

def _check_disponibilite(**context):
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "mobilite_paris"),
        user=os.getenv("POSTGRES_USER", "e4_user"),
        password=os.getenv("POSTGRES_PASSWORD", "e4_password_2026"),
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM velib_disponibilite;")
    n = cur.fetchone()[0]
    cur.close(); conn.close()
    print(f"✅ velib_disponibilite : {n} lignes")
    if n == 0:
        raise ValueError("❌ Table velib_disponibilite vide !")

def _check_evenements(**context):
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "mobilite_paris"),
        user=os.getenv("POSTGRES_USER", "e4_user"),
        password=os.getenv("POSTGRES_PASSWORD", "e4_password_2026"),
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM evenements_qfap;")
    n = cur.fetchone()[0]
    cur.close(); conn.close()
    print(f"✅ evenements_qfap : {n} lignes")
    if n == 0:
        raise ValueError("❌ Table evenements_qfap vide !")

def _check_cyclable(**context):
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "mobilite_paris"),
        user=os.getenv("POSTGRES_USER", "e4_user"),
        password=os.getenv("POSTGRES_PASSWORD", "e4_password_2026"),
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM amenagements_cyclables;")
    n = cur.fetchone()[0]
    cur.close(); conn.close()
    print(f"✅ amenagements_cyclables : {n} lignes")
    if n == 0:
        raise ValueError("❌ Table amenagements_cyclables vide !")

def _rapport_final(**context):
    ti = context["ti"]
    print("=" * 50)
    print("RAPPORT QUALITÉ DONNÉES — E4 Mobilité Paris")
    print("=" * 50)
    print("✅ velib_stations       : OK")
    print("✅ velib_disponibilite  : OK")
    print("✅ evenements_qfap      : OK")
    print("✅ amenagements_cyclables: OK")
    print("Toutes les tables sont alimentées.")

with DAG(
    dag_id="qualite_donnees_quotidiennes",
    description="Vérifie la qualité des données en base chaque matin",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 7 * * *",
    catchup=False,
    tags=["qualite", "monitoring"],
) as dag:

    t1 = PythonOperator(task_id="check_velib_stations",     python_callable=_check_velib)
    t2 = PythonOperator(task_id="check_velib_disponibilite",python_callable=_check_disponibilite)
    t3 = PythonOperator(task_id="check_evenements",         python_callable=_check_evenements)
    t4 = PythonOperator(task_id="check_cyclable",           python_callable=_check_cyclable)
    t5 = PythonOperator(task_id="rapport_final",            python_callable=_rapport_final)

    [t1, t2, t3, t4] >> t5