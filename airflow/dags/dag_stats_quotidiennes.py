
"""
DAG : Calcule et log les statistiques quotidiennes
Fréquence : quotidienne à 8h
"""


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import os

default_args = {
    "owner": "e4",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def _get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "mobilite_paris"),
        user=os.getenv("POSTGRES_USER", "e4_user"),
        password=os.getenv("POSTGRES_PASSWORD", "e4_password_2026"),
    )

def _stats_velib(**context):
    conn = _get_conn()
    cur  = conn.cursor()
    cur.execute("""
        SELECT
            COUNT(DISTINCT station_id)  AS nb_stations,
            SUM(nb_velos_disponibles)   AS total_velos,
            SUM(velos_electriques)      AS electriques,
            SUM(velos_mecaniques)       AS mecaniques
        FROM (
            SELECT DISTINCT ON (station_id)
                station_id, nb_velos_disponibles,
                velos_electriques, velos_mecaniques
            FROM velib_disponibilite
            ORDER BY station_id, actualisation_donnee DESC
        ) t
    """)
    row = cur.fetchone()
    cur.close(); conn.close()
    print(f"🚲 Vélib — Stations: {row[0]} | Vélos: {row[1]} | Élec: {row[2]} | Meca: {row[3]}")
    context["ti"].xcom_push(key="velib_stats", value={
        "nb_stations": row[0], "total_velos": row[1],
        "electriques": row[2], "mecaniques":  row[3]
    })

def _stats_evenements(**context):
    conn = _get_conn()
    cur  = conn.cursor()
    cur.execute("""
        SELECT COUNT(*), COUNT(DISTINCT code_postal)
        FROM evenements_qfap
    """)
    row = cur.fetchone()
    cur.close(); conn.close()
    print(f"🎭 Événements — Total: {row[0]} | Arrondissements: {row[1]}")
    context["ti"].xcom_push(key="evenements_stats", value={
        "total": row[0], "arrondissements": row[1]
    })

def _stats_cyclable(**context):
    conn = _get_conn()
    cur  = conn.cursor()
    cur.execute("""
        SELECT COUNT(*), ROUND(SUM(longueur)::numeric, 2),
               COUNT(DISTINCT amenagement)
        FROM amenagements_cyclables
    """)
    row = cur.fetchone()
    cur.close(); conn.close()
    print(f"🛣️ Cyclable — Aménagements: {row[0]} | Km: {row[1]} | Types: {row[2]}")
    context["ti"].xcom_push(key="cyclable_stats", value={
        "nb": row[0], "km": float(row[1]) if row[1] else 0, "types": row[2]
    })

def _rapport_consolide(**context):
    ti = context["ti"]
    v  = ti.xcom_pull(key="velib_stats",      task_ids="stats_velib")
    e  = ti.xcom_pull(key="evenements_stats",  task_ids="stats_evenements")
    c  = ti.xcom_pull(key="cyclable_stats",    task_ids="stats_cyclable")
    print("=" * 55)
    print(f"  RAPPORT QUOTIDIEN — {datetime.now().strftime('%Y-%m-%d')}")
    print("=" * 55)
    print(f"  🚲 Vélib       : {v['nb_stations']} stations | {v['total_velos']} vélos")
    print(f"  🎭 Événements  : {e['total']} événements | {e['arrondissements']} arrondissements")
    print(f"  🛣️ Cyclable    : {c['nb']} aménagements | {c['km']} km")
    print("=" * 55)

with DAG(
    dag_id="stats_quotidiennes",
    description="Calcule et log les statistiques quotidiennes de toutes les tables",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 8 * * *",
    catchup=False,
    tags=["stats", "reporting"],
) as dag:

    t1 = PythonOperator(task_id="stats_velib",       python_callable=_stats_velib)
    t2 = PythonOperator(task_id="stats_evenements",  python_callable=_stats_evenements)
    t3 = PythonOperator(task_id="stats_cyclable",    python_callable=_stats_cyclable)
    t4 = PythonOperator(task_id="rapport_consolide", python_callable=_rapport_consolide)

    [t1, t2, t3] >> t4