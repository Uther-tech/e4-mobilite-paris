
# Pourquoi ETL ici ? 
# L'API Vélib retourne des données brutes JSON → on transforme en Python (nettoyage, typage, validation) 
# AVANT d'insérer en base. C'est la définition exacte d'un ETL


# DAG : etl_velib_pipeline.py
# Logique : ETL — Extract (API Vélib) → Transform (Python) → Load (PostgreSQL)
# Pourquoi ETL : la transformation est réalisée AVANT le chargement en base,
#                directement dans le code Python (nettoyage, typage, validation).
# Schedule : toutes les heures (@hourly)


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import requests
import os
import logging

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────
default_args = {
    "owner": "e4",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

PG_CONN = {
    "host":     os.getenv("POSTGRES_HOST",     "postgres"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname":   os.getenv("POSTGRES_DB",       "mobilite_paris"),
    "user":     os.getenv("POSTGRES_USER",     "e4_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "e4_password_2026"),
}

VELIB_API_URL = (
    "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets"
    "/velib-disponibilite-en-temps-reel/records"
)


# ─────────────────────────────────────────
# ÉTAPE 1 — EXTRACT
# ─────────────────────────────────────────
def extract_velib(**context):
    """
    EXTRACT : Appel à l'API publique Vélib (OpenData Paris).
    Récupère jusqu'à 100 enregistrements temps réel.
    Pousse les données brutes dans XCom pour la tâche suivante.
    """
    logger.info("🔵 [EXTRACT] Interrogation API Vélib OpenData Paris...")
    all_records = []
    offset = 0
    limit  = 100

    while True:
        resp = requests.get(
            VELIB_API_URL,
            params={"limit": limit, "offset": offset},
            timeout=30,
        )
        resp.raise_for_status()
        payload = resp.json()
        records = payload.get("results", [])
        all_records.extend(records)

        total = payload.get("total_count", 0)
        offset += limit
        if offset >= total or not records:
            break

    logger.info(f"🔵 [EXTRACT] {len(all_records)} enregistrements bruts récupérés")
    context["ti"].xcom_push(key="raw_velib", value=all_records)


# ─────────────────────────────────────────
# ÉTAPE 2 — TRANSFORM
# ─────────────────────────────────────────
def transform_velib(**context):
    """
    TRANSFORM : Nettoyage et homogénéisation des données brutes.
    - Suppression des enregistrements sans station_id ou nom
    - Typage explicite (int, float)
    - Normalisation des champs coordonnées
    - Log des enregistrements rejetés
    """
    logger.info("🟡 [TRANSFORM] Début du nettoyage des données...")
    raw = context["ti"].xcom_pull(key="raw_velib", task_ids="extract_velib")

    cleaned  = []
    rejected = 0

    for rec in raw:
        # Règle 1 : station_id et nom obligatoires
        if not rec.get("stationcode") or not rec.get("name"):
            logger.warning(f"⚠️  [TRANSFORM] Rejeté (id/nom manquant) : {rec}")
            rejected += 1
            continue

        # Règle 2 : coordonnées obligatoires
        geo = rec.get("coordonnees_geo") or {}
        lat = geo.get("lat")
        lon = geo.get("lon")
        if lat is None or lon is None:
            logger.warning(f"⚠️  [TRANSFORM] Rejeté (coordonnées manquantes) : {rec.get('stationcode')}")
            rejected += 1
            continue

        cleaned.append({
            "station_id":             str(rec["stationcode"]).strip(),
            "nom_station":            str(rec["name"]).strip(),
            "nb_velos_disponibles":   int(rec.get("numbikesavailable") or 0),
            "velos_electriques":      int(rec.get("ebike")            or 0),
            "velos_mecaniques":       int(rec.get("mechanical")       or 0),
            "nb_bornettes_libres":    int(rec.get("numdocksavailable") or 0),
            "capacite":               int(rec.get("capacity")          or 0),
            "statut":                 str(rec.get("is_installed", "NON")),
            "latitude":               float(lat),
            "longitude":              float(lon),
        })

    logger.info(
        f"🟡 [TRANSFORM] {len(cleaned)} valides | {rejected} rejetés "
        f"| Taux qualité : {round(len(cleaned)/len(raw)*100,1)}%"
    )
    context["ti"].xcom_push(key="clean_velib", value=cleaned)


# ─────────────────────────────────────────
# ÉTAPE 3 — LOAD
# ─────────────────────────────────────────
def load_velib(**context):
    """
    LOAD : Insertion en base PostgreSQL avec stratégie UPSERT.
    - INSERT ... ON CONFLICT DO UPDATE (pas de doublons)
    - Mise à jour de la date de collecte à chaque passage
    """
    logger.info("🟢 [LOAD] Connexion à PostgreSQL...")
    clean = context["ti"].xcom_pull(key="clean_velib", task_ids="transform_velib")

    if not clean:
        logger.warning("🟢 [LOAD] Aucune donnée à charger.")
        return

    conn = psycopg2.connect(**PG_CONN)
    cur  = conn.cursor()

    upsert_sql = """
        INSERT INTO velib_stations (
            station_id, nom_station,
            nb_velos_disponibles, velos_electriques, velos_mecaniques,
            nb_bornettes_libres, capacite, statut,
            latitude, longitude, date_collecte
        ) VALUES (
            %(station_id)s, %(nom_station)s,
            %(nb_velos_disponibles)s, %(velos_electriques)s, %(velos_mecaniques)s,
            %(nb_bornettes_libres)s, %(capacite)s, %(statut)s,
            %(latitude)s, %(longitude)s, NOW()
        )
        ON CONFLICT (station_id) DO UPDATE SET
            nom_station            = EXCLUDED.nom_station,
            nb_velos_disponibles   = EXCLUDED.nb_velos_disponibles,
            velos_electriques      = EXCLUDED.velos_electriques,
            velos_mecaniques       = EXCLUDED.velos_mecaniques,
            nb_bornettes_libres    = EXCLUDED.nb_bornettes_libres,
            capacite               = EXCLUDED.capacite,
            statut                 = EXCLUDED.statut,
            date_collecte          = NOW();
    """

    count = 0
    for row in clean:
        cur.execute(upsert_sql, row)
        count += 1

    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"🟢 [LOAD] {count} stations chargées/mises à jour en base ✅")


# ─────────────────────────────────────────
# DÉFINITION DU DAG
# ─────────────────────────────────────────
with DAG(
    dag_id="etl_velib_pipeline",
    description="ETL Vélib : Extract API OpenData → Transform Python → Load PostgreSQL",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    default_args=default_args,
    catchup=False,
    tags=["etl", "velib", "e4"],
) as dag:

    task_extract = PythonOperator(
        task_id="extract_velib",
        python_callable=extract_velib,
        provide_context=True,
    )

    task_transform = PythonOperator(
        task_id="transform_velib",
        python_callable=transform_velib,
        provide_context=True,
    )

    task_load = PythonOperator(
        task_id="load_velib",
        python_callable=load_velib,
        provide_context=True,
    )

    # ── Chaîne ETL explicite ──────────────
    # Extract → Transform → Load
    task_extract >> task_transform >> task_load