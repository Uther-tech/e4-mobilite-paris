
### DAG : Pipeline ELT pour les événements QFAP (logique Extract → Load RAW → Transform in DB)

# Pourquoi ELT ici ? Le CSV événements est chargé brut dans une table staging (sans transformation), 
# puis la transformation est faite directement en SQL dans PostgreSQL. C'est la définition exacte d'un ELT.


# DAG : elt_evenements_pipeline.py
# Logique : ELT — Extract (CSV) → Load RAW staging → Transform SQL (PostgreSQL)
# Pourquoi ELT : les données brutes sont d'abord chargées telles quelles en base
#                dans une table staging, puis transformées via des requêtes SQL
#                directement dans le moteur PostgreSQL.
# Schedule : tous les jours à 6h00




# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# import psycopg2
# import csv
# import os
# import logging

# logger = logging.getLogger(__name__)

# default_args = {
#     "owner": "e4",
#     "retries": 2,
#     "retry_delay": timedelta(minutes=3),
#     "email_on_failure": False,
# }

# PG_CONN = {
#     "host":     os.getenv("POSTGRES_HOST",     "postgres"),
#     "port":     int(os.getenv("POSTGRES_PORT", "5432")),
#     "dbname":   os.getenv("POSTGRES_DB",       "mobilite_paris"),
#     "user":     os.getenv("POSTGRES_USER",     "e4_user"),
#     "password": os.getenv("POSTGRES_PASSWORD", "e4_password_2026"),
# }

# CSV_PATH = "/data/in/que-faire-a-paris.csv"


# # ─────────────────────────────────────────
# # ÉTAPE 1 — EXTRACT + LOAD RAW (staging)
# # ─────────────────────────────────────────
# def extract_load_staging(**context):
#     logger.info(f"🔵 [EXTRACT] Lecture du fichier CSV : {CSV_PATH}")

#     if not os.path.exists(CSV_PATH):
#         raise FileNotFoundError(f"❌ Fichier introuvable : {CSV_PATH}")

#     rows = []
#     with open(CSV_PATH, encoding="utf-8-sig") as f:  # utf-8-sig gère le BOM ✅
#         reader = csv.DictReader(f, delimiter=";")
#         for row in reader:
#             rows.append(row)

#     logger.info(f"🔵 [EXTRACT] {len(rows)} lignes extraites du CSV")
#     logger.info("🟢 [LOAD RAW] Chargement brut en table evenements_staging...")

#     conn = psycopg2.connect(**PG_CONN)
#     cur  = conn.cursor()

#     cur.execute("TRUNCATE TABLE evenements_staging;")

#     # ✅ Colonnes alignées exactement avec init.sql
#     insert_sql = """
#         INSERT INTO evenements_staging (
#             raw_id, raw_event_id, raw_url,
#             raw_titre, raw_description, raw_lieu,
#             raw_adresse, raw_cp, raw_ville,
#             raw_date_debut, raw_date_fin,
#             raw_coordonnees, raw_transport, raw_type_prix
#         ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#     """

#     count = 0
#     for row in rows:
#         cur.execute(insert_sql, (
#             row.get("ID"),
#             row.get("Event ID"),
#             row.get("URL"),
#             row.get("Titre"),
#             row.get("Description"),
#             row.get("Nom du lieu"),
#             row.get("Adresse du lieu"),
#             row.get("Code postal"),        # ✅ nom exact du header CSV
#             row.get("Ville"),
#             row.get("Date de début"),
#             row.get("Date de fin"),
#             row.get("Coordonnées géographiques"),
#             row.get("Transport"),
#             row.get("Type de prix"),       # ✅ raw_type_prix
#         ))
#         count += 1

#     conn.commit()
#     cur.close()
#     conn.close()
#     logger.info(f"🟢 [LOAD RAW] {count} lignes brutes chargées en staging ✅")
#     context["ti"].xcom_push(key="staging_count", value=count)


# # ─────────────────────────────────────────
# # ÉTAPE 2 — TRANSFORM IN DB (SQL)
# # ─────────────────────────────────────────
# def transform_in_db(**context):
#     staging_count = context["ti"].xcom_pull(key="staging_count", task_ids="extract_load_staging")
#     logger.info(f"🟡 [TRANSFORM IN DB] Transformation SQL de {staging_count} lignes staging...")

#     conn = psycopg2.connect(**PG_CONN)
#     cur  = conn.cursor()

#     cur.execute("""
#         INSERT INTO evenements_qfap (
#             id, url, titre,
#             nom_lieu, adresse_lieu, code_postal, ville,
#             date_debut, date_fin,
#                   "Transport", "Type de prix"  
#         )
#         SELECT
#             TRIM(raw_id),                                               -- ✅ PK obligatoire
#             TRIM(raw_url),
#             TRIM(raw_titre),
#             TRIM(raw_lieu),
#             TRIM(raw_adresse),
#             TRIM(raw_cp),
#             TRIM(raw_ville),
#             TO_TIMESTAMP(NULLIF(TRIM(raw_date_debut), ''), 'YYYY-MM-DD"T"HH24:MI:SS'),
#             TO_TIMESTAMP(NULLIF(TRIM(raw_date_fin),   ''), 'YYYY-MM-DD"T"HH24:MI:SS'),
#             TRIM(raw_transport),
#             TRIM(raw_type_prix)                                         -- ✅ type_prix (pas type_evenement)
#         FROM evenements_staging
#         WHERE raw_id          IS NOT NULL
#           AND raw_titre        IS NOT NULL
#           AND TRIM(raw_titre) <> ''
#           AND TRIM(raw_cp)    ~ '^750[0-9]{2}$'                        -- filtre Paris
#         ON CONFLICT (id) DO UPDATE SET                                  -- ✅ ON CONFLICT sur la vraie PK
#             titre        = EXCLUDED.titre,
#             nom_lieu     = EXCLUDED.nom_lieu,
#             adresse_lieu = EXCLUDED.adresse_lieu,
#             code_postal  = EXCLUDED.code_postal,
#             date_debut   = EXCLUDED.date_debut,
#             date_fin     = EXCLUDED.date_fin,
#             url          = EXCLUDED.url,
#             transport    = EXCLUDED.transport,
#                      "Type de prix" = EXCLUDED."Type de prix";         
#     """)
#     inserted = cur.rowcount
#     logger.info(f"🟡 [TRANSFORM IN DB] {inserted} événements valides insérés/mis à jour ✅")

#     cur.execute("""
#         SELECT COUNT(*) FROM evenements_staging
#         WHERE raw_id    IS NULL
#            OR raw_titre IS NULL
#            OR TRIM(raw_titre) = ''
#            OR TRIM(raw_cp) !~ '^750[0-9]{2}$'
#     """)
#     rejected = cur.fetchone()[0]
#     logger.warning(f"⚠️  [TRANSFORM IN DB] {rejected} lignes rejetées (hors Paris, titre ou id vide)")

#     conn.commit()
#     cur.close()
#     conn.close()


# # ─────────────────────────────────────────
# # ÉTAPE 3 — CONTRÔLE QUALITÉ POST-LOAD
# # ─────────────────────────────────────────
# def quality_check(**context):
#     logger.info("🔍 [QUALITY CHECK] Vérification post-chargement...")
#     conn = psycopg2.connect(**PG_CONN)
#     cur  = conn.cursor()

#     cur.execute("SELECT COUNT(*) FROM evenements_qfap;")
#     total = cur.fetchone()[0]

#     cur.execute("SELECT COUNT(*) FROM evenements_staging;")
#     staged = cur.fetchone()[0]

#     cur.close()
#     conn.close()

#     taux = round(total / staged * 100, 1) if staged > 0 else 0
#     logger.info(f"🔍 [QUALITY CHECK] {total} événements en base | staging : {staged} | taux : {taux}%")

#     if total == 0:
#         raise ValueError("❌ [QUALITY CHECK] Table evenements_qfap vide après ELT !")
#     if taux < 50:
#         logger.warning(f"⚠️  [QUALITY CHECK] Taux de chargement faible : {taux}% (événements hors Paris exclus)")
#     else:
#         logger.info(f"✅ [QUALITY CHECK] ELT validé — taux de chargement : {taux}%")


# # ─────────────────────────────────────────
# # DÉFINITION DU DAG
# # ─────────────────────────────────────────
# with DAG(
#     dag_id="elt_evenements_pipeline",
#     description="ELT Événements QFAP : Extract CSV → Load RAW staging → Transform SQL PostgreSQL",
#     schedule_interval="0 6 * * *",
#     start_date=datetime(2026, 1, 1),
#     default_args=default_args,
#     catchup=False,
#     tags=["elt", "evenements", "e4"],
# ) as dag:

#     task_extract_load = PythonOperator(
#         task_id="extract_load_staging",
#         python_callable=extract_load_staging,
#         provide_context=True,
#     )

#     task_transform = PythonOperator(
#         task_id="transform_in_db",
#         python_callable=transform_in_db,
#         provide_context=True,
#     )

#     task_quality = PythonOperator(
#         task_id="quality_check",
#         python_callable=quality_check,
#         provide_context=True,
#     )

#     task_extract_load >> task_transform >> task_quality




from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import csv
import os
import logging

logger = logging.getLogger(__name__)

default_args = {
    "owner": "e4",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

PG_CONN = {
    "host":     os.getenv("POSTGRES_HOST",     "postgres"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname":   os.getenv("POSTGRES_DB",       "mobilite_paris"),
    "user":     os.getenv("POSTGRES_USER",     "e4_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "e4_password_2026"),
}

CSV_PATH = "/data/in/que-faire-a-paris.csv"


# ─────────────────────────────────────────
# ÉTAPE 1 — EXTRACT + LOAD RAW (staging)
# ─────────────────────────────────────────
def extract_load_staging(**context):
    logger.info(f"🔵 [EXTRACT] Lecture du fichier CSV : {CSV_PATH}")

    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"❌ Fichier introuvable : {CSV_PATH}")

    rows = []
    with open(CSV_PATH, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            rows.append(row)

    logger.info(f"🔵 [EXTRACT] {len(rows)} lignes extraites du CSV")
    logger.info("🟢 [LOAD RAW] Chargement brut en table evenements_staging...")

    conn = psycopg2.connect(**PG_CONN)
    cur  = conn.cursor()

    cur.execute("TRUNCATE TABLE evenements_staging;")

    insert_sql = """
        INSERT INTO evenements_staging (
            raw_id, raw_event_id, raw_url,
            raw_titre, raw_description, raw_lieu,
            raw_adresse, raw_cp, raw_ville,
            raw_date_debut, raw_date_fin,
            raw_coordonnees, raw_transport, raw_type_prix
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    count = 0
    for row in rows:
        cur.execute(insert_sql, (
            row.get("ID"),
            row.get("Event ID"),
            row.get("URL"),
            row.get("Titre"),
            row.get("Description"),
            row.get("Nom du lieu"),
            row.get("Adresse du lieu"),
            row.get("Code postal"),
            row.get("Ville"),
            row.get("Date de début"),
            row.get("Date de fin"),
            row.get("Coordonnées géographiques"),
            row.get("Transport"),
            row.get("Type de prix"),
        ))
        count += 1

    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"🟢 [LOAD RAW] {count} lignes brutes chargées en staging ✅")
    context["ti"].xcom_push(key="staging_count", value=count)


# ─────────────────────────────────────────
# ÉTAPE 2 — TRANSFORM IN DB (SQL)
# ─────────────────────────────────────────
def transform_in_db(**context):
    staging_count = context["ti"].xcom_pull(key="staging_count", task_ids="extract_load_staging")
    logger.info(f"🟡 [TRANSFORM IN DB] Transformation SQL de {staging_count} lignes staging...")

    conn = psycopg2.connect(**PG_CONN)
    cur  = conn.cursor()

    cur.execute("""
        INSERT INTO evenements_qfap (
            id, url, titre,
            nom_lieu, adresse_lieu, code_postal, ville,
            date_debut, date_fin,
            "Transport", "Type de prix"
        )
        SELECT
            TRIM(raw_id),
            TRIM(raw_url),
            TRIM(raw_titre),
            TRIM(raw_lieu),
            TRIM(raw_adresse),
            TRIM(raw_cp),
            TRIM(raw_ville),
            TO_TIMESTAMP(NULLIF(TRIM(raw_date_debut), ''), 'YYYY-MM-DD"T"HH24:MI:SS'),
            TO_TIMESTAMP(NULLIF(TRIM(raw_date_fin),   ''), 'YYYY-MM-DD"T"HH24:MI:SS'),
            TRIM(raw_transport),
            TRIM(raw_type_prix)
        FROM evenements_staging
        WHERE raw_id          IS NOT NULL
          AND raw_titre        IS NOT NULL
          AND TRIM(raw_titre) <> ''
          AND TRIM(raw_cp)    ~ '^750[0-9]{2}$'
        ON CONFLICT (id) DO UPDATE SET
            titre          = EXCLUDED.titre,
            nom_lieu       = EXCLUDED.nom_lieu,
            adresse_lieu   = EXCLUDED.adresse_lieu,
            code_postal    = EXCLUDED.code_postal,
            date_debut     = EXCLUDED.date_debut,
            date_fin       = EXCLUDED.date_fin,
            url            = EXCLUDED.url,
            "Transport"    = EXCLUDED."Transport",
            "Type de prix" = EXCLUDED."Type de prix";
    """)
    inserted = cur.rowcount
    logger.info(f"🟡 [TRANSFORM IN DB] {inserted} événements valides insérés/mis à jour ✅")

    cur.execute("""
        SELECT COUNT(*) FROM evenements_staging
        WHERE raw_id    IS NULL
           OR raw_titre IS NULL
           OR TRIM(raw_titre) = ''
           OR TRIM(raw_cp) !~ '^750[0-9]{2}$'
    """)
    rejected = cur.fetchone()[0]
    logger.warning(f"⚠️  [TRANSFORM IN DB] {rejected} lignes rejetées (hors Paris, titre ou id vide)")

    conn.commit()
    cur.close()
    conn.close()


# ─────────────────────────────────────────
# ÉTAPE 3 — CONTRÔLE QUALITÉ POST-LOAD
# ─────────────────────────────────────────
def quality_check(**context):
    logger.info("🔍 [QUALITY CHECK] Vérification post-chargement...")
    conn = psycopg2.connect(**PG_CONN)
    cur  = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM evenements_qfap;")
    total = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM evenements_staging;")
    staged = cur.fetchone()[0]

    cur.close()
    conn.close()

    taux = round(total / staged * 100, 1) if staged > 0 else 0
    logger.info(f"🔍 [QUALITY CHECK] {total} événements en base | staging : {staged} | taux : {taux}%")

    if total == 0:
        raise ValueError("❌ [QUALITY CHECK] Table evenements_qfap vide après ELT !")
    if taux < 50:
        logger.warning(f"⚠️  [QUALITY CHECK] Taux de chargement faible : {taux}% (événements hors Paris exclus)")
    else:
        logger.info(f"✅ [QUALITY CHECK] ELT validé — taux de chargement : {taux}%")


# ─────────────────────────────────────────
# DÉFINITION DU DAG
# ─────────────────────────────────────────
with DAG(
    dag_id="elt_evenements_pipeline",
    description="ELT Événements QFAP : Extract CSV → Load RAW staging → Transform SQL PostgreSQL",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    default_args=default_args,
    catchup=False,
    tags=["elt", "evenements", "e4"],
) as dag:

    task_extract_load = PythonOperator(
        task_id="extract_load_staging",
        python_callable=extract_load_staging,
        provide_context=True,
    )

    task_transform = PythonOperator(
        task_id="transform_in_db",
        python_callable=transform_in_db,
        provide_context=True,
    )

    task_quality = PythonOperator(
        task_id="quality_check",
        python_callable=quality_check,
        provide_context=True,
    )

    task_extract_load >> task_transform >> task_quality