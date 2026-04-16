"""
DAG : elt_cyclable_pipeline.py
Logique : ELT — Extract (CSV) → Load RAW staging → Transform SQL (PostgreSQL)
Pourquoi ELT : données brutes chargées telles quelles en staging,
               puis transformées via SQL dans le moteur PostgreSQL.
Schedule : tous les jours à 6h30
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import csv
import os
import json
import logging

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────
default_args = {
    "owner": "e4",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

PG_CONN = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "mobilite_paris"),
    "user": os.getenv("POSTGRES_USER", "e4_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "e4_password_2026"),
}

CSV_PATH = "/data/in/amenagements-cyclables.csv"


# ─────────────────────────────────────────
# UTILITAIRE — Log pipeline
# ─────────────────────────────────────────
def log_pipeline(conn, dag_id, task_id, pipeline_type, etape, statut,
                 nb_lignes=0, message="", duree_ms=0):
    """
    Enregistre chaque étape ELT dans pipeline_logs.
    Permet la traçabilité complète pour le rapport E4 (C8/C10).
    """
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO pipeline_logs
            (dag_id, task_id, pipeline_type, etape, statut, nb_lignes, message, duree_ms)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (dag_id, task_id, pipeline_type, etape, statut, nb_lignes, message, duree_ms))
    conn.commit()
    cur.close()


# ─────────────────────────────────────────
# ÉTAPE 1 — EXTRACT + LOAD RAW (staging)
# ─────────────────────────────────────────
def extract_load_staging(**context):
    """
    EXTRACT + LOAD RAW : Lecture du CSV aménagements cyclables
    et chargement BRUT en table amenagements_staging.
    Aucune transformation : toutes les valeurs restent en TEXT.
    La staging est vidée avant chaque chargement (full reload).
    """
    import time
    t0 = time.time()

    logger.info(f"🔵 [EXTRACT] Lecture du fichier CSV : {CSV_PATH}")

    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"❌ Fichier introuvable : {CSV_PATH}")

    rows = []
    with open(CSV_PATH, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            rows.append(row)

    logger.info(f"🔵 [EXTRACT] {len(rows)} lignes extraites du CSV")

    logger.info("🟢 [LOAD RAW] Chargement brut en table amenagements_staging...")
    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()

    # Vidage staging avant rechargement
    cur.execute("TRUNCATE TABLE amenagements_staging;")

    insert_sql = """
        INSERT INTO amenagements_staging (
            raw_osm_id,
            raw_nom,
            raw_amenagement,
            raw_cote_amenagement,
            raw_sens,
            raw_surface,
            raw_arrondissement,
            raw_coronapiste,
            raw_longueur,
            raw_source,
            raw_date_export,
            raw_geo_point_2d,
            raw_all
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    count = 0
    for row in rows:
        cur.execute(insert_sql, (
            row.get("OSM_id") or row.get("osm_id"),
            row.get("Nom") or row.get("nom"),
            row.get("Aménagement") or row.get("amenagement"),
            row.get("Côté aménagement") or row.get("cote_amenagement"),
            row.get("Sens") or row.get("sens"),
            row.get("Surface") or row.get("surface"),
            row.get("Arrondissement") or row.get("arrondissement"),
            row.get("Coronapiste") or row.get("coronapiste"),
            row.get("Longueur") or row.get("longueur"),
            row.get("Source") or row.get("source"),
            row.get("Date export") or row.get("date_export"),
            row.get("geo_point_2d"),
            json.dumps(dict(row), ensure_ascii=False),
        ))
        count += 1

    conn.commit()
    duree = int((time.time() - t0) * 1000)

    log_pipeline(
        conn,
        "elt_cyclable_pipeline",
        "extract_load_staging",
        "ELT",
        "EXTRACT+LOAD",
        "SUCCESS",
        nb_lignes=count,
        message=f"CSV chargé brut en staging : {CSV_PATH}",
        duree_ms=duree,
    )

    cur.close()
    conn.close()

    logger.info(f"🟢 [LOAD RAW] {count} lignes brutes chargées en staging ✅ ({duree}ms)")
    context["ti"].xcom_push(key="staging_count", value=count)


# ─────────────────────────────────────────
# ÉTAPE 2 — TRANSFORM IN DB (SQL)
# ─────────────────────────────────────────
def transform_in_db(**context):
    """
    TRANSFORM IN DB (PostgreSQL)

    On supprime le ON CONFLICT (ta table n'a pas de contrainte UNIQUE/PK sur osm_id).
    Stratégie :
    - TRUNCATE de amenagements_cyclables (full reload)
    - INSERT depuis la staging en dédupliquant sur osm_id (la plus récente selon raw_date_export)
    - osm_id reste en TEXT (car table amenagements_cyclables.osm_id est text)
    """
    import time
    t0 = time.time()

    staging_count = context["ti"].xcom_pull(
        key="staging_count", task_ids="extract_load_staging"
    ) or 0
    logger.info(f"🟡 [TRANSFORM IN DB] Transformation SQL de {staging_count} lignes staging...")

    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()

    # Full reload de la table cible
    cur.execute("TRUNCATE TABLE amenagements_cyclables;")

    # Insertion en dédupliquant sur osm_id (texte numérique uniquement)
    cur.execute("""
        WITH cleaned AS (
            SELECT
                CASE
                    WHEN TRIM(raw_osm_id) ~ '^[0-9]+$' THEN TRIM(raw_osm_id)
                    ELSE NULL
                END AS osm_id_norm,  -- osm_id est TEXT dans ta DB

                TRIM(raw_nom) AS nom_norm,
                TRIM(raw_amenagement) AS amenagement_norm,
                TRIM(raw_cote_amenagement) AS cote_amenagement_norm,
                TRIM(raw_sens) AS sens_norm,
                TRIM(raw_surface) AS surface_norm,

                CASE
                    WHEN TRIM(raw_arrondissement) ~ '^[0-9]+$'
                    THEN TRIM(raw_arrondissement)::bigint
                    ELSE NULL
                END AS arrondissement_norm,

                TRIM(raw_coronapiste) AS coronapiste_norm,

                -- longueur : support virgule décimale
                CASE
                    WHEN REPLACE(TRIM(raw_longueur), ',', '.') ~ '^[0-9]+(\\.[0-9]+)?$'
                    THEN REPLACE(TRIM(raw_longueur), ',', '.')::numeric
                    ELSE NULL
                END AS longueur_norm,

                TRIM(raw_source) AS source_norm,
                TRIM(raw_date_export) AS date_export_norm,
                TRIM(raw_geo_point_2d) AS geo_point_2d_norm,

                raw_date_export
            FROM amenagements_staging
            WHERE raw_osm_id IS NOT NULL
              AND TRIM(raw_osm_id) <> ''
        ),
        dedup AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY osm_id_norm
                    ORDER BY raw_date_export DESC NULLS LAST
                ) AS rn
            FROM cleaned
            WHERE osm_id_norm IS NOT NULL
        )
        INSERT INTO amenagements_cyclables (
            osm_id,
            nom,
            amenagement,
            cote_amenagement,
            sens,
            surface,
            arrondissement,
            coronapiste,
            longueur,
            source,
            date_export,
            geo_point_2d
        )
        SELECT
            osm_id_norm,
            nom_norm,
            amenagement_norm,
            cote_amenagement_norm,
            sens_norm,
            surface_norm,
            arrondissement_norm,
            coronapiste_norm,
            longueur_norm,
            source_norm,
            date_export_norm,
            geo_point_2d_norm
        FROM dedup
        WHERE rn = 1;
    """)

    inserted = cur.rowcount

    # ── Comptage des rejets ───────────────────────────────────────
    cur.execute("""
        SELECT COUNT(*)
        FROM amenagements_staging
        WHERE raw_osm_id IS NULL
           OR TRIM(raw_osm_id) = ''
           OR TRIM(raw_osm_id) !~ '^[0-9]+$'
    """)
    rejected_null = cur.fetchone()[0]

    # ── Comptage des doublons (sur osm_id numérique) ─────
    cur.execute("""
        WITH valid AS (
            SELECT TRIM(raw_osm_id) AS osm_text
            FROM amenagements_staging
            WHERE raw_osm_id IS NOT NULL
              AND TRIM(raw_osm_id) <> ''
              AND TRIM(raw_osm_id) ~ '^[0-9]+$'
        )
        SELECT (COUNT(*) - COUNT(DISTINCT osm_text)) AS nb_doublons
        FROM valid;
    """)
    rejected_dupes = cur.fetchone()[0]

    conn.commit()
    duree = int((time.time() - t0) * 1000)

    log_pipeline(
        conn,
        "elt_cyclable_pipeline",
        "transform_in_db",
        "ELT",
        "TRANSFORM",
        "SUCCESS",
        nb_lignes=inserted,
        message=f"{rejected_null} null/non-num | {rejected_dupes} doublons dédupliqués",
        duree_ms=duree,
    )

    cur.close()
    conn.close()

    logger.info(f"🟡 [TRANSFORM IN DB] {inserted} aménagements insérés ✅")
    logger.warning(f"⚠️  [TRANSFORM IN DB] {rejected_null} lignes rejetées (osm_id null/non-numérique)")
    logger.warning(f"⚠️  [TRANSFORM IN DB] {rejected_dupes} doublons dédupliqués")


# ─────────────────────────────────────────
# ÉTAPE 3 — QUALITY CHECK
# ─────────────────────────────────────────
def quality_check(**context):
    """
    QUALITY CHECK : Contrôle post-chargement.
    """
    import time
    t0 = time.time()

    logger.info("🔍 [QUALITY CHECK] Vérification post-ELT aménagements cyclables...")
    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM amenagements_cyclables;")
    total = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM amenagements_staging;")
    staged = cur.fetchone()[0]

    cur.execute("""
        SELECT
            COUNT(DISTINCT arrondissement) AS nb_arrondissements,
            COUNT(DISTINCT amenagement)    AS nb_types,
            ROUND(SUM(longueur)::NUMERIC / 1000, 1) AS km_total
        FROM amenagements_cyclables
        WHERE longueur IS NOT NULL;
    """)
    stats = cur.fetchone()

    cur.close()
    conn.close()

    taux = round(total / staged * 100, 1) if staged > 0 else 0
    duree = int((time.time() - t0) * 1000)

    logger.info(
        f"🔍 [QUALITY CHECK] {total} aménagements en base | staging : {staged} | taux : {taux}%"
    )
    logger.info(
        f"🔍 [QUALITY CHECK] {stats[0]} arrondissements | {stats[1]} types | {stats[2]} km total"
    )

    if total == 0:
        raise ValueError("❌ [QUALITY CHECK] Table amenagements_cyclables vide après ELT !")

    if taux < 50:
        logger.warning(f"⚠️  [QUALITY CHECK] Taux de chargement faible : {taux}%")
    else:
        logger.info(f"✅ [QUALITY CHECK] ELT validé — taux de chargement : {taux}%")

    conn2 = psycopg2.connect(**PG_CONN)
    log_pipeline(
        conn2,
        "elt_cyclable_pipeline",
        "quality_check",
        "ELT",
        "QUALITY_CHECK",
        "SUCCESS",
        nb_lignes=total,
        message=f"Taux={taux}% | {stats[2]}km | {stats[0]} arrondissements",
        duree_ms=duree,
    )
    conn2.close()


# ─────────────────────────────────────────
# DÉFINITION DU DAG
# ─────────────────────────────────────────
with DAG(
    dag_id="elt_cyclable_pipeline",
    description="ELT Aménagements cyclables : Extract CSV → Load RAW staging → Transform SQL PostgreSQL",
    schedule_interval="30 4 * * *",  # 06:30 Paris (avril/CEST)
    start_date=datetime(2026, 1, 1),
    default_args=default_args,
    catchup=False,
    tags=["elt", "cyclable", "e4"],
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