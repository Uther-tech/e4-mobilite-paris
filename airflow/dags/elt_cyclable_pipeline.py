

# """
# DAG : elt_cyclable_pipeline.py
# Logique : ELT — Extract (CSV) → Load RAW staging → Transform SQL (PostgreSQL)
# Pourquoi ELT : données brutes chargées telles quelles en staging,
#                puis transformées via SQL dans le moteur PostgreSQL.
# Schedule : tous les jours à 6h30
# """

# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# import psycopg2
# import csv
# import os
# import json
# import logging

# logger = logging.getLogger(__name__)

# # ─────────────────────────────────────────
# # CONFIGURATION
# # ─────────────────────────────────────────
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

# CSV_PATH = "/data/in/amenagements-cyclables.csv"


# # ─────────────────────────────────────────
# # UTILITAIRE — Log pipeline
# # ─────────────────────────────────────────
# def log_pipeline(conn, dag_id, task_id, pipeline_type, etape, statut,
#                  nb_lignes=0, message="", duree_ms=0):
#     """
#     Enregistre chaque étape ELT dans pipeline_logs.
#     Permet la traçabilité complète pour le rapport E4 (C8/C10).
#     """
#     cur = conn.cursor()
#     cur.execute("""
#         INSERT INTO pipeline_logs
#             (dag_id, task_id, pipeline_type, etape, statut, nb_lignes, message, duree_ms)
#         VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
#     """, (dag_id, task_id, pipeline_type, etape, statut, nb_lignes, message, duree_ms))
#     conn.commit()
#     cur.close()


# # ─────────────────────────────────────────
# # ÉTAPE 1 — EXTRACT + LOAD RAW (staging)
# # ─────────────────────────────────────────
# def extract_load_staging(**context):
#     """
#     EXTRACT + LOAD RAW : Lecture du CSV aménagements cyclables
#     et chargement BRUT en table amenagements_staging.
#     Aucune transformation : toutes les valeurs restent en TEXT.
#     La staging est vidée avant chaque chargement (full reload).
#     """
#     import time
#     t0 = time.time()

#     logger.info(f"🔵 [EXTRACT] Lecture du fichier CSV : {CSV_PATH}")

#     if not os.path.exists(CSV_PATH):
#         raise FileNotFoundError(f"❌ Fichier introuvable : {CSV_PATH}")

#     rows = []
#     with open(CSV_PATH, encoding="utf-8") as f:
#         reader = csv.DictReader(f, delimiter=";")
#         for row in reader:
#             rows.append(row)

#     logger.info(f"🔵 [EXTRACT] {len(rows)} lignes extraites du CSV")

#     logger.info("🟢 [LOAD RAW] Chargement brut en table amenagements_staging...")
#     conn = psycopg2.connect(**PG_CONN)
#     cur  = conn.cursor()

#     # Vidage staging avant rechargement
#     cur.execute("TRUNCATE TABLE amenagements_staging;")

#     insert_sql = """
#         INSERT INTO amenagements_staging (
#             raw_osm_id,
#             raw_nom,
#             raw_amenagement,
#             raw_cote_amenagement,
#             raw_sens,
#             raw_surface,
#             raw_arrondissement,
#             raw_coronapiste,
#             raw_longueur,
#             raw_source,
#             raw_date_export,
#             raw_geo_point_2d,
#             raw_all
#         ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#     """

#     count = 0
#     for row in rows:
#         cur.execute(insert_sql, (
#             row.get("OSM_id")                          or row.get("osm_id"),
#             row.get("Nom")                             or row.get("nom"),
#             row.get("Aménagement")                     or row.get("amenagement"),
#             row.get("Côté aménagement")                or row.get("cote_amenagement"),
#             row.get("Sens")                            or row.get("sens"),
#             row.get("Surface")                         or row.get("surface"),
#             row.get("Arrondissement")                  or row.get("arrondissement"),
#             row.get("Coronapiste")                     or row.get("coronapiste"),
#             row.get("Longueur")                        or row.get("longueur"),
#             row.get("Source")                          or row.get("source"),
#             row.get("Date export")                     or row.get("date_export"),
#             row.get("geo_point_2d"),
#             json.dumps(dict(row), ensure_ascii=False), # ligne brute complète en JSON
#         ))
#         count += 1

#     conn.commit()
#     duree = int((time.time() - t0) * 1000)

#     log_pipeline(conn, "elt_cyclable_pipeline", "extract_load_staging",
#                  "ELT", "EXTRACT+LOAD", "SUCCESS",
#                  nb_lignes=count,
#                  message=f"CSV chargé brut en staging : {CSV_PATH}",
#                  duree_ms=duree)

#     cur.close()
#     conn.close()
#     logger.info(f"🟢 [LOAD RAW] {count} lignes brutes chargées en staging ✅ ({duree}ms)")
#     context["ti"].xcom_push(key="staging_count", value=count)


# # ─────────────────────────────────────────
# # ÉTAPE 2 — TRANSFORM IN DB (SQL)
# # ─────────────────────────────────────────
# def transform_in_db(**context):
#     """
#     TRANSFORM IN DB : Transformation directement en SQL dans PostgreSQL.
#     - Nettoyage TRIM sur tous les champs TEXT
#     - Cast NUMERIC sur la longueur (avec gestion des valeurs nulles)
#     - Filtre : osm_id non null obligatoire
#     - Upsert ON CONFLICT (osm_id) DO UPDATE
#     - Rejets comptabilisés et loggués
#     """
#     import time
#     t0 = time.time()

#     staging_count = context["ti"].xcom_pull(
#         key="staging_count", task_ids="extract_load_staging"
#     )
#     logger.info(f"🟡 [TRANSFORM IN DB] Transformation SQL de {staging_count} lignes staging...")

#     conn = psycopg2.connect(**PG_CONN)
#     cur  = conn.cursor()

#     # ── Insertion des lignes valides ──────────────────────────────
#     cur.execute("""
#         INSERT INTO amenagements_cyclables (
#             osm_id,
#             nom,
#             amenagement,
#             cote_amenagement,
#             sens,
#             surface,
#             arrondissement,
#             coronapiste,
#             longueur,
#             source,
#             date_export,
#             geo_point_2d
#         )
#         SELECT
#             TRIM(raw_osm_id),
#             TRIM(raw_nom),
#             TRIM(raw_amenagement),
#             TRIM(raw_cote_amenagement),
#             TRIM(raw_sens),
#             TRIM(raw_surface),
#                     CASE
#             WHEN TRIM(raw_arrondissement) ~ '^[0-9]+$'
#             THEN TRIM(raw_arrondissement)::bigint
#             ELSE NULL
#         END,
#             TRIM(raw_coronapiste),
#             -- Cast NUMERIC avec fallback NULL si valeur non numérique
#             CASE
#                 WHEN TRIM(raw_longueur) ~ '^[0-9]+(\.[0-9]+)?$'
#                 THEN TRIM(raw_longueur)::NUMERIC
#                 ELSE NULL
#             END,
#             TRIM(raw_source),
#             TRIM(raw_date_export),
#             TRIM(raw_geo_point_2d)
#         FROM amenagements_staging
#         WHERE raw_osm_id IS NOT NULL
#           AND TRIM(raw_osm_id) <> ''
#         ON CONFLICT (osm_id) DO UPDATE SET
#             nom              = EXCLUDED.nom,
#             amenagement      = EXCLUDED.amenagement,
#             cote_amenagement = EXCLUDED.cote_amenagement,
#             sens             = EXCLUDED.sens,
#             surface          = EXCLUDED.surface,
#             arrondissement   = EXCLUDED.arrondissement,
#             coronapiste      = EXCLUDED.coronapiste,
#             longueur         = EXCLUDED.longueur,
#             source           = EXCLUDED.source,
#             date_export      = EXCLUDED.date_export,
#             geo_point_2d     = EXCLUDED.geo_point_2d;
#     """)
#     inserted = cur.rowcount

#     # ── Comptage des rejets ───────────────────────────────────────
#     cur.execute("""
#         SELECT COUNT(*) FROM amenagements_staging
#         WHERE raw_osm_id IS NULL OR TRIM(raw_osm_id) = ''
#     """)
#     rejected = cur.fetchone()[0]

#     conn.commit()
#     duree = int((time.time() - t0) * 1000)

#     log_pipeline(conn, "elt_cyclable_pipeline", "transform_in_db",
#                  "ELT", "TRANSFORM", "SUCCESS",
#                  nb_lignes=inserted,
#                  message=f"{rejected} lignes rejetées (osm_id manquant)",
#                  duree_ms=duree)

#     cur.close()
#     conn.close()

#     logger.info(f"🟡 [TRANSFORM IN DB] {inserted} aménagements insérés/mis à jour ✅")
#     logger.warning(f"⚠️  [TRANSFORM IN DB] {rejected} lignes rejetées (osm_id manquant)")


# # ─────────────────────────────────────────
# # ÉTAPE 3 — QUALITY CHECK
# # ─────────────────────────────────────────
# def quality_check(**context):
#     """
#     QUALITY CHECK : Contrôle post-chargement.
#     - Compte les lignes en base vs staging
#     - Calcule le taux de chargement
#     - Lève une erreur si table vide
#     - Log le résultat dans pipeline_logs
#     """
#     import time
#     t0 = time.time()

#     logger.info("🔍 [QUALITY CHECK] Vérification post-ELT aménagements cyclables...")
#     conn = psycopg2.connect(**PG_CONN)
#     cur  = conn.cursor()

#     cur.execute("SELECT COUNT(*) FROM amenagements_cyclables;")
#     total = cur.fetchone()[0]

#     cur.execute("SELECT COUNT(*) FROM amenagements_staging;")
#     staged = cur.fetchone()[0]

#     # Stats supplémentaires utiles pour le rapport E4
#     cur.execute("""
#         SELECT
#             COUNT(DISTINCT arrondissement) AS nb_arrondissements,
#             COUNT(DISTINCT amenagement)    AS nb_types,
#             ROUND(SUM(longueur)::NUMERIC / 1000, 1) AS km_total
#         FROM amenagements_cyclables
#         WHERE longueur IS NOT NULL;
#     """)
#     stats = cur.fetchone()

#     cur.close()
#     conn.close()

#     taux = round(total / staged * 100, 1) if staged > 0 else 0
#     duree = int((time.time() - t0) * 1000)

#     logger.info(
#         f"🔍 [QUALITY CHECK] {total} aménagements en base "
#         f"| staging : {staged} | taux : {taux}%"
#     )
#     logger.info(
#         f"🔍 [QUALITY CHECK] "
#         f"{stats[0]} arrondissements | "
#         f"{stats[1]} types | "
#         f"{stats[2]} km total"
#     )

#     if total == 0:
#         raise ValueError("❌ [QUALITY CHECK] Table amenagements_cyclables vide après ELT !")

#     if taux < 50:
#         logger.warning(f"⚠️  [QUALITY CHECK] Taux de chargement faible : {taux}%")
#     else:
#         logger.info(f"✅ [QUALITY CHECK] ELT validé — taux de chargement : {taux}%")

#     conn2 = psycopg2.connect(**PG_CONN)
#     log_pipeline(conn2, "elt_cyclable_pipeline", "quality_check",
#                  "ELT", "QUALITY_CHECK", "SUCCESS",
#                  nb_lignes=total,
#                  message=f"Taux={taux}% | {stats[2]}km | {stats[0]} arrondissements",
#                  duree_ms=duree)
#     conn2.close()


# # ─────────────────────────────────────────
# # DÉFINITION DU DAG
# # ─────────────────────────────────────────
# with DAG(
#     dag_id="elt_cyclable_pipeline",
#     description="ELT Aménagements cyclables : Extract CSV → Load RAW staging → Transform SQL PostgreSQL",
#     schedule_interval="30 6 * * *",   # tous les jours à 6h30 (après elt_evenements à 6h00)
#     start_date=datetime(2026, 1, 1),
#     default_args=default_args,
#     catchup=False,
#     tags=["elt", "cyclable", "e4"],
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

#     # ── Chaîne ELT explicite ─────────────────────────
#     # Extract+Load RAW → Transform SQL → Quality Check
#     task_extract_load >> task_transform >> task_quality



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
    "host":     os.getenv("POSTGRES_HOST",     "postgres"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname":   os.getenv("POSTGRES_DB",       "mobilite_paris"),
    "user":     os.getenv("POSTGRES_USER",     "e4_user"),
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
    cur  = conn.cursor()

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
            row.get("OSM_id")                          or row.get("osm_id"),
            row.get("Nom")                             or row.get("nom"),
            row.get("Aménagement")                     or row.get("amenagement"),
            row.get("Côté aménagement")                or row.get("cote_amenagement"),
            row.get("Sens")                            or row.get("sens"),
            row.get("Surface")                         or row.get("surface"),
            row.get("Arrondissement")                  or row.get("arrondissement"),
            row.get("Coronapiste")                     or row.get("coronapiste"),
            row.get("Longueur")                        or row.get("longueur"),
            row.get("Source")                          or row.get("source"),
            row.get("Date export")                     or row.get("date_export"),
            row.get("geo_point_2d"),
            json.dumps(dict(row), ensure_ascii=False), # ligne brute complète en JSON
        ))
        count += 1

    conn.commit()
    duree = int((time.time() - t0) * 1000)

    log_pipeline(conn, "elt_cyclable_pipeline", "extract_load_staging",
                 "ELT", "EXTRACT+LOAD", "SUCCESS",
                 nb_lignes=count,
                 message=f"CSV chargé brut en staging : {CSV_PATH}",
                 duree_ms=duree)

    cur.close()
    conn.close()
    logger.info(f"🟢 [LOAD RAW] {count} lignes brutes chargées en staging ✅ ({duree}ms)")
    context["ti"].xcom_push(key="staging_count", value=count)


# ─────────────────────────────────────────
# ÉTAPE 2 — TRANSFORM IN DB (SQL)
# ─────────────────────────────────────────
def transform_in_db(**context):
    """
    TRANSFORM IN DB : Transformation directement en SQL dans PostgreSQL.
    - Nettoyage TRIM sur tous les champs TEXT
    - Cast NUMERIC sur la longueur (avec gestion des valeurs nulles)
    - Filtre : osm_id non null obligatoire
    - DÉDUPLIQUER : garder une seule ligne par osm_id (la plus récente selon raw_date_export)
    - Upsert ON CONFLICT (osm_id) DO UPDATE
    - Rejets comptabilisés et loggués
    """
    import time
    t0 = time.time()

    staging_count = context["ti"].xcom_pull(
        key="staging_count", task_ids="extract_load_staging"
    ) or 0
    logger.info(f"🟡 [TRANSFORM IN DB] Transformation SQL de {staging_count} lignes staging...")

    conn = psycopg2.connect(**PG_CONN)
    cur  = conn.cursor()

    # ── Insertion des lignes valides (dédupliquées) ──────────────────────────────
    # We deduplicate by choosing the most recent raw_date_export for each TRIM(raw_osm_id).
    cur.execute("""
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
            TRIM(raw_osm_id),
            TRIM(raw_nom),
            TRIM(raw_amenagement),
            TRIM(raw_cote_amenagement),
            TRIM(raw_sens),
            TRIM(raw_surface),
            CASE
                WHEN TRIM(raw_arrondissement) ~ '^[0-9]+$'
                THEN TRIM(raw_arrondissement)::bigint
                ELSE NULL
            END,
            TRIM(raw_coronapiste),
            CASE
                WHEN TRIM(raw_longueur) ~ '^[0-9]+(\.[0-9]+)?$'
                THEN TRIM(raw_longueur)::NUMERIC
                ELSE NULL
            END,
            TRIM(raw_source),
            TRIM(raw_date_export),
            TRIM(raw_geo_point_2d)
        FROM (
            SELECT *
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY TRIM(raw_osm_id) ORDER BY raw_date_export DESC NULLS LAST) AS rn
                FROM amenagements_staging
                WHERE raw_osm_id IS NOT NULL AND TRIM(raw_osm_id) <> ''
            ) s
            WHERE s.rn = 1
        ) AS dedup
        ON CONFLICT (osm_id) DO UPDATE SET
            nom              = EXCLUDED.nom,
            amenagement      = EXCLUDED.amenagement,
            cote_amenagement = EXCLUDED.cote_amenagement,
            sens             = EXCLUDED.sens,
            surface          = EXCLUDED.surface,
            arrondissement   = EXCLUDED.arrondissement,
            coronapiste      = EXCLUDED.coronapiste,
            longueur         = EXCLUDED.longueur,
            source           = EXCLUDED.source,
            date_export      = EXCLUDED.date_export,
            geo_point_2d     = EXCLUDED.geo_point_2d;
    """)
    inserted = cur.rowcount

    # ── Comptage des rejets ───────────────────────────────────────
    cur.execute("""
        SELECT COUNT(*) FROM amenagements_staging
        WHERE raw_osm_id IS NULL OR TRIM(raw_osm_id) = ''
    """)
    rejected_null = cur.fetchone()[0]

    # ── Comptage des doublons ─────────────────────────────────────
    cur.execute("""
        SELECT COUNT(*) - COUNT(DISTINCT TRIM(raw_osm_id)) AS nb_doublons
        FROM amenagements_staging
        WHERE raw_osm_id IS NOT NULL AND TRIM(raw_osm_id) <> ''
    """)
    rejected_dupes = cur.fetchone()[0]

    conn.commit()
    duree = int((time.time() - t0) * 1000)

    log_pipeline(conn, "elt_cyclable_pipeline", "transform_in_db",
                 "ELT", "TRANSFORM", "SUCCESS",
                 nb_lignes=inserted,
                 message=f"{rejected_null} null | {rejected_dupes} doublons",
                 duree_ms=duree)

    cur.close()
    conn.close()

    logger.info(f"🟡 [TRANSFORM IN DB] {inserted} aménagements insérés/mis à jour ✅")
    logger.warning(f"⚠️  [TRANSFORM IN DB] {rejected_null} lignes rejetées (osm_id null)")
    logger.warning(f"⚠️  [TRANSFORM IN DB] {rejected_dupes} doublons dédupliqués")

# ─────────────────────────────────────────
# ÉTAPE 3 — QUALITY CHECK
# ─────────────────────────────────────────
def quality_check(**context):
    """
    QUALITY CHECK : Contrôle post-chargement.
    - Compte les lignes en base vs staging
    - Calcule le taux de chargement
    - Lève une erreur si table vide
    - Log le résultat dans pipeline_logs
    """
    import time
    t0 = time.time()

    logger.info("🔍 [QUALITY CHECK] Vérification post-ELT aménagements cyclables...")
    conn = psycopg2.connect(**PG_CONN)
    cur  = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM amenagements_cyclables;")
    total = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM amenagements_staging;")
    staged = cur.fetchone()[0]

    # Stats supplémentaires utiles pour le rapport E4
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
        f"🔍 [QUALITY CHECK] {total} aménagements en base "
        f"| staging : {staged} | taux : {taux}%"
    )
    logger.info(
        f"🔍 [QUALITY CHECK] "
        f"{stats[0]} arrondissements | "
        f"{stats[1]} types | "
        f"{stats[2]} km total"
    )

    if total == 0:
        raise ValueError("❌ [QUALITY CHECK] Table amenagements_cyclables vide après ELT !")

    if taux < 50:
        logger.warning(f"⚠️  [QUALITY CHECK] Taux de chargement faible : {taux}%")
    else:
        logger.info(f"✅ [QUALITY CHECK] ELT validé — taux de chargement : {taux}%")

    conn2 = psycopg2.connect(**PG_CONN)
    log_pipeline(conn2, "elt_cyclable_pipeline", "quality_check",
                 "ELT", "QUALITY_CHECK", "SUCCESS",
                 nb_lignes=total,
                 message=f"Taux={taux}% | {stats[2]}km | {stats[0]} arrondissements",
                 duree_ms=duree)
    conn2.close()


# ─────────────────────────────────────────
# DÉFINITION DU DAG
# ─────────────────────────────────────────
with DAG(
    dag_id="elt_cyclable_pipeline",
    description="ELT Aménagements cyclables : Extract CSV → Load RAW staging → Transform SQL PostgreSQL",
    schedule_interval="30 6 * * *",   # tous les jours à 6h30 (après elt_evenements à 6h00)
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

    # ── Chaîne ELT explicite ─────────────────────────
    # Extract+Load RAW → Transform SQL → Quality Check
    task_extract_load >> task_transform >> task_quality