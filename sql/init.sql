-- =========================================================
-- E4 - Data Mobilité Paris
-- Init Postgres schema
-- =========================================================

CREATE SCHEMA IF NOT EXISTS public;

-- ---------------------------------------------------------
-- 1) Vélib - Stations (référentiel)
-- Fichier: velib-emplacement-des-stations.csv
-- ---------------------------------------------------------
DROP TABLE IF EXISTS velib_stations CASCADE;

CREATE TABLE velib_stations (
  station_id                TEXT PRIMARY KEY,               -- "Identifiant station"
  nom_station               TEXT,                           -- "Nom de la station"
  capacite_station          INTEGER,                        -- "Capacité de la station"
  coordonnees_geo           TEXT,                           -- "Coordonnées géographiques"
  station_opening_hours     TEXT                            -- "station_opening_hours"
);

CREATE INDEX IF NOT EXISTS idx_velib_stations_nom
  ON velib_stations (nom_station);

-- ---------------------------------------------------------
-- 2) Vélib - Disponibilité (snapshots temps réel)
-- Fichier: velib-disponibilite-en-temps-reel.csv
-- Remarque: "Actualisation de la donnée" = timestamp snapshot
-- ---------------------------------------------------------
DROP TABLE IF EXISTS velib_disponibilite CASCADE;

CREATE TABLE velib_disponibilite (
  id                         BIGSERIAL PRIMARY KEY,
  station_id                 TEXT,                         -- "Identifiant station"
  nom_station                TEXT,                         -- "Nom station"
  station_en_fonctionnement  BOOLEAN,                      -- "Station en fonctionnement"
  capacite_station           INTEGER,                      -- "Capacité de la station"
  nb_bornettes_libres        INTEGER,                      -- "Nombre bornettes libres"
  nb_velos_disponibles       INTEGER,                      -- "Nombre total vélos disponibles"
  velos_mecaniques           INTEGER,                      -- "Vélos mécaniques disponibles"
  velos_electriques          INTEGER,                      -- "Vélos électriques disponibles"
  borne_paiement             BOOLEAN,                      -- "Borne de paiement disponible"
  retour_velib_possible      BOOLEAN,                      -- "Retour vélib possible"
  actualisation_donnee       TIMESTAMP,                    -- "Actualisation de la donnée"
  coordonnees_geo            TEXT,                         -- "Coordonnées géographiques"
  commune                    TEXT,                         -- "Nom communes équipées"
  code_insee                 TEXT,                         -- "Code INSEE communes équipées"
  station_opening_hours      TEXT,                         -- "station_opening_hours"
  inserted_at                TIMESTAMP DEFAULT NOW()
);

-- Index pratiques
CREATE INDEX IF NOT EXISTS idx_velib_dispo_station_time
  ON velib_disponibilite (station_id, actualisation_donnee DESC);

CREATE INDEX IF NOT EXISTS idx_velib_dispo_actualisation
  ON velib_disponibilite (actualisation_donnee DESC);

-- FK optionnelle (on la met en "NOT VALID" pour éviter de bloquer si données sales)
ALTER TABLE velib_disponibilite
  ADD CONSTRAINT fk_velib_dispo_station
  FOREIGN KEY (station_id) REFERENCES velib_stations(station_id)
  NOT VALID;

-- ---------------------------------------------------------
-- 3) Événements - "Que faire à Paris"
-- Fichier: que-faire-a-paris.csv
-- Vu le nombre de colonnes: on stocke beaucoup en TEXT
-- + on typpe dates principales.
-- ---------------------------------------------------------
DROP TABLE IF EXISTS evenements_qfap CASCADE;

CREATE TABLE evenements_qfap (
  id                         TEXT PRIMARY KEY,             -- "ID"
  event_id                   TEXT,                         -- "Event ID"
  url                        TEXT,                         -- "URL"
  titre                      TEXT,                         -- "Titre"
  chapeau                    TEXT,                         -- "Chapeau"
  description                TEXT,                         -- "Description"
  date_debut                 TIMESTAMP,                    -- "Date de début"
  date_fin                   TIMESTAMP,                    -- "Date de fin"
  occurrences                TEXT,                         -- "Occurrences"
  description_date           TEXT,                         -- "Description de la date"
  url_image                  TEXT,                         -- "URL de l'image"
  alt_image                  TEXT,                         -- "Texte alternatif de l'image"
  credit_image               TEXT,                         -- "Crédit de l'image"
  locations                  TEXT,                         -- "locations"
  nom_lieu                   TEXT,                         -- "Nom du lieu"
  adresse_lieu               TEXT,                         -- "Adresse du lieu"
  code_postal                TEXT,                         -- "Code postal"
  ville                      TEXT,                         -- "Ville"
  coordonnees_geo            TEXT,                         -- "Coordonnées géographiques"

  acces_pmr                  TEXT,                         -- "Accès PMR"
  acces_mal_voyant           TEXT,                         -- "Accès mal voyant"
  acces_mal_entendant        TEXT,                         -- "Accès mal entendant"
  acces_lsf                  TEXT,                         -- "Accès langage des signes"
  acces_def_mentales         TEXT,                         -- "Accès personnes déficientes mentales"

  transport                  TEXT,                         -- "Transport"
  url_contact                TEXT,                         -- "Url de contact"
  telephone_contact          TEXT,                         -- "Téléphone de contact"
  email_contact              TEXT,                         -- "Email de contact"

  url_facebook               TEXT,                         -- "URL Facebook associée"
  url_twitter                TEXT,                         -- "URL Twitter associée"

  type_prix                  TEXT,                         -- "Type de prix"
  detail_prix                TEXT,                         -- "Détail du prix"
  type_acces                 TEXT,                         -- "Type d'accès"

  url_reservation            TEXT,                         -- "URL de réservation"
  url_reservation_texte      TEXT,                         -- "URL de réservation - Texte"
  date_maj                   TIMESTAMP,                    -- "Date de mise à jour"

  image_couverture           TEXT,                         -- "Image de couverture"
  programmes                 TEXT,                         -- "Programmes"

  en_ligne_address_url       TEXT,                         -- "En ligne - address_url"
  en_ligne_address_url_text  TEXT,                         -- "En ligne - address_url_text"
  en_ligne_address_text      TEXT,                         -- "En ligne - address_text"

  title_event                TEXT,
  audience                   TEXT,
  childrens                  TEXT,
  "group"                    TEXT,
  locale                     TEXT,
  rank                       TEXT,
  weight                     TEXT,
  qfap_tags                  TEXT,
  universe_tags              TEXT,
  event_indoor               TEXT,
  event_pets_allowed         TEXT,
  contact_organisation_name  TEXT,
  contact_url_text           TEXT,

  url_vimeo                  TEXT,
  url_deezer                 TEXT,
  url_tiktok                 TEXT,
  url_twitch                 TEXT,
  url_spotify                TEXT,
  url_youtube                TEXT,
  url_bandcamp               TEXT,
  url_linkedin               TEXT,
  url_snapchat               TEXT,
  url_whatsapp               TEXT,
  url_instagram              TEXT,
  url_messenger              TEXT,
  url_pinterest              TEXT,
  url_soundcloud             TEXT,

  univers                    TEXT,
  inserted_at                TIMESTAMP DEFAULT NOW()
);

-- Index utiles pour recherche
CREATE INDEX IF NOT EXISTS idx_evenements_dates
  ON evenements_qfap (date_debut, date_fin);

CREATE INDEX IF NOT EXISTS idx_evenements_code_postal
  ON evenements_qfap (code_postal);

-- ---------------------------------------------------------
-- 4) Aménagements cyclables
-- Fichier: amenagements-cyclables.csv
-- ---------------------------------------------------------
DROP TABLE IF EXISTS amenagements_cyclables CASCADE;

CREATE TABLE amenagements_cyclables (
  osm_id                      TEXT PRIMARY KEY,            -- "OSM_id"
  nom                         TEXT,                       -- "Nom"
  amenagement                 TEXT,                       -- "Aménagement"
  cote_amenagement            TEXT,                       -- "Côté aménagement"
  sens                        TEXT,                       -- "Sens"
  surface                     TEXT,                       -- "Surface"
  arrondissement              TEXT,                       -- "Arrondissement"
  bois                        TEXT,                       -- "Bois"
  coronapiste                 TEXT,                       -- "Coronapiste"
  amenagement_temporaire      TEXT,                       -- "Aménagement temporaire"
  infra_bidirectionnelle      TEXT,                       -- "Infrastructure bidirectionnelle"
  voie_sens_unique            TEXT,                       -- "Voie à sens unique"
  position_amenagement        TEXT,                       -- "Position aménagement"
  vmax_autorisee              TEXT,                       -- "Vitesse maximale autorisée"
  date_export                 TEXT,                       -- "Date export"
  source                      TEXT,                       -- "Source"
  longueur                    NUMERIC,                    -- "Longueur"
  geo_shape                   TEXT,                       -- "geo_shape"
  geo_point_2d                TEXT,                       -- "geo_point_2d"
  inserted_at                 TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_amenagements_arrondissement
  ON amenagements_cyclables (arrondissement);

CREATE INDEX IF NOT EXISTS idx_amenagements_type
  ON amenagements_cyclables (amenagement);

  -- =========================================================
-- 5) STAGING — Événements bruts (logique ELT)
-- Utilisé par le DAG elt_evenements_pipeline
-- Extract → Load RAW ici → Transform SQL vers evenements_qfap
-- =========================================================
DROP TABLE IF EXISTS evenements_staging CASCADE;

CREATE TABLE evenements_staging (
  id               BIGSERIAL PRIMARY KEY,
  raw_id           TEXT,                  -- "ID" brut
  raw_event_id     TEXT,                  -- "Event ID" brut
  raw_url          TEXT,                  -- "URL" brute
  raw_titre        TEXT,                  -- "Titre" brut
  raw_description  TEXT,                  -- "Description" brute
  raw_lieu         TEXT,                  -- "Nom du lieu" brut
  raw_adresse      TEXT,                  -- "Adresse du lieu" brute
  raw_cp           TEXT,                  -- "Code postal" brut (TEXT : pas encore validé)
  raw_ville        TEXT,                  -- "Ville" brute
  raw_date_debut   TEXT,                  -- TEXT volontaire : pas encore casté en TIMESTAMP
  raw_date_fin     TEXT,                  -- TEXT volontaire : pas encore casté en TIMESTAMP
  raw_coordonnees  TEXT,                  -- "Coordonnées géographiques" brutes
  raw_transport    TEXT,                  -- "Transport" brut
  raw_type_prix    TEXT,                  -- "Type de prix" brut
  raw_all          TEXT,                  -- JSON complet de la ligne brute (audit/traçabilité)
  loaded_at        TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_evt_staging_cp
  ON evenements_staging (raw_cp);

CREATE INDEX IF NOT EXISTS idx_evt_staging_loaded
  ON evenements_staging (loaded_at DESC);

COMMENT ON TABLE evenements_staging IS
  'ELT — Table staging : données brutes CSV evenements avant transformation SQL vers evenements_qfap';

-- =========================================================
-- 6) STAGING — Aménagements cyclables bruts (logique ELT)
-- Utilisé par le DAG elt_cyclable_pipeline
-- =========================================================
DROP TABLE IF EXISTS amenagements_staging CASCADE;

CREATE TABLE amenagements_staging (
  id                    BIGSERIAL PRIMARY KEY,
  raw_osm_id            TEXT,             -- "OSM_id" brut
  raw_nom               TEXT,             -- "Nom" brut
  raw_amenagement       TEXT,             -- "Aménagement" brut
  raw_cote_amenagement  TEXT,             -- "Côté aménagement" brut
  raw_sens              TEXT,             -- "Sens" brut
  raw_surface           TEXT,             -- "Surface" brute
  raw_arrondissement    TEXT,             -- "Arrondissement" brut
  raw_coronapiste       TEXT,             -- "Coronapiste" brut
  raw_longueur          TEXT,             -- TEXT volontaire : pas encore casté en NUMERIC
  raw_source            TEXT,             -- "Source" brute
  raw_date_export       TEXT,             -- "Date export" brute (TEXT : pas encore casté)
  raw_geo_point_2d      TEXT,             -- "geo_point_2d" brut
  raw_all               TEXT,             -- JSON complet de la ligne brute (audit/traçabilité)
  loaded_at             TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cycl_staging_arrdt
  ON amenagements_staging (raw_arrondissement);

CREATE INDEX IF NOT EXISTS idx_cycl_staging_loaded
  ON amenagements_staging (loaded_at DESC);

COMMENT ON TABLE amenagements_staging IS
  'ELT — Table staging : données brutes CSV amenagements avant transformation SQL vers amenagements_cyclables';

-- =========================================================
-- 7) LOGS PIPELINE — Traçabilité des exécutions DAG
-- Alimenté par chaque DAG (ETL et ELT)
-- Permet de prouver l'automatisation pour E4
-- =========================================================
DROP TABLE IF EXISTS pipeline_logs CASCADE;

CREATE TABLE pipeline_logs (
  id             BIGSERIAL PRIMARY KEY,
  dag_id         TEXT        NOT NULL,          -- ex: 'etl_velib_pipeline'
  task_id        TEXT        NOT NULL,          -- ex: 'load_velib'
  pipeline_type  TEXT        NOT NULL,          -- 'ETL' ou 'ELT'
  etape          TEXT        NOT NULL,          -- 'EXTRACT' | 'TRANSFORM' | 'LOAD'
  statut         TEXT        NOT NULL,          -- 'SUCCESS' | 'ERROR'
  nb_lignes      INTEGER     DEFAULT 0,         -- nb de lignes traitées
  message        TEXT,                          -- détail ou erreur
  duree_ms       INTEGER,                       -- durée en millisecondes
  executed_at    TIMESTAMP   DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_logs_dag
  ON pipeline_logs (dag_id, executed_at DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_logs_statut
  ON pipeline_logs (statut, executed_at DESC);

COMMENT ON TABLE pipeline_logs IS
  'Traçabilité complète des pipelines ETL/ELT — alimentation automatique par les DAGs Airflow';