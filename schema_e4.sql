--
-- PostgreSQL database dump
--

\restrict zYIIrR6v20P1aYz42383WdrawWeBtNhoecOXcIh7vfzvEOT2NATOfJeJZR8V7G1

-- Dumped from database version 16.13 (Debian 16.13-1.pgdg13+1)
-- Dumped by pg_dump version 16.13 (Debian 16.13-1.pgdg13+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: amenagements_cyclables; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.amenagements_cyclables (
    osm_id text NOT NULL,
    nom text,
    amenagement text,
    cote_amenagement text,
    sens text,
    surface text,
    arrondissement bigint,
    bois text,
    coronapiste text,
    amenagement_temporaire text,
    infra_bidirectionnelle text,
    voie_sens_unique text,
    position_amenagement text,
    vmax_autorisee text,
    date_export text,
    source text,
    longueur double precision,
    geo_shape text,
    geo_point_2d text
);


--
-- Name: amenagements_staging; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.amenagements_staging (
    id bigint NOT NULL,
    raw_osm_id text,
    raw_nom text,
    raw_amenagement text,
    raw_cote_amenagement text,
    raw_sens text,
    raw_surface text,
    raw_arrondissement text,
    raw_coronapiste text,
    raw_longueur text,
    raw_source text,
    raw_date_export text,
    raw_geo_point_2d text,
    raw_all text,
    loaded_at timestamp without time zone DEFAULT now()
);


--
-- Name: amenagements_staging_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.amenagements_staging_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: amenagements_staging_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.amenagements_staging_id_seq OWNED BY public.amenagements_staging.id;


--
-- Name: evenements_qfap; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.evenements_qfap (
    id text NOT NULL,
    event_id bigint,
    url text,
    titre text,
    chapeau text,
    description text,
    date_debut timestamp without time zone,
    date_fin timestamp without time zone,
    occurrences text,
    description_date text,
    url_image text,
    alt_image text,
    credit_image text,
    locations text,
    nom_lieu text,
    adresse_lieu text,
    code_postal text,
    ville text,
    coordonnees_geo text,
    "Acc├¿s PMR" double precision,
    "Acc├¿s mal voyant" double precision,
    "Acc├¿s mal entendant" double precision,
    "Acc├¿s langage des signes" double precision,
    "Acc├¿s personnes d├®ficientes mentales" double precision,
    "Transport" text,
    "Url de contact" text,
    "T├®l├®phone de contact" text,
    "Email de contact" text,
    "URL Facebook associ├®e" text,
    "URL Twitter associ├®e" text,
    "Type de prix" text,
    "D├®tail du prix" text,
    "Type d'acc├¿s" text,
    "URL de r├®servation" text,
    "URL de r├®servation - Texte" text,
    "Date de mise ├á jour" timestamp without time zone,
    "Image de couverture" double precision,
    "Programmes" text,
    "En ligne - address_url" text,
    "En ligne - address_url_text" text,
    "En ligne - address_text" text,
    title_event text,
    audience text,
    childrens text,
    "group" text,
    locale text,
    rank double precision,
    weight bigint,
    qfap_tags text,
    universe_tags text,
    event_indoor bigint,
    event_pets_allowed bigint,
    contact_organisation_name text,
    contact_url_text text,
    "URL Vim├®o associ├®e" text,
    "URL Deezer associ├®e" text,
    "URL Tiktok associ├®e" text,
    "URL Twitch associ├®e" double precision,
    "URL Spotify associ├®e" text,
    "URL YouTube associ├®e" text,
    "URL Bandcamp associ├®e" double precision,
    "URL LinkedIn associ├®e" text,
    "URL Snapchat associ├®e" double precision,
    "URL WhatsApp associ├®e" double precision,
    "URL Instagram associ├®e" text,
    "URL Messenger associ├®e" double precision,
    "URL Pinterest associ├®e" double precision,
    "URL Soundcloud associ├®e" text,
    univers text
);


--
-- Name: evenements_staging; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.evenements_staging (
    id bigint NOT NULL,
    raw_id text,
    raw_event_id text,
    raw_url text,
    raw_titre text,
    raw_description text,
    raw_lieu text,
    raw_adresse text,
    raw_cp text,
    raw_ville text,
    raw_date_debut text,
    raw_date_fin text,
    raw_coordonnees text,
    raw_transport text,
    raw_type_prix text,
    raw_all text,
    loaded_at timestamp without time zone DEFAULT now()
);


--
-- Name: evenements_staging_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.evenements_staging_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: evenements_staging_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.evenements_staging_id_seq OWNED BY public.evenements_staging.id;


--
-- Name: pipeline_logs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.pipeline_logs (
    id bigint NOT NULL,
    dag_id text NOT NULL,
    task_id text NOT NULL,
    pipeline_type text NOT NULL,
    etape text NOT NULL,
    statut text NOT NULL,
    nb_lignes integer DEFAULT 0,
    message text,
    duree_ms integer,
    executed_at timestamp without time zone DEFAULT now()
);


--
-- Name: pipeline_logs_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.pipeline_logs_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: pipeline_logs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.pipeline_logs_id_seq OWNED BY public.pipeline_logs.id;


--
-- Name: velib_disponibilite; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.velib_disponibilite (
    station_id bigint,
    nom_station text,
    station_en_fonctionnement boolean,
    capacite_station bigint,
    nb_bornettes_libres bigint,
    nb_velos_disponibles bigint,
    velos_mecaniques bigint,
    velos_electriques bigint,
    borne_paiement boolean,
    retour_velib_possible boolean,
    actualisation_donnee timestamp without time zone,
    coordonnees_geo text,
    commune text,
    code_insee bigint,
    station_opening_hours double precision
);


--
-- Name: velib_stations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.velib_stations (
    station_id text NOT NULL,
    nom_station text,
    capacite_station bigint,
    coordonnees_geo text,
    station_opening_hours double precision,
    nb_velos_disponibles integer DEFAULT 0,
    velos_electriques integer DEFAULT 0,
    velos_mecaniques integer DEFAULT 0,
    bornes_disponibles integer DEFAULT 0,
    bornes_indisponibles integer DEFAULT 0,
    est_installee boolean DEFAULT true,
    est_retournante boolean DEFAULT false,
    derniere_maj timestamp without time zone DEFAULT now()
);


--
-- Name: amenagements_staging id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.amenagements_staging ALTER COLUMN id SET DEFAULT nextval('public.amenagements_staging_id_seq'::regclass);


--
-- Name: evenements_staging id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.evenements_staging ALTER COLUMN id SET DEFAULT nextval('public.evenements_staging_id_seq'::regclass);


--
-- Name: pipeline_logs id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pipeline_logs ALTER COLUMN id SET DEFAULT nextval('public.pipeline_logs_id_seq'::regclass);


--
-- Name: amenagements_cyclables amenagements_cyclables_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.amenagements_cyclables
    ADD CONSTRAINT amenagements_cyclables_pkey PRIMARY KEY (osm_id);


--
-- Name: amenagements_staging amenagements_staging_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.amenagements_staging
    ADD CONSTRAINT amenagements_staging_pkey PRIMARY KEY (id);


--
-- Name: evenements_qfap evenements_qfap_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.evenements_qfap
    ADD CONSTRAINT evenements_qfap_pkey PRIMARY KEY (id);


--
-- Name: evenements_staging evenements_staging_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.evenements_staging
    ADD CONSTRAINT evenements_staging_pkey PRIMARY KEY (id);


--
-- Name: pipeline_logs pipeline_logs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pipeline_logs
    ADD CONSTRAINT pipeline_logs_pkey PRIMARY KEY (id);


--
-- Name: velib_stations velib_stations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.velib_stations
    ADD CONSTRAINT velib_stations_pkey PRIMARY KEY (station_id);


--
-- PostgreSQL database dump complete
--

\unrestrict zYIIrR6v20P1aYz42383WdrawWeBtNhoecOXcIh7vfzvEOT2NATOfJeJZR8V7G1

