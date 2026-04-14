--
-- PostgreSQL database dump
--

\restrict D5QOBGI5SxdKJoBHWmQr6XxBhRK0a7BC4qctYN2cALZ17KqmpcTURO6Yw8beFbs

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
-- Name: ab_permission; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ab_permission (
    id integer NOT NULL,
    name character varying(100) NOT NULL
);


--
-- Name: ab_permission_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.ab_permission_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: ab_permission_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.ab_permission_id_seq OWNED BY public.ab_permission.id;


--
-- Name: ab_permission_view; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ab_permission_view (
    id integer NOT NULL,
    permission_id integer,
    view_menu_id integer
);


--
-- Name: ab_permission_view_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.ab_permission_view_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: ab_permission_view_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.ab_permission_view_id_seq OWNED BY public.ab_permission_view.id;


--
-- Name: ab_permission_view_role; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ab_permission_view_role (
    id integer NOT NULL,
    permission_view_id integer,
    role_id integer
);


--
-- Name: ab_permission_view_role_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.ab_permission_view_role_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: ab_permission_view_role_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.ab_permission_view_role_id_seq OWNED BY public.ab_permission_view_role.id;


--
-- Name: ab_register_user; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ab_register_user (
    id integer NOT NULL,
    first_name character varying(256) NOT NULL,
    last_name character varying(256) NOT NULL,
    username character varying(512) NOT NULL,
    password character varying(256),
    email character varying(512) NOT NULL,
    registration_date timestamp without time zone,
    registration_hash character varying(256)
);


--
-- Name: ab_register_user_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.ab_register_user_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: ab_register_user_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.ab_register_user_id_seq OWNED BY public.ab_register_user.id;


--
-- Name: ab_role; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ab_role (
    id integer NOT NULL,
    name character varying(64) NOT NULL
);


--
-- Name: ab_role_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.ab_role_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: ab_role_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.ab_role_id_seq OWNED BY public.ab_role.id;


--
-- Name: ab_user; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ab_user (
    id integer NOT NULL,
    first_name character varying(256) NOT NULL,
    last_name character varying(256) NOT NULL,
    username character varying(512) NOT NULL,
    password character varying(256),
    active boolean,
    email character varying(512) NOT NULL,
    last_login timestamp without time zone,
    login_count integer,
    fail_login_count integer,
    created_on timestamp without time zone,
    changed_on timestamp without time zone,
    created_by_fk integer,
    changed_by_fk integer
);


--
-- Name: ab_user_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.ab_user_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: ab_user_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.ab_user_id_seq OWNED BY public.ab_user.id;


--
-- Name: ab_user_role; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ab_user_role (
    id integer NOT NULL,
    user_id integer,
    role_id integer
);


--
-- Name: ab_user_role_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.ab_user_role_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: ab_user_role_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.ab_user_role_id_seq OWNED BY public.ab_user_role.id;


--
-- Name: ab_view_menu; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ab_view_menu (
    id integer NOT NULL,
    name character varying(250) NOT NULL
);


--
-- Name: ab_view_menu_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.ab_view_menu_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: ab_view_menu_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.ab_view_menu_id_seq OWNED BY public.ab_view_menu.id;


--
-- Name: alembic_version; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.alembic_version (
    version_num character varying(32) NOT NULL
);


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
-- Name: callback_request; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.callback_request (
    id integer NOT NULL,
    created_at timestamp with time zone NOT NULL,
    priority_weight integer NOT NULL,
    callback_data json NOT NULL,
    callback_type character varying(20) NOT NULL,
    processor_subdir character varying(2000)
);


--
-- Name: callback_request_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.callback_request_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: callback_request_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.callback_request_id_seq OWNED BY public.callback_request.id;


--
-- Name: connection; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.connection (
    id integer NOT NULL,
    conn_id character varying(250) NOT NULL,
    conn_type character varying(500) NOT NULL,
    description text,
    host character varying(500),
    schema character varying(500),
    login text,
    password text,
    port integer,
    is_encrypted boolean,
    is_extra_encrypted boolean,
    extra text
);


--
-- Name: connection_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.connection_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: connection_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.connection_id_seq OWNED BY public.connection.id;


--
-- Name: dag; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dag (
    dag_id character varying(250) NOT NULL,
    root_dag_id character varying(250),
    is_paused boolean,
    is_subdag boolean,
    is_active boolean,
    last_parsed_time timestamp with time zone,
    last_pickled timestamp with time zone,
    last_expired timestamp with time zone,
    scheduler_lock boolean,
    pickle_id integer,
    fileloc character varying(2000),
    processor_subdir character varying(2000),
    owners character varying(2000),
    description text,
    default_view character varying(25),
    schedule_interval text,
    timetable_description character varying(1000),
    max_active_tasks integer NOT NULL,
    max_active_runs integer,
    has_task_concurrency_limits boolean NOT NULL,
    has_import_errors boolean DEFAULT false,
    next_dagrun timestamp with time zone,
    next_dagrun_data_interval_start timestamp with time zone,
    next_dagrun_data_interval_end timestamp with time zone,
    next_dagrun_create_after timestamp with time zone
);


--
-- Name: dag_code; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dag_code (
    fileloc_hash bigint NOT NULL,
    fileloc character varying(2000) NOT NULL,
    last_updated timestamp with time zone NOT NULL,
    source_code text NOT NULL
);


--
-- Name: dag_owner_attributes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dag_owner_attributes (
    dag_id character varying(250) NOT NULL,
    owner character varying(500) NOT NULL,
    link character varying(500) NOT NULL
);


--
-- Name: dag_pickle; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dag_pickle (
    id integer NOT NULL,
    pickle bytea,
    created_dttm timestamp with time zone,
    pickle_hash bigint
);


--
-- Name: dag_pickle_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.dag_pickle_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: dag_pickle_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.dag_pickle_id_seq OWNED BY public.dag_pickle.id;


--
-- Name: dag_run; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dag_run (
    id integer NOT NULL,
    dag_id character varying(250) NOT NULL,
    queued_at timestamp with time zone,
    execution_date timestamp with time zone NOT NULL,
    start_date timestamp with time zone,
    end_date timestamp with time zone,
    state character varying(50),
    run_id character varying(250) NOT NULL,
    creating_job_id integer,
    external_trigger boolean,
    run_type character varying(50) NOT NULL,
    conf bytea,
    data_interval_start timestamp with time zone,
    data_interval_end timestamp with time zone,
    last_scheduling_decision timestamp with time zone,
    dag_hash character varying(32),
    log_template_id integer,
    updated_at timestamp with time zone,
    clear_number integer NOT NULL
);


--
-- Name: dag_run_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.dag_run_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: dag_run_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.dag_run_id_seq OWNED BY public.dag_run.id;


--
-- Name: dag_run_note; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dag_run_note (
    user_id integer,
    dag_run_id integer NOT NULL,
    content character varying(1000),
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);


--
-- Name: dag_schedule_dataset_reference; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dag_schedule_dataset_reference (
    dataset_id integer NOT NULL,
    dag_id character varying(250) NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);


--
-- Name: dag_tag; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dag_tag (
    name character varying(100) NOT NULL,
    dag_id character varying(250) NOT NULL
);


--
-- Name: dag_warning; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dag_warning (
    dag_id character varying(250) NOT NULL,
    warning_type character varying(50) NOT NULL,
    message text NOT NULL,
    "timestamp" timestamp with time zone NOT NULL
);


--
-- Name: dagrun_dataset_event; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dagrun_dataset_event (
    dag_run_id integer NOT NULL,
    event_id integer NOT NULL
);


--
-- Name: dataset; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dataset (
    id integer NOT NULL,
    uri character varying(3000) NOT NULL,
    extra json NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    is_orphaned boolean DEFAULT false NOT NULL
);


--
-- Name: dataset_dag_run_queue; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dataset_dag_run_queue (
    dataset_id integer NOT NULL,
    target_dag_id character varying(250) NOT NULL,
    created_at timestamp with time zone NOT NULL
);


--
-- Name: dataset_event; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dataset_event (
    id integer NOT NULL,
    dataset_id integer NOT NULL,
    extra json NOT NULL,
    source_task_id character varying(250),
    source_dag_id character varying(250),
    source_run_id character varying(250),
    source_map_index integer DEFAULT '-1'::integer,
    "timestamp" timestamp with time zone NOT NULL
);


--
-- Name: dataset_event_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.dataset_event_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: dataset_event_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.dataset_event_id_seq OWNED BY public.dataset_event.id;


--
-- Name: dataset_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.dataset_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: dataset_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.dataset_id_seq OWNED BY public.dataset.id;


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
-- Name: import_error; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.import_error (
    id integer NOT NULL,
    "timestamp" timestamp with time zone,
    filename character varying(1024),
    stacktrace text,
    processor_subdir character varying(2000)
);


--
-- Name: import_error_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.import_error_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: import_error_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.import_error_id_seq OWNED BY public.import_error.id;


--
-- Name: job; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.job (
    id integer NOT NULL,
    dag_id character varying(250),
    state character varying(20),
    job_type character varying(30),
    start_date timestamp with time zone,
    end_date timestamp with time zone,
    latest_heartbeat timestamp with time zone,
    executor_class character varying(500),
    hostname character varying(500),
    unixname character varying(1000)
);


--
-- Name: job_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.job_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: job_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.job_id_seq OWNED BY public.job.id;


--
-- Name: log; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.log (
    id integer NOT NULL,
    dttm timestamp with time zone,
    dag_id character varying(250),
    task_id character varying(250),
    map_index integer,
    event character varying(30),
    execution_date timestamp with time zone,
    owner character varying(500),
    owner_display_name character varying(500),
    extra text
);


--
-- Name: log_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.log_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: log_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.log_id_seq OWNED BY public.log.id;


--
-- Name: log_template; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.log_template (
    id integer NOT NULL,
    filename text NOT NULL,
    elasticsearch_id text NOT NULL,
    created_at timestamp with time zone NOT NULL
);


--
-- Name: log_template_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.log_template_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: log_template_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.log_template_id_seq OWNED BY public.log_template.id;


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
-- Name: rendered_task_instance_fields; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.rendered_task_instance_fields (
    dag_id character varying(250) NOT NULL,
    task_id character varying(250) NOT NULL,
    run_id character varying(250) NOT NULL,
    map_index integer DEFAULT '-1'::integer NOT NULL,
    rendered_fields json NOT NULL,
    k8s_pod_yaml json
);


--
-- Name: serialized_dag; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.serialized_dag (
    dag_id character varying(250) NOT NULL,
    fileloc character varying(2000) NOT NULL,
    fileloc_hash bigint NOT NULL,
    data json,
    data_compressed bytea,
    last_updated timestamp with time zone NOT NULL,
    dag_hash character varying(32) NOT NULL,
    processor_subdir character varying(2000)
);


--
-- Name: session; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.session (
    id integer NOT NULL,
    session_id character varying(255),
    data bytea,
    expiry timestamp without time zone
);


--
-- Name: session_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.session_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: session_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.session_id_seq OWNED BY public.session.id;


--
-- Name: sla_miss; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.sla_miss (
    task_id character varying(250) NOT NULL,
    dag_id character varying(250) NOT NULL,
    execution_date timestamp with time zone NOT NULL,
    email_sent boolean,
    "timestamp" timestamp with time zone,
    description text,
    notification_sent boolean
);


--
-- Name: slot_pool; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.slot_pool (
    id integer NOT NULL,
    pool character varying(256),
    slots integer,
    description text,
    include_deferred boolean NOT NULL
);


--
-- Name: slot_pool_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.slot_pool_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: slot_pool_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.slot_pool_id_seq OWNED BY public.slot_pool.id;


--
-- Name: task_fail; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.task_fail (
    id integer NOT NULL,
    task_id character varying(250) NOT NULL,
    dag_id character varying(250) NOT NULL,
    run_id character varying(250) NOT NULL,
    map_index integer DEFAULT '-1'::integer NOT NULL,
    start_date timestamp with time zone,
    end_date timestamp with time zone,
    duration integer
);


--
-- Name: task_fail_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.task_fail_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: task_fail_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.task_fail_id_seq OWNED BY public.task_fail.id;


--
-- Name: task_instance; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.task_instance (
    task_id character varying(250) NOT NULL,
    dag_id character varying(250) NOT NULL,
    run_id character varying(250) NOT NULL,
    map_index integer DEFAULT '-1'::integer NOT NULL,
    start_date timestamp with time zone,
    end_date timestamp with time zone,
    duration double precision,
    state character varying(20),
    try_number integer,
    max_tries integer DEFAULT '-1'::integer,
    hostname character varying(1000),
    unixname character varying(1000),
    job_id integer,
    pool character varying(256) NOT NULL,
    pool_slots integer NOT NULL,
    queue character varying(256),
    priority_weight integer,
    operator character varying(1000),
    custom_operator_name character varying(1000),
    queued_dttm timestamp with time zone,
    queued_by_job_id integer,
    pid integer,
    executor_config bytea,
    updated_at timestamp with time zone,
    external_executor_id character varying(250),
    trigger_id integer,
    trigger_timeout timestamp without time zone,
    next_method character varying(1000),
    next_kwargs json
);


--
-- Name: task_instance_note; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.task_instance_note (
    user_id integer,
    task_id character varying(250) NOT NULL,
    dag_id character varying(250) NOT NULL,
    run_id character varying(250) NOT NULL,
    map_index integer NOT NULL,
    content character varying(1000),
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);


--
-- Name: task_map; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.task_map (
    dag_id character varying(250) NOT NULL,
    task_id character varying(250) NOT NULL,
    run_id character varying(250) NOT NULL,
    map_index integer NOT NULL,
    length integer NOT NULL,
    keys json,
    CONSTRAINT ck_task_map_task_map_length_not_negative CHECK ((length >= 0))
);


--
-- Name: task_outlet_dataset_reference; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.task_outlet_dataset_reference (
    dataset_id integer NOT NULL,
    dag_id character varying(250) NOT NULL,
    task_id character varying(250) NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);


--
-- Name: task_reschedule; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.task_reschedule (
    id integer NOT NULL,
    task_id character varying(250) NOT NULL,
    dag_id character varying(250) NOT NULL,
    run_id character varying(250) NOT NULL,
    map_index integer DEFAULT '-1'::integer NOT NULL,
    try_number integer NOT NULL,
    start_date timestamp with time zone NOT NULL,
    end_date timestamp with time zone NOT NULL,
    duration integer NOT NULL,
    reschedule_date timestamp with time zone NOT NULL
);


--
-- Name: task_reschedule_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.task_reschedule_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: task_reschedule_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.task_reschedule_id_seq OWNED BY public.task_reschedule.id;


--
-- Name: trigger; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.trigger (
    id integer NOT NULL,
    classpath character varying(1000) NOT NULL,
    kwargs json NOT NULL,
    created_date timestamp with time zone NOT NULL,
    triggerer_id integer
);


--
-- Name: trigger_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.trigger_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: trigger_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.trigger_id_seq OWNED BY public.trigger.id;


--
-- Name: variable; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.variable (
    id integer NOT NULL,
    key character varying(250),
    val text,
    description text,
    is_encrypted boolean
);


--
-- Name: variable_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.variable_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: variable_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.variable_id_seq OWNED BY public.variable.id;


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
-- Name: xcom; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.xcom (
    dag_run_id integer NOT NULL,
    task_id character varying(250) NOT NULL,
    map_index integer DEFAULT '-1'::integer NOT NULL,
    key character varying(512) NOT NULL,
    dag_id character varying(250) NOT NULL,
    run_id character varying(250) NOT NULL,
    value bytea,
    "timestamp" timestamp with time zone NOT NULL
);


--
-- Name: ab_permission id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ab_permission ALTER COLUMN id SET DEFAULT nextval('public.ab_permission_id_seq'::regclass);


--
-- Name: ab_permission_view id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ab_permission_view ALTER COLUMN id SET DEFAULT nextval('public.ab_permission_view_id_seq'::regclass);


--
-- Name: ab_permission_view_role id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ab_permission_view_role ALTER COLUMN id SET DEFAULT nextval('public.ab_permission_view_role_id_seq'::regclass);


--
-- Name: ab_register_user id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ab_register_user ALTER COLUMN id SET DEFAULT nextval('public.ab_register_user_id_seq'::regclass);


--
-- Name: ab_role id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ab_role ALTER COLUMN id SET DEFAULT nextval('public.ab_role_id_seq'::regclass);


--
-- Name: ab_user id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ab_user ALTER COLUMN id SET DEFAULT nextval('public.ab_user_id_seq'::regclass);


--
-- Name: ab_user_role id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ab_user_role ALTER COLUMN id SET DEFAULT nextval('public.ab_user_role_id_seq'::regclass);


--
-- Name: ab_view_menu id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ab_view_menu ALTER COLUMN id SET DEFAULT nextval('public.ab_view_menu_id_seq'::regclass);


--
-- Name: amenagements_staging id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.amenagements_staging ALTER COLUMN id SET DEFAULT nextval('public.amenagements_staging_id_seq'::regclass);


--
-- Name: callback_request id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.callback_request ALTER COLUMN id SET DEFAULT nextval('public.callback_request_id_seq'::regclass);


--
-- Name: connection id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.connection ALTER COLUMN id SET DEFAULT nextval('public.connection_id_seq'::regclass);


--
-- Name: dag_pickle id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dag_pickle ALTER COLUMN id SET DEFAULT nextval('public.dag_pickle_id_seq'::regclass);


--
-- Name: dag_run id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dag_run ALTER COLUMN id SET DEFAULT nextval('public.dag_run_id_seq'::regclass);


--
-- Name: dataset id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dataset ALTER COLUMN id SET DEFAULT nextval('public.dataset_id_seq'::regclass);


--
-- Name: dataset_event id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dataset_event ALTER COLUMN id SET DEFAULT nextval('public.dataset_event_id_seq'::regclass);


--
-- Name: evenements_staging id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.evenements_staging ALTER COLUMN id SET DEFAULT nextval('public.evenements_staging_id_seq'::regclass);


--
-- Name: import_error id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.import_error ALTER COLUMN id SET DEFAULT nextval('public.import_error_id_seq'::regclass);


--
-- Name: job id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.job ALTER COLUMN id SET DEFAULT nextval('public.job_id_seq'::regclass);


--
-- Name: log id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.log ALTER COLUMN id SET DEFAULT nextval('public.log_id_seq'::regclass);


--
-- Name: log_template id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.log_template ALTER COLUMN id SET DEFAULT nextval('public.log_template_id_seq'::regclass);


--
-- Name: pipeline_logs id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pipeline_logs ALTER COLUMN id SET DEFAULT nextval('public.pipeline_logs_id_seq'::regclass);


--
-- Name: session id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.session ALTER COLUMN id SET DEFAULT nextval('public.session_id_seq'::regclass);


--
-- Name: slot_pool id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.slot_pool ALTER COLUMN id SET DEFAULT nextval('public.slot_pool_id_seq'::regclass);


--
-- Name: task_fail id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_fail ALTER COLUMN id SET DEFAULT nextval('public.task_fail_id_seq'::regclass);


--
-- Name: task_reschedule id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_reschedule ALTER COLUMN id SET DEFAULT nextval('public.task_reschedule_id_seq'::regclass);


--
-- Name: trigger id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trigger ALTER COLUMN id SET DEFAULT nextval('public.trigger_id_seq'::regclass);


--
-- Name: variable id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.variable ALTER COLUMN id SET DEFAULT nextval('public.variable_id_seq'::regclass);


--
-- Name: ab_permission ab_permission_name_uq; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ab_permission
    ADD CONSTRAINT ab_permission_name_uq UNIQUE (name);


--
-- Name: ab_permission ab_permission_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ab_permission
    ADD CONSTRAINT ab_permission_pkey PRIMARY KEY (id);


--
-- Name: ab_permission_view ab_permission_view_permission_id_view_menu_id_uq; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ab_permission_view
    ADD CONSTRAINT ab_permission_view_permission_id_view_menu_id_uq UNIQUE (permission_id, view_menu_id);


--
-- Name: ab_permission_view ab_permission_view_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ab_permission_view
    ADD CONSTRAINT ab_permission_view_pkey PRIMARY KEY (id);


--
-- Name: ab_permission_view_role ab_permission_view_role_permission_view_id_role_id_uq; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ab_permission_view_role
    ADD CONSTRAINT ab_permission_view_role_permission_view_id_role_id_uq UNIQUE (permission_view_id, role_id);


--
-- Name: ab_permission_view_role ab_permission_view_role_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ab_permission_view_role
    ADD CONSTRAINT ab_permission_view_role_pkey PRIMARY KEY (id);


--
-- Name: ab_register_user ab_register_user_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ab_register_user
    ADD CONSTRAINT ab_register_user_pkey PRIMARY KEY (id);


--
-- Name: ab_register_user ab_register_user_username_uq; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ab_register_user
    ADD CONSTRAINT ab_register_user_username_uq UNIQUE (username);


--
-- Name: ab_role ab_role_name_uq; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ab_role
    ADD CONSTRAINT ab_role_name_uq UNIQUE (name);


--
-- Name: ab_role ab_role_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ab_role
    ADD CONSTRAINT ab_role_pkey PRIMARY KEY (id);


--
-- Name: ab_user ab_user_email_uq; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ab_user
    ADD CONSTRAINT ab_user_email_uq UNIQUE (email);


--
-- Name: ab_user ab_user_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ab_user
    ADD CONSTRAINT ab_user_pkey PRIMARY KEY (id);


--
-- Name: ab_user_role ab_user_role_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ab_user_role
    ADD CONSTRAINT ab_user_role_pkey PRIMARY KEY (id);


--
-- Name: ab_user_role ab_user_role_user_id_role_id_uq; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ab_user_role
    ADD CONSTRAINT ab_user_role_user_id_role_id_uq UNIQUE (user_id, role_id);


--
-- Name: ab_user ab_user_username_uq; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ab_user
    ADD CONSTRAINT ab_user_username_uq UNIQUE (username);


--
-- Name: ab_view_menu ab_view_menu_name_uq; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ab_view_menu
    ADD CONSTRAINT ab_view_menu_name_uq UNIQUE (name);


--
-- Name: ab_view_menu ab_view_menu_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ab_view_menu
    ADD CONSTRAINT ab_view_menu_pkey PRIMARY KEY (id);


--
-- Name: alembic_version alembic_version_pkc; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.alembic_version
    ADD CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num);


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
-- Name: callback_request callback_request_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.callback_request
    ADD CONSTRAINT callback_request_pkey PRIMARY KEY (id);


--
-- Name: connection connection_conn_id_uq; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.connection
    ADD CONSTRAINT connection_conn_id_uq UNIQUE (conn_id);


--
-- Name: connection connection_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.connection
    ADD CONSTRAINT connection_pkey PRIMARY KEY (id);


--
-- Name: dag_code dag_code_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dag_code
    ADD CONSTRAINT dag_code_pkey PRIMARY KEY (fileloc_hash);


--
-- Name: dag_owner_attributes dag_owner_attributes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dag_owner_attributes
    ADD CONSTRAINT dag_owner_attributes_pkey PRIMARY KEY (dag_id, owner);


--
-- Name: dag_pickle dag_pickle_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dag_pickle
    ADD CONSTRAINT dag_pickle_pkey PRIMARY KEY (id);


--
-- Name: dag dag_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dag
    ADD CONSTRAINT dag_pkey PRIMARY KEY (dag_id);


--
-- Name: dag_run dag_run_dag_id_execution_date_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dag_run
    ADD CONSTRAINT dag_run_dag_id_execution_date_key UNIQUE (dag_id, execution_date);


--
-- Name: dag_run dag_run_dag_id_run_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dag_run
    ADD CONSTRAINT dag_run_dag_id_run_id_key UNIQUE (dag_id, run_id);


--
-- Name: dag_run_note dag_run_note_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dag_run_note
    ADD CONSTRAINT dag_run_note_pkey PRIMARY KEY (dag_run_id);


--
-- Name: dag_run dag_run_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dag_run
    ADD CONSTRAINT dag_run_pkey PRIMARY KEY (id);


--
-- Name: dag_tag dag_tag_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dag_tag
    ADD CONSTRAINT dag_tag_pkey PRIMARY KEY (name, dag_id);


--
-- Name: dag_warning dag_warning_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dag_warning
    ADD CONSTRAINT dag_warning_pkey PRIMARY KEY (dag_id, warning_type);


--
-- Name: dagrun_dataset_event dagrun_dataset_event_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dagrun_dataset_event
    ADD CONSTRAINT dagrun_dataset_event_pkey PRIMARY KEY (dag_run_id, event_id);


--
-- Name: dataset_event dataset_event_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dataset_event
    ADD CONSTRAINT dataset_event_pkey PRIMARY KEY (id);


--
-- Name: dataset dataset_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dataset
    ADD CONSTRAINT dataset_pkey PRIMARY KEY (id);


--
-- Name: dataset_dag_run_queue datasetdagrunqueue_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dataset_dag_run_queue
    ADD CONSTRAINT datasetdagrunqueue_pkey PRIMARY KEY (dataset_id, target_dag_id);


--
-- Name: dag_schedule_dataset_reference dsdr_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dag_schedule_dataset_reference
    ADD CONSTRAINT dsdr_pkey PRIMARY KEY (dataset_id, dag_id);


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
-- Name: import_error import_error_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.import_error
    ADD CONSTRAINT import_error_pkey PRIMARY KEY (id);


--
-- Name: job job_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.job
    ADD CONSTRAINT job_pkey PRIMARY KEY (id);


--
-- Name: log log_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.log
    ADD CONSTRAINT log_pkey PRIMARY KEY (id);


--
-- Name: log_template log_template_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.log_template
    ADD CONSTRAINT log_template_pkey PRIMARY KEY (id);


--
-- Name: pipeline_logs pipeline_logs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pipeline_logs
    ADD CONSTRAINT pipeline_logs_pkey PRIMARY KEY (id);


--
-- Name: rendered_task_instance_fields rendered_task_instance_fields_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.rendered_task_instance_fields
    ADD CONSTRAINT rendered_task_instance_fields_pkey PRIMARY KEY (dag_id, task_id, run_id, map_index);


--
-- Name: serialized_dag serialized_dag_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.serialized_dag
    ADD CONSTRAINT serialized_dag_pkey PRIMARY KEY (dag_id);


--
-- Name: session session_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.session
    ADD CONSTRAINT session_pkey PRIMARY KEY (id);


--
-- Name: session session_session_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.session
    ADD CONSTRAINT session_session_id_key UNIQUE (session_id);


--
-- Name: sla_miss sla_miss_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sla_miss
    ADD CONSTRAINT sla_miss_pkey PRIMARY KEY (task_id, dag_id, execution_date);


--
-- Name: slot_pool slot_pool_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.slot_pool
    ADD CONSTRAINT slot_pool_pkey PRIMARY KEY (id);


--
-- Name: slot_pool slot_pool_pool_uq; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.slot_pool
    ADD CONSTRAINT slot_pool_pool_uq UNIQUE (pool);


--
-- Name: task_fail task_fail_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_fail
    ADD CONSTRAINT task_fail_pkey PRIMARY KEY (id);


--
-- Name: task_instance_note task_instance_note_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_instance_note
    ADD CONSTRAINT task_instance_note_pkey PRIMARY KEY (task_id, dag_id, run_id, map_index);


--
-- Name: task_instance task_instance_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_instance
    ADD CONSTRAINT task_instance_pkey PRIMARY KEY (dag_id, task_id, run_id, map_index);


--
-- Name: task_map task_map_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_map
    ADD CONSTRAINT task_map_pkey PRIMARY KEY (dag_id, task_id, run_id, map_index);


--
-- Name: task_reschedule task_reschedule_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_reschedule
    ADD CONSTRAINT task_reschedule_pkey PRIMARY KEY (id);


--
-- Name: task_outlet_dataset_reference todr_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_outlet_dataset_reference
    ADD CONSTRAINT todr_pkey PRIMARY KEY (dataset_id, dag_id, task_id);


--
-- Name: trigger trigger_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trigger
    ADD CONSTRAINT trigger_pkey PRIMARY KEY (id);


--
-- Name: variable variable_key_uq; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.variable
    ADD CONSTRAINT variable_key_uq UNIQUE (key);


--
-- Name: variable variable_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.variable
    ADD CONSTRAINT variable_pkey PRIMARY KEY (id);


--
-- Name: velib_stations velib_stations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.velib_stations
    ADD CONSTRAINT velib_stations_pkey PRIMARY KEY (station_id);


--
-- Name: xcom xcom_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.xcom
    ADD CONSTRAINT xcom_pkey PRIMARY KEY (dag_run_id, task_id, map_index, key);


--
-- Name: dag_id_state; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX dag_id_state ON public.dag_run USING btree (dag_id, state);


--
-- Name: idx_ab_register_user_username; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_ab_register_user_username ON public.ab_register_user USING btree (lower((username)::text));


--
-- Name: idx_ab_user_username; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_ab_user_username ON public.ab_user USING btree (lower((username)::text));


--
-- Name: idx_dag_run_dag_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_dag_run_dag_id ON public.dag_run USING btree (dag_id);


--
-- Name: idx_dag_run_queued_dags; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_dag_run_queued_dags ON public.dag_run USING btree (state, dag_id) WHERE ((state)::text = 'queued'::text);


--
-- Name: idx_dag_run_running_dags; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_dag_run_running_dags ON public.dag_run USING btree (state, dag_id) WHERE ((state)::text = 'running'::text);


--
-- Name: idx_dagrun_dataset_events_dag_run_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_dagrun_dataset_events_dag_run_id ON public.dagrun_dataset_event USING btree (dag_run_id);


--
-- Name: idx_dagrun_dataset_events_event_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_dagrun_dataset_events_event_id ON public.dagrun_dataset_event USING btree (event_id);


--
-- Name: idx_dataset_id_timestamp; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_dataset_id_timestamp ON public.dataset_event USING btree (dataset_id, "timestamp");


--
-- Name: idx_fileloc_hash; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_fileloc_hash ON public.serialized_dag USING btree (fileloc_hash);


--
-- Name: idx_job_dag_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_job_dag_id ON public.job USING btree (dag_id);


--
-- Name: idx_job_state_heartbeat; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_job_state_heartbeat ON public.job USING btree (state, latest_heartbeat);


--
-- Name: idx_last_scheduling_decision; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_last_scheduling_decision ON public.dag_run USING btree (last_scheduling_decision);


--
-- Name: idx_log_dag; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_log_dag ON public.log USING btree (dag_id);


--
-- Name: idx_log_dttm; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_log_dttm ON public.log USING btree (dttm);


--
-- Name: idx_log_event; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_log_event ON public.log USING btree (event);


--
-- Name: idx_next_dagrun_create_after; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_next_dagrun_create_after ON public.dag USING btree (next_dagrun_create_after);


--
-- Name: idx_root_dag_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_root_dag_id ON public.dag USING btree (root_dag_id);


--
-- Name: idx_task_fail_task_instance; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_task_fail_task_instance ON public.task_fail USING btree (dag_id, task_id, run_id, map_index);


--
-- Name: idx_task_reschedule_dag_run; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_task_reschedule_dag_run ON public.task_reschedule USING btree (dag_id, run_id);


--
-- Name: idx_task_reschedule_dag_task_run; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_task_reschedule_dag_task_run ON public.task_reschedule USING btree (dag_id, task_id, run_id, map_index);


--
-- Name: idx_uri_unique; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_uri_unique ON public.dataset USING btree (uri);


--
-- Name: idx_xcom_key; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_xcom_key ON public.xcom USING btree (key);


--
-- Name: idx_xcom_task_instance; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_xcom_task_instance ON public.xcom USING btree (dag_id, task_id, run_id, map_index);


--
-- Name: job_type_heart; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX job_type_heart ON public.job USING btree (job_type, latest_heartbeat);


--
-- Name: sm_dag; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX sm_dag ON public.sla_miss USING btree (dag_id);


--
-- Name: ti_dag_run; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ti_dag_run ON public.task_instance USING btree (dag_id, run_id);


--
-- Name: ti_dag_state; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ti_dag_state ON public.task_instance USING btree (dag_id, state);


--
-- Name: ti_job_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ti_job_id ON public.task_instance USING btree (job_id);


--
-- Name: ti_pool; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ti_pool ON public.task_instance USING btree (pool, state, priority_weight);


--
-- Name: ti_state; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ti_state ON public.task_instance USING btree (state);


--
-- Name: ti_state_lkp; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ti_state_lkp ON public.task_instance USING btree (dag_id, task_id, run_id, state);


--
-- Name: ti_trigger_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ti_trigger_id ON public.task_instance USING btree (trigger_id);


--
-- Name: ab_permission_view ab_permission_view_permission_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ab_permission_view
    ADD CONSTRAINT ab_permission_view_permission_id_fkey FOREIGN KEY (permission_id) REFERENCES public.ab_permission(id);


--
-- Name: ab_permission_view_role ab_permission_view_role_permission_view_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ab_permission_view_role
    ADD CONSTRAINT ab_permission_view_role_permission_view_id_fkey FOREIGN KEY (permission_view_id) REFERENCES public.ab_permission_view(id);


--
-- Name: ab_permission_view_role ab_permission_view_role_role_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ab_permission_view_role
    ADD CONSTRAINT ab_permission_view_role_role_id_fkey FOREIGN KEY (role_id) REFERENCES public.ab_role(id);


--
-- Name: ab_permission_view ab_permission_view_view_menu_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ab_permission_view
    ADD CONSTRAINT ab_permission_view_view_menu_id_fkey FOREIGN KEY (view_menu_id) REFERENCES public.ab_view_menu(id);


--
-- Name: ab_user ab_user_changed_by_fk_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ab_user
    ADD CONSTRAINT ab_user_changed_by_fk_fkey FOREIGN KEY (changed_by_fk) REFERENCES public.ab_user(id);


--
-- Name: ab_user ab_user_created_by_fk_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ab_user
    ADD CONSTRAINT ab_user_created_by_fk_fkey FOREIGN KEY (created_by_fk) REFERENCES public.ab_user(id);


--
-- Name: ab_user_role ab_user_role_role_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ab_user_role
    ADD CONSTRAINT ab_user_role_role_id_fkey FOREIGN KEY (role_id) REFERENCES public.ab_role(id);


--
-- Name: ab_user_role ab_user_role_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ab_user_role
    ADD CONSTRAINT ab_user_role_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.ab_user(id);


--
-- Name: dag_owner_attributes dag.dag_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dag_owner_attributes
    ADD CONSTRAINT "dag.dag_id" FOREIGN KEY (dag_id) REFERENCES public.dag(dag_id) ON DELETE CASCADE;


--
-- Name: dag_run_note dag_run_note_dr_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dag_run_note
    ADD CONSTRAINT dag_run_note_dr_fkey FOREIGN KEY (dag_run_id) REFERENCES public.dag_run(id) ON DELETE CASCADE;


--
-- Name: dag_run_note dag_run_note_user_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dag_run_note
    ADD CONSTRAINT dag_run_note_user_fkey FOREIGN KEY (user_id) REFERENCES public.ab_user(id);


--
-- Name: dag_tag dag_tag_dag_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dag_tag
    ADD CONSTRAINT dag_tag_dag_id_fkey FOREIGN KEY (dag_id) REFERENCES public.dag(dag_id) ON DELETE CASCADE;


--
-- Name: dagrun_dataset_event dagrun_dataset_event_dag_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dagrun_dataset_event
    ADD CONSTRAINT dagrun_dataset_event_dag_run_id_fkey FOREIGN KEY (dag_run_id) REFERENCES public.dag_run(id) ON DELETE CASCADE;


--
-- Name: dagrun_dataset_event dagrun_dataset_event_event_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dagrun_dataset_event
    ADD CONSTRAINT dagrun_dataset_event_event_id_fkey FOREIGN KEY (event_id) REFERENCES public.dataset_event(id) ON DELETE CASCADE;


--
-- Name: dag_warning dcw_dag_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dag_warning
    ADD CONSTRAINT dcw_dag_id_fkey FOREIGN KEY (dag_id) REFERENCES public.dag(dag_id) ON DELETE CASCADE;


--
-- Name: dataset_dag_run_queue ddrq_dag_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dataset_dag_run_queue
    ADD CONSTRAINT ddrq_dag_fkey FOREIGN KEY (target_dag_id) REFERENCES public.dag(dag_id) ON DELETE CASCADE;


--
-- Name: dataset_dag_run_queue ddrq_dataset_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dataset_dag_run_queue
    ADD CONSTRAINT ddrq_dataset_fkey FOREIGN KEY (dataset_id) REFERENCES public.dataset(id) ON DELETE CASCADE;


--
-- Name: dag_schedule_dataset_reference dsdr_dag_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dag_schedule_dataset_reference
    ADD CONSTRAINT dsdr_dag_id_fkey FOREIGN KEY (dag_id) REFERENCES public.dag(dag_id) ON DELETE CASCADE;


--
-- Name: dag_schedule_dataset_reference dsdr_dataset_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dag_schedule_dataset_reference
    ADD CONSTRAINT dsdr_dataset_fkey FOREIGN KEY (dataset_id) REFERENCES public.dataset(id) ON DELETE CASCADE;


--
-- Name: rendered_task_instance_fields rtif_ti_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.rendered_task_instance_fields
    ADD CONSTRAINT rtif_ti_fkey FOREIGN KEY (dag_id, task_id, run_id, map_index) REFERENCES public.task_instance(dag_id, task_id, run_id, map_index) ON DELETE CASCADE;


--
-- Name: task_fail task_fail_ti_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_fail
    ADD CONSTRAINT task_fail_ti_fkey FOREIGN KEY (dag_id, task_id, run_id, map_index) REFERENCES public.task_instance(dag_id, task_id, run_id, map_index) ON DELETE CASCADE;


--
-- Name: task_instance task_instance_dag_run_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_instance
    ADD CONSTRAINT task_instance_dag_run_fkey FOREIGN KEY (dag_id, run_id) REFERENCES public.dag_run(dag_id, run_id) ON DELETE CASCADE;


--
-- Name: dag_run task_instance_log_template_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dag_run
    ADD CONSTRAINT task_instance_log_template_id_fkey FOREIGN KEY (log_template_id) REFERENCES public.log_template(id);


--
-- Name: task_instance_note task_instance_note_ti_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_instance_note
    ADD CONSTRAINT task_instance_note_ti_fkey FOREIGN KEY (dag_id, task_id, run_id, map_index) REFERENCES public.task_instance(dag_id, task_id, run_id, map_index) ON DELETE CASCADE;


--
-- Name: task_instance_note task_instance_note_user_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_instance_note
    ADD CONSTRAINT task_instance_note_user_fkey FOREIGN KEY (user_id) REFERENCES public.ab_user(id);


--
-- Name: task_instance task_instance_trigger_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_instance
    ADD CONSTRAINT task_instance_trigger_id_fkey FOREIGN KEY (trigger_id) REFERENCES public.trigger(id) ON DELETE CASCADE;


--
-- Name: task_map task_map_task_instance_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_map
    ADD CONSTRAINT task_map_task_instance_fkey FOREIGN KEY (dag_id, task_id, run_id, map_index) REFERENCES public.task_instance(dag_id, task_id, run_id, map_index) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: task_reschedule task_reschedule_dr_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_reschedule
    ADD CONSTRAINT task_reschedule_dr_fkey FOREIGN KEY (dag_id, run_id) REFERENCES public.dag_run(dag_id, run_id) ON DELETE CASCADE;


--
-- Name: task_reschedule task_reschedule_ti_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_reschedule
    ADD CONSTRAINT task_reschedule_ti_fkey FOREIGN KEY (dag_id, task_id, run_id, map_index) REFERENCES public.task_instance(dag_id, task_id, run_id, map_index) ON DELETE CASCADE;


--
-- Name: task_outlet_dataset_reference todr_dag_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_outlet_dataset_reference
    ADD CONSTRAINT todr_dag_id_fkey FOREIGN KEY (dag_id) REFERENCES public.dag(dag_id) ON DELETE CASCADE;


--
-- Name: task_outlet_dataset_reference todr_dataset_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_outlet_dataset_reference
    ADD CONSTRAINT todr_dataset_fkey FOREIGN KEY (dataset_id) REFERENCES public.dataset(id) ON DELETE CASCADE;


--
-- Name: xcom xcom_task_instance_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.xcom
    ADD CONSTRAINT xcom_task_instance_fkey FOREIGN KEY (dag_id, task_id, run_id, map_index) REFERENCES public.task_instance(dag_id, task_id, run_id, map_index) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict D5QOBGI5SxdKJoBHWmQr6XxBhRK0a7BC4qctYN2cALZ17KqmpcTURO6Yw8beFbs

