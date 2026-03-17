import streamlit as st
import requests
import pandas as pd
import os

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────
API_BASE = os.getenv("API_BASE_URL", "http://localhost:8000")
API_KEY  = os.getenv("API_KEY", "e4-api-key-mobilite-paris-2026")
HEADERS  = {"X-API-Key": API_KEY}

st.set_page_config(page_title="Mobilité Paris", page_icon="🚲", layout="wide")

# ─────────────────────────────────────────
# HELPER
# ─────────────────────────────────────────
def get(endpoint: str, params: dict = {}) -> dict:
    try:
        r = requests.get(
            f"{API_BASE}{endpoint}",
            headers=HEADERS,
            params=params,
            timeout=10
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"Erreur API ({endpoint}) : {e}")
        return {}

# ─────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────
page = st.sidebar.radio("Navigation", [
    "🏠 Accueil",
    "🚲 Vélib — Stations",
    "📊 Vélib — Disponibilité",
    "🎭 Événements Paris",
    "🛣️ Aménagements Cyclables",
    "🔎 Éditeur SQL"          # ← NOUVEAU
])

# ─────────────────────────────────────────
# PAGE : ACCUEIL
# ─────────────────────────────────────────
if page == "🏠 Accueil":
    st.title("🚲 Mobilité Paris — Dashboard E4")
    st.markdown("---")

    # ── Ligne 1 : Vélib ──────────────────
    st.subheader("🚲 Vélib")
    stats_v = get("/api/velib/stats")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Stations",            stats_v.get("nb_stations",           "—"))
    c2.metric("Vélos disponibles",   stats_v.get("total_velos",           "—"))
    c3.metric("Électriques",         stats_v.get("total_electriques",     "—"))
    c4.metric("Mécaniques",          stats_v.get("total_mecaniques",      "—"))
    c5.metric("Bornettes libres",    stats_v.get("total_bornettes_libres","—"))

    st.markdown("---")

    # ── Ligne 2 : Événements ─────────────
    st.subheader("🎭 Événements")
    stats_e = get("/api/evenements/stats")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total événements",    stats_e.get("total_evenements",   "—"))
    c2.metric("Arrondissements",     stats_e.get("nb_arrondissements", "—"))
    c3.metric("Premier événement",   str(stats_e.get("premier_evenement", "—"))[:10])
    c4.metric("Dernier événement",   str(stats_e.get("dernier_evenement",  "—"))[:10])

    st.markdown("---")

    # ── Ligne 3 : Cyclable ───────────────
    st.subheader("🛣️ Aménagements Cyclables")
    stats_c = get("/api/cyclable/stats")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Aménagements",        stats_c.get("nb_amenagements",    "—"))
    c2.metric("Longueur totale (km)",stats_c.get("longueur_totale_km", "—"))
    c3.metric("Types d'aménagement", stats_c.get("nb_types",           "—"))
    c4.metric("Arrondissements",     stats_c.get("nb_arrondissements", "—"))

    st.markdown("---")
    st.success("✅ Pipeline E4 opérationnel — PostgreSQL · FastAPI · Airflow · Streamlit")

# ─────────────────────────────────────────
# PAGE : VÉLIB STATIONS
# ─────────────────────────────────────────
elif page == "🚲 Vélib — Stations":
    st.title("🚲 Stations Vélib")

    limit = st.slider("Nombre de stations à afficher", 10, 1000, 100, step=10)
    data  = get("/api/velib/stations", {"limit": limit})

    if data and "data" in data:
        df = pd.DataFrame(data["data"])
        st.caption(f"{data['count']} stations récupérées")

        # Carte
        if "latitude" in df.columns and "longitude" in df.columns:
            st.subheader("📍 Carte des stations")
            df_map = df[["latitude", "longitude"]].dropna().rename(
                columns={"latitude": "lat", "longitude": "lon"}
            )
            st.map(df_map)

        # Tableau
        st.subheader("📋 Détail des stations")
        st.dataframe(df, use_container_width=True)

# ─────────────────────────────────────────
# PAGE : VÉLIB DISPONIBILITÉ
# ─────────────────────────────────────────
elif page == "📊 Vélib — Disponibilité":
    st.title("📊 Disponibilité Vélib — Temps réel")

    limit = st.slider("Nombre de stations", 10, 1000, 200, step=10)
    data  = get("/api/velib/disponibilite", {"limit": limit})

    if data and "data" in data:
        df = pd.DataFrame(data["data"])
        st.caption(f"{data['count']} stations récupérées")

        # KPIs dynamiques
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("🚲 Vélos disponibles", int(df["nb_velos_disponibles"].sum()))
        c2.metric("⚡ Électriques",        int(df["velos_electriques"].sum()))
        c3.metric("🔩 Mécaniques",         int(df["velos_mecaniques"].sum()))
        c4.metric("🔲 Bornettes libres",   int(df["nb_bornettes_libres"].sum()))

        # Top 10
        st.subheader("🏆 Top 10 — Stations les plus fournies")
        top10 = (
            df.nlargest(10, "nb_velos_disponibles")
              .set_index("nom_station")
            [["nb_velos_disponibles", "velos_electriques", "velos_mecaniques"]]
        )
        st.bar_chart(top10)

        # Tableau complet
        st.subheader("📋 Tableau complet")
        st.dataframe(df, use_container_width=True)

# ─────────────────────────────────────────
# PAGE : ÉVÉNEMENTS
# ─────────────────────────────────────────
elif page == "🎭 Événements Paris":
    st.title("🎭 Événements Culturels — Paris")

    col1, col2 = st.columns(2)
    limit       = col1.slider("Nombre d'événements", 10, 500, 50, step=10)
    code_postal = col2.text_input("Filtrer par code postal (ex: 75011)", value="")

    params = {"limit": limit}
    if code_postal.strip():
        params["code_postal"] = code_postal.strip()

    data = get("/api/evenements/", params)

    if data and "data" in data:
        df = pd.DataFrame(data["data"])
        st.caption(f"{data['count']} événements récupérés")

        # Répartition par arrondissement
        if "code_postal" in df.columns:
            st.subheader("📊 Top 10 arrondissements")
            cp_counts = df["code_postal"].value_counts().head(10)
            st.bar_chart(cp_counts)

        # Tableau
        st.subheader("📋 Liste des événements")
        cols = [c for c in ["titre", "nom_lieu", "adresse_lieu", "code_postal", "date_debut", "date_fin"] if c in df.columns]
        st.dataframe(df[cols], use_container_width=True)

# ─────────────────────────────────────────
# PAGE : AMÉNAGEMENTS CYCLABLES
# ─────────────────────────────────────────
elif page == "🛣️ Aménagements Cyclables":
    st.title("🛣️ Aménagements Cyclables — Paris")

    col1, col2     = st.columns(2)
    limit          = col1.slider("Nombre d'aménagements", 10, 1000, 200, step=10)
    arrondissement = col2.text_input("Filtrer par arrondissement (ex: 75011)", value="")

    params = {"limit": limit}
    if arrondissement.strip():
        params["arrondissement"] = arrondissement.strip()

    data = get("/api/cyclable/", params)

    if data and "data" in data:
        df = pd.DataFrame(data["data"])
        st.caption(f"{data['count']} aménagements récupérés")

        # KPIs
        c1, c2, c3 = st.columns(3)
        c1.metric("🛣️ Aménagements",      len(df))
        if "longueur" in df.columns:
            c2.metric("📏 Longueur totale (km)", round(df["longueur"].sum(), 2))
        if "amenagement" in df.columns:
            c3.metric("🔢 Types",            df["amenagement"].nunique())

        # Répartition par type
        if "amenagement" in df.columns:
            st.subheader("📊 Répartition par type d'aménagement")
            st.bar_chart(df["amenagement"].value_counts().head(10))

        # Tableau
        st.subheader("📋 Liste des aménagements")
        cols = [c for c in ["nom", "amenagement", "arrondissement", "longueur", "sens", "surface"] if c in df.columns]
        st.dataframe(df[cols], use_container_width=True)
        
        
        # ─────────────────────────────────────────
# PAGE : ÉDITEUR SQL
# ─────────────────────────────────────────
elif page == "🔎 Éditeur SQL":
    import psycopg2
    import plotly.express as px

    st.title("🔎 Éditeur SQL — Mobilité Paris")
    st.caption("Requêtes directes sur la base PostgreSQL · Lecture seule")

    # ── Connexion ────────────────────────
    @st.cache_resource
    def get_conn():
        return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),    # ← CORRECT
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "mobilite_paris"),
        user=os.getenv("POSTGRES_USER", "e4_user"),
        password=os.getenv("POSTGRES_PASSWORD", "e4_password_2026")
    )
    # ── Requêtes pré-définies ────────────
    PRESETS = {
        "-- Choisir une requête prédéfinie --": "",

        "🚲 Top 20 stations Vélib les plus équipées": """
SELECT nom_station, nb_velos_disponibles,
       velos_electriques, velos_mecaniques, nb_bornettes_libres
FROM velib_disponibilite
ORDER BY nb_velos_disponibles DESC
LIMIT 20;
""",
        "📊 Répartition mécanique vs électrique par station": """
SELECT nom_station,
       velos_mecaniques,
       velos_electriques,
       nb_velos_disponibles
FROM velib_disponibilite
WHERE nb_velos_disponibles > 0
ORDER BY nb_velos_disponibles DESC
LIMIT 30;
""",
        "🎭 Événements par arrondissement (top 15)": """
SELECT code_postal,
       COUNT(*) AS nb_evenements
FROM evenements_qfap
WHERE code_postal IS NOT NULL
GROUP BY code_postal
ORDER BY nb_evenements DESC
LIMIT 15;
""",
        "🎭 Prochains événements": """
SELECT titre, nom_lieu, code_postal, date_debut, date_fin
FROM evenements_qfap
ORDER BY date_debut DESC
LIMIT 50;
""",
        "🛣️ Types d'aménagements cyclables": """
SELECT amenagement,
       COUNT(*)                        AS nb,
       ROUND(SUM(longueur)::numeric,2) AS km_total
FROM amenagements_cyclables
WHERE amenagement IS NOT NULL
GROUP BY amenagement
ORDER BY km_total DESC;
""",
        "🛣️ Aménagements par arrondissement": """
SELECT arrondissement,
       COUNT(*)                        AS nb_amenagements,
       ROUND(SUM(longueur)::numeric,2) AS km_total
FROM amenagements_cyclables
WHERE arrondissement IS NOT NULL
GROUP BY arrondissement
ORDER BY km_total DESC
LIMIT 20;
""",
        "📈 Stations Vélib avec bornettes libres": """
SELECT nom_station,
       nb_bornettes_libres,
       nb_velos_disponibles,
       ROUND(
           nb_bornettes_libres::numeric /
           NULLIF(nb_bornettes_libres + nb_velos_disponibles, 0) * 100
       , 1) AS pct_libre
FROM velib_disponibilite
ORDER BY pct_libre DESC
LIMIT 20;
""",
    }

    preset = st.selectbox("📋 Requêtes prédéfinies", list(PRESETS.keys()))

    default_query = PRESETS[preset] if preset != "-- Choisir une requête prédéfinie --" else "SELECT * FROM velib_stations LIMIT 10;"
    query = st.text_area("✏️ Requête SQL", value=default_query, height=180)

    # ── Sécurité minimale ────────────────
    def is_safe(q: str) -> bool:
        forbidden = ["drop", "delete", "truncate", "insert", "update", "alter", "create"]
        q_lower = q.lower()
        return not any(kw in q_lower for kw in forbidden)

    col_run, col_info = st.columns([1, 5])
    run = col_run.button("▶️ Exécuter", type="primary")
    col_info.caption("⚠️ Lecture seule — DROP / DELETE / INSERT / UPDATE interdits")

    if run:
        if not query.strip():
            st.warning("Entrez une requête SQL.")
        elif not is_safe(query):
            st.error("❌ Requête non autorisée (lecture seule uniquement).")
        else:
            try:
                conn = get_conn()
                df   = pd.read_sql(query, conn)
                st.success(f"✅ {len(df)} ligne(s) retournée(s)")

                # ── Tableau ───────────────────────
                st.subheader("📋 Résultats")
                st.dataframe(df, use_container_width=True)

                # ── Visualisation auto ────────────
                st.subheader("📊 Visualisation")
                num_cols = df.select_dtypes("number").columns.tolist()
                str_cols = df.select_dtypes("object").columns.tolist()

                if len(num_cols) >= 1 and len(str_cols) >= 1:
                    col1, col2, col3 = st.columns(3)
                    x_col    = col1.selectbox("Axe X (catégorie)", str_cols,  index=0)
                    y_col    = col2.selectbox("Axe Y (valeur)",    num_cols,  index=0)
                    chart_t  = col3.selectbox("Type de graphique", ["Bar","Line","Scatter","Pie"])

                    if chart_t == "Bar":
                        fig = px.bar(df, x=x_col, y=y_col, title=f"{y_col} par {x_col}", color=y_col)
                    elif chart_t == "Line":
                        fig = px.line(df, x=x_col, y=y_col, title=f"{y_col} par {x_col}")
                    elif chart_t == "Scatter":
                        fig = px.scatter(df, x=x_col, y=y_col, title=f"{y_col} vs {x_col}", color=x_col)
                    elif chart_t == "Pie":
                        fig = px.pie(df, names=x_col, values=y_col, title=f"Répartition {y_col}")

                    st.plotly_chart(fig, use_container_width=True)

                elif len(num_cols) >= 2:
                    st.info("Deux colonnes numériques détectées — scatter automatique")
                    fig = px.scatter(df, x=num_cols[0], y=num_cols[1])
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Sélectionnez une requête avec colonnes texte + numérique pour visualiser.")

                # ── Export CSV ────────────────────
                st.download_button(
                    "⬇️ Télécharger CSV",
                    df.to_csv(index=False).encode("utf-8"),
                    file_name="export_mobilite_paris.csv",
                    mime="text/csv"
                )

            except Exception as e:
                st.error(f"❌ Erreur SQL : {e}")