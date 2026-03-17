import pandas as pd

# ── Vélib Stations ──────────────────────────────────────────
def transform_velib_stations(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip() for c in df.columns]
    df = df.rename(columns={
        "Identifiant station"       : "station_id",
        "Nom de la station"         : "nom_station",
        "Capacité de la station"    : "capacite_station",
        "Coordonnées géographiques" : "coordonnees_geo",
        "station_opening_hours"     : "station_opening_hours"
    })
    df["station_id"]       = df["station_id"].astype(str).str.strip()
    df["capacite_station"] = pd.to_numeric(df["capacite_station"], errors="coerce")
    df = df.drop_duplicates(subset=["station_id"])
    return df[["station_id","nom_station","capacite_station",
               "coordonnees_geo","station_opening_hours"]]

# ── Vélib Disponibilité ─────────────────────────────────────
def transform_velib_disponibilite(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip() for c in df.columns]
    df = df.rename(columns={
        "Identifiant station"               : "station_id",
        "Nom station"                       : "nom_station",
        "Station en fonctionnement"         : "station_en_fonctionnement",
        "Capacité de la station"            : "capacite_station",
        "Nombre bornettes libres"           : "nb_bornettes_libres",
        "Nombre total vélos disponibles"    : "nb_velos_disponibles",
        "Vélos mécaniques disponibles"      : "velos_mecaniques",
        "Vélos électriques disponibles"     : "velos_electriques",
        "Borne de paiement disponible"      : "borne_paiement",
        "Retour vélib possible"             : "retour_velib_possible",
        "Actualisation de la donnée"        : "actualisation_donnee",
        "Coordonnées géographiques"         : "coordonnees_geo",
        "Nom communes équipées"             : "commune",
        "Code INSEE communes équipées"      : "code_insee",
        "station_opening_hours"             : "station_opening_hours"
    })
    bool_cols = ["station_en_fonctionnement","borne_paiement","retour_velib_possible"]
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].map({"Oui": True, "Non": False, "OUI": True, "NON": False})
    int_cols = ["capacite_station","nb_bornettes_libres",
                "nb_velos_disponibles","velos_mecaniques","velos_electriques"]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["actualisation_donnee"] = pd.to_datetime(
        df["actualisation_donnee"], errors="coerce", dayfirst=True
    )
    return df

# ── Événements ──────────────────────────────────────────────
def transform_evenements(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip() for c in df.columns]
    rename = {
        "ID"                         : "id",
        "Event ID"                   : "event_id",
        "URL"                        : "url",
        "Titre"                      : "titre",
        "Chapeau"                    : "chapeau",
        "Description"                : "description",
        "Date de début"              : "date_debut",
        "Date de fin"                : "date_fin",
        "Occurrences"                : "occurrences",
        "Description de la date"     : "description_date",
        "URL de l'image"             : "url_image",
        "Texte alternatif de l'image": "alt_image",
        "Crédit de l'image"          : "credit_image",
        "locations"                  : "locations",
        "Nom du lieu"                : "nom_lieu",
        "Adresse du lieu"            : "adresse_lieu",
        "Code postal"                : "code_postal",
        "Ville"                      : "ville",
        "Coordonnées géographiques"  : "coordonnees_geo",
    }
    df = df.rename(columns=rename)
    for col in ["date_debut","date_fin","Date de mise à jour"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
    df["id"] = df["id"].astype(str).str.strip()
    df = df.drop_duplicates(subset=["id"])
    return df

# ── Aménagements cyclables ──────────────────────────────────
def transform_amenagements(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip() for c in df.columns]
    df = df.rename(columns={
        "OSM_id"                            : "osm_id",
        "Nom"                               : "nom",
        "Aménagement"                       : "amenagement",
        "Côté aménagement"                  : "cote_amenagement",
        "Sens"                              : "sens",
        "Surface"                           : "surface",
        "Arrondissement"                    : "arrondissement",
        "Bois"                              : "bois",
        "Coronapiste"                       : "coronapiste",
        "Aménagement temporaire"            : "amenagement_temporaire",
        "Infrastructure bidirectionnelle"   : "infra_bidirectionnelle",
        "Voie à sens unique"                : "voie_sens_unique",
        "Position aménagement"              : "position_amenagement",
        "Vitesse maximale autorisée"        : "vmax_autorisee",
        "Date export"                       : "date_export",
        "Source"                            : "source",
        "Longueur"                          : "longueur",
        "geo_shape"                         : "geo_shape",
        "geo_point_2d"                      : "geo_point_2d"
    })
    df["longueur"] = pd.to_numeric(df["longueur"], errors="coerce")
    df["osm_id"]   = df["osm_id"].astype(str).str.strip()
    df = df.drop_duplicates(subset=["osm_id"])
    return df