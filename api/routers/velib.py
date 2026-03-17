
## Rôle : Expose les données Vélib (stations + disponibilité). Le jury pourra tester ces routes en direct.

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import create_engine, text
import os

router = APIRouter()

def get_engine():
    url = (
        f"postgresql+psycopg2://"
        f"{os.getenv('POSTGRES_USER', 'e4_user')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'e4_password_2026')}@"
        f"{os.getenv('POSTGRES_HOST', 'postgres')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/"
        f"{os.getenv('POSTGRES_DB', 'mobilite_paris')}"
    )
    return create_engine(url)

@router.get("/stations", summary="Liste toutes les stations Vélib")
def get_stations(limit: int = Query(100, ge=1, le=1000)):
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT * FROM velib_stations LIMIT :limit"),
            {"limit": limit}
        ).mappings().all()
    return {"count": len(rows), "data": [dict(r) for r in rows]}

@router.get("/stations/{station_id}", summary="Détail d'une station Vélib")
def get_station(station_id: str):
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM velib_stations WHERE station_id = :sid"),
            {"sid": station_id}
        ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Station introuvable")
    return dict(row)

@router.get("/disponibilite", summary="Disponibilité temps réel des vélos")
def get_disponibilite(limit: int = Query(100, ge=1, le=1000)):
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT DISTINCT ON (station_id)
                    station_id, nom_station, nb_velos_disponibles,
                    velos_mecaniques, velos_electriques,
                    nb_bornettes_libres, actualisation_donnee
                FROM velib_disponibilite
                ORDER BY station_id, actualisation_donnee DESC
                LIMIT :limit
            """),
            {"limit": limit}
        ).mappings().all()
    return {"count": len(rows), "data": [dict(r) for r in rows]}

@router.get("/stats", summary="Statistiques globales Vélib")
def get_stats():
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT
                COUNT(DISTINCT station_id)     AS nb_stations,
                SUM(nb_velos_disponibles)      AS total_velos,
                SUM(velos_mecaniques)          AS total_mecaniques,
                SUM(velos_electriques)         AS total_electriques,
                SUM(nb_bornettes_libres)       AS total_bornettes_libres
            FROM (
                SELECT DISTINCT ON (station_id)
                    station_id, nb_velos_disponibles,
                    velos_mecaniques, velos_electriques,
                    nb_bornettes_libres
                FROM velib_disponibilite
                ORDER BY station_id, actualisation_donnee DESC
            ) t
        """)).mappings().first()
    return dict(row)




# from fastapi import APIRouter, HTTPException, Query
# from sqlalchemy import create_engine, text
# import os

# router = APIRouter()

# def get_engine():
#     url = (
#         f"postgresql+psycopg2://"
#         f"{os.getenv('POSTGRES_USER','e4_user')}:"
#         f"{os.getenv('POSTGRES_PASSWORD','e4_password_2026')}@"
#         f"{os.getenv('POSTGRES_HOST','postgres')}:"
#         f"{os.getenv('POSTGRES_PORT','5432')}/"
#         f"{os.getenv('POSTGRES_DB','mobilite_paris')}"
#     )
#     return create_engine(url)


# @router.get("/stations", summary="Liste toutes les stations Vélib")
# def get_stations(
#     limit: int = Query(100, ge=1, le=1000, description="Nombre de résultats (max 1000)"),
#     offset: int = Query(0, ge=0, description="Décalage pour la pagination")
# ):
#     """
#     Retourne la liste des stations Vélib avec leur capacité et localisation.
#     """
#     engine = get_engine()
#     with engine.connect() as conn:
#         result = conn.execute(
#             text("SELECT * FROM velib_stations LIMIT :limit OFFSET :offset"),
#             {"limit": limit, "offset": offset}
#         )
#         rows = [dict(r._mapping) for r in result]
#     return {"total": len(rows), "limit": limit, "offset": offset, "data": rows}


# @router.get("/stations/{station_id}", summary="Détail d'une station Vélib")
# def get_station(station_id: str):
#     """
#     Retourne les informations d'une station Vélib par son identifiant.
#     """
#     engine = get_engine()
#     with engine.connect() as conn:
#         result = conn.execute(
#             text("SELECT * FROM velib_stations WHERE station_id = :sid"),
#             {"sid": station_id}
#         )
#         row = result.fetchone()
#     if not row:
#         raise HTTPException(status_code=404, detail=f"Station {station_id} introuvable")
#     return dict(row._mapping)


# @router.get("/disponibilite", summary="Disponibilité temps réel des vélos")
# def get_disponibilite(
#     limit: int = Query(100, ge=1, le=1000),
#     offset: int = Query(0, ge=0)
# ):
#     """
#     Retourne les données de disponibilité des vélos (mécaniques et électriques).
#     """
#     engine = get_engine()
#     with engine.connect() as conn:
#         result = conn.execute(
#             text("""
#                 SELECT station_id, nom_station, nb_velos_disponibles,
#                        velos_mecaniques, velos_electriques,
#                        nb_bornettes_libres, actualisation_donnee
#                 FROM velib_disponibilite
#                 ORDER BY actualisation_donnee DESC
#                 LIMIT :limit OFFSET :offset
#             """),
#             {"limit": limit, "offset": offset}
#         )
#         rows = [dict(r._mapping) for r in result]
#     return {"total": len(rows), "data": rows}


# @router.get("/stats", summary="Statistiques globales Vélib")
# def get_stats():
#     """
#     Retourne des statistiques globales : total vélos disponibles,
#     répartition mécanique/électrique, taux d'occupation.
#     """
#     engine = get_engine()
#     with engine.connect() as conn:
#         result = conn.execute(text("""
#             SELECT
#                 COUNT(DISTINCT station_id)          AS nb_stations,
#                 SUM(nb_velos_disponibles)            AS total_velos,
#                 SUM(velos_mecaniques)                AS total_mecaniques,
#                 SUM(velos_electriques)               AS total_electriques,
#                 SUM(nb_bornettes_libres)             AS total_bornettes_libres,
#                 SUM(capacite_station)                AS capacite_totale
#             FROM velib_disponibilite
#             WHERE actualisation_donnee = (
#                 SELECT MAX(actualisation_donnee) FROM velib_disponibilite
#             )
#         """))
#         row = result.fetchone()
#     return dict(row._mapping)





