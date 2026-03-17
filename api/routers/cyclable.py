## Rôle : Expose les données des aménagements cyclables (pistes, coronapistes…).

from fastapi import APIRouter, Query
from sqlalchemy import create_engine, text
from typing import Optional
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


@router.get("/", summary="Liste des aménagements cyclables")
def get_amenagements(
    limit:          int           = Query(100, ge=1, le=1000),
    offset:         int           = Query(0, ge=0),
    arrondissement: Optional[str] = Query(None, description="Ex: 75011")
):
    engine = get_engine()
    query  = """
        SELECT osm_id, nom, amenagement, arrondissement,
               longueur, sens, surface
        FROM amenagements_cyclables
    """
    params = {"limit": limit, "offset": offset}
    if arrondissement:
        query += " WHERE arrondissement = :arr"
        params["arr"] = arrondissement
    query += " ORDER BY longueur DESC NULLS LAST LIMIT :limit OFFSET :offset"

    with engine.connect() as conn:
        rows = conn.execute(text(query), params).mappings().all()
    return {"count": len(rows), "data": [dict(r) for r in rows]}

@router.get("/stats", summary="Statistiques des aménagements cyclables")
def get_stats_cyclable():
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT
                COUNT(*)                        AS nb_amenagements,
                ROUND(SUM(longueur)::numeric, 2) AS longueur_totale_km,
                COUNT(DISTINCT amenagement)     AS nb_types,
                COUNT(DISTINCT arrondissement)  AS nb_arrondissements
            FROM amenagements_cyclables
        """)).mappings().first()
    return dict(row)




# from fastapi import APIRouter, Query
# from sqlalchemy import create_engine, text
# from typing import Optional
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


# @router.get("/", summary="Liste des aménagements cyclables")
# def get_amenagements(
#     limit:         int           = Query(100, ge=1, le=1000),
#     offset:        int           = Query(0, ge=0),
#     arrondissement: Optional[str] = Query(None, description="Filtrer par arrondissement (ex: 75001)")
# ):
#     """
#     Retourne les aménagements cyclables de Paris.
#     Filtre optionnel par arrondissement.
#     """
#     engine = get_engine()
#     query  = "SELECT osm_id, nom, amenagement, arrondissement, longueur, sens, surface FROM amenagements_cyclables"
#     params = {"limit": limit, "offset": offset}

#     if arrondissement:
#         query += " WHERE arrondissement = :arr"
#         params["arr"] = arrondissement

#     query += " ORDER BY longueur DESC NULLS LAST LIMIT :limit OFFSET :offset"

#     with engine.connect() as conn:
#         result = conn.execute(text(query), params)
#         rows   = [dict(r._mapping) for r in result]
#     return {"total": len(rows), "data": rows}


# @router.get("/stats", summary="Statistiques des aménagements cyclables")
# def get_stats_cyclable():
#     """
#     Retourne des statistiques : longueur totale, types d'aménagements,
#     nombre de coronapistes.
#     """
#     engine = get_engine()
#     with engine.connect() as conn:
#         result = conn.execute(text("""
#             SELECT
#                 COUNT(*)                    AS nb_amenagements,
#                 ROUND(SUM(longueur)::numeric, 2) AS longueur_totale_km,
#                 COUNT(DISTINCT amenagement) AS nb_types,
#                 COUNT(DISTINCT arrondissement) AS nb_arrondissements
#             FROM amenagements_cyclables
#         """))
#         row = result.fetchone()
#     return dict(row._mapping)