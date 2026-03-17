# Rôle : Expose les événements culturels parisiens avec filtres par date et arrondissement

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

@router.get("/", summary="Liste des événements parisiens")
def get_evenements(
    limit:       int           = Query(50, ge=1, le=500),
    offset:      int           = Query(0, ge=0),
    code_postal: Optional[str] = Query(None, description="Ex: 75001")
):
    engine = get_engine()
    query  = """
        SELECT id, titre, date_debut, date_fin,
               nom_lieu, adresse_lieu, code_postal, ville
        FROM evenements_qfap
    """
    params = {"limit": limit, "offset": offset}
    if code_postal:
        query += " WHERE code_postal = :cp"
        params["cp"] = code_postal
    query += " ORDER BY date_debut DESC LIMIT :limit OFFSET :offset"

    with engine.connect() as conn:
        rows = conn.execute(text(query), params).mappings().all()
    return {"count": len(rows), "data": [dict(r) for r in rows]}

@router.get("/stats", summary="Statistiques des événements")
def get_stats_evenements():
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT
                COUNT(*)                        AS total_evenements,
                COUNT(DISTINCT code_postal)     AS nb_arrondissements,
                MIN(date_debut)                 AS premier_evenement,
                MAX(date_fin)                   AS dernier_evenement
            FROM evenements_qfap
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


# @router.get("/", summary="Liste des événements parisiens")
# def get_evenements(
#     limit:      int            = Query(50, ge=1, le=500),
#     offset:     int            = Query(0, ge=0),
#     code_postal: Optional[str] = Query(None, description="Filtrer par code postal (ex: 75001)")
# ):
#     """
#     Retourne les événements culturels de Paris.
#     Filtre optionnel par code postal.
#     """
#     engine = get_engine()
#     query  = "SELECT id, titre, date_debut, date_fin, nom_lieu, adresse_lieu, code_postal, ville FROM evenements_qfap"
#     params = {"limit": limit, "offset": offset}

#     if code_postal:
#         query += " WHERE code_postal = :cp"
#         params["cp"] = code_postal

#     query += " ORDER BY date_debut DESC LIMIT :limit OFFSET :offset"

#     with engine.connect() as conn:
#         result = conn.execute(text(query), params)
#         rows   = [dict(r._mapping) for r in result]
#     return {"total": len(rows), "data": rows}


# @router.get("/stats", summary="Statistiques des événements")
# def get_stats_evenements():
#     """
#     Retourne le nombre total d'événements et la répartition par arrondissement.
#     """
#     engine = get_engine()
#     with engine.connect() as conn:
#         result = conn.execute(text("""
#             SELECT
#                 COUNT(*)                            AS total_evenements,
#                 COUNT(DISTINCT code_postal)         AS nb_arrondissements,
#                 MIN(date_debut)                     AS premier_evenement,
#                 MAX(date_fin)                       AS dernier_evenement
#             FROM evenements_qfap
#         """))
#         row = result.fetchone()
#     return dict(row._mapping)