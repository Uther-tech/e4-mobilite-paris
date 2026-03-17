

from fastapi import FastAPI, Security, HTTPException, status
from fastapi.security.api_key import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
import os

from routers import velib, evenements, cyclable

app = FastAPI(
    title="API Mobilité Paris – E4",
    description="""
API REST du projet E4 – Data Mobilité Paris.

## Authentification
Toutes les routes nécessitent un header **X-API-KEY**.

## Données disponibles
- 🚲 **Vélib** : stations et disponibilité temps réel
- 🎭 **Événements** : agenda culturel parisien
- 🛣️ **Aménagements cyclables** : pistes et voies cyclables
""",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

API_KEY        = os.getenv("API_KEY", "e4-api-key-mobilite-paris-2026")
api_key_header = APIKeyHeader(name="X-API-KEY", auto_error=False)

async def check_api_key(key: str = Security(api_key_header)):
    if key != API_KEY:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Clé API invalide"
        )
    return key

app.include_router(
    velib.router,
    prefix="/api/velib",
    tags=["🚲 Vélib"],
    dependencies=[Security(check_api_key)]
)
app.include_router(
    evenements.router,
    prefix="/api/evenements",
    tags=["🎭 Événements"],
    dependencies=[Security(check_api_key)]
)
app.include_router(
    cyclable.router,
    prefix="/api/cyclable",
    tags=["🛣️ Aménagements Cyclables"],
    dependencies=[Security(check_api_key)]
)

@app.get("/", tags=["Health"])
def root():
    return {"status": "ok", "projet": "E4 Mobilité Paris"}

@app.get("/health", tags=["Health"])
def health():
    return {"status": "healthy"}



# from fastapi import FastAPI, Security, HTTPException, status
# from fastapi.security.api_key import APIKeyHeader
# from fastapi.middleware.cors import CORSMiddleware
# import os

# from routers import velib, evenements, cyclable

# # ── Configuration ──────────────────────────────────────────
# API_KEY       = os.getenv("API_KEY", "e4-api-key-mobilite-paris-2026")
# api_key_header = APIKeyHeader(name="X-API-KEY", auto_error=False)

# # ── Application FastAPI ────────────────────────────────────
# app = FastAPI(
#     title="API Mobilité Paris – E4",
#     description="""
# API REST du projet E4 – Data Mobilité Paris.

# ## Authentification
# Toutes les routes nécessitent un header **X-API-KEY**.

# ## Données disponibles
# - 🚲 **Vélib** : stations et disponibilité temps réel
# - 🎭 **Événements** : agenda culturel parisien
# - 🛣️ **Aménagements cyclables** : pistes et voies cyclables
#     """,
#     version="1.0.0"
# )

# # ── CORS (autorise Streamlit à appeler l'API) ──────────────
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_methods=["GET"],
#     allow_headers=["*"],
# )

# # ── Vérification clé API ───────────────────────────────────
# def check_api_key(key: str = Security(api_key_header)):
#     if key != API_KEY:
#         raise HTTPException(
#             status_code=status.HTTP_403_FORBIDDEN,
#             detail="Clé API invalide ou absente. Ajoutez le header X-API-KEY."
#         )
#     return key

# # ── Routes de base ─────────────────────────────────────────
# @app.get("/", tags=["Health"], summary="Racine de l'API")
# def root():
#     return {"status": "ok", "projet": "E4 Mobilité Paris", "version": "1.0.0"}

# @app.get("/health", tags=["Health"], summary="Vérification santé de l'API")
# def health():
#     return {"status": "healthy"}

# # ── Inclusion des routers ──────────────────────────────────
# app.include_router(
#     velib.router,
#     prefix="/api/velib",
#     tags=["Vélib"],
#     dependencies=[Security(check_api_key)]
# )
# app.include_router(
#     evenements.router,
#     prefix="/api/evenements",
#     tags=["Événements"],
#     dependencies=[Security(check_api_key)]
# )
# app.include_router(
#     cyclable.router,
#     prefix="/api/cyclable",
#     tags=["Aménagements Cyclables"],
#     dependencies=[Security(check_api_key)]
# )