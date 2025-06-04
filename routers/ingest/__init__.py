from fastapi import APIRouter
from . import wikipedia, spn, mediacloud, gdelt

router = APIRouter(prefix="/api/ingest", tags=["ingest"])

router.include_router(wikipedia.router)
router.include_router(spn.router)
router.include_router(mediacloud.router)
router.include_router(gdelt.router)