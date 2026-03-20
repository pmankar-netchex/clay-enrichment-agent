"""
GTM Enrichment Agent — FastAPI application entry point.
"""

import logging
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.services.database import init_db
from app.services.batch_review import start_flush_loop, stop_flush_loop
from app.routers import classify, static_lookup, synthesise, review

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up — initialising DuckDB ...")
    init_db()
    start_flush_loop()
    logger.info("Startup complete")
    yield
    stop_flush_loop()
    logger.info("Shutting down")


app = FastAPI(
    title="GTM Enrichment Agent",
    description="Pre-intelligence layer for Clay GTM list enrichment",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["POST", "GET"],
    allow_headers=["*"],
)

# Mount routers under /api/v1
prefix = "/api/v1"
app.include_router(classify.router, prefix=prefix, tags=["Agent Calls"])
app.include_router(static_lookup.router, prefix=prefix, tags=["Agent Calls"])
app.include_router(synthesise.router, prefix=prefix, tags=["Agent Calls"])
app.include_router(review.router, prefix=prefix, tags=["Review"])


@app.get("/health")
async def health():
    return {"status": "ok"}
