import logging
import os
import threading
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from app.database import SessionLocal, init_db

load_dotenv()

logger = logging.getLogger(__name__)


def _run_ingestion_thread(symbols: list[str]) -> None:
    """Run ingestion in a background thread with its own DB session."""
    from app.ingestion import IngestionService

    db = SessionLocal()
    try:
        logger.info("Background ingestion started for symbols: %s", symbols)
        result = IngestionService(db).ingest(symbols)
        logger.info(
            "Background ingestion complete — %d rows, %d symbols processed, failed: %s",
            result.total_rows,
            result.symbols_processed,
            result.failed_symbols,
        )
    except Exception:
        logger.exception("Background ingestion failed unexpectedly")
    finally:
        db.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()

    ingest_on_startup = os.getenv("INGEST_ON_STARTUP", "true").strip().lower()
    if ingest_on_startup == "true":
        raw_symbols = os.getenv("SYMBOLS", "INFY.NS,TCS.NS,RELIANCE.NS,HDFCBANK.NS,WIPRO.NS")
        symbols = [s.strip().upper() for s in raw_symbols.split(",") if s.strip()]
        thread = threading.Thread(
            target=_run_ingestion_thread, args=(symbols,), daemon=True
        )
        thread.start()
        logger.info("Startup ingestion thread launched for: %s", symbols)

    yield


app = FastAPI(
    title="Stock Data Intelligence Dashboard",
    description="Mini financial data platform — OHLCV data, metrics, and comparisons.",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS — restrict origins in production via environment variable
allowed_origins = os.getenv("ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from app.routes import router
app.include_router(router)


@app.get("/")
def root():
    return RedirectResponse(url="/static/index.html")


# Serve the frontend dashboard from the static/ directory (task 5.5)
app.mount("/static", StaticFiles(directory="static"), name="static")
