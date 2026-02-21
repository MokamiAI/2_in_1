from fastapi import FastAPI
from contextlib import asynccontextmanager
import asyncio
import logging

from app.core.config import validate_settings
from app.workers.bureau_sync_worker import bureau_sync_loop
from app.workers.recommendation_worker import recommendation_loop

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("nerve-service")

bureau_task: asyncio.Task | None = None
reco_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global bureau_task, reco_task

    logger.info("Starting Nerve Bureau + Recommendation service...")
    validate_settings()

    # Start background workers
    bureau_task = asyncio.create_task(bureau_sync_loop(), name="bureau_sync_loop")
    reco_task = asyncio.create_task(recommendation_loop(), name="recommendation_loop")

    logger.info("Bureau sync worker started.")
    logger.info("Recommendation worker started.")

    try:
        yield
    finally:
        logger.info("Shutting down workers...")

        for t in (bureau_task, reco_task):
            if t:
                t.cancel()

        for tname, t in (("bureau", bureau_task), ("recommendation", reco_task)):
            if not t:
                continue
            try:
                await t
            except asyncio.CancelledError:
                logger.info("%s worker cancelled cleanly.", tname)
            except Exception as e:
                logger.exception("%s worker crashed during shutdown: %s", tname, e)

        logger.info("Shutdown complete.")


app = FastAPI(
    title="Nerve Bureau Auto Sync",
    version="1.1.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health():
    return {
        "ok": True,
        "service": "Nerve Bureau Auto Sync",
        "workers": {
            "bureau_sync": bureau_task is not None and not bureau_task.done(),
            "recommendation": reco_task is not None and not reco_task.done(),
        },
    }
