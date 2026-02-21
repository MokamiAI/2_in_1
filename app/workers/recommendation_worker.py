from __future__ import annotations

import asyncio
import logging
from typing import List, Dict, Any

from app.core.config import settings
from app.db.supabase_client import supabase
from app.services.recommendation_engine import generate_recommendation_for_customer

logger = logging.getLogger("recommendation-worker")


async def _run_one(user_id: str, sem: asyncio.Semaphore) -> None:
    async with sem:
        try:
            # generate_recommendation_for_customer is sync (supabase client is sync),
            # so run it in a thread to avoid blocking the event loop
            result = await asyncio.to_thread(generate_recommendation_for_customer, user_id)

            status = result.get("status")
            if status == "success":
                logger.info("✅ Recommendation generated for user_id=%s", user_id)
            else:
                logger.info("↩️ Skipped user_id=%s reason=%s", user_id, result.get("reason"))

        except Exception as e:
            logger.exception("❌ Recommendation failed for user_id=%s error=%s", user_id, e)


async def recommendation_loop() -> None:
    """
    Loop:
      - fetch users with bureau data but no recommendations
      - generate recommendations concurrently (safe)
      - sleep and repeat
    """
    poll_seconds = getattr(settings, "RECO_POLL_INTERVAL_SECONDS", 30)
    batch_size = getattr(settings, "RECO_BATCH_SIZE", 50)
    concurrency = getattr(settings, "RECO_CONCURRENCY", 10)

    sem = asyncio.Semaphore(concurrency)

    logger.info(
        "Recommendation worker started: poll=%ss batch=%s concurrency=%s",
        poll_seconds, batch_size, concurrency
    )

    while True:
        try:
            rows: List[Dict[str, Any]] = (
                supabase()
                .rpc("clients_ready_for_recommendations", {"p_bureau": "XDS", "p_limit": batch_size})
                .execute()
                .data or []
            )

            if not rows:
                await asyncio.sleep(poll_seconds)
                continue

            user_ids = [r["user_id"] for r in rows if r.get("user_id")]
            logger.info("Found %d users ready for recommendations", len(user_ids))

            tasks = [asyncio.create_task(_run_one(uid, sem)) for uid in user_ids]
            await asyncio.gather(*tasks)

            await asyncio.sleep(poll_seconds)

        except Exception as e:
            logger.exception("recommendation_loop crashed: %s", e)
            await asyncio.sleep(poll_seconds)
