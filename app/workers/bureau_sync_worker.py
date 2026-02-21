from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

from app.core.config import settings
from app.db.supabase_client import supabase
from app.services.xds_verification import run_xds_for_user, insert_verification_log

logger = logging.getLogger(__name__)


def _ensure_verification_request(user_id: str) -> Optional[str]:
    """
    Create a verification_requests row only if there is no active one.
    Your DB already has a partial unique index on active requests.
    """
    # Check active
    existing = (
        supabase()
        .table("verification_requests")
        .select("id,status,attempts,max_attempts")
        .eq("user_id", user_id)
        .eq("bureau", settings.BUREAU)
        .in_("status", ["pending", "processing", "retry"])
        .limit(1)
        .execute()
    )

    if existing.data:
        return existing.data[0]["id"]

    # Create new
    try:
        created = (
            supabase()
            .table("verification_requests")
            .insert(
                {
                    "user_id": user_id,
                    "bureau": settings.BUREAU,
                    "status": "pending",
                    "attempts": 0,
                    "max_attempts": 3,
                    "priority": 5,
                }
            )
            .execute()
        )
        if created.data:
            return created.data[0]["id"]
    except Exception:
        # If a race hits the unique partial index, fetch again
        existing2 = (
            supabase()
            .table("verification_requests")
            .select("id")
            .eq("user_id", user_id)
            .eq("bureau", settings.BUREAU)
            .in_("status", ["pending", "processing", "retry"])
            .limit(1)
            .execute()
        )
        if existing2.data:
            return existing2.data[0]["id"]

    return None


def _mark_request_processing(request_id: str) -> None:
    """
    Uses DB RPC so attempts increments atomically.
    """
    supabase().rpc("mark_verification_request_processing", {"p_request_id": request_id}).execute()


def _mark_request_done(request_id: str, status: str, last_error: Optional[str]) -> None:
    supabase().rpc(
        "mark_verification_request_done",
        {"p_request_id": request_id, "p_status": status, "p_last_error": last_error},
    ).execute()


def _process_one_user(user: Dict[str, Any]) -> Dict[str, Any]:
    """
    Runs inside a thread (because zeep + requests are blocking).
    Returns result dict with user_id and request_id included.
    """
    user_id = user["user_id"]

    request_id = _ensure_verification_request(user_id)
    if not request_id:
        return {"user_id": user_id, "status": "failed", "error": "Could not create verification_request"}

    try:
        _mark_request_processing(request_id)

        # call your verification pipeline (this already skips if verified)
        result = run_xds_for_user(request_id=request_id, user=user)
        result["user_id"] = user_id
        result["request_id"] = request_id
        return result

    except Exception as e:
        # If something unexpected happened, mark retry
        insert_verification_log(
            user_id=user_id,
            request_id=request_id,
            step="worker_exception",
            status="failed",
            message=str(e),
        )
        return {"user_id": user_id, "request_id": request_id, "status": "failed", "error": str(e)}


async def bureau_sync_loop() -> None:
    """
    Forever loop:
      - pulls candidates via RPC
      - processes them concurrently (ThreadPoolExecutor)
      - marks verification_requests completed/retry
      - sleeps
    """
    max_workers = min(10, max(1, settings.BATCH_SIZE))  # safe cap
    executor = ThreadPoolExecutor(max_workers=max_workers)

    while True:
        try:
            # 1) Pull ONLY unverified clients (RPC already filters out verified)
            resp = supabase().rpc(
                "get_clients_needing_bureau_data",
                {"p_bureau": settings.BUREAU, "p_limit": settings.BATCH_SIZE},
            ).execute()

            users: List[Dict[str, Any]] = resp.data or []
            if not users:
                await asyncio.sleep(settings.POLL_INTERVAL_SECONDS)
                continue

            # 2) Process concurrently (blocking IO in threads)
            loop = asyncio.get_running_loop()
            tasks = [loop.run_in_executor(executor, _process_one_user, user) for user in users]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # 3) Mark verification_requests done
            for r in results:
                if isinstance(r, Exception):
                    logger.warning("Worker task exception: %s", r)
                    continue

                request_id = r.get("request_id")
                if not request_id:
                    continue

                status = r.get("status")

                # Treat skipped as completed (already verified)
                if status in ("success", "skipped"):
                    _mark_request_done(request_id, "completed", None)
                else:
                    # retry on failures
                    _mark_request_done(request_id, "retry", r.get("error"))

            await asyncio.sleep(settings.POLL_INTERVAL_SECONDS)

        except Exception as e:
            logger.exception("bureau_sync_loop crashed: %s", e)
            await asyncio.sleep(settings.POLL_INTERVAL_SECONDS)
