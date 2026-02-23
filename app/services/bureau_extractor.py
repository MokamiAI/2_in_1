# =============================================================================
# bureau_extractor.py
# =============================================================================
# Reads bureau_profiles.raw_payload, extracts all relevant signals,
# computes the recommendation level, and upserts into bureau_features.
#
# RECOMMENDATION LEVEL:
#   Level 1 — has credit score (presage_score OR nlr_score)
#   Level 2 — no credit score BUT (employed OR has active directorship)
#             NOTE: employed + director together = Level 2 (more generous)
#   Level 3 — no credit score, not employed BUT has any directorship
#   Level 4 — nothing available — age-only fallback
# =============================================================================

from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Optional

from postgrest.exceptions import APIError

from app.db.supabase_client import supabase


# =============================================================================
# Utilities
# =============================================================================

def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(float(str(value).replace(",", "").strip()))
    except Exception:
        return default


def _safe_bool_yn(value: Any) -> Optional[bool]:
    """Convert Yes/No/Access disabled strings to bool or None."""
    if value is None:
        return None
    s = str(value).strip().lower()
    if not s:
        return None
    if "access disabled" in s:
        return None
    if s in ("yes", "true", "1"):
        return True
    if s in ("no", "false", "0"):
        return False
    return None


def _calc_age(birth_date: Any) -> Optional[int]:
    """Calculate age from ISO date string or date object."""
    if not birth_date:
        return None
    try:
        if isinstance(birth_date, str):
            dob = date.fromisoformat(birth_date[:10])
        elif isinstance(birth_date, date):
            dob = birth_date
        else:
            return None
        today = date.today()
        return today.year - dob.year - (
            (today.month, today.day) < (dob.month, dob.day)
        )
    except Exception:
        return None


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


# =============================================================================
# Raw payload parsers
# =============================================================================

def _parse_safps(value: Any) -> str:
    """Return 'clean' | 'listed' | 'unknown'."""
    if value is None:
        return "unknown"
    s = str(value).strip().lower()
    if "access disabled" in s:
        return "unknown"
    if s in ("yes", "true", "1"):
        return "listed"
    if s in ("no", "false", "0"):
        return "clean"
    return "unknown"


def _parse_contacts(contacts: list) -> Dict[str, Any]:
    """
    Extract contact signals from contacts array.
    Returns:
        residential_address_count, postal_address_count,
        has_cellular, has_email
    """
    residential = 0
    postal = 0
    has_cellular = False
    has_email = False

    for c in contacts:
        if not isinstance(c, dict):
            continue
        ctype = str(c.get("contact_type") or "").strip().lower()
        if ctype == "address:residential":
            residential += 1
        elif ctype == "address:postal":
            postal += 1
        elif "tel:cellular" in ctype:
            has_cellular = True
        elif ctype == "email":
            has_email = True

    return {
        "residential_address_count": residential,
        "postal_address_count": postal,
        "has_cellular": has_cellular,
        "has_email": has_email,
    }


def _parse_employment(employment: list) -> Dict[str, Any]:
    """
    Extract employment signals.
    Returns:
        employment_history_count
    """
    return {
        "employment_history_count": len([e for e in employment if isinstance(e, dict)])
    }


def _parse_principals(principals: list) -> Dict[str, Any]:
    """
    Extract directorship signals from principals array.
    Statuses observed: 'Active', 'Resigned', 'Inactive'
    Returns:
        total_directorships, active_directorships,
        resigned_directorships, inactive_directorships
    """
    total = 0
    active = 0
    resigned = 0
    inactive = 0

    for p in principals:
        if not isinstance(p, dict):
            continue
        total += 1
        status = str(p.get("principal_status") or "").strip().lower()
        if status == "active":
            active += 1
        elif status == "resigned":
            resigned += 1
        elif status == "inactive":
            inactive += 1

    return {
        "total_directorships": total,
        "active_directorships": active,
        "resigned_directorships": resigned,
        "inactive_directorships": inactive,
    }


def _parse_enquiries(
    enquiries: list,
) -> Dict[str, int]:
    """
    Count credit enquiries in the last 30 and 90 days.
    Returns:
        recent_enquiries_30d, recent_enquiries_90d
    """
    now = datetime.now(timezone.utc)
    cutoff_30 = now - timedelta(days=30)
    cutoff_90 = now - timedelta(days=90)
    count_30 = 0
    count_90 = 0

    for e in enquiries:
        if not isinstance(e, dict):
            continue
        d = e.get("enquiry_date")
        if not d:
            continue
        try:
            dt = datetime.fromisoformat(str(d)[:10]).replace(tzinfo=timezone.utc)
            if dt >= cutoff_30:
                count_30 += 1
            if dt >= cutoff_90:
                count_90 += 1
        except Exception:
            continue

    return {
        "recent_enquiries_30d": count_30,
        "recent_enquiries_90d": count_90,
    }


# =============================================================================
# Recommendation level computation
# =============================================================================

def _compute_recommendation_level(
    has_credit_score: bool,
    is_employed: bool,
    active_directorships: int,
    total_directorships: int,
) -> tuple[int, str]:
    """
    Returns (level, reason) tuple.

    Level 1: credit score present
    Level 2: no credit score BUT employed OR has active directorship
             (employed + director together = Level 2, more generous)
    Level 3: no credit score, not employed, no active directorship
             BUT has any directorship history
    Level 4: nothing — age-only fallback
    """
    if has_credit_score:
        return 1, "Credit score available (presage or nlr)"

    # Level 2: employed and/or has active directorship
    if is_employed or active_directorships > 0:
        reasons = []
        if is_employed:
            reasons.append("currently employed")
        if active_directorships > 0:
            reasons.append(f"{active_directorships} active directorship(s)")
        return 2, "No credit score — " + " and ".join(reasons)

    # Level 3: has some directorship history (resigned/inactive)
    if total_directorships > 0:
        return 3, (
            f"No credit score, not employed — "
            f"{total_directorships} directorship(s) in history (none active)"
        )

    # Level 4: nothing available
    return 4, "No credit score, no employment, no directorships — age-only fallback"


# =============================================================================
# Main extractor
# =============================================================================

def extract_bureau_features(bureau_profile_id: str) -> Dict[str, Any]:
    """
    Reads a single bureau_profiles row by ID, extracts all signals
    from raw_payload, computes the recommendation level, and upserts
    a row into bureau_features.

    Returns a status dict:
        {"status": "success", "bureau_profile_id": ..., "client_id": ...,
         "recommendation_level": int, "mode": "inserted" | "already_exists"}
    or
        {"status": "skipped", "reason": str}
    or
        {"status": "error", "reason": str}
    """

    # ------------------------------------------------------------------
    # 1. Load bureau_profile row
    # ------------------------------------------------------------------
    try:
        res = (
            supabase()
            .table("bureau_profiles")
            .select(
                "id, user_id, presage_score, nlr_score, "
                "current_employer, home_affairs_verified_yn, "
                "home_affairs_deceased_status, safps_listing_yn, "
                "raw_payload, status"
            )
            .eq("id", bureau_profile_id)
            .limit(1)
            .execute()
        )
    except Exception as e:
        return {"status": "error", "reason": f"DB fetch error: {e}"}

    if not res.data:
        return {"status": "skipped", "reason": "bureau_profile not found"}

    bp = res.data[0]

    if bp.get("status") != "success":
        return {
            "status": "skipped",
            "reason": f"bureau_profile status is '{bp.get('status')}' — skipping",
        }

    client_id: str = bp.get("user_id")
    if not client_id:
        return {"status": "skipped", "reason": "bureau_profile has no user_id"}

    # ------------------------------------------------------------------
    # 2. Check if features already extracted for this profile
    # ------------------------------------------------------------------
    try:
        existing = (
            supabase()
            .table("bureau_features")
            .select("id")
            .eq("bureau_profile_id", bureau_profile_id)
            .limit(1)
            .execute()
        )
        if existing.data:
            return {
                "status": "success",
                "mode": "already_exists",
                "bureau_profile_id": bureau_profile_id,
                "client_id": client_id,
            }
    except Exception:
        pass  # If check fails, continue and attempt insert

    # ------------------------------------------------------------------
    # 3. Parse raw_payload
    # ------------------------------------------------------------------
    raw = bp.get("raw_payload") or {}

    # raw_payload may arrive as a string (Supabase JSON column)
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            raw = {}

    parsed   = raw.get("parsed") or {}
    profile  = parsed.get("profile") or {}
    contacts = parsed.get("contacts") or []
    employment = parsed.get("employment") or []
    principals = parsed.get("principals") or []
    enquiries  = parsed.get("credit_enquiries") or []

    # ------------------------------------------------------------------
    # 4. Identity signals
    # ------------------------------------------------------------------
    is_deceased         = _safe_bool_yn(
        bp.get("home_affairs_deceased_status")
        or profile.get("home_affairs_deceased_status")
    )
    is_identity_verified = _safe_bool_yn(
        bp.get("home_affairs_verified_yn")
        or profile.get("home_affairs_verified_yn")
    )
    safps_status = _parse_safps(
        bp.get("safps_listing_yn")
        or profile.get("safps_listing_yn")
    )
    marital_status = (
        profile.get("marital_status_desc")
        or profile.get("marital_status")
        or None
    )
    gender = profile.get("gender") or None

    birth_date = profile.get("birth_date") or profile.get("date_of_birth")
    age = _calc_age(birth_date)

    # ------------------------------------------------------------------
    # 5. Credit signals
    # ------------------------------------------------------------------
    presage_score = _safe_int(bp.get("presage_score"), 0) or None
    nlr_score     = _safe_int(bp.get("nlr_score"), 0) or None

    # presage takes priority; nlr as fallback; None if neither
    if presage_score and presage_score > 0:
        effective_credit_score = presage_score
    elif nlr_score and nlr_score > 0:
        effective_credit_score = nlr_score
    else:
        effective_credit_score = None

    has_credit_score = effective_credit_score is not None

    # ------------------------------------------------------------------
    # 6. Employment signals
    # ------------------------------------------------------------------
    current_employer = (
        str(bp.get("current_employer") or "").strip()
        or str(profile.get("current_employer") or "").strip()
        or None
    )
    is_employed = bool(current_employer)
    emp_signals = _parse_employment(employment)

    # ------------------------------------------------------------------
    # 7. Directorship signals
    # ------------------------------------------------------------------
    dir_signals = _parse_principals(principals)

    # Override total_directorships with bureau-level field if present
    # (bureau_profiles.number_of_company_director is the raw field)
    bureau_director_count = _safe_int(
        profile.get("number_of_company_director"), 0
    )
    if bureau_director_count > 0 and dir_signals["total_directorships"] == 0:
        # Principals array was empty but bureau reported directors
        dir_signals["total_directorships"] = bureau_director_count

    # ------------------------------------------------------------------
    # 8. Contact signals
    # ------------------------------------------------------------------
    contact_signals  = _parse_contacts(contacts)
    enquiry_signals  = _parse_enquiries(enquiries)

    # Supplement has_email from profile if not found in contacts
    if not contact_signals["has_email"] and profile.get("email"):
        contact_signals["has_email"] = True

    # ------------------------------------------------------------------
    # 9. Compute recommendation level
    # ------------------------------------------------------------------
    rec_level, rec_reason = _compute_recommendation_level(
        has_credit_score=has_credit_score,
        is_employed=is_employed,
        active_directorships=dir_signals["active_directorships"],
        total_directorships=dir_signals["total_directorships"],
    )

    # ------------------------------------------------------------------
    # 10. Build insert row
    # ------------------------------------------------------------------
    row: Dict[str, Any] = {
        "id":                       str(uuid.uuid4()),
        "client_id":                client_id,
        "bureau_profile_id":        bureau_profile_id,

        # Identity
        "is_deceased":              is_deceased,
        "is_identity_verified":     is_identity_verified,
        "safps_status":             safps_status,
        "marital_status":           marital_status,
        "gender":                   gender,
        "age":                      age,

        # Credit
        "presage_score":            presage_score,
        "nlr_score":                nlr_score,
        "effective_credit_score":   effective_credit_score,

        # Employment
        "current_employer":         current_employer,
        "is_employed":              is_employed,
        "employment_history_count": emp_signals["employment_history_count"],

        # Directorships
        "total_directorships":      dir_signals["total_directorships"],
        "active_directorships":     dir_signals["active_directorships"],
        "resigned_directorships":   dir_signals["resigned_directorships"],
        "inactive_directorships":   dir_signals["inactive_directorships"],

        # Contacts
        "residential_address_count": contact_signals["residential_address_count"],
        "postal_address_count":      contact_signals["postal_address_count"],
        "has_cellular":              contact_signals["has_cellular"],
        "has_email":                 contact_signals["has_email"],

        # Enquiries
        "recent_enquiries_30d":     enquiry_signals["recent_enquiries_30d"],
        "recent_enquiries_90d":     enquiry_signals["recent_enquiries_90d"],

        # Recommendation level
        "recommendation_level":        rec_level,
        "recommendation_level_reason": rec_reason,

        "extracted_at": _now_iso(),
        "created_at":   _now_iso(),
    }

    # ------------------------------------------------------------------
    # 11. Insert into bureau_features
    # ------------------------------------------------------------------
    try:
        supabase().table("bureau_features").insert(row).execute()
    except APIError as e:
        return {"status": "error", "reason": f"Insert failed: {e}"}

    return {
        "status":                 "success",
        "mode":                   "inserted",
        "bureau_profile_id":      bureau_profile_id,
        "client_id":              client_id,
        "recommendation_level":   rec_level,
        "recommendation_level_reason": rec_reason,
        "age":                    age,
        "has_credit_score":       has_credit_score,
        "is_employed":            is_employed,
        "active_directorships":   dir_signals["active_directorships"],
        "total_directorships":    dir_signals["total_directorships"],
    }


# =============================================================================
# Batch extractor
# =============================================================================

def extract_bureau_features_for_all_pending() -> Dict[str, Any]:
    """
    Finds all successful bureau_profiles that do NOT yet have a
    bureau_features row and runs the extractor on each.

    Returns a summary dict with counts.
    """
    # Find bureau_profiles with no matching bureau_features row
    try:
        profiles = (
            supabase()
            .table("bureau_profiles")
            .select("id")
            .eq("status", "success")
            .execute()
            .data
            or []
        )
    except Exception as e:
        return {"status": "error", "reason": f"Could not load profiles: {e}"}

    if not profiles:
        return {"status": "success", "processed": 0, "skipped": 0, "errors": 0}

    # Get already-extracted profile IDs
    try:
        extracted = (
            supabase()
            .table("bureau_features")
            .select("bureau_profile_id")
            .execute()
            .data
            or []
        )
        extracted_ids = {row["bureau_profile_id"] for row in extracted}
    except Exception:
        extracted_ids = set()

    pending = [p["id"] for p in profiles if p["id"] not in extracted_ids]

    processed = 0
    skipped   = 0
    errors    = 0

    for profile_id in pending:
        result = extract_bureau_features(profile_id)
        if result["status"] == "success":
            if result.get("mode") == "already_exists":
                skipped += 1
            else:
                processed += 1
        elif result["status"] == "skipped":
            skipped += 1
        else:
            errors += 1

    return {
        "status":    "success",
        "processed": processed,
        "skipped":   skipped,
        "errors":    errors,
        "total":     len(profiles),
        "pending":   len(pending),
    }


# =============================================================================
# Convenience: get latest features for a client
# =============================================================================

def get_latest_bureau_features(client_id: str) -> Optional[Dict[str, Any]]:
    """
    Returns the most recently extracted bureau_features row for a client.
    Returns None if no features exist yet.
    """
    try:
        res = (
            supabase()
            .table("bureau_features")
            .select("*")
            .eq("client_id", client_id)
            .order("extracted_at", desc=True)
            .limit(1)
            .execute()
        )
        return res.data[0] if res.data else None
    except Exception:
        return None