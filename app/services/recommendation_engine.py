# =============================================================================
# recommendation_engine.py
# =============================================================================
# Reads bureau_features + fnb_product_master to recommend products
# based on client's primary_interest.
#
# TIER LOGIC:
#   tier 1 = entry product  (most accessible — lowest barrier)
#   tier 2 = step-up product within the SAME product category
#
#   Engine shows tier 1 as BEST RECOMMENDATION.
#   Engine shows tier 2 as NEXT BEST RECOMMENDATION.
#   If client qualifies for NEITHER tier → best reason carries sorry message,
#   next best remains null.
#
# CATALOGUE ITEM SELECTION (rank-based):
#   Each product has a catalogue_items array, each item having a "rank" field.
#   Rank 1 is always the floor/entry item.
#   Rank 2 is the minimum rank to receive a Best recommendation.
#
#   Profile score (0–5) determines which rank is selected:
#     Score 0 → no qualifying product (sorry message on best_reason, next_best null)
#     Score 1 → Best = Rank 2,  Next Best = Rank 1
#     Score 2 → Best = Rank 3,  Next Best = Rank 2
#     Score 3 → Best = Rank 4,  Next Best = Rank 3
#     Score 4 → Best = Rank 5,  Next Best = Rank 4
#     Score 5 → Best = Rank 6,  Next Best = Rank 5
#
#   Capping: if score exceeds available ranks, client receives the highest
#   available rank. Next Best is always one rank below Best.
#
#   Score signals:
#     credit_score >= 700   +2
#     credit_score 600–699  +1
#     credit_score < 600    +0
#     is_employed           +1
#     active_director       +1
#     no adverse listings   +1
#   Max = 5
#
# REASON FIELD:
#   Built from catalogue item highlights only.
#   No URLs, no "FNB" prefix, no "credit" language.
#   amount_range and example_repayment appended where present.
#
# SORRY MESSAGE (score = 0):
#   best_product_name  → null
#   best_reason        → "Sorry, I couldn't find any qualifying products
#                         that meet your profile at the moment."
#   next_best fields   → all null
#
# SCHEMA:
#   Writes to fnb_recommendations — 19 interests × 8 columns = 152 columns.
#
# INTEREST → COLUMN PREFIX MAP:
#   FNB Account Opening                    → acct_
#   FNB Connect - SIM                      → sim_
#   FNB Connect - Phone - Under R300       → phn_u300_
#   FNB Connect - Phone - R300-R600        → phn_300600_
#   FNB Connect - Phone - R600+            → phn_600p_
#   FNB Insurance - Car                    → ins_car_
#   FNB Insurance - Home                   → ins_home_
#   FNB Insurance - Life                   → ins_life_
#   FNB Insurance - Legacy Plan            → ins_legacy_
#   FNB Insurance - Funeral Cover          → ins_funeral_
#   FNB Insurance - Income Protector       → ins_income_
#   FNB Loan - Personal Loan               → loan_pl_
#   FNB Loan - Credit Switch               → loan_cs_
#   FNB Loan - Vehicle Finance Dealership  → loan_vfd_
#   FNB Loan - Vehicle Finance Private     → loan_vfp_
#   FNB Loan - Vehicle Finance Leisure     → loan_vfl_
#   FNB Loan - Building Loan               → loan_build_
#   FNB Loan - Refinance Loan              → loan_refi_
#   FNB Loan - Further Loan                → loan_furt_
# =============================================================================

from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from postgrest.exceptions import APIError

from app.db.supabase_client import supabase
from app.services.bureau_extractor import (
    extract_bureau_features,
    get_latest_bureau_features,
)

SORRY_MESSAGE = (
    "Sorry, I couldn't find any qualifying products that meet your profile at the moment."
)

# =============================================================================
# Interest → column prefix  (matches fnb_recommendations schema exactly)
# =============================================================================

INTEREST_COLUMN_PREFIX: Dict[str, str] = {
    "FNB Account Opening":                   "acct_",
    "FNB Connect - SIM":                     "sim_",
    "FNB Connect - Phone - Under R300":      "phn_u300_",
    "FNB Connect - Phone - R300-R600":       "phn_300600_",
    "FNB Connect - Phone - R600+":           "phn_600p_",
    "FNB Insurance - Car":                   "ins_car_",
    "FNB Insurance - Home":                  "ins_home_",
    "FNB Insurance - Life":                  "ins_life_",
    "FNB Insurance - Legacy Plan":           "ins_legacy_",
    "FNB Insurance - Funeral Cover":         "ins_funeral_",
    "FNB Insurance - Income Protector":      "ins_income_",
    "FNB Loan - Personal Loan":              "loan_pl_",
    "FNB Loan - Credit Switch":              "loan_cs_",
    "FNB Loan - Vehicle Finance Dealership": "loan_vfd_",
    "FNB Loan - Vehicle Finance Private":    "loan_vfp_",
    "FNB Loan - Vehicle Finance Leisure":    "loan_vfl_",
    "FNB Loan - Building Loan":              "loan_build_",
    "FNB Loan - Refinance Loan":             "loan_refi_",
    "FNB Loan - Further Loan":               "loan_furt_",
}

ALL_INTERESTS: List[str] = list(INTEREST_COLUMN_PREFIX.keys())

# Flow groupings — used only for option_recommendations JSONB snapshot
FLOW_REPRESENTATIVE_INTERESTS: Dict[str, List[str]] = {
    "Account": ["FNB Account Opening"],
    "Connect": [
        "FNB Connect - SIM",
        "FNB Connect - Phone - Under R300",
        "FNB Connect - Phone - R300-R600",
        "FNB Connect - Phone - R600+",
    ],
    "Insurance": [
        "FNB Insurance - Car",
        "FNB Insurance - Home",
        "FNB Insurance - Funeral Cover",
        "FNB Insurance - Life",
        "FNB Insurance - Income Protector",
        "FNB Insurance - Legacy Plan",
    ],
    "Loan": [
        "FNB Loan - Personal Loan",
        "FNB Loan - Credit Switch",
        "FNB Loan - Building Loan",
        "FNB Loan - Further Loan",
        "FNB Loan - Refinance Loan",
        "FNB Loan - Vehicle Finance Dealership",
        "FNB Loan - Vehicle Finance Private",
        "FNB Loan - Vehicle Finance Leisure",
    ],
}

INTEREST_TO_FLOW: Dict[str, str] = {
    interest: flow
    for flow, interests in FLOW_REPRESENTATIVE_INTERESTS.items()
    for interest in interests
}


# =============================================================================
# Utilities
# =============================================================================

def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(float(str(value).replace(",", "").strip()))
    except Exception:
        return default


def _parse_json_field(value: Any) -> Any:
    """Parse a JSONB field that may arrive as string or already parsed."""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return None
    return None


# =============================================================================
# Text cleaning
# =============================================================================

def _clean_text(text: str) -> str:
    """Strip FNB branding and credit language from display text."""
    replacements = [
        ("FNB ", ""),
        (" via FNB App", ""),
        ("FNB App", "the app"),
        ("FNB Connect", "Connect"),
        ("FNB-to-FNB", "same-bank"),
        ("credit check", "profile check"),
        ("Credit check", "Profile check"),
    ]
    result = text
    for old, new in replacements:
        result = result.replace(old, new)
    while "  " in result:
        result = result.replace("  ", " ")
    return result.strip()


# =============================================================================
# Profile scoring  (0–7)
# =============================================================================

def _score_profile(features: Dict[str, Any]) -> int:
    """
    Score client bureau profile. Range 0–7.

    Score → Rank unlocked:
      0 → no qualifying product (sorry message)
      1 → Rank 2 (Best), Rank 1 (Next Best)
      2 → Rank 3 (Best), Rank 2 (Next Best)
      3 → Rank 4 (Best), Rank 3 (Next Best)
      4 → Rank 5 (Best), Rank 4 (Next Best)
      5 → Rank 6 (Best), Rank 5 (Next Best)
      6 → Rank 7 (Best), Rank 6 (Next Best)
      7 → Rank 8 (Best), Rank 7 (Next Best)

    Signals:
      credit_score >= 700  → +2
      credit_score 600–699 → +1
      credit_score < 600   → +0
      is_employed          → +1
      active_directorships = 1     → +1
      active_directorships = 2–3   → +2
      active_directorships = 4+    → +3
      no adverse listings  → +1
    """
    score = 0

    effective_score = features.get("effective_credit_score")
    if effective_score is not None:
        cs = _safe_int(effective_score, 0)
        if cs >= 700:
            score += 2
        elif cs >= 600:
            score += 1
        # < 600 → +0

    if features.get("is_employed"):
        score += 1

    active_dirs = _safe_int(features.get("active_directorships"), 0)
    # Fallback: if has_active_directorship flag is set but count is 0, treat as 1
    if active_dirs == 0 and features.get("has_active_directorship"):
        active_dirs = 1
    if active_dirs >= 4:
        score += 3
    elif active_dirs >= 2:
        score += 2
    elif active_dirs == 1:
        score += 1

    adverse = _safe_int(features.get("adverse_accounts"), 0)
    safps   = features.get("safps_status", "unknown")
    if adverse == 0 and safps != "listed":
        score += 1

    return score


# =============================================================================
# Catalogue item selection (rank-based)
# =============================================================================

def _get_catalogue_items(product: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Parse and return catalogue_items array sorted ascending by rank.
    Falls back to empty list if field is missing or malformed.
    """
    raw = _parse_json_field(product.get("catalogue_items"))
    if not isinstance(raw, list):
        return []
    items = [i for i in raw if isinstance(i, dict) and "rank" in i]
    return sorted(items, key=lambda x: int(x.get("rank", 0)))


def _select_catalogue_items_by_rank(
    product: Dict[str, Any],
    features: Dict[str, Any],
) -> tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], bool]:
    """
    Select best and next_best catalogue items based on profile score.

    Returns (best_item, next_best_item, is_sorry) where:
      is_sorry=True  → score is 0, client does not qualify
      is_sorry=False → normal ranked selection

    Rank selection:
      score 0 → sorry (no best, no next_best)
      score 1 → best=rank2, next_best=rank1
      score 2 → best=rank3, next_best=rank2
      score N → best=rank(N+1), next_best=rank(N)
      Capped at highest available rank in the product's catalogue_items.
    """
    items = _get_catalogue_items(product)
    if not items:
        return None, None, True

    score = _score_profile(features)

    # Score 0 — client does not qualify
    if score == 0:
        return None, None, True

    max_rank = len(items)  # e.g. 6 items → ranks 1–6

    # Best rank index (0-based): score 1 → index 1 (rank 2), capped at max
    best_idx      = min(score, max_rank - 1)
    next_best_idx = best_idx - 1  # always one step below best

    best_item      = items[best_idx]
    next_best_item = items[next_best_idx] if next_best_idx >= 0 else None

    return best_item, next_best_item, False


# =============================================================================
# Reason builder
# =============================================================================

def _build_reason_from_catalogue_item(item: Dict[str, Any]) -> str:
    """
    Build reason string from highlights. Appends amount_range and
    example_repayment where present. No URLs, no FNB branding.
    """
    parts: List[str] = []

    highlights = item.get("highlights") or []
    if isinstance(highlights, list):
        cleaned = [
            _clean_text(h)
            for h in highlights
            if h and "http" not in str(h).lower()
        ]
        if cleaned:
            parts.append(" | ".join(cleaned))

    amount_range = item.get("amount_range")
    if amount_range:
        parts.append(f"Amount: {_clean_text(str(amount_range))}")

    example = item.get("example_repayment")
    if example and "http" not in str(example).lower():
        parts.append(_clean_text(str(example)))

    return " — ".join(parts) if parts else ""


def _extract_product_info(
    catalogue_item: Optional[Dict[str, Any]],
    product: Dict[str, Any],
    is_sorry: bool = False,
) -> Dict[str, Any]:
    """
    Build the product info dict from a selected catalogue item.

    When is_sorry=True and catalogue_item is None:
      product_name → null
      reason       → SORRY_MESSAGE
      product_code → null
      tier         → null

    Otherwise:
      product_name → from catalogue item's product_name / name / plan_name
      reason       → from catalogue item's highlights + amount fields
      product_code → from the master product record
      tier         → from the master product record
    """
    if is_sorry or catalogue_item is None:
        return {
            "product_code": None,
            "product_name": None,
            "tier":         None,
            "reason":       SORRY_MESSAGE if is_sorry else None,
        }

    raw_name = (
        catalogue_item.get("product_name")
        or catalogue_item.get("name")
        or catalogue_item.get("plan_name")
        or ""
    )
    reason = _build_reason_from_catalogue_item(catalogue_item)

    return {
        "product_code": product.get("product_code"),
        "product_name": _clean_text(raw_name),
        "tier":         product.get("tier"),
        "reason":       reason,
    }


# =============================================================================
# DB helpers
# =============================================================================

def _get_client(client_id: str) -> Optional[Dict[str, Any]]:
    try:
        res = (
            supabase()
            .table("clients")
            .select("id, primary_interest, date_of_birth, age")
            .eq("id", client_id)
            .limit(1)
            .execute()
        )
        return res.data[0] if res.data else None
    except Exception:
        return None


def _get_existing_recommendation(client_id: str) -> Optional[Dict[str, Any]]:
    try:
        res = (
            supabase()
            .table("fnb_recommendations")
            .select(
                "customer_id, primary_interest_snapshot, " +
                ", ".join(
                    f"{p}best_product_name"
                    for p in INTEREST_COLUMN_PREFIX.values()
                )
            )
            .eq("customer_id", client_id)
            .limit(1)
            .execute()
        )
        return res.data[0] if res.data else None
    except Exception:
        return None


def _get_products_for_interest(
    primary_interest: str,
) -> Dict[int, Dict[str, Any]]:
    """Load all active tiers for a primary_interest. Returns {tier: product}."""
    try:
        res = (
            supabase()
            .table("fnb_product_master")
            .select("*")
            .eq("primary_interest", primary_interest)
            .eq("is_active", True)
            .order("tier")
            .execute()
        )
        return {row["tier"]: row for row in (res.data or [])}
    except Exception:
        return {}


# =============================================================================
# Eligibility checks
# =============================================================================

def _passes_hard_gates(
    product: Dict[str, Any],
    features: Dict[str, Any],
    client_age: Optional[int],
) -> bool:
    if features.get("is_deceased") is True:
        return False
    min_age = _safe_int(product.get("min_age"), 18)
    age     = client_age or features.get("age") or 0
    if age > 0 and age < min_age:
        return False
    return True


def _passes_credit_gates(
    product: Dict[str, Any],
    features: Dict[str, Any],
    rec_level: int,
) -> bool:
    credit_check     = product.get("credit_check", False)
    employment_req   = product.get("employment_required", False)
    min_credit_score = product.get("min_credit_score")
    effective_score  = features.get("effective_credit_score")
    is_employed      = features.get("is_employed", False)
    has_active_dir   = (
        features.get("has_active_directorship", False)
        or features.get("active_directorships", 0) > 0
    )
    safps = features.get("safps_status", "unknown")

    if safps == "listed" and credit_check:
        return False

    if rec_level == 1:
        if credit_check and effective_score is None:
            return False
        if credit_check and min_credit_score and effective_score is not None:
            if effective_score < min_credit_score:
                return False
        if employment_req and not is_employed:
            return False
        return True

    if rec_level == 2:
        if employment_req and not (is_employed or has_active_dir):
            return False
        return True

    if rec_level == 3:
        if credit_check and effective_score is not None and min_credit_score:
            if effective_score < min_credit_score:
                return False
        return True

    if rec_level == 4:
        if credit_check or employment_req:
            return False
        return True

    return True


# =============================================================================
# Core recommendation logic — per interest
# =============================================================================

def _recommend_for_interest(
    primary_interest: str,
    features: Dict[str, Any],
    client_age: Optional[int],
) -> Dict[str, Any]:
    """
    Produce best + next_best for one primary_interest.

    tier 1 product → BEST   (entry, most accessible)
    tier 2 product → NEXT BEST  (step-up within same category)

    Within each tier, catalogue item is selected by rank using profile score:
      score 0 → sorry message on best, null next_best
      score 1 → best=rank2, next_best=rank1
      score N → best=rank(N+1), next_best=rank(N), capped at max available rank

    If tier 1 fails eligibility but tier 2 passes → tier 2 promoted to BEST.
    If both fail → best and next_best are None for this interest.
    """
    rec_level = _safe_int(features.get("recommendation_level"), 4)
    products  = _get_products_for_interest(primary_interest)

    empty = {
        "primary_interest": primary_interest,
        "flow":             INTEREST_TO_FLOW.get(primary_interest),
        "best":             None,
        "next_best":        None,
    }

    if not products:
        return empty

    tier1 = products.get(1)
    tier2 = products.get(2)

    tier1_ok = (
        tier1 is not None
        and _passes_hard_gates(tier1, features, client_age)
        and _passes_credit_gates(tier1, features, rec_level)
    )
    tier2_ok = (
        tier2 is not None
        and _passes_hard_gates(tier2, features, client_age)
        and _passes_credit_gates(tier2, features, rec_level)
    )

    best      = None
    next_best = None

    if tier1_ok:
        best_item, next_best_item, is_sorry = _select_catalogue_items_by_rank(
            tier1, features
        )
        best = _extract_product_info(best_item, tier1, is_sorry=is_sorry)
        if next_best_item:
            next_best = _extract_product_info(next_best_item, tier1, is_sorry=False)

    if tier2_ok:
        best_item2, next_best_item2, is_sorry2 = _select_catalogue_items_by_rank(
            tier2, features
        )
        # Only surface tier 2 next_best if we have a real best from tier 1
        if best is not None and not best.get("reason") == SORRY_MESSAGE:
            if best_item2:
                next_best = _extract_product_info(best_item2, tier2, is_sorry=False)

    # Tier 1 failed eligibility but tier 2 passed — promote tier 2 to best
    if (best is None or best.get("reason") == SORRY_MESSAGE) and tier2_ok:
        best_item2, next_best_item2, is_sorry2 = _select_catalogue_items_by_rank(
            tier2, features
        )
        best      = _extract_product_info(best_item2, tier2, is_sorry=is_sorry2)
        next_best = (
            _extract_product_info(next_best_item2, tier2, is_sorry=False)
            if next_best_item2 else None
        )

    return {
        "primary_interest": primary_interest,
        "flow":             INTEREST_TO_FLOW.get(primary_interest),
        "best":             best,
        "next_best":        next_best,
    }


# =============================================================================
# Run ALL 19 interests — primary interest always first
# =============================================================================

def _recommend_all_interests(
    primary_interest: str,
    features: Dict[str, Any],
    client_age: Optional[int],
) -> Dict[str, Dict[str, Any]]:
    """
    Run _recommend_for_interest for all 19 interests.
    Primary interest is always processed first, then the remaining 18.
    Returns { interest_string: result_dict }.
    """
    results: Dict[str, Dict[str, Any]] = {}

    results[primary_interest] = _recommend_for_interest(
        primary_interest, features, client_age
    )

    for interest in ALL_INTERESTS:
        if interest == primary_interest:
            continue
        results[interest] = _recommend_for_interest(interest, features, client_age)

    return results


# =============================================================================
# Change detection + gap detection
# =============================================================================

def _primary_interest_changed(
    existing: Optional[Dict[str, Any]],
    current_primary_interest: str,
) -> bool:
    if not existing:
        return True
    stored = existing.get("primary_interest_snapshot") or ""
    return stored.strip().lower() != current_primary_interest.strip().lower()


def _has_null_product_columns(rec: Dict[str, Any]) -> bool:
    """
    Returns True if ANY of the 19 best_product_name columns is null —
    meaning the row has gaps that need filling.
    """
    for prefix in INTEREST_COLUMN_PREFIX.values():
        if rec.get(f"{prefix}best_product_name") is None:
            return True
    return False


# =============================================================================
# Row builder
# Writes all 152 per-product columns + option_recommendations JSONB.
# =============================================================================

def _build_recommendation_row(
    client_id: str,
    primary_interest: str,
    all_interest_results: Dict[str, Dict[str, Any]],
    bureau_profile_id: str,
) -> Dict[str, Any]:
    """
    Build the complete upsert row for fnb_recommendations.

    19 interests × 8 columns = 152 per-product columns written.
    Columns written per interest:
      {prefix}best_product_code
      {prefix}best_product_name
      {prefix}best_product_tier
      {prefix}best_product_reason
      {prefix}next_best_product_code
      {prefix}next_best_product_name
      {prefix}next_best_product_tier
      {prefix}next_best_product_reason
    """
    product_columns: Dict[str, Any] = {}

    for interest, prefix in INTEREST_COLUMN_PREFIX.items():
        result    = all_interest_results.get(interest, {})
        best      = result.get("best") or {}
        next_best = result.get("next_best") or {}

        product_columns[f"{prefix}best_product_code"]        = best.get("product_code")
        product_columns[f"{prefix}best_product_name"]        = best.get("product_name")
        product_columns[f"{prefix}best_product_tier"]        = best.get("tier")
        product_columns[f"{prefix}best_product_reason"]      = best.get("reason")
        product_columns[f"{prefix}next_best_product_code"]   = next_best.get("product_code")
        product_columns[f"{prefix}next_best_product_name"]   = next_best.get("product_name")
        product_columns[f"{prefix}next_best_product_tier"]   = next_best.get("tier")
        product_columns[f"{prefix}next_best_product_reason"] = next_best.get("reason")

    option_recommendations: Dict[str, Any] = {
        interest: {
            "best":      all_interest_results.get(interest, {}).get("best"),
            "next_best": all_interest_results.get(interest, {}).get("next_best"),
        }
        for interest in ALL_INTERESTS
    }

    row: Dict[str, Any] = {
        "customer_id":               client_id,
        "generated_at":              _now_iso(),
        "enrichment_complete":       True,
        "bureau_profile_id":         bureau_profile_id,
        "primary_interest_snapshot": primary_interest,
        "updated_at":                _now_iso(),
        "option_recommendations":    option_recommendations,
        **product_columns,
    }

    return row


# =============================================================================
# Main entry point
# =============================================================================

def generate_recommendation_for_customer(
    customer_id: str,
) -> Dict[str, Any]:
    """
    Full recommendation run for one customer.

      1. Load client + primary_interest from clients table
      2. Ensure bureau_features exist (extract if missing)
      3. Skip if primary_interest unchanged AND row is already complete
      4. Run primary interest FIRST, then all 18 remaining interests
      5. Write all 152 per-product columns in a single upserts
    """

    # 1. Load client — must exist in clients table
    client = _get_client(customer_id)
    if not client:
        return {"status": "skipped", "reason": "client_not_found"}

    primary_interest = (client.get("primary_interest") or "").strip()
    if not primary_interest:
        return {"status": "skipped", "reason": "no_primary_interest_set"}

    client_age = _safe_int(client.get("age"), 0) or None

    # 2. Get bureau features — extract if missing
    features = get_latest_bureau_features(customer_id)

    if not features:
        try:
            bp_res = (
                supabase()
                .table("bureau_profiles")
                .select("id")
                .eq("user_id", customer_id)
                .eq("status", "success")
                .order("verified_at", desc=True)
                .limit(1)
                .execute()
            )
            if bp_res.data:
                bp_id          = bp_res.data[0]["id"]
                extract_result = extract_bureau_features(bp_id)
                if extract_result["status"] != "success":
                    return {
                        "status": "skipped",
                        "reason": f"bureau_extraction_failed: {extract_result.get('reason')}",
                    }
                features = get_latest_bureau_features(customer_id)
        except Exception as e:
            return {"status": "skipped", "reason": f"bureau_features_load_error: {e}"}

    if not features:
        return {"status": "skipped", "reason": "no_bureau_features_available"}

    if features.get("is_deceased") is True:
        return {"status": "skipped", "reason": "client_is_deceased"}

    bureau_profile_id = features.get("bureau_profile_id", "")

    # 3. Check existing row — skip only if unchanged AND complete
    existing         = _get_existing_recommendation(customer_id)
    interest_changed = _primary_interest_changed(existing, primary_interest)
    has_gaps         = existing is not None and _has_null_product_columns(existing)

    if existing and not interest_changed and not has_gaps:
        return {
            "status":           "success",
            "mode":             "no_change",
            "customer_id":      customer_id,
            "primary_interest": primary_interest,
            "message":          "Recommendation is current and complete",
        }

    # 4. Run all 19 interests — primary interest first
    all_interest_results = _recommend_all_interests(
        primary_interest, features, client_age
    )

    # 5. Build and upsert
    row = _build_recommendation_row(
        client_id=customer_id,
        primary_interest=primary_interest,
        all_interest_results=all_interest_results,
        bureau_profile_id=bureau_profile_id,
    )

    try:
        if existing:
            (
                supabase()
                .table("fnb_recommendations")
                .update(row)
                .eq("customer_id", customer_id)
                .execute()
            )
            mode = "updated"
        else:
            row["id"]         = f"rec-{uuid.uuid4()}"
            row["created_at"] = _now_iso()
            (
                supabase()
                .table("fnb_recommendations")
                .insert(row)
                .execute()
            )
            mode = "inserted"

    except APIError as e:
        return {"status": "error", "reason": f"upsert_failed: {e}"}

    primary_result = all_interest_results.get(primary_interest, {})

    return {
        "status":            "success",
        "mode":              mode,
        "customer_id":       customer_id,
        "primary_interest":  primary_interest,
        "interest_changed":  interest_changed,
        "gap_filled":        has_gaps and not interest_changed,
        "flow":              primary_result.get("flow"),
        "best_product":      primary_result.get("best"),
        "next_best_product": primary_result.get("next_best"),
    }


# =============================================================================
# Batch runner
# Guarantees every client in clients table has a complete recommendation row
# =============================================================================

def generate_recommendations_for_all_pending() -> Dict[str, Any]:
    """
    Ensures EVERY client in the clients table has a complete recommendation row.

    Runs for a client when ANY of the following is true:
      1. No row exists in fnb_recommendations yet
      2. primary_interest has changed since last recommendation
      3. Existing row has null values in any per-product column (gap-fill)

    Clients without a primary_interest are skipped.
    """
    try:
        clients = (
            supabase()
            .table("clients")
            .select("id, primary_interest")
            .not_.is_("primary_interest", "null")
            .execute()
            .data
            or []
        )
    except Exception as e:
        return {"status": "error", "reason": f"Could not load clients: {e}"}

    # Load all existing rows in one query — keyed by customer_id for O(1) lookup
    try:
        existing_rows_list = (
            supabase()
            .table("fnb_recommendations")
            .select(
                "customer_id, primary_interest_snapshot, " +
                ", ".join(
                    f"{p}best_product_name"
                    for p in INTEREST_COLUMN_PREFIX.values()
                )
            )
            .execute()
            .data
            or []
        )
        existing_map: Dict[str, Dict[str, Any]] = {
            r["customer_id"]: r for r in existing_rows_list
        }
    except Exception as e:
        return {"status": "error", "reason": f"Could not load existing recommendations: {e}"}

    results = {
        "inserted":   0,
        "updated":    0,
        "gap_filled": 0,
        "no_change":  0,
        "skipped":    0,
        "errors":     0,
        "total":      len(clients),
    }

    for c in clients:
        cid              = c["id"]
        primary_interest = (c.get("primary_interest") or "").strip()

        if not primary_interest:
            results["skipped"] += 1
            continue

        existing = existing_map.get(cid)

        no_row           = existing is None
        interest_changed = (
            existing is not None
            and (existing.get("primary_interest_snapshot") or "").strip().lower()
                != primary_interest.strip().lower()
        )
        has_gaps = existing is not None and _has_null_product_columns(existing)

        if not no_row and not interest_changed and not has_gaps:
            results["no_change"] += 1
            continue

        outcome = generate_recommendation_for_customer(cid)
        mode    = outcome.get("mode") or outcome.get("status")

        if mode == "inserted":
            results["inserted"] += 1
        elif mode == "updated":
            if has_gaps and not interest_changed:
                results["gap_filled"] += 1
            else:
                results["updated"] += 1
        elif mode == "no_change":
            results["no_change"] += 1
        elif outcome.get("status") == "skipped":
            results["skipped"] += 1
        else:
            results["errors"] += 1

    return {"status": "success", **results}