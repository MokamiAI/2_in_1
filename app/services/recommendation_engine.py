from __future__ import annotations

import time
import uuid
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

from postgrest.exceptions import APIError

from app.db.supabase_client import supabase

BUREAU = "XDS"

# -----------------------------
# Small utilities
# -----------------------------
def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(float(str(x)))
    except Exception:
        return default


def _is_blank(x: Any) -> bool:
    return x is None or (isinstance(x, str) and not x.strip())


def _calc_age(dob: Optional[str | date]) -> Optional[int]:
    if not dob:
        return None
    try:
        if isinstance(dob, str):
            dob_dt = date.fromisoformat(dob[:10])
        else:
            dob_dt = dob
        today = date.today()
        return today.year - dob_dt.year - ((today.month, today.day) < (dob_dt.month, dob_dt.day))
    except Exception:
        return None


# -----------------------------
# Supabase fetchers
# -----------------------------
def _get_latest_bureau_profile(customer_id: str) -> Optional[Dict[str, Any]]:
    rows = (
        supabase()
        .table("bureau_profiles")
        .select("*")
        .eq("user_id", customer_id)
        .eq("bureau", BUREAU)
        .eq("status", "success")
        .order("verified_at", desc=True)
        .limit(1)
        .execute()
        .data
        or []
    )
    return rows[0] if rows else None


def _get_existing_recommendation_row(customer_id: str) -> Optional[Dict[str, Any]]:
    res = (
        supabase()
        .table("fnb_recommendations")
        .select("*")
        .eq("customer_id", customer_id)
        .limit(1)
        .execute()
    )
    return res.data[0] if res.data else None


def _get_client(customer_id: str) -> Optional[Dict[str, Any]]:
    """
    Robust select: DO NOT reference columns that may not exist (PostgREST fails whole query).
    We only require id + primary_interest; DOB/income are optional.
    """
    select_attempts = [
        "id,primary_interest,date_of_birth,monthly_income,income",
        "id,primary_interest,date_of_birth",
        "id,primary_interest",
    ]

    for sel in select_attempts:
        try:
            res = (
                supabase()
                .table("clients")
                .select(sel)
                .eq("id", customer_id)
                .limit(1)
                .execute()
            )
            return res.data[0] if res.data else None
        except APIError as e:
            msg = str(e)
            if "42703" in msg or "does not exist" in msg:
                continue
            raise
    return None


# -----------------------------
# Catalog loading + caching
# -----------------------------
_catalog_cache: Optional[List[Dict[str, Any]]] = None
_catalog_cache_ts: float = 0.0
_CATALOG_TTL_SECONDS = 300  # refresh every 5 minutes


def _load_catalog() -> List[Dict[str, Any]]:
    """
    Build catalog from insurance_products + products (eligibility_rules/benefits).
    """
    ins = (
        supabase()
        .table("insurance_products")
        .select("id,name,product_code,product_type,description,underwriting_type,active")
        .eq("active", True)
        .execute()
        .data
        or []
    )

    prod = (
        supabase()
        .table("products")
        .select("id,product_code,product_name,eligibility_rules,benefits,option")
        .execute()
        .data
        or []
    )
    by_code = {p["product_code"]: p for p in prod if p.get("product_code")}

    catalog: List[Dict[str, Any]] = []
    for x in ins:
        code = x.get("product_code")
        p = by_code.get(code, {})
        catalog.append(
            {
                "source_id": x.get("id"),
                "product_code": code,
                "name": x.get("name") or p.get("product_name") or code,
                "product_type": x.get("product_type"),
                "description": x.get("description"),
                "underwriting_type": x.get("underwriting_type"),
                "eligibility_rules": p.get("eligibility_rules") or {},
                "benefits": p.get("benefits"),
                "option": p.get("option"),
            }
        )
    return catalog


def load_catalog_cached() -> List[Dict[str, Any]]:
    global _catalog_cache, _catalog_cache_ts
    now = time.time()
    if _catalog_cache and (now - _catalog_cache_ts) < _CATALOG_TTL_SECONDS:
        return _catalog_cache
    _catalog_cache = _load_catalog()
    _catalog_cache_ts = now
    return _catalog_cache


# -----------------------------
# Flow classification (FIXED)
# -----------------------------
def _classify_flow(item: Dict[str, Any]) -> str:
    code = (item.get("product_code") or "").upper()
    pt = (item.get("product_type") or "").lower()
    name = (item.get("name") or "").lower()

    # Account
    if code in {"FNB_EASY_SMART", "FNB_ASPIRE", "FNB_FUSION_ASPIRE"} or "banking account" in pt or "account" in pt:
        return "Account"

    # Connect
    if code.startswith("FNB_CONNECT") or "sim" in name or "telecom" in pt or "device contract" in pt:
        return "Connect"

    # Loan / Credit
    if code in {"FNB_PERSONAL_LOAN", "FNB_CREDIT_CARD", "FNB_OVERDRAFT"} or "loan" in pt or "credit card" in pt:
        return "Loan"

    return "Insurance"


# -----------------------------
# Bureau signals (user-specific)
# -----------------------------
def _bureau_signals(bp: Dict[str, Any]) -> Dict[str, Any]:
    presage = _safe_int(bp.get("presage_score"), 0)
    nlr = _safe_int(bp.get("nlr_score"), 0)
    credit_score = presage if presage > 0 else (nlr if nlr > 0 else None)

    employed = bool((bp.get("current_employer") or "").strip())
    home_affairs_verified = str(bp.get("home_affairs_verified_yn") or "").strip().lower() in ("yes", "true", "1")
    deceased = str(bp.get("home_affairs_deceased_status") or "").strip().lower() in ("yes", "true", "1")
    safps = str(bp.get("safps_listing_yn") or "").strip().lower() in ("yes", "true", "1")

    return {
        "credit_score": credit_score,
        "employed": employed,
        "home_affairs_verified": home_affairs_verified,
        "deceased": deceased,
        "safps": safps,
    }


# -----------------------------
# Eligibility + scoring
# -----------------------------
def _eligibility_passes(
    rules: Dict[str, Any],
    *,
    age: Optional[int],
    income: Optional[int],
    signals: Dict[str, Any],
) -> Tuple[bool, str]:
    if not isinstance(rules, dict):
        rules = {}

    if signals.get("deceased") is True:
        return False, "Not eligible"

    min_age = rules.get("min_age")
    if min_age is not None and age is not None and age < int(min_age):
        return False, "Not eligible"

    min_income = rules.get("min_income")
    if min_income is not None and income is not None and income < int(min_income):
        return False, "Not eligible"

    if rules.get("employment_required") is True and not signals.get("employed"):
        return False, "Not eligible"

    if rules.get("credit_check") is True:
        min_score = rules.get("min_credit_score")
        cs = signals.get("credit_score")
        if min_score is not None:
            if cs is None:
                return False, "Not eligible"
            if int(cs) < int(min_score):
                return False, "Not eligible"

    return True, "Eligible"


def _score_item(
    item: Dict[str, Any],
    *,
    age: Optional[int],
    income: Optional[int],
    signals: Dict[str, Any],
    primary_interest: Optional[str],
) -> float:
    rules = item.get("eligibility_rules") or {}
    ok, _ = _eligibility_passes(rules, age=age, income=income, signals=signals)
    if not ok:
        return -999.0

    flow = _classify_flow(item)
    score = 0.0

    cs = signals.get("credit_score")
    employed = signals.get("employed")
    verified = signals.get("home_affairs_verified")
    safps = signals.get("safps")

    # Baseline user signals
    if verified:
        score += 2.0
    if employed:
        score += 2.0
    if cs is not None:
        score += float(cs) / 150.0  # 600->4.0, 750->5.0

    # Penalize fraud listing
    if safps:
        score -= 8.0

    # Flow weighting
    credit_required = rules.get("credit_check") is True
    min_score = rules.get("min_credit_score")

    if flow == "Loan":
        # Must have score; heavily boosted
        if cs is None:
            return -999.0
        score += 6.0
        if min_score and cs >= int(min_score):
            score += 2.0
        if employed:
            score += 2.0

    elif flow == "Account":
        # Prefer easy onboarding (no credit check)
        if not credit_required:
            score += 3.0
        else:
            score += 1.0 if cs is not None else -1.5

    elif flow == "Connect":
        # Prefer prepaid when score missing; contract when score exists
        name = (item.get("name") or "").lower()
        code = (item.get("product_code") or "").lower()
        is_prepaid = ("prepaid" in name) or ("prepaid" in code)
        if is_prepaid:
            score += 3.0 if cs is None else 1.0
        else:
            score += 3.0 if cs is not None else 0.5

    else:
        # Insurance/legal/health
        uw = (item.get("underwriting_type") or "").lower()
        if uw in ("none", "no_medicals"):
            score += 2.0
        elif uw in ("risk_based", "medical_scheme_required"):
            score += 1.0

    # Primary interest soft boost (doesn't dominate)
    if primary_interest:
        pi = primary_interest.lower().strip()
        if pi and pi in (item.get("name") or "").lower():
            score += 2.5
        if pi and pi in (item.get("product_type") or "").lower():
            score += 1.0

    return score


def _pick_top2_unique(arr: List[Tuple[float, Dict[str, Any]]]) -> List[Tuple[float, Dict[str, Any]]]:
    arr = sorted(arr, key=lambda x: x[0], reverse=True)
    chosen: List[Tuple[float, Dict[str, Any]]] = []
    seen = set()

    for score, item in arr:
        if score <= -999:
            continue
        code = (item.get("product_code") or item.get("name") or "").strip().lower()
        if not code or code in seen:
            continue
        chosen.append((score, item))
        seen.add(code)
        if len(chosen) == 2:
            break
    return chosen


# -----------------------------
# Reasons (benefit-driven + light personalisation)
# -----------------------------
def _benefit_reason(item: Dict[str, Any], flow: str, signals: Dict[str, Any]) -> str:
    """
    Return ONLY product benefits (preferred) or product description (fallback).
    No user/bureau signals appended.
    """
    benefits = item.get("benefits")
    desc = (item.get("description") or "").strip()

    # benefits in your `products` table may be:
    # - a Python list
    # - a JSON string
    # - a plain string
    if isinstance(benefits, list) and benefits:
        return ", ".join([str(b) for b in benefits[:3]]).strip()

    if isinstance(benefits, str) and benefits.strip():
        return benefits.strip()

    return desc or "Good match for your needs"
    return base


def _apply_flow_if_missing(
    update: Dict[str, Any],
    existing: Optional[Dict[str, Any]],
    flow: str,
    best_item: Dict[str, Any],
    best_score: float,
    nxt_item: Optional[Dict[str, Any]],
    nxt_score: Optional[float],
    signals: Dict[str, Any],
) -> None:
    def field(name: str) -> Any:
        return existing.get(name) if existing else None

    best_reason = _benefit_reason(best_item, flow, signals)
    nxt_reason = _benefit_reason(nxt_item, flow, signals) if nxt_item else None

    if flow == "Account":
        if _is_blank(field("account_rec_1_name")):
            update["account_rec_1_name"] = best_item["name"]
            update["account_rec_1_reason"] = best_reason
        if nxt_item and _is_blank(field("account_rec_2_name")):
            update["account_rec_2_name"] = nxt_item["name"]
            update["account_rec_2_reason"] = nxt_reason

    elif flow == "Connect":
        if _is_blank(field("connect_rec_1_name")):
            update["connect_rec_1_name"] = best_item["name"]
            update["connect_rec_1_reason"] = best_reason
        if nxt_item and _is_blank(field("connect_rec_2_name")):
            update["connect_rec_2_name"] = nxt_item["name"]
            update["connect_rec_2_reason"] = nxt_reason

    elif flow == "Insurance":
        if _is_blank(field("insurance_rec_1_name")):
            update["insurance_rec_1_name"] = best_item["name"]
            update["insurance_rec_1_reason"] = best_reason
        if nxt_item and _is_blank(field("insurance_rec_2_name")):
            update["insurance_rec_2_name"] = nxt_item["name"]
            update["insurance_rec_2_reason"] = nxt_reason

    elif flow == "Loan":
        if _is_blank(field("loan_rec_1_name")):
            update["loan_rec_1_name"] = best_item["name"]
            update["loan_rec_1_reason"] = best_reason
        if nxt_item and _is_blank(field("loan_rec_2_name")):
            update["loan_rec_2_name"] = nxt_item["name"]
            update["loan_rec_2_reason"] = nxt_reason


# -----------------------------
# Main public function
# -----------------------------
def generate_recommendation_for_customer(customer_id: str, catalog: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    """
    - Requires bureau data (XDS) success
    - Uses catalog from insurance_products (+ products rules)
    - Picks best+next best per flow (Account/Connect/Insurance/Loan)
    - Inserts if none exists; otherwise updates only missing fields (no overwrites)
    """
    bp = _get_latest_bureau_profile(customer_id)
    if not bp:
        return {"status": "skipped", "reason": "no_bureau_data"}

    signals = _bureau_signals(bp)

    client = _get_client(customer_id) or {}
    primary_interest = (client.get("primary_interest") or "").strip() or None

    dob = client.get("date_of_birth")
    age = _calc_age(dob)

    income_val = client.get("monthly_income") or client.get("income")
    income = _safe_int(income_val, 0) if income_val is not None else None

    existing = _get_existing_recommendation_row(customer_id)

    catalog = catalog or load_catalog_cached()

    # score and group by flow
    by_flow: Dict[str, List[Tuple[float, Dict[str, Any]]]] = {"Account": [], "Connect": [], "Insurance": [], "Loan": []}
    for item in catalog:
        flow = _classify_flow(item)
        score = _score_item(item, age=age, income=income, signals=signals, primary_interest=primary_interest)
        by_flow.setdefault(flow, []).append((score, item))

    # top2 per flow (deduped)
    picks: Dict[str, Tuple[Tuple[float, Dict[str, Any]], Optional[Tuple[float, Dict[str, Any]]]]] = {}
    for flow, arr in by_flow.items():
        top = _pick_top2_unique(arr)
        if not top:
            continue
        best = top[0]
        nxt = top[1] if len(top) > 1 else None
        picks[flow] = (best, nxt)

    if not picks:
        return {"status": "skipped", "reason": "no_eligible_products"}

    # Build option_recommendations + generated_config_ids (optional columns)
    option_recos: Dict[str, Any] = {}
    gen_codes: List[str] = []

    for flow, (best, nxt) in picks.items():
        best_score, best_item = best
        nxt_score, nxt_item = (nxt if nxt else (None, None))

        gen_codes.append(best_item.get("product_code") or best_item.get("name") or "")
        if nxt_item:
            gen_codes.append(nxt_item.get("product_code") or nxt_item.get("name") or "")

        option_recos[flow] = {
            "best": {
                "product_code": best_item.get("product_code"),
                "name": best_item.get("name"),
                "reason": _benefit_reason(best_item, flow, signals),
                "score": round(best_score, 2),
            },
            "next_best": (
                {
                    "product_code": nxt_item.get("product_code"),
                    "name": nxt_item.get("name"),
                    "reason": _benefit_reason(nxt_item, flow, signals),
                    "score": round(float(nxt_score), 2) if nxt_score is not None else None,
                }
                if nxt_item
                else None
            ),
        }

    # Insert new row
    if not existing:
        rec_id = f"rec-{uuid.uuid4()}"
        row: Dict[str, Any] = {
            "id": rec_id,
            "customer_id": customer_id,
            "generated_at": _now_iso(),
            "enrichment_complete": True,
            "generated_options": [],  # keep your existing column
        }

        # Optional columns if you added them
        if "bureau_profile_id" in (supabase().table("fnb_recommendations").select("bureau_profile_id").limit(1).execute().data[0] if False else {}):
            # (we don't actually want to query schema here; leaving for clarity)
            pass

        # Fill recommendations for all flows
        for flow, (best, nxt) in picks.items():
            best_score, best_item = best
            nxt_score, nxt_item = (nxt if nxt else (None, None))
            _apply_flow_if_missing(row, None, flow, best_item, best_score, nxt_item, nxt_score, signals)

        # Add optional columns if present in table (safe: try update after insert)
        supabase().table("fnb_recommendations").insert(row).execute()

        # Best-effort update of new columns (won't break if columns not present)
        try:
            supabase().table("fnb_recommendations").update(
                {
                    "bureau_profile_id": bp.get("id"),
                    "option_recommendations": option_recos,
                    "generated_config_ids": [],  # keep as int[] later when you map to config IDs
                }
            ).eq("customer_id", customer_id).execute()
        except Exception:
            pass

        return {"status": "success", "mode": "inserted", "customer_id": customer_id, "recommendation_id": rec_id}

    # Update existing row: ONLY fill missing fields
    update: Dict[str, Any] = {"enrichment_complete": True}
    before_keys = set(update.keys())

    for flow, (best, nxt) in picks.items():
        best_score, best_item = best
        nxt_score, nxt_item = (nxt if nxt else (None, None))
        _apply_flow_if_missing(update, existing, flow, best_item, best_score, nxt_item, nxt_score, signals)

    # also store option_recommendations if column exists AND not already present/empty
    try:
        # if existing option_recommendations is empty, fill it once
        if "option_recommendations" in existing and (existing.get("option_recommendations") in (None, {}, "{}", "")):
            update["option_recommendations"] = option_recos
    except Exception:
        pass

    if set(update.keys()) == before_keys:
        return {"status": "skipped", "reason": "already_complete", "customer_id": customer_id}

    supabase().table("fnb_recommendations").update(update).eq("customer_id", customer_id).execute()
    return {"status": "success", "mode": "updated_missing", "customer_id": customer_id}
