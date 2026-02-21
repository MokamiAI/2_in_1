from __future__ import annotations

import time
import logging
from typing import Dict, Any, Optional

from app.core.config import settings
from app.db.supabase_client import supabase
from app.services import xds_client
from app.services.xds_parse import parse_match_xml, parse_report_xml

logger = logging.getLogger(__name__)


def insert_verification_log(
    *,
    user_id: str,
    request_id: Optional[str],
    step: str,
    status: str,
    message: str,
    payload: Optional[Dict[str, Any]] = None,
) -> None:
    try:
        supabase().table("verification_logs").insert({
            "user_id": user_id,
            "request_id": request_id,
            "bureau": settings.BUREAU,
            "step": step,
            "status": status,
            "message": message,
            "payload": payload or {},
        }).execute()
    except Exception as e:
        logger.warning("Failed to write verification log: %s", e)


def client_has_bureau_profile(user_id: str) -> bool:
    res = supabase().table("bureau_profiles") \
        .select("id") \
        .eq("user_id", user_id) \
        .eq("bureau", settings.BUREAU) \
        .limit(1).execute()
    return bool(res.data)


def insert_bureau_enquiry(*, user_id: str, request_payload: dict) -> Optional[str]:
    try:
        res = supabase().table("bureau_enquiries").insert({
            "user_id": user_id,
            "bureau": settings.BUREAU,
            "product_id": settings.PRODUCT_ID,
            "report_id": settings.REPORT_ID,
            "request_payload": request_payload,
            "status": "processing",
        }).execute()
        return res.data[0]["id"] if res.data else None
    except Exception:
        return None


def update_bureau_enquiry(enquiry_row_id: str, fields: dict) -> None:
    if fields:
        supabase().table("bureau_enquiries").update(fields).eq("id", enquiry_row_id).execute()


def _upsert_bureau_profile(user_id: str, enquiry_ids: Dict[str, Any], parsed_profile: Dict[str, Any], raw_payload: Dict[str, Any]) -> str:
    """
    Returns bureau_profile_id
    """
    # Find existing
    existing = supabase().table("bureau_profiles") \
        .select("id") \
        .eq("user_id", user_id) \
        .eq("bureau", settings.BUREAU) \
        .limit(1).execute()

    row = {
        "user_id": user_id,
        "bureau": settings.BUREAU,

        # identifiers
        "consumer_id": enquiry_ids.get("consumer_id") or parsed_profile.get("consumer_id"),
        "enquiry_id": enquiry_ids.get("enquiry_id"),
        "enquiry_result_id": enquiry_ids.get("enquiry_result_id"),
        "reference_no": enquiry_ids.get("reference_no") or parsed_profile.get("reference_no"),
        "external_reference": parsed_profile.get("external_reference"),

        # report
        "report_id": settings.REPORT_ID,
        "report_name": parsed_profile.get("report_name"),

        # identity and details
        "initials": parsed_profile.get("initials"),
        "first_name": parsed_profile.get("first_name"),
        "second_name": parsed_profile.get("second_name"),
        "third_name": parsed_profile.get("third_name"),
        "surname": parsed_profile.get("surname"),
        "id_number": parsed_profile.get("id_number"),
        "passport_number": parsed_profile.get("passport_number"),
        "birth_date": parsed_profile.get("birth_date"),

        "gender": parsed_profile.get("gender"),
        "title_desc": parsed_profile.get("title_desc"),
        "marital_status_desc": parsed_profile.get("marital_status_desc"),
        "privacy_status": parsed_profile.get("privacy_status"),

        "residential_address": parsed_profile.get("residential_address"),
        "postal_address": parsed_profile.get("postal_address"),
        "telephone_home": parsed_profile.get("telephone_home"),
        "telephone_work": parsed_profile.get("telephone_work"),
        "cellular": parsed_profile.get("cellular"),
        "email": parsed_profile.get("email"),
        "current_employer": parsed_profile.get("current_employer"),

        # fraud + flags
        "safps_listing_yn": parsed_profile.get("safps_listing_yn"),
        "home_affairs_verified_yn": parsed_profile.get("home_affairs_verified_yn"),
        "home_affairs_deceased_status": parsed_profile.get("home_affairs_deceased_status"),
        "home_affairs_deceased_date": parsed_profile.get("home_affairs_deceased_date"),
        "employer_fraud_verified_yn": parsed_profile.get("employer_fraud_verified_yn"),
        "protective_verification_yn": parsed_profile.get("protective_verification_yn"),

        # property/directors
        "total_property": parsed_profile.get("total_property"),
        "purchase_price": parsed_profile.get("purchase_price"),
        "number_of_company_director": parsed_profile.get("number_of_company_director"),

        # subscriber/enquiry input details
        "subscriber_enquiry_date": parsed_profile.get("subscriber_enquiry_date"),
        "subscriber_name": parsed_profile.get("subscriber_name"),
        "subscriber_username": parsed_profile.get("subscriber_username"),
        "enquiry_type": parsed_profile.get("enquiry_type"),
        "enquiry_reason": parsed_profile.get("enquiry_reason"),
        "enquiry_input": parsed_profile.get("enquiry_input"),

        # raw store
        "raw_payload": raw_payload,
        "status": "success",
        "error_message": None,
    }

    if existing.data:
        bp_id = existing.data[0]["id"]
        supabase().table("bureau_profiles").update(row).eq("id", bp_id).execute()
        return bp_id

    created = supabase().table("bureau_profiles").insert(row).execute()
    return created.data[0]["id"]


def _replace_child_rows(*, bureau_profile_id: str, table: str, rows: list[dict]) -> None:
    """
    Idempotent: delete existing child rows for this profile and insert fresh set.
    """
    supabase().table(table).delete().eq("bureau_profile_id", bureau_profile_id).execute()
    if rows:
        for r in rows:
            r["bureau_profile_id"] = bureau_profile_id
        supabase().table(table).insert(rows).execute()


def run_xds_for_user(*, request_id: Optional[str], user: Dict[str, Any]) -> Dict[str, Any]:
    user_id = user["user_id"]

    # ✅ HARD SKIP: if bureau data exists, do not send request
    if client_has_bureau_profile(user_id):
        insert_verification_log(
            user_id=user_id, request_id=request_id,
            step="skip", status="success",
            message="Skipped: bureau_profiles already exists for this user.",
        )
        return {"status": "skipped", "reason": "already_verified"}

    start = time.time()
    birth_date_str = str(user["birth_date"])[:10]

    request_payload = {
        "id_number": user.get("id_number") or "",
        "passport_no": "",
        "first_name": user.get("first_name") or "",
        "surname": user.get("surname") or "",
        "birth_date": birth_date_str,
        "your_reference": "NervePRODManual",
        "product_id": settings.PRODUCT_ID,
        "report_id": settings.REPORT_ID,
    }

    enquiry_row_id = insert_bureau_enquiry(user_id=user_id, request_payload=request_payload)
    if not enquiry_row_id:
        insert_verification_log(
            user_id=user_id, request_id=request_id,
            step="skip", status="success",
            message="Skipped: already processing or already verified (DB prevented duplicate).",
        )
        return {"status": "skipped", "reason": "already_verified_or_processing"}

    insert_verification_log(
        user_id=user_id, request_id=request_id,
        step="bureau_enquiry_created", status="success",
        message="Created bureau_enquiries receipt row",
        payload={"bureau_enquiry_id": enquiry_row_id},
    )

    try:
        z = xds_client.get_zeep_client()

        insert_verification_log(user_id=user_id, request_id=request_id, step="login", status="processing", message="Calling XDS Login()")
        ticket = xds_client.login(z)
        if not ticket:
            update_bureau_enquiry(enquiry_row_id, {"status": "failed", "error_message": "Empty ticket"})
            insert_verification_log(user_id=user_id, request_id=request_id, step="login", status="failed", message="Empty ticket")
            return {"status": "failed", "error": "Empty ticket", "bureau_enquiry_id": enquiry_row_id}

        ticket_preview = xds_client.mask_ticket(ticket)
        update_bureau_enquiry(enquiry_row_id, {"ticket_preview": ticket_preview})

        insert_verification_log(user_id=user_id, request_id=request_id, step="ticket_validation", status="processing", message="Calling IsTicketValid()", payload={"ticket_preview": ticket_preview})
        if not xds_client.is_ticket_valid(z, ticket):
            update_bureau_enquiry(enquiry_row_id, {"status": "failed", "error_message": "Ticket invalid"})
            insert_verification_log(user_id=user_id, request_id=request_id, step="ticket_validation", status="failed", message="Ticket invalid")
            return {"status": "failed", "error": "Ticket invalid", "bureau_enquiry_id": enquiry_row_id}

        insert_verification_log(user_id=user_id, request_id=request_id, step="consumer_match", status="processing", message="Calling ConnectConsumerMatch()", payload=request_payload)
        match_xml = xds_client.connect_consumer_match(
            z, ticket,
            id_number=request_payload["id_number"],
            first_name=request_payload["first_name"],
            surname=request_payload["surname"],
            birth_date=request_payload["birth_date"],
            your_reference=request_payload["your_reference"],
        )
        match_ids = parse_match_xml(match_xml)

        update_bureau_enquiry(enquiry_row_id, {
            "match_xml": match_xml,
            "consumer_id": match_ids.get("consumer_id"),
            "enquiry_id": match_ids.get("enquiry_id"),
            "enquiry_result_id": match_ids.get("enquiry_result_id"),
            "reference_no": match_ids.get("reference_no"),
        })

        if not match_ids.get("enquiry_id") or not match_ids.get("enquiry_result_id"):
            duration_ms = int((time.time() - start) * 1000)
            update_bureau_enquiry(enquiry_row_id, {"status": "failed", "error_message": "No EnquiryID/EnquiryResultID returned", "duration_ms": duration_ms})
            return {"status": "failed", "error": "No match IDs returned", "bureau_enquiry_id": enquiry_row_id}

        insert_verification_log(user_id=user_id, request_id=request_id, step="get_result", status="processing", message="Calling ConnectGetResult()")
        report_xml = xds_client.connect_get_result(z, ticket, match_ids["enquiry_id"], match_ids["enquiry_result_id"])

        duration_ms = int((time.time() - start) * 1000)
        update_bureau_enquiry(enquiry_row_id, {"report_xml": report_xml, "status": "success", "duration_ms": duration_ms})

        # ✅ Parse & store into your tables
        parsed = parse_report_xml(report_xml)

        raw_payload = {
            "match_xml": match_xml,
            "report_xml": report_xml,
            "parsed": parsed,
        }

        bureau_profile_id = _upsert_bureau_profile(
            user_id=user_id,
            enquiry_ids=match_ids,
            parsed_profile=parsed["profile"],
            raw_payload=raw_payload,
        )

        # ✅ Idempotent child table persistence
        _replace_child_rows(bureau_profile_id=bureau_profile_id, table="bureau_contact_history", rows=parsed["contacts"])
        _replace_child_rows(bureau_profile_id=bureau_profile_id, table="bureau_employment_history", rows=parsed["employment"])
        _replace_child_rows(bureau_profile_id=bureau_profile_id, table="bureau_principal_links", rows=parsed["principals"])

        # bureau_credit_enquiries includes extra columns in your schema;
        # we fill what we can and keep it idempotent
        # Ensure your bureau_credit_enquiries table has these columns or remove extras:
        credit_rows = []
        for ce in parsed["credit_enquiries"]:
            credit_rows.append({
                "enquiry_date": ce.get("enquiry_date"),
                "requested_by": ce.get("requested_by"),
                "credit_type": ce.get("credit_type"),
                "contact_number": ce.get("contact_number"),
                "enquiry_reason": ce.get("enquiry_reason"),
            })
        _replace_child_rows(bureau_profile_id=bureau_profile_id, table="bureau_credit_enquiries", rows=credit_rows)

        insert_verification_log(user_id=user_id, request_id=request_id, step="persist_profile", status="success", message="Stored bureau_profiles + child tables")
        return {"status": "success", "bureau_enquiry_id": enquiry_row_id, "bureau_profile_id": bureau_profile_id}

    except Exception as e:
        duration_ms = int((time.time() - start) * 1000)
        update_bureau_enquiry(enquiry_row_id, {"status": "failed", "error_message": str(e), "duration_ms": duration_ms})
        insert_verification_log(user_id=user_id, request_id=request_id, step="exception", status="failed", message=str(e))
        return {"status": "failed", "error": str(e), "bureau_enquiry_id": enquiry_row_id}
