from __future__ import annotations
from typing import Any, Dict, List, Optional
import xml.etree.ElementTree as ET

def _strip(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag

def _text(el: Optional[ET.Element]) -> Optional[str]:
    if el is None:
        return None
    t = (el.text or "").strip()
    return t if t else None

def _find_first(root: ET.Element, tag: str) -> Optional[str]:
    for el in root.iter():
        if _strip(el.tag).lower() == tag.lower():
            v = _text(el)
            if v is not None:
                return v
    return None

def _find_all(root: ET.Element, tag: str) -> List[ET.Element]:
    out = []
    for el in root.iter():
        if _strip(el.tag).lower() == tag.lower():
            out.append(el)
    return out

def parse_match_xml(match_xml: str) -> Dict[str, Optional[str]]:
    out = {"consumer_id": None, "enquiry_id": None, "enquiry_result_id": None, "reference_no": None}
    try:
        root = ET.fromstring(match_xml)
        out["consumer_id"] = _find_first(root, "ConsumerID")
        out["enquiry_id"] = _find_first(root, "EnquiryID")
        out["enquiry_result_id"] = _find_first(root, "EnquiryResultID")
        out["reference_no"] = _find_first(root, "Reference")
    except Exception:
        pass
    return out

def parse_report_xml(report_xml: str) -> Dict[str, Any]:
    """
    Returns:
      profile: dict for bureau_profiles
      contacts: list[dict] for bureau_contact_history
      employment: list[dict] for bureau_employment_history
      principals: list[dict] for bureau_principal_links
      credit_enquiries: list[dict] for bureau_credit_enquiries
    """
    profile: Dict[str, Any] = {}
    contacts: List[Dict[str, Any]] = []
    employment: List[Dict[str, Any]] = []
    principals: List[Dict[str, Any]] = []
    credit_enquiries: List[Dict[str, Any]] = []

    root = ET.fromstring(report_xml)

    # --- Report Info
    profile["report_id"] = _find_first(root, "ReportID")
    profile["report_name"] = _find_first(root, "ReportName")

    # --- ConsumerDetail (core identity + contacts)
    profile["consumer_id"] = _find_first(root, "ConsumerID")
    profile["initials"] = _find_first(root, "Initials")
    profile["first_name"] = _find_first(root, "FirstName")
    profile["second_name"] = _find_first(root, "SecondName")
    profile["third_name"] = _find_first(root, "ThirdName")
    profile["surname"] = _find_first(root, "Surname")
    profile["id_number"] = _find_first(root, "IDNo")
    profile["passport_number"] = _find_first(root, "PassportNo")

    bd = _find_first(root, "BirthDate")
    if bd:
        profile["birth_date"] = bd[:10]

    profile["gender"] = _find_first(root, "Gender") or _find_first(root, "GenderInd")
    profile["title_desc"] = _find_first(root, "TitleDesc")
    profile["marital_status_desc"] = _find_first(root, "MaritalStatusDesc")
    profile["privacy_status"] = _find_first(root, "PrivacyStatus")

    profile["residential_address"] = _find_first(root, "ResidentialAddress")
    profile["postal_address"] = _find_first(root, "PostalAddress")
    profile["telephone_home"] = _find_first(root, "HomeTelephoneNo")
    profile["telephone_work"] = _find_first(root, "WorkTelephoneNo")
    profile["cellular"] = _find_first(root, "CellularNo")
    profile["email"] = _find_first(root, "EmailAddress")
    profile["current_employer"] = _find_first(root, "EmployerDetail")
    profile["reference_no"] = _find_first(root, "ReferenceNo")
    profile["external_reference"] = _find_first(root, "ExternalReference")

    # --- Fraud indicators summary
    profile["safps_listing_yn"] = _find_first(root, "SAFPSListingYN")
    profile["home_affairs_verified_yn"] = _find_first(root, "HomeAffairsVerificationYN")
    profile["home_affairs_deceased_status"] = _find_first(root, "HomeAffairsDeceasedStatus")
    had = _find_first(root, "HomeAffairsDeceasedDate")
    if had:
        profile["home_affairs_deceased_date"] = had[:10]
    profile["employer_fraud_verified_yn"] = _find_first(root, "EmployerFraudVerificationYN")
    profile["protective_verification_yn"] = _find_first(root, "ProtectiveVerificationYN")

    # --- Property summary
    tp = _find_first(root, "TotalProperty")
    if tp and tp.isdigit():
        profile["total_property"] = int(tp)
    pp = _find_first(root, "PurchasePrice")
    if pp:
        try:
            profile["purchase_price"] = float(pp)
        except Exception:
            pass

    # --- Director summary
    nd = _find_first(root, "NumberOfCompanyDirector")
    if nd and nd.isdigit():
        profile["number_of_company_director"] = int(nd)

    # --- Subscriber input details -> bureau_credit_enquiries
    sid = _find_all(root, "SubscriberInputDetails")
    for block in sid:
        enquiry_date = _text(block.find("./EnquiryDate"))
        enquiry_type = _text(block.find("./EnquiryType"))
        subscriber_name = _text(block.find("./SubscriberName"))
        subscriber_username = _text(block.find("./SubscriberUserName"))
        enquiry_input = _text(block.find("./EnquiryInput"))
        enquiry_reason = _text(block.find("./EnquiryReason"))

        credit_enquiries.append({
            "enquiry_date": enquiry_date[:10] if enquiry_date else None,
            "requested_by": subscriber_username or subscriber_name,
            "credit_type": enquiry_type,
            "contact_number": None,
            "enquiry_reason": enquiry_reason,
            "enquiry_input": enquiry_input,
            "subscriber_name": subscriber_name,
            "subscriber_username": subscriber_username,
        })

        profile["subscriber_name"] = subscriber_name
        profile["subscriber_username"] = subscriber_username
        profile["subscriber_enquiry_date"] = enquiry_date
        profile["enquiry_type"] = enquiry_type
        profile["enquiry_reason"] = enquiry_reason
        profile["enquiry_input"] = enquiry_input

    # --- Address history -> contact_history (store as contact_type='address:<type>')
    for ah in _find_all(root, "ConsumerAddressHistory"):
        update_date = _find_first(ah, "LastUpdatedDate")
        addr_type = _find_first(ah, "AddressType") or "Unknown"
        addr = _find_first(ah, "Address")
        if addr:
            contacts.append({
                "update_date": update_date[:10] if update_date else None,
                "contact_type": f"address:{addr_type.lower()}",
                "contact_value": addr,
            })

    # --- Telephone history -> contact_history
    for th in _find_all(root, "ConsumerTelephoneHistory"):
        update_date = _find_first(th, "LastUpdatedDate")
        tel_type = _find_first(th, "TelephoneType") or "Unknown"
        tel_no = _find_first(th, "TelephoneNo")
        if tel_no:
            contacts.append({
                "update_date": update_date[:10] if update_date else None,
                "contact_type": f"tel:{tel_type.lower()}",
                "contact_value": tel_no,
            })

    # --- Email history -> contact_history
    for eh in _find_all(root, "ConsumerEmailHistory"):
        update_date = _find_first(eh, "LastUpdatedDate")
        email = _find_first(eh, "EmailAddress")
        if email:
            contacts.append({
                "update_date": update_date[:10] if update_date else None,
                "contact_type": "email",
                "contact_value": email,
            })

    # --- Employment history -> employment_history
    for emp in _find_all(root, "ConsumerEmploymentHistory"):
        update_date = _find_first(emp, "LastUpdatedDate")
        employer = _find_first(emp, "EmployerDetail")
        designation = _find_first(emp, "Designation")
        if employer:
            employment.append({
                "update_date": update_date[:10] if update_date else None,
                "employer": employer,
                "designation": designation,
            })

    # --- Director ship links -> principal links
    for d in _find_all(root, "ConsumerDirectorShipLink"):
        principals.append({
            "company_name": _find_first(d, "CommercialName"),
            "company_reg_no": _find_first(d, "RegistrationNo"),
            "company_address": _find_first(d, "PhysicalAddress"),
            "industry_category": _find_first(d, "SICDesc"),
            "principal_status": _find_first(d, "DirectorStatus"),
            "commercial_status": _find_first(d, "CommercialStatus"),
            "date_of_inception": None,  # not provided in your sample
            "director_appointment_date": _find_first(d, "AppointmentDate"),
        })

    return {
        "profile": profile,
        "contacts": contacts,
        "employment": employment,
        "principals": principals,
        "credit_enquiries": credit_enquiries,
    }
