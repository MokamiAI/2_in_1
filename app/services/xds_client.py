import re
import requests
from zeep import Client as ZeepClient
from zeep.transports import Transport
from app.core.config import settings

def get_zeep_client() -> ZeepClient:
    session = requests.Session()
    session.headers.update({"User-Agent": "Nerve-XDS-AutoSync/1.0"})
    transport = Transport(session=session, timeout=60)
    return ZeepClient(wsdl=settings.XDS_WSDL, transport=transport)

def mask_ticket(ticket: str) -> str:
    t = (ticket or "").strip()
    if not t:
        return "***"
    if len(t) <= 10:
        return f"{t[0]}***{t[-1]}"
    return f"{t[:6]}...{t[-6:]}"

def extract_tag(xml: str, tag: str) -> str:
    m = re.search(rf"<{tag}>(.*?)</{tag}>", xml, re.IGNORECASE | re.DOTALL)
    return m.group(1).strip() if m else ""

def login(z: ZeepClient) -> str:
    return str(z.service.Login(settings.XDS_USERNAME, settings.XDS_PASSWORD)).strip()

def is_ticket_valid(z: ZeepClient, ticket: str) -> bool:
    res = z.service.IsTicketValid((ticket or "").strip())
    return str(res).strip().lower() == "true"

def connect_consumer_match(z: ZeepClient, ticket: str, *, id_number: str, first_name: str, surname: str, birth_date: str, your_reference: str) -> str:
    # Your rule: ALWAYS send ID + name + surname + DOB
    return str(
        z.service.ConnectConsumerMatch(
            ConnectTicket=(ticket or "").strip(),
            EnquiryReason="CreditAssesment",
            ProductId=settings.PRODUCT_ID,
            IdNumber=id_number or "",
            PassportNo="",
            FirstName=first_name or "",
            Surname=surname or "",
            BirthDate=birth_date or "",
            YourReference=your_reference,
            VoucherCode="",
        )
    )

def connect_get_result(z: ZeepClient, ticket: str, enquiry_id: str, enquiry_result_id: str) -> str:
    return str(
        z.service.ConnectGetResult(
            ConnectTicket=(ticket or "").strip(),
            EnquiryID=str(enquiry_id),
            EnquiryResultID=str(enquiry_result_id),
            ProductID=settings.PRODUCT_ID,
            BonusXML="",
        )
    )
