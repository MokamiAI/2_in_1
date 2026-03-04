import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    # Supabase
    SUPABASE_URL = os.getenv("SUPABASE_URL", "")
    SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")

    # XDS
    XDS_WSDL = os.getenv("XDS_WSDL", "https://www.web.xds.co.za/xdsconnect/XDSConnectWS.asmx?WSDL")
    XDS_USERNAME = os.getenv("XDS_USERNAME", "")
    XDS_PASSWORD = os.getenv("XDS_PASSWORD", "")

    # Defaults
    BUREAU = "XDS"
    PRODUCT_ID = int(os.getenv("XDS_PRODUCT_ID", "15"))
    REPORT_ID = int(os.getenv("XDS_REPORT_ID", "1"))

    # Worker
    POLL_INTERVAL_SECONDS = int(os.getenv("BUREAU_POLL_INTERVAL_SECONDS", "30"))
    BATCH_SIZE = int(os.getenv("BUREAU_BATCH_SIZE", "20"))
    FRESHNESS_DAYS = int(os.getenv("BUREAU_FRESHNESS_DAYS", "30"))
    
    RECO_POLL_INTERVAL_SECONDS: int = 30
    RECO_BATCH_SIZE: int = 50
    RECO_CONCURRENCY: int = 10


settings = Settings()

def validate_settings():
    missing = []
    if not settings.SUPABASE_URL:
        missing.append("SUPABASE_URL")
    if not settings.SUPABASE_SERVICE_ROLE_KEY:
        missing.append("SUPABASE_SERVICE_ROLE_KEY")
    if not settings.XDS_USERNAME:
        missing.append("XDS_USERNAME")
    if not settings.XDS_PASSWORD:
        missing.append("XDS_PASSWORD")
    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")
