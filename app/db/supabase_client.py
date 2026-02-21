from supabase import create_client, Client
from app.core.config import settings

_supabase: Client | None = None

def supabase() -> Client:
    global _supabase
    if _supabase is None:
        _supabase = create_client(settings.SUPABASE_URL, settings.SUPABASE_SERVICE_ROLE_KEY)
    return _supabase
