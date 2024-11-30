from datetime import datetime, timezone

def get_current_utc_iso():
    """
    Returns the current UTC time in ISO 8601 format.
    
    If you want the 'Z' suffix instead of '+00:00', it is added.
    """
    return datetime.now(timezone.utc).isoformat()


