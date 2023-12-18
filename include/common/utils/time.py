from datetime import datetime, timezone

def get_current_utc_timestamp():
    current_utc_timestamp = datetime.utcnow().replace(tzinfo=timezone.utc)
    return current_utc_timestamp.strftime('%Y-%m-%d %H:%M:%S %Z')