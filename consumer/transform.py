from datetime import datetime
from typing import Optional, Dict


def transform_event(event: dict) -> Optional[Dict]:
    """
    Clean and enrich a raw Kafka event.
    Returns None if the event is invalid.
    """

    # Mandatory fields
    if event.get("transaction_id") is None or event.get("amount") is None:
        return None

    processing_time = datetime.utcnow().isoformat()

    # Handle event_time
    event_time = event.get("event_time")
    if event_time is None:
        event_time = processing_time
        is_late_event = False
    else:
        try:
            event_time_dt = datetime.fromisoformat(event_time)
            is_late_event = (datetime.utcnow() - event_time_dt).seconds > 60
        except Exception:
            event_time = processing_time
            is_late_event = False

    transformed_event = {
        "transaction_id": int(event["transaction_id"]),
        "user_id": int(event.get("user_id", -1)),
        "amount": float(event["amount"]),
        "currency": event.get("currency", "EUR"),
        "status": event.get("status") or "UNKNOWN",
        "event_time": event_time,
        "processing_time": processing_time,
        "is_late_event": is_late_event,
    }

    return transformed_event
