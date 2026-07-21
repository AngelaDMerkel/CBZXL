import json
from datetime import datetime, timezone


PROTOCOL_PREFIX = "CBZXL_EVENT "


def utc_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def encode_event(event_type, *, level="info", message="", **data):
    payload = {
        "protocol": 1,
        "type": event_type,
        "level": level,
        "message": message,
        "at": utc_now(),
        **data,
    }
    return PROTOCOL_PREFIX + json.dumps(payload, separators=(",", ":"), sort_keys=True)


def parse_event(line):
    text = line.strip()
    if not text.startswith(PROTOCOL_PREFIX):
        return None
    try:
        payload = json.loads(text[len(PROTOCOL_PREFIX):])
    except (TypeError, json.JSONDecodeError):
        return None
    if payload.get("protocol") != 1 or not isinstance(payload.get("type"), str):
        return None
    return payload


class EventEmitter:
    def __init__(self, stream):
        self.stream = stream

    def emit(self, event_type, *, level="info", message="", **data):
        print(encode_event(event_type, level=level, message=message, **data), file=self.stream, flush=True)

