from Channel_data_pipeline import run
from Videos_data_pipeline import run_video_pipeline
import functions_framework
from datetime import datetime, timezone
from dateutil import parser
from flask import Request
import json

@functions_framework.http
def main(request: Request):

    request_json = request.get_json()
    timestamp = request_json.get("updated")
    event_time = parser.parse(timestamp)
    event_age = (datetime.now(timezone.utc) - event_time).total_seconds()
    event_age_ms = event_age * 1000

    # Ignore events that are too old
    max_age_ms = 10000
    if event_age_ms > max_age_ms:
        return "Timeout"
    run()
    run_video_pipeline()
    return json.dumps({"status": "200", "message": "Function executed successfully"}), 200
