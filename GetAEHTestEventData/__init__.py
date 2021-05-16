from typing import List
from .activityBlob import activityBlob as ab
from .jsonSerializer import jsonSerializer as js
import logging
import json
import azure.functions as func


def main(events: List[func.EventHubEvent]):

    container_name       = "adsazuksdevedwcontainer"
    location_rdh         = "Sandbox/POC/RDH/test-aeh-101/manual_capture"
    storage_account_name = "adsazuksdevdatalake"
    storage_account_key  = "G6Ndtzsa4N4ld8GtaTCcSYYIGEreudtmDABE+o90FnZqOlJSD1ZehD5Xib5J0BeEoCLiq4a/+kP0i5hdTdpBrw=="

    for event in events:
        event_body = event.get_body()
        event_body = event.get_body().decode('utf8').replace("'", '"')
        #json_data = json.dumps(event_data, default=js.json_serial)

        complete_message = {
            "SequenceNumber" : event.sequence_number,
            "Offset" : event.offset,
            "EnqueuedTimeUtc" : event.enqueued_time,
            "Body" : event_body
        }
        
        complete_message = json.dumps(complete_message, default=js.json_serial)

        logging.info(f'  SequenceNumber = {event.sequence_number}')
        logging.info(f'  Offset = {event.offset}')
        logging.info(f'  EnqueuedTimeUtc = {event.enqueued_time}')
        logging.info(f'  Body= {event_body}')
        logging.info(f'  CompleteMessage= {complete_message}')

        ab.initialize_storage_account(storage_account_key=storage_account_key, storage_account_name=storage_account_name)
        ab.upload_file_to_directory(complete_message, container_name, location_rdh, event.sequence_number)