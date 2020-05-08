import logging

from app import App
from app import Frames
import protoc


def process_heartbeat(
        app: App,
        frames: Frames          # LEVEL 2 WORKER HEARTBEAT
)-> App:
    service_name = frames[0]
    logging.debug("heartbeat for service: %s", service_name)
    return app


def handle(
        app: App,
        frames: Frames          # LEVEL 1 WORKER
)-> App:
    msg_type = frames[0]
    body = frames[1:]
    if msg_type == protoc.HEARTBEAT:
        return process_heartbeat(app, body)
    return app
