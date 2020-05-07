import logging

from app import App
from app import Frames


def handle(
        app: App,
        frames: Frames          # CLIENT LEVEL 1
)-> App:
    service_name = frames[0]
    body = frames[1:]
    # consult service lookup table for the address to deal this to
    logging.info("msg for %s - %s", service_name, body)
    return app
