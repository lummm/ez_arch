import logging
import time

from app import App
from app import Frames
from env import ENV
import state
import protoc


def process_heartbeat(
        app: App,
        worker_addr: bytes,
        frames: Frames          # LEVEL 2 WORKER HEARTBEAT
)-> App:
    service_name = frames[0]
    logging.debug("heartbeat for service: %s", service_name)
    state.broadcast_heartbeat(app, worker_addr, service_name)
    return app


def handle(
        app: App,
        worker_addr: bytes,
        frames: Frames          # LEVEL 1 WORKER
)-> App:
    msg_type = frames[0]
    body = frames[1:]
    if msg_type == protoc.HEARTBEAT:
        return process_heartbeat(app, worker_addr, body)
    return app


def remove_worker(
        app: App,
        worker_addr: bytes
)-> App:
    logging.debug("removing worker at addr: %s", worker_addr)
    app.worker_expiry.pop(worker_addr, None)
    for service_name in app.service_addrs:
        app.service_addrs[service_name].remove(worker_addr)
    return app


def purge_dead_workers(app: App)-> App:
    now = time.time()
    items = list(app.worker_expiry.items())
    for w, expiry_ts in items:
        if expiry_ts < now:
            app = remove_worker(app, w)
    return app
