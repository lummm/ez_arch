import logging
import time

from app import App
from app import Frames
from env import ENV
import protoc


def broadcast_heartbeat(
        app: App,
        worker_addr: bytes,
        service_name: bytes
)-> App:
    app.broker_dealer.send_multipart(
        [protoc.HEARTBEAT, service_name, worker_addr]
    )
    return app


def broadcast_worker_engaged(
        app: App,
        worker_addr: bytes
)-> App:
    app.broker_dealer.send_multipart(
        [protoc.WORKER_ENGAGED, worker_addr]
    )
    return app


def broadcast_worker_unengaged(
        app: App,
        worker_addr: bytes
)-> App:
    app.broker_dealer.send_multipart(
        [protoc.WORKER_UNENGAGED, worker_addr]
    )
    return app


def handle_heartbeat(
        app: App,
        frames: Frames          # B_STATE LEVEL 1 HEARTBEAT
)-> App:
    service_name = frames[0]
    worker_addr = frames[1]
    if not service_name in app.service_addrs:
        app.service_addrs[service_name] = set()
    app.service_addrs[service_name].add(worker_addr)
    app.worker_expiry[worker_addr] = time.time() + ENV.WORKER_LIFETIME_S
    logging.debug("workers: %s", app.service_addrs)
    return app


def handle_worker_engaged(
        app: App,
        frames: Frames
)-> App:
    worker_addr = frames[0]
    if not worker_addr in app.worker_tasks:
        app.worker_tasks[worker_addr] = 0
    app.worker_tasks[worker_addr] += 1
    logging.debug("worker engaged: %s", worker_addr)
    return app


def handle_worker_unengaged(
        app: App,
        frames: Frames
)-> App:
    worker_addr = frames[0]
    if not worker_addr in app.worker_tasks:
        app.worker_tasks[worker_addr] = 1
    app.worker_tasks[worker_addr] -= 1
    logging.debug("worker unengaged: %s", worker_addr)
    return app


def handle(
        app: App,
        frames: Frames          # B_STATE FLAT
)-> App:
    return_addr = frames[0]
    msg_type = frames[1]
    rest = frames[2:]
    if msg_type == protoc.HEARTBEAT:
        return handle_heartbeat(app, rest)
    if msg_type == protoc.WORKER_ENGAGED:
        return handle_worker_engaged(app, rest)
    if msg_type == protoc.WORKER_UNENGAGED:
        return handle_worker_unengaged(app, rest)
    logging.error("no such broker state message type: %s", msg_type)
    return app
