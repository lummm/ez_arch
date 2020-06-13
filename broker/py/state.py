import logging
import time

import app
from apptypes import Frames
from env import ENV
import protoc


def broadcast_heartbeat(
        worker_addr: bytes,
        service_name: bytes
) -> None:
    app.state().broker_dealer.send_multipart(
        [protoc.HEARTBEAT, service_name, worker_addr]
    )
    return


def broadcast_worker_engaged(
        worker_addr: bytes
) -> None:
    app.state().broker_dealer.send_multipart(
        [protoc.WORKER_ENGAGED, worker_addr]
    )
    return


def broadcast_worker_unengaged(
        worker_addr: bytes
) -> None:
    app.state().broker_dealer.send_multipart(
        [protoc.WORKER_UNENGAGED, worker_addr]
    )
    return


def handle_heartbeat(
        frames: Frames          # B_STATE LEVEL 1 HEARTBEAT
) -> None:
    service_name = frames[0]
    worker_addr = frames[1]
    if service_name not in app.state().service_addrs:
        app.state().service_addrs[service_name] = set()
    if worker_addr not in app.state().service_addrs[service_name]:
        logging.info("service %s has new worker %s", service_name, worker_addr)
        app.state().service_addrs[service_name].add(worker_addr)
    app.state().worker_expiry[worker_addr] = time.time() \
        + ENV.WORKER_LIFETIME_S
    return


def handle_worker_engaged(
        frames: Frames
) -> None:
    worker_addr = frames[0]
    if worker_addr not in app.state().worker_tasks:
        app.state().worker_tasks[worker_addr] = 0
    app.state().worker_tasks[worker_addr] += 1
    logging.debug("Worker engaged: %s.  Tasks: %s",
                  worker_addr, app.state().worker_tasks[worker_addr])
    return


def handle_worker_unengaged(
        frames: Frames
) -> None:
    worker_addr = frames[0]
    if worker_addr not in app.state().worker_tasks:
        app.state().worker_tasks[worker_addr] = 1
    app.state().worker_tasks[worker_addr] -= 1
    logging.debug("Worker unengaged: %s.  Tasks: %s",
                  worker_addr, app.state().worker_tasks[worker_addr])
    return
