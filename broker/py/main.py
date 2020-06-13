#!/usr/bin/env python3

import logging

import zmq

import app
from apptypes import Frames
import handlers
import conn
from env import ENV
import protoc
import worker


def setup_logging() -> None:
    logging.basicConfig(
        level=ENV.LOG_LEVEL,
        format=f"%(asctime)s.%(msecs)03d "
        "%(levelname)s %(module)s - %(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return


def handle_broker_broadcast(
        frames: Frames          # B_STATE FLAT + ROUTING
) -> None:
    logging.debug("broadcast frames %s", frames)
    handlers.state_msg_handle(frames)
    return


def handle_req_frames(
        frames: Frames          # INPUT FLAT + ROUTING
) -> None:
    logging.debug("request frames %s", frames)
    in_pipe_addr = frames[0]
    return_addr = frames[1]
    assert b"" == frames[2]
    msg_type = frames[3]
    assert msg_type == protoc.CLIENT
    body = frames[4:]
    app.update(in_pipe_addr=in_pipe_addr)
    handlers.client_msg_handle(return_addr, body)
    return


def handle_worker_frames(
        frames: Frames          # INPUT FLAT + ROUTING
) -> None:
    logging.debug("worker frames %s", frames)
    worker_pipe_addr = frames[0]
    return_addr = frames[1]
    assert b"" == frames[2]
    msg_type = frames[3]
    assert msg_type == protoc.WORKER
    body = frames[4:]
    app.update(worker_pipe_addr=worker_pipe_addr)
    handlers.worker_msg_handle(return_addr, body)
    return


def loop_body() -> None:
    items = app.state().poller.poll(ENV.POLL_INTERVAL_MS)
    items_dict = dict(items)

    def work_on_socket(socket, handler) -> None:
        if socket in items_dict:
            frames = socket.recv_multipart()
            handler(frames)
        return
    work_on_socket(app.state().broker_sub, handle_broker_broadcast)
    work_on_socket(app.state().worker_router, handle_worker_frames)
    work_on_socket(app.state().in_router, handle_req_frames)
    worker.purge_dead_workers()
    return


def main():
    setup_logging()
    conn.connect()
    while True:
        loop_body()
    return


if __name__ == "__main__":
    main()
