#!/usr/bin/env python3

import logging

import zmq

from app import App
from app import Frames
import client_msg
import conn
from env import ENV
import protoc
import state
import worker


def setup_logging()-> None:
    logging.basicConfig(
        level=ENV.LOG_LEVEL,
        format=f"%(asctime)s.%(msecs)03d "
        "%(levelname)s %(module)s - %(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return


def handle_broker_broadcast(
        app: App,
        frames: Frames          # B_STATE FLAT
)-> App:
    return state.handle(app, frames)


# def handle_input_frames(
#         app: App,
#         frames: Frames          # INPUT FLAT
# )-> App:
#     in_pipe_addr = frames[0]
#     return_addr = frames[1]
#     assert b"" == frames[2]
#     msg_type = frames[3]
#     body = frames[4:]
#     app = app._replace(in_pipe_addr = in_pipe_addr)
#     if msg_type == protoc.WORKER:
#         app = worker.handle(app, return_addr, body)
#     elif msg_type == protoc.CLIENT:
#         app = client_msg.handle(app, return_addr, body)
#     else:
#         logging.error("unknown msg type: %s", msg_type)
#     return app


def handle_req_frames(
        app: App,
        frames: Frames          # INPUT FLAT
)-> App:
    in_pipe_addr = frames[0]
    return_addr = frames[1]
    assert b"" == frames[2]
    msg_type = frames[3]
    assert msg_type == protoc.CLIENT
    body = frames[4:]
    app = app._replace(in_pipe_addr = in_pipe_addr)
    app = client_msg.handle(app, return_addr, body)
    return app


def handle_worker_frames(
        app: App,
        frames: Frames          # INPUT FLAT
)-> App:
    worker_pipe_addr = frames[0]
    return_addr = frames[1]
    assert b"" == frames[2]
    msg_type = frames[3]
    assert msg_type == protoc.WORKER
    body = frames[4:]
    app = app._replace(worker_pipe_addr = worker_pipe_addr)
    app = worker.handle(app, return_addr, body)
    return app


def loop_body(app: App)-> App:
    items = app.poller.poll(ENV.POLL_INTERVAL_MS)
    # should really check state changes first
    for socket, _event in items:
        frames = socket.recv_multipart()
        if socket == app.in_router:
            app = handle_req_frames(app, frames)
        if socket == app.broker_sub:
            app = handle_broker_broadcast(app, frames)
        if socket == app.worker_router:
            app = handle_worker_frames(app, frames)
    app = worker.purge_dead_workers(app)
    return app


def loop(app: App)-> None:
    while True:
        app = loop_body(app)
    return


def main():
    setup_logging()
    app = App()
    app = conn.connect(app)
    loop(app)
    return


if __name__ == "__main__":
    main()
