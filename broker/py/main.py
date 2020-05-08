#!/usr/bin/env python3

import logging

import zmq

from app import App
from app import Frames
import client_msg
import conn
from env import ENV
import protoc
import worker_msg


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
    logging.info("broker broadcast: %s", frames)
    return app


def handle_input_frames(
        app: App,
        frames: Frames          # INPUT FLAT
)-> App:
    return_addr = frames[0]
    assert b"" == frames[1]
    msg_type = frames[2]
    body = frames[3:]
    if msg_type == protoc.WORKER:
        app = worker_msg.handle(app, body)
    elif msg_type == protoc.CLIENT:
        app = client_msg.handle(app, body)
    else:
        logging.error("unknown msg type: %s", msg_type)
    return app


def loop_body(app: App)-> App:
    items = app.poller.poll(app.poll_interval_ms)
    for socket, _event in items:
        frames = socket.recv_multipart()
        logging.debug("got frames: %s", frames)
        if socket == app.in_router:
            app = handle_input_frames(app, frames)
        if socket == app.broker_sub:
            app = handle_broker_broadcast(app, frames)
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
