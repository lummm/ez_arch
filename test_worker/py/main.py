#!/usr/bin/env python3

import logging
from threading import Thread
import time
from typing import NamedTuple
import os

from app import App
from app import Frames
import msg


class _ENV(NamedTuple):
    PORT: int = int(os.environ["PORT"])
ENV = _ENV()


def setup_logging()-> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format=f"%(asctime)s.%(msecs)03d "
        "%(levelname)s %(module)s - %(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return


def mock_handler(
        app: App,
        req_body: Frames
)-> Frames:
    time.sleep(2)
    return [b"ECHO"] + req_body


def loop_body(app: App)-> App:
    items = app.poller.poll(app.poll_interval_ms)
    for socket, _event in items:
        frames = socket.recv_multipart()
        logging.info("recvd frames: %s", frames)
        pipe_addr = frames[0]
        router_addr = frames[1]
        assert b"" == frames[2]
        my_addr = frames[3]
        assert b"" == frames[4]
        client_return_addr = frames[5:7]
        assert b"" == frames[7]
        req_body = frames[8:]
        reply = mock_handler(app, req_body)
        msg.send_response(app, client_return_addr, reply)
    return app


def run_work_loop(app: App)-> None:
    app = msg.connect(app)
    while True:
        app = loop_body(app)
    return


def run_hb_loop(app: App)-> None:
    app = msg.connect(app)
    while True:
        msg.send_heartbeat(app)
        time.sleep(app.hb_interval_s)
    return


def main():
    setup_logging()
    app = App(
        con_s = "tcp://127.0.0.1:{}".format(ENV.PORT),
        service_name = b"TEST_SERVICE",
    )
    t = Thread(target = run_hb_loop, args = (app,))
    t.start()
    run_work_loop(app)
    return


if __name__ == "__main__":
    main()
