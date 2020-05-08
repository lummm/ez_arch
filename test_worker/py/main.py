#!/usr/bin/env python3

import logging
from threading import Thread
import os
import time

from app import App
from app import Frames
import msg


def setup_logging()-> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format=f"%(asctime)s.%(msecs)03d "
        "%(levelname)s %(module)s - %(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return


def loop_body(app: App)-> App:
    items = app.poller.poll(app.poll_interval_ms)
    for socket, _event in items:
        frames = socket.recv_multipart()
        logging.debug("recvd frames: %s", frames)
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
        in_con_s = "tcp://127.0.0.1:9005",
        out_con_s = "tcp://127.0.0.1:9000",
        service_name = b"TEST_SERVICE",
    )
    t = Thread(target = run_hb_loop, args = (app,))
    t.start()
    run_work_loop(app)
    return


if __name__ == "__main__":
    main()
