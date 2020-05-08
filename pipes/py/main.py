#!/usr/bin/env python3

import logging

import zmq

from app import App
from env import ENV


def setup_logging()-> None:
    logging.basicConfig(
        level=ENV.LOG_LEVEL,
        format=f"%(asctime)s.%(msecs)03d "
        "%(levelname)s %(module)s - %(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return


def loop_body(app: App)-> None:
    items = app.poller.poll(app.poll_interval_ms)
    for socket, _event in items:
        if socket == app.dealer:
            frames = app.dealer.recv_multipart()
            logging.debug("upstream frames: %s", frames)
            app.router.send_multipart(frames)
        if socket == app.router:
            frames = app.router.recv_multipart()
            logging.debug("downstream frames: %s", frames)
            if app.dealer:
                app.dealer.send_multipart(frames[1:])
            if app.pub:
                app.pub.send_multipart(frames[1:])
    return


def pipe_loop(app: App)-> None:
    while True:
        loop_body(app)
    return


def connect(
        app: App
)-> App:
    pub = app.c.socket(zmq.PUB)
    dealer = app.c.socket(zmq.DEALER)
    router = app.c.socket(zmq.ROUTER)
    app.poller.register(dealer, zmq.POLLIN)
    app.poller.register(router, zmq.POLLIN)
    def bind(s: zmq.Socket, port: int, name: str)-> zmq.Socket:
        if port == -1:
            return None
        con_s = "tcp://*:{}".format(port)
        s.bind(con_s)
        logging.info("%s bound to %s", name, con_s)
        return s
    pub = bind(pub, ENV.BROADCAST_PORT, "pub")
    dealer = bind(dealer, ENV.DEALER_PORT, "dealer")
    bind(router, ENV.IN_PORT, "router")
    return app._replace(
        dealer = dealer,
        pub = pub,
        router = router,
    )


def main():
    setup_logging()
    app = App()
    app = connect(app)
    pipe_loop(app)
    return


if __name__ == "__main__":
    main()
