import logging

import zmq

from app import App
from env import ENV


def _connect_socket(
        socket: zmq.Socket,
        host: str,
        port: int,
        name: str = "socket"
)-> None:
    con_s = "tcp://{}:{}".format(host, port)
    socket.connect(con_s)
    logging.info("%s connected to %s", name, con_s)
    return

def connect(app: App)-> App:
    # init
    broker_dealer = app.c.socket(zmq.DEALER)
    broker_sub = app.c.socket(zmq.SUB)
    broker_sub.setsockopt(zmq.SUBSCRIBE, b"")
    in_router = app.c.socket(zmq.ROUTER)
    worker_router = app.c.socket(zmq.ROUTER)
    # poll
    app.poller.register(broker_sub, zmq.POLLIN)
    app.poller.register(in_router, zmq.POLLIN)
    app.poller.register(worker_router, zmq.POLLIN)
    # connect
    _connect_socket(
        broker_dealer,
        ENV.BROKER_PIPE_HOST, ENV.BROKER_PIPE_IN_PORT,
        "broker state dealer"
    )
    _connect_socket(
        broker_sub,
        ENV.BROKER_PIPE_HOST, ENV.BROKER_BROADCAST_PORT,
        "broker state sub"
    )
    _connect_socket(
        in_router,
        ENV.IN_PIPE_HOST, ENV.IN_PIPE_PORT,
        "router input"
    )
    _connect_socket(
        worker_router,
        ENV.WORKER_PIPE_HOST, ENV.WORKER_PIPE_PORT,
        "worker router"
    )
    return app._replace(
        broker_dealer = broker_dealer,
        broker_sub = broker_sub,
        in_router = in_router,
        worker_router = worker_router,
    )
