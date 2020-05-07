#!/usr/bin/env python3

import os
from typing import List

import zmq


INPUT_PORT = os.environ["IN_PIPE_PORT_A"]


def request(
        c: zmq.Context,
        service: bytes,
        body: List[bytes]
)-> None:
    req = c.socket(zmq.REQ)
    req.connect("tcp://127.0.0.1:{}".format(INPUT_PORT))
    req.send_multipart(
        [b"\x02", service] + body
    )
    return


def main():
    c = zmq.Context()
    request(c, b"TEST_SERVICE", [b'hey'])
    return


if __name__ == "__main__":
    main()
