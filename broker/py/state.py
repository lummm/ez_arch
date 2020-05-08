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
        [b"", protoc.HEARTBEAT, service_name, worker_addr]
    )
    return app
