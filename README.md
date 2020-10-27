# EZ ARCH
'EZ Arch' is a framework to facilitate communication between a large number of services.
The design goals of the architecture are to enable a large number of specialized services
and to enable straightforward horizontal scaling of each service.

<!-- ## Why is this useful? -->

## Using this
This repo provides the 'plumbing' of the framework, and a broker for requests.
<!-- For a demo, see [docker-compose.yaml](https://github.com/tengelisconsulting/ez_arch/blob/master/docker-compose.yaml). -->

The python worker API lives here: [ezpy](https://github.com/tengelisconsulting/ezpy).
I'll include other language clients if I need one, but you can write your own fairly easily.
See [worker protocol](https://github.com/tengelisconsulting/ez_arch/blob/master/worker_protocol.md)


## Details
See [env](https://github.com/tengelisconsulting/ez_arch/blob/master/env) for reference.

Workers connect to a ROUTER at WORKER_PORT.
Workers send heartbeats that specify the name of the service they provide.
Upon receiving a worker heartbeat, the framework saves the address of the worker's DEALER socket and adds it to a list of addresses to which it can
send work for that particular service.  Workers are selected based on 'least tasks assigned'.

The contents of each message are specified in [spec.org](https://raw.githubusercontent.com/tengelisconsulting/ez_arch/master/spec.org).

### Backpressure
Until there are BACKPRESSURE_THRESHOLD jobs queued on a worker, requests don't block at all.
At this point, the request process will block for (queued jobs count) * BACKPRESSURE_RATIO milliseconds.


## Next steps
- The only queuing of jobs takes place on a given worker's DEALER socket.
If a worker dies with frames queued on its socket, these frames are lost.
To resolve this, I intend to expect a worker ACK when a worker takes on a job, so that the job is queued in an Elixir process.
