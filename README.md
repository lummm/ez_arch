# EZ ARCH
'EZ Arch' is a framework to facilitate communication between a number of services.
The design goals of the architecture are to enable a large number of specialized services 
and to enable straightforward horizontal scaling of each service.


## Using this
This repo provides the 'plumbing' of the framework, and a broker for requests.
For a demo, see [docker-compose.yaml](https://github.com/tengelisconsulting/ez_arch/blob/master/docker-compose.yaml).

Ensure you have both docker and docker-compose installed locally, as well as the ez_arch_worker pypi package (pip install ez_arch_worker).
Build the 'plumbing' by running ./hooks/build (from the top level directory).  Build the test worker with 'docker-compose build'.
Bring up the infrastructure and the test worker with 'docker-compose up'.  Now, send some requests by running ./client/test_client.py.

The python worker API lives here: [ez_arch_worker](https://github.com/tengelisconsulting/ez_arch_worker).
It still undergoes breaking changes regularly.


## Primary use case
I use this with openresty (see [ez_client.lua](https://github.com/tengelisconsulting/ez_arch/blob/master/client/ez_client.lua))
to drive web backends.
To split services into such small resolution as this architecture seeks to achieve, including an http handler in each service is too cumbersome.
Using zeromq means that the biggest performance consideration of introducing a service is
serialization of the data to be processed such that it can be sent over tcp.
Assuming we keep payloads small, this makes the overhead of factoring code into small services negligible.


## Details
This is described in terms of zeromq sockets,
and will use the port names in [env](https://github.com/tengelisconsulting/ez_arch/blob/master/env) for reference:

In order to allow multiple brokers, there is a ROUTER-DEALER pipe established from 'IN_PIPE_PORT_A' to 'IN_PIPE_PORT_B' (ie. dealer at port B).
Each broker connects a ROUTER to IN_PIPE_PORT_B, where it listens for client requests, or for worker responses.

In order for brokers to synchronize state, there is a ROUTER to PUB pipe set up from 'BROKER_PIPE_PORT_A' to 'BROKER_PIPE_BROADCAST_PORT'.
Each broker connects a SUB to the broadcast port.  Worker heartbeats are sent into this pipe, and thus shared across brokers.

Workers connect to a DEALER-ROUTER pipe set up from WORKER_PIPE_A to WORKER_PIPE_B (dealer at port B).
Workers send heartbeats that specify the name of the service they provide.
Upon receiving a worker heartbeat, a broker saves the address of the worker's DEALER socket and adds it to a list of addresses to which it can
send work for that particular service.  Via the broker broadcast pipe, these updates are shared among multiple brokers.

Additionally, brokers must keep track of which workers are engaged in work, or have failed to heartbeat within the specified timeout.
They do so internally, and again share updates via the broker broadcast pipe.  Workers are selected based on 'least tasks assigned'.

The contents of each message are specified in [spec.org](https://raw.githubusercontent.com/tengelisconsulting/ez_arch/master/spec.org).


## Notes
- The only queuing of jobs takes place on a given worker's DEALER socket.
If a worker dies with frames queued on its socket, these frames are lost.
However, as the architecture is intended to favour sub-second responses from workers,
this should not be an issue if requests are made across multiple retries of intervals longer than one second,
which is the case by default.
If large numbers of messages are being queued for a given service, then either more workers for the service need to be added,
or the nature of the service is inappropriate for this architecture.
In this case, try to split up the work performed by the service into smaller services.
