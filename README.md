# Distributed Systems @ University of Tartu

This repository contains the initial code for the practice sessions of the Distributed Systems course at the University of Tartu.

## System Model

### Overview

The system is a distributed microservice system deployed with Docker Compose.
The system architecture is client-server/service-oriented, where the `orchestrator` is the central coordinator.
The frontend communicates with the `orchestrator` over HTTP, where backend services communicate using gRPC.
The main backend services are: `fraud_detection`, `transaction_verification`, `suggestions`, `order_queue`, and `order_executor` nodes.

A `/checkout` request is implemented by first `orchestrator` receiving an order from frontend, calling the validation services and collecting their results.
If the order is valid, it is forwarded to the `order_queue`.
The queue decouples validation from later execution.
One `order_executor` leader later dequeues and executes orders.
The executors form a small replicated group and use a simplified Raft-style leader election, where only one node consumes from the queue at a time.

The communication in-between backend services is synchronous RPC, because `orchestrator` waits for service responses before replying to the frontend.
The system propagates vector clocks between services to track causal ordering of events.
Failures are assumed to be crashes/timeouts/unreachable-node failure.
If the leader executor crashes, another executor is elected, but if the queue restarts, the queued orders would be lost because they are stored in process' memory.

### Running the code with Docker Compose [recommended]

To run the code, you need to clone this repository, make sure you have Docker and Docker Compose installed, and run the following command in the root folder of the repository:

```bash
docker compose up
```

This will start the system with the multiple services. Each service will be restarted automatically when you make changes to the code, so you don't have to restart the system manually while developing. If you want to know how the services are started and configured, check the `docker-compose.yaml` file.

To run the replicated order executor with Raft-style leader election, scale it explicitly:

```bash
docker compose up --build --scale order_executor=3
```

You can increase this to `4`, `5`, etc. The replicas discover each other through Docker Compose DNS, elect a single leader, and only that leader dequeues from `order_queue`. For fault tolerance, use at least `3` replicas, because a 2-node Raft cluster cannot keep quorum after one node fails.

The checkpoint evaluations will be done using the code that is started with Docker Compose, so make sure that your code works with Docker Compose.

If, for some reason, changes to the code are not reflected, try to force rebuilding the Docker images with the following command:

```bash
docker compose up --build
```

### Run the code locally

Even though you can run the code locally, it is recommended to use Docker and Docker Compose to run the code. This way you don't have to install any dependencies locally and you can easily run the code on any platform.

If you want to run the code locally, you need to install the following dependencies:

backend services:
- Python 3.8 or newer
- pip
- [grpcio-tools](https://grpc.io/docs/languages/python/quickstart/)
- requirements.txt dependencies from each service

frontend service:
- It's a simple static HTML page, you can open `frontend/src/index.html` in your browser.

And then run each service individually.
