import os
import random
import socket
import sys
import threading
import time
from concurrent import futures

import grpc

FILE = __file__ if "__file__" in globals() else os.getenv("PYTHONFILE", "")
order_queue_grpc_path = os.path.abspath(os.path.join(FILE, "../../../utils/pb/order_queue"))
sys.path.insert(0, order_queue_grpc_path)
order_executor_grpc_path = os.path.abspath(os.path.join(FILE, "../../../utils/pb/order_executor"))
sys.path.insert(0, order_executor_grpc_path)

import order_executor_pb2 as order_executor
import order_executor_pb2_grpc as order_executor_grpc
import order_queue_pb2 as order_queue
import order_queue_pb2_grpc as order_queue_grpc

THIS_NODE = "order_executor"
NODES = ["orchestrator", "transaction", "fraud", "suggestions", "order_queue", "order_executor"]

SERVICE_NAME = os.getenv("ORDER_EXECUTOR_SERVICE_NAME", "order_executor")
SERVICE_PORT = int(os.getenv("ORDER_EXECUTOR_PORT", "50055"))
ORDER_QUEUE_HOST = os.getenv("ORDER_QUEUE_HOST", "order_queue")
ORDER_QUEUE_PORT = int(os.getenv("ORDER_QUEUE_PORT", "50054"))
DISCOVERY_INTERVAL_SECONDS = float(os.getenv("ORDER_EXECUTOR_DISCOVERY_INTERVAL_SECONDS", "2"))
HEARTBEAT_INTERVAL_SECONDS = float(os.getenv("ORDER_EXECUTOR_HEARTBEAT_INTERVAL_SECONDS", "1"))
WORKER_INTERVAL_SECONDS = float(os.getenv("ORDER_EXECUTOR_WORKER_INTERVAL_SECONDS", "2"))
ELECTION_TIMEOUT_MIN_SECONDS = float(os.getenv("ORDER_EXECUTOR_ELECTION_TIMEOUT_MIN_SECONDS", "3"))
ELECTION_TIMEOUT_MAX_SECONDS = float(os.getenv("ORDER_EXECUTOR_ELECTION_TIMEOUT_MAX_SECONDS", "5"))


def zero_clocks():
    return {node: 0 for node in NODES}


def tick(clock, node):
    new_clock = dict(clock)
    new_clock[node] = new_clock.get(node, 0) + 1
    return new_clock


def merge_clock(a, b):
    merged = {}
    for node in NODES:
        merged[node] = max(a.get(node, 0), b.get(node, 0))
    return merged


class RaftExecutor(order_executor_grpc.OrderExecutorServiceServicer):
    def __init__(self):
        self.lock = threading.RLock()
        self.stop_event = threading.Event()
        self.node_id = self._resolve_self_ip()
        self.current_term = 0
        self.voted_for = ""
        self.role = "follower"
        self.leader_id = ""
        self.peers = []
        self.vector_clock = zero_clocks()
        self.election_deadline = 0.0
        self._reset_election_deadline()

    def RequestVote(self, request, context):
        with self.lock:
            if request.term > self.current_term:
                self._step_down(request.term, "")

            vote_granted = False
            if request.term == self.current_term and self.voted_for in ("", request.candidate_id):
                self.voted_for = request.candidate_id
                self.role = "follower"
                self.leader_id = ""
                self._reset_election_deadline()
                vote_granted = True

            return order_executor.VoteResponse(
                term=self.current_term,
                vote_granted=vote_granted,
                responder_id=self.node_id,
            )

    def AppendEntries(self, request, context):
        with self.lock:
            if request.term < self.current_term:
                return order_executor.AppendEntriesResponse(
                    term=self.current_term,
                    success=False,
                    responder_id=self.node_id,
                )

            if request.term > self.current_term or self.role != "follower" or self.leader_id != request.leader_id:
                self.current_term = request.term
                self.voted_for = ""

            self.role = "follower"
            self.leader_id = request.leader_id
            self._reset_election_deadline()

            return order_executor.AppendEntriesResponse(
                term=self.current_term,
                success=True,
                responder_id=self.node_id,
            )

    def GetStatus(self, request, context):
        with self.lock:
            return order_executor.StatusResponse(
                node_id=self.node_id,
                role=self.role,
                current_term=self.current_term,
                leader_id=self.leader_id,
                peers=list(self.peers),
            )

    def start_background_threads(self):
        threads = [
            threading.Thread(target=self._discovery_loop, daemon=True),
            threading.Thread(target=self._election_loop, daemon=True),
            threading.Thread(target=self._heartbeat_loop, daemon=True),
            threading.Thread(target=self._worker_loop, daemon=True),
        ]
        for thread in threads:
            thread.start()

    def stop(self):
        self.stop_event.set()

    def _resolve_self_ip(self):
        try:
            return socket.gethostbyname(socket.gethostname())
        except socket.gaierror:
            return os.getenv("HOSTNAME", "unknown-node")

    def _reset_election_deadline(self):
        timeout = random.uniform(ELECTION_TIMEOUT_MIN_SECONDS, ELECTION_TIMEOUT_MAX_SECONDS)
        self.election_deadline = time.monotonic() + timeout

    def _step_down(self, new_term, leader_id):
        self.current_term = new_term
        self.voted_for = ""
        self.role = "follower"
        self.leader_id = leader_id
        self._reset_election_deadline()

    def _discover_peer_ips(self):
        peers = set()
        try:
            for ip_address in socket.gethostbyname_ex(SERVICE_NAME)[2]:
                if ip_address != self.node_id:
                    peers.add(ip_address)
            for info in socket.getaddrinfo(SERVICE_NAME, SERVICE_PORT, socket.AF_INET, socket.SOCK_STREAM):
                ip_address = info[4][0]
                if ip_address != self.node_id:
                    peers.add(ip_address)
        except socket.gaierror:
            return []
        return sorted(peers)

    def _cluster_size(self, peers):
        return len(peers) + 1

    def _quorum_size(self, peers):
        return (self._cluster_size(peers) // 2) + 1

    def _discovery_loop(self):
        while not self.stop_event.is_set():
            discovered_peers = self._discover_peer_ips()
            with self.lock:
                self.peers = discovered_peers
            time.sleep(DISCOVERY_INTERVAL_SECONDS)

    def _election_loop(self):
        while not self.stop_event.is_set():
            should_start = False
            with self.lock:
                should_start = self.role != "leader" and time.monotonic() >= self.election_deadline

            if should_start:
                self._start_election()

            time.sleep(0.2)

    def _start_election(self):
        with self.lock:
            self.current_term += 1
            election_term = self.current_term
            self.role = "candidate"
            self.voted_for = self.node_id
            self.leader_id = ""
            self._reset_election_deadline()
            peers = list(self.peers)

        votes = 1

        for peer in peers:
            try:
                with grpc.insecure_channel(f"{peer}:{SERVICE_PORT}") as channel:
                    stub = order_executor_grpc.OrderExecutorServiceStub(channel)
                    response = stub.RequestVote(
                        order_executor.VoteRequest(
                            term=election_term,
                            candidate_id=self.node_id,
                        ),
                        timeout=1.5,
                    )
            except grpc.RpcError:
                continue

            if response.term > election_term:
                with self.lock:
                    if response.term > self.current_term:
                        self._step_down(response.term, "")
                return

            if response.vote_granted:
                votes += 1

        with self.lock:
            if self.current_term != election_term or self.role != "candidate":
                return

            if votes >= self._quorum_size(peers):
                self.role = "leader"
                self.leader_id = self.node_id
                print(
                    f"INFO: Node {self.node_id} became leader for term {self.current_term} "
                    f"with quorum {self._quorum_size(peers)}/{self._cluster_size(peers)}."
                )
            else:
                self.role = "follower"
                self.voted_for = ""
                self._reset_election_deadline()

    def _heartbeat_loop(self):
        while not self.stop_event.is_set():
            with self.lock:
                is_leader = self.role == "leader"
                heartbeat_term = self.current_term
                peers = list(self.peers)

            if is_leader:
                for peer in peers:
                    try:
                        with grpc.insecure_channel(f"{peer}:{SERVICE_PORT}") as channel:
                            stub = order_executor_grpc.OrderExecutorServiceStub(channel)
                            response = stub.AppendEntries(
                                order_executor.AppendEntriesRequest(
                                    term=heartbeat_term,
                                    leader_id=self.node_id,
                                ),
                                timeout=1.5,
                            )
                    except grpc.RpcError:
                        continue

                    if response.term > heartbeat_term:
                        with self.lock:
                            if response.term > self.current_term:
                                self._step_down(response.term, "")
                        break

            time.sleep(HEARTBEAT_INTERVAL_SECONDS)

    def _worker_loop(self):
        while not self.stop_event.is_set():
            with self.lock:
                is_leader = self.role == "leader"

            if is_leader:
                self._attempt_dequeue()

            time.sleep(WORKER_INTERVAL_SECONDS)

    def _attempt_dequeue(self):
        request_clock = tick(self.vector_clock, THIS_NODE)
        try:
            with grpc.insecure_channel(f"{ORDER_QUEUE_HOST}:{ORDER_QUEUE_PORT}") as channel:
                stub = order_queue_grpc.OrderQueueServiceStub(channel)
                request = order_queue.DequeueRequest()
                request.vector_clock.update(request_clock)
                response = stub.Dequeue(request, timeout=2)
        except grpc.RpcError as error:
            print(f"WARNING: Leader {self.node_id} failed to reach order_queue: {error}")
            return

        self.vector_clock = merge_clock(request_clock, dict(response.vector_clock))

        if not response.ok:
            return

        print(
            f"INFO: Leader {self.node_id} dequeued order {response.order_id} for user {response.user}. "
            "Order is being executed..."
        )


def serve():
    service = RaftExecutor()
    service.start_background_threads()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    order_executor_grpc.add_OrderExecutorServiceServicer_to_server(service, server)
    server.add_insecure_port(f"[::]:{SERVICE_PORT}")
    server.start()

    print(f"INFO: Order executor node started on port {SERVICE_PORT} with node id {service.node_id}.")

    try:
        server.wait_for_termination()
    finally:
        service.stop()


if __name__ == "__main__":
    serve()
