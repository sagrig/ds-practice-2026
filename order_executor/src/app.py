import os
import random
import socket
import sys
import threading
import time
from concurrent import futures

import grpc
import uuid

FILE = __file__ if "__file__" in globals() else os.getenv("PYTHONFILE", "")
order_queue_grpc_path = os.path.abspath(os.path.join(FILE, "../../../utils/pb/order_queue"))
sys.path.insert(0, order_queue_grpc_path)
order_executor_grpc_path = os.path.abspath(os.path.join(FILE, "../../../utils/pb/order_executor"))
sys.path.insert(0, order_executor_grpc_path)
books_database_grpc_path = os.path.abspath(os.path.join(FILE, "../../../utils/pb/books_database"))
sys.path.insert(0, books_database_grpc_path)
payment_grpc_path = os.path.abspath(os.path.join(FILE, "../../../utils/pb/payment"))
sys.path.insert(0, payment_grpc_path)
notification_grpc_path = os.path.abspath(os.path.join(FILE, "../../../utils/pb/notification"))
sys.path.insert(0, notification_grpc_path)

import books_database_pb2 as books_database
import books_database_pb2_grpc as books_database_grpc
import order_executor_pb2 as order_executor
import order_executor_pb2_grpc as order_executor_grpc
import order_queue_pb2 as order_queue
import order_queue_pb2_grpc as order_queue_grpc
import payment_pb2 as payment
import payment_pb2_grpc as payment_grpc
import notification_pb2 as notification
import notification_pb2_grpc as notification_grpc

SERVICE_NAME = os.getenv("ORDER_EXECUTOR_SERVICE_NAME", "order_executor")
SERVICE_PORT = int(os.getenv("ORDER_EXECUTOR_PORT", "50055"))
ORDER_QUEUE_HOST = os.getenv("ORDER_QUEUE_HOST", "order_queue")
ORDER_QUEUE_PORT = int(os.getenv("ORDER_QUEUE_PORT", "50054"))
BOOKS_DB_HOST = os.getenv("BOOKS_DB_HOST", "books_db_1")
BOOKS_DB_PORT = int(os.getenv("BOOKS_DB_PORT", "50056"))
PAYMENT_HOST = os.getenv("PAYMENT_HOST", "payment")
PAYMENT_PORT = int(os.getenv("PAYMENT_PORT", "50057"))
NOTIFICATION_HOST = os.getenv("NOTIFICATION_HOST", "notification")
NOTIFICATION_PORT = int(os.getenv("NOTIFICATION_PORT", "50058"))
DISCOVERY_INTERVAL_SECONDS = float(os.getenv("ORDER_EXECUTOR_DISCOVERY_INTERVAL_SECONDS", "2"))
HEARTBEAT_INTERVAL_SECONDS = float(os.getenv("ORDER_EXECUTOR_HEARTBEAT_INTERVAL_SECONDS", "1"))
WORKER_INTERVAL_SECONDS = float(os.getenv("ORDER_EXECUTOR_WORKER_INTERVAL_SECONDS", "2"))
ELECTION_TIMEOUT_MIN_SECONDS = float(os.getenv("ORDER_EXECUTOR_ELECTION_TIMEOUT_MIN_SECONDS", "3"))
ELECTION_TIMEOUT_MAX_SECONDS = float(os.getenv("ORDER_EXECUTOR_ELECTION_TIMEOUT_MAX_SECONDS", "5"))
LEADER_LEASE_MULTIPLIER = float(os.getenv("ORDER_EXECUTOR_LEADER_LEASE_MULTIPLIER", "2.5"))

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
        self.election_deadline = 0.0
        self.last_quorum_timestamp = 0.0
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
        self.last_quorum_timestamp = 0.0
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
                self.last_quorum_timestamp = time.monotonic()
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
                successful_responses = 1
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

                    if response.success:
                        successful_responses += 1
                else:
                    with self.lock:
                        if (
                            self.role == "leader"
                            and self.current_term == heartbeat_term
                            and successful_responses >= self._quorum_size(peers)
                        ):
                            self.last_quorum_timestamp = time.monotonic()

            time.sleep(HEARTBEAT_INTERVAL_SECONDS)

    def _worker_loop(self):
        while not self.stop_event.is_set():
            with self.lock:
                is_leader = self.role == "leader"
                has_quorum = self._has_active_quorum_locked()

            if is_leader and has_quorum:
                self._attempt_dequeue()

            time.sleep(WORKER_INTERVAL_SECONDS)

    def _has_active_quorum_locked(self):
        lease_seconds = max(HEARTBEAT_INTERVAL_SECONDS * LEADER_LEASE_MULTIPLIER, HEARTBEAT_INTERVAL_SECONDS)
        return (time.monotonic() - self.last_quorum_timestamp) <= lease_seconds

    def _attempt_dequeue(self):
        try:
            with grpc.insecure_channel(f"{ORDER_QUEUE_HOST}:{ORDER_QUEUE_PORT}") as channel:
                stub = order_queue_grpc.OrderQueueServiceStub(channel)
                request = order_queue.DequeueRequest()
                response = stub.Dequeue(request, timeout=2)
        except grpc.RpcError as error:
            print(f"WARNING: Leader {self.node_id} failed to reach order_queue: {error}")
            return

        if not response.ok:
            return

        print(
            f"INFO: Leader {self.node_id} dequeued order {response.order_id} for user {response.user}. "
            "Order is being executed..."
        )
        self._execute_order(response.order_id, response.user, response.items)

    def _execute_order(self, order_id, user, items):
        item_quantities = {}
        for item in items:
            if item.quantity <= 0:
                print(
                    f"WARNING: Order {order_id} for user {user} rejected because "
                    f"book '{item.title}' has invalid quantity {item.quantity}."
                )
                return
            item_quantities[item.title] = item_quantities.get(item.title, 0) + item.quantity

        requested_items = [
            {"title": title, "quantity": quantity}
            for title, quantity in item_quantities.items()
        ]
        if not requested_items:
            print(f"WARNING: Order {order_id} for user {user} rejected because it has no items.")
            return

        # Phase 0: read current stock and validate
        current_stocks = {}
        for item in requested_items:
            read_response = self._read_stock(item["title"])
            if not read_response.ok:
                print(
                    f"WARNING: Order {order_id} for user {user} could not be executed because "
                    f"book '{item['title']}' was not found in the books database."
                )
                return

            current_stocks[item["title"]] = read_response.value

        insufficient_books = [
            item["title"]
            for item in requested_items
            if current_stocks[item["title"]] < item["quantity"]
        ]
        if insufficient_books:
            print(
                f"INFO: Order {order_id} for user {user} rejected due to insufficient stock for "
                f"{', '.join(insufficient_books)}."
            )
            return

        # Prepare transaction: compute new stock values but don't apply yet
        new_stocks = {item["title"]: current_stocks[item["title"]] - item["quantity"] for item in requested_items}
        transaction_id = str(uuid.uuid4())
        amount = float(sum(item["quantity"] for item in requested_items))
        participants = [
            (
                "books database",
                lambda: self._prepare_books_database(transaction_id, new_stocks),
                lambda: self._commit_books_database(transaction_id),
                lambda: self._abort_books_database(transaction_id),
            ),
            (
                "payment",
                lambda: self._prepare_payment(transaction_id, order_id, user, amount),
                lambda: self._commit_payment(transaction_id, order_id),
                lambda: self._abort_payment(transaction_id, order_id),
            ),
            (
                "notification",
                lambda: self._prepare_notification(transaction_id, order_id, user),
                lambda: self._commit_notification(transaction_id, order_id),
                lambda: self._abort_notification(transaction_id, order_id),
            ),
        ]

        if not self._two_phase_commit(order_id, transaction_id, participants):
            return

        print(
            f"INFO: Order {order_id} for user {user} executed and committed successfully. "
            f"Updated items: {requested_items} (tx={transaction_id})"
        )

    def _two_phase_commit(self, order_id, transaction_id, participants):
        prepared_participants = []

        # Phase 1: ask every participant if they are ready to commit
        for name, prepare, _, abort in participants:
            try:
                prepare()
                prepared_participants.append((name, abort))
                print(f"INFO: 2PC prepare accepted by {name} for order {order_id} (tx={transaction_id}).")
            except Exception as error:
                print(
                    f"WARNING: Order {order_id} prepare failed on {name}; "
                    f"aborting transaction {transaction_id}: {error}"
                )
                self._abort_prepared_participants(order_id, transaction_id, prepared_participants)
                return False


        # Phase 2: send the final decision. Participans execute once receive Commit
        commit_ok = True
        for name, _, commit, _ in participants:
            if not self._retry_participant_call(
                lambda commit=commit: commit(),
                operation="commit",
                participant_name=name,
                order_id=order_id,
                transaction_id=transaction_id,
            ):
                commit_ok = False

        if not commit_ok:
            print(
                f"ERROR: Order {order_id} is in-doubt because one or more participants "
                f"could not acknowledge the commit decision (tx={transaction_id})."
            )
            return False

        return True

    def _read_stock(self, title):
        with grpc.insecure_channel(f"{BOOKS_DB_HOST}:{BOOKS_DB_PORT}") as channel:
            stub = books_database_grpc.BooksDatabaseServiceStub(channel)
            return stub.Read(books_database.ReadRequest(key=title), timeout=2)

    def _prepare_books_database(self, transaction_id, new_stocks):
        with grpc.insecure_channel(f"{BOOKS_DB_HOST}:{BOOKS_DB_PORT}") as channel:
            stub = books_database_grpc.BooksDatabaseServiceStub(channel)
            for title, new_stock in new_stocks.items():
                response = stub.Prepare(
                    books_database.PrepareRequest(
                        transaction_id=transaction_id,
                        key=title,
                        value=new_stock,
                    ),
                    timeout=2,
                )
                if not response.ready:
                    raise RuntimeError(f"books database rejected '{title}': {response.message}")

    def _commit_books_database(self, transaction_id):
        with grpc.insecure_channel(f"{BOOKS_DB_HOST}:{BOOKS_DB_PORT}") as channel:
            stub = books_database_grpc.BooksDatabaseServiceStub(channel)
            response = stub.Commit(books_database.CommitRequest(transaction_id=transaction_id), timeout=2)
            if not response.success:
                raise RuntimeError(response.message)

    def _abort_books_database(self, transaction_id):
        with grpc.insecure_channel(f"{BOOKS_DB_HOST}:{BOOKS_DB_PORT}") as channel:
            stub = books_database_grpc.BooksDatabaseServiceStub(channel)
            response = stub.Abort(books_database.AbortRequest(transaction_id=transaction_id), timeout=2)
            if not response.aborted:
                raise RuntimeError(response.message)

    def _prepare_payment(self, transaction_id, order_id, user, amount):
        with grpc.insecure_channel(f"{PAYMENT_HOST}:{PAYMENT_PORT}") as channel:
            stub = payment_grpc.PaymentServiceStub(channel)
            response = stub.Prepare(
                payment.PrepareRequest(
                    transaction_id=transaction_id,
                    order_id=order_id,
                    amount=amount,
                    user=user,
                ),
                timeout=2,
            )
            if not response.ready:
                raise RuntimeError(response.message)

    def _commit_payment(self, transaction_id, order_id):
        with grpc.insecure_channel(f"{PAYMENT_HOST}:{PAYMENT_PORT}") as channel:
            stub = payment_grpc.PaymentServiceStub(channel)
            response = stub.Commit(
                payment.CommitRequest(transaction_id=transaction_id, order_id=order_id),
                timeout=2,
            )
            if not response.success:
                raise RuntimeError(response.message)

    def _abort_payment(self, transaction_id, order_id):
        with grpc.insecure_channel(f"{PAYMENT_HOST}:{PAYMENT_PORT}") as channel:
            stub = payment_grpc.PaymentServiceStub(channel)
            response = stub.Abort(
                payment.AbortRequest(transaction_id=transaction_id, order_id=order_id),
                timeout=2,
            )
            if not response.aborted:
                raise RuntimeError(response.message)

    def _prepare_notification(self, transaction_id, order_id, user):
        with grpc.insecure_channel(f"{NOTIFICATION_HOST}:{NOTIFICATION_PORT}") as channel:
            stub = notification_grpc.NotificationServiceStub(channel)
            response = stub.Prepare(
                notification.PrepareRequest(
                    transaction_id=transaction_id,
                    order_id=order_id,
                    user=user,
                    message=f"Order {order_id} is being processed",
                ),
                timeout=2,
            )
            if not response.ready:
                raise RuntimeError(response.message)

    def _commit_notification(self, transaction_id, order_id):
        with grpc.insecure_channel(f"{NOTIFICATION_HOST}:{NOTIFICATION_PORT}") as channel:
            stub = notification_grpc.NotificationServiceStub(channel)
            response = stub.Commit(
                notification.CommitRequest(transaction_id=transaction_id, order_id=order_id),
                timeout=2,
            )
            if not response.success:
                raise RuntimeError(response.message)

    def _abort_notification(self, transaction_id, order_id):
        with grpc.insecure_channel(f"{NOTIFICATION_HOST}:{NOTIFICATION_PORT}") as channel:
            stub = notification_grpc.NotificationServiceStub(channel)
            response = stub.Abort(
                notification.AbortRequest(transaction_id=transaction_id, order_id=order_id),
                timeout=2,
            )
            if not response.aborted:
                raise RuntimeError(response.message)

    def _abort_prepared_participants(self, order_id, transaction_id, prepared_participants):
        for name, abort in reversed(prepared_participants):
            self._retry_participant_call(
                lambda abort=abort: abort(),
                operation="abort",
                participant_name=name,
                order_id=order_id,
                transaction_id=transaction_id,
            )

    def _retry_participant_call(self, call, operation, participant_name, order_id, transaction_id):
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                call()
                print(
                    f"INFO: 2PC {operation} acknowledged by {participant_name} "
                    f"for order {order_id} (tx={transaction_id})."
                )
                return True
            except Exception as error:
                print(
                    f"WARNING: 2PC {operation} attempt {attempt}/{max_attempts} failed "
                    f"for {participant_name} on order {order_id} (tx={transaction_id}): {error}"
                )
                time.sleep(0.2 * attempt)
        return False

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
