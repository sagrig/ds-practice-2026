import os
import sys
import threading
import uuid
from concurrent import futures

import grpc

FILE = __file__ if "__file__" in globals() else os.getenv("PYTHONFILE", "")
books_database_grpc_path = os.path.abspath(os.path.join(FILE, "../../../utils/pb/books_database"))
sys.path.insert(0, books_database_grpc_path)

import books_database_pb2 as books_database
import books_database_pb2_grpc as books_database_grpc

BOOKS_DB_PORT = int(os.getenv("BOOKS_DB_PORT", "50056"))
REPLICA_NAME = os.getenv("BOOKS_DB_REPLICA_NAME", "books_db_1")
PRIMARY_NAME = os.getenv("BOOKS_DB_PRIMARY_NAME", "books_db_1")
PEER_REPLICAS = [peer.strip() for peer in os.getenv("BOOKS_DB_REPLICA_NAMES", "").split(",") if peer.strip()]

INITIAL_BOOKS = {
    "Book A": 10,
    "Book B": 10,
    "Book C": 10,
    "Distributed Systems": 5,
}


class ReplicatedBooksDatabase(books_database_grpc.BooksDatabaseServiceServicer):
    def __init__(self):
        self.lock = threading.RLock()
        self.store = dict(INITIAL_BOOKS)
        self.applied_request_ids = set()
        self.staged_writes = {}
        self.transaction_decisions = {}

    def Read(self, request, context):
        if not self._is_primary():
            return self._forward_read(request)

        with self.lock:
            if request.key not in self.store:
                return books_database.ReadResponse(
                    ok=False,
                    message=f"Book '{request.key}' was not found.",
                    key=request.key,
                    value=0,
                    served_by=REPLICA_NAME,
                    is_primary=True,
                )

            return books_database.ReadResponse(
                ok=True,
                message="Read completed.",
                key=request.key,
                value=self.store[request.key],
                served_by=REPLICA_NAME,
                is_primary=True,
            )

    def Write(self, request, context):
        if not self._is_primary():
            return self._forward_write(request)

        request_id = str(uuid.uuid4())
        ok, message, acknowledgements = self._apply_replicated_write(
            request.key,
            request.value,
            request_id,
        )
        if not ok:
            return books_database.WriteResponse(
                ok=False,
                message=message,
                key=request.key,
                value=request.value,
                committed_by=REPLICA_NAME,
                acknowledgements=acknowledgements,
            )

        return books_database.WriteResponse(
            ok=True,
            message="Write committed.",
            key=request.key,
            value=request.value,
            committed_by=REPLICA_NAME,
            acknowledgements=acknowledgements,
        )

    def ReplicateWrite(self, request, context):
        with self.lock:
            if request.request_id in self.applied_request_ids:
                return books_database.ReplicateWriteResponse(
                    ok=True,
                    message="Write already applied.",
                    replica_name=REPLICA_NAME,
                )

            self.store[request.key] = request.value
            self.applied_request_ids.add(request.request_id)

        return books_database.ReplicateWriteResponse(
            ok=True,
            message="Replica write applied.",
            replica_name=REPLICA_NAME,
        )

    def Prepare(self, request, context):
        if not self._is_primary():
            return self._forward_prepare(request)

        with self.lock:
            transaction_id = request.transaction_id
            decision = self.transaction_decisions.get(transaction_id)
            if decision == "committed":
                return books_database.PrepareResponse(
                    ready=True,
                    message="Transaction was already committed.",
                )
            if decision == "aborted":
                return books_database.PrepareResponse(
                    ready=False,
                    message="Transaction was already aborted.",
                )

            if request.key not in self.store:
                return books_database.PrepareResponse(
                    ready=False,
                    message=f"Book '{request.key}' was not found.",
                )
            if request.value < 0:
                return books_database.PrepareResponse(
                    ready=False,
                    message=f"Prepared stock for '{request.key}' cannot be negative.",
                )

            staged = self.staged_writes.setdefault(transaction_id, {})
            existing_value = staged.get(request.key)
            if existing_value is not None and existing_value != request.value:
                return books_database.PrepareResponse(
                    ready=False,
                    message=(
                        f"Conflicting staged values for '{request.key}' in transaction "
                        f"{transaction_id}."
                    ),
                )

            staged[request.key] = request.value

        return books_database.PrepareResponse(ready=True, message="Write staged for transaction.")

    def Commit(self, request, context):
        if not self._is_primary():
            return self._forward_commit(request)

        with self.lock:
            transaction_id = request.transaction_id
            decision = self.transaction_decisions.get(transaction_id)
            if decision == "committed":
                return books_database.CommitResponse(success=True, message="Transaction already committed.")
            if decision == "aborted":
                return books_database.CommitResponse(success=False, message="Transaction was already aborted.")

            writes = dict(self.staged_writes.get(transaction_id, {}))
            if not writes:
                return books_database.CommitResponse(success=False, message="No staged writes for transaction.")

        ok, message = self._execute_stock_updates(transaction_id, writes)
        if not ok:
            return books_database.CommitResponse(success=False, message=message)

        with self.lock:
            self.staged_writes.pop(transaction_id, None)
            self.transaction_decisions[transaction_id] = "committed"

        return books_database.CommitResponse(success=True, message="Transaction committed.")

    def Abort(self, request, context):
        if not self._is_primary():
            return self._forward_abort(request)

        with self.lock:
            transaction_id = request.transaction_id
            decision = self.transaction_decisions.get(transaction_id)
            if decision == "committed":
                return books_database.AbortResponse(
                    aborted=False,
                    message="Transaction was already committed and cannot be aborted.",
                )

            self.staged_writes.pop(transaction_id, None)
            self.transaction_decisions[transaction_id] = "aborted"

        return books_database.AbortResponse(aborted=True, message="Transaction aborted.")

    def GetStatus(self, request, context):
        return books_database.StatusResponse(
            replica_name=REPLICA_NAME,
            is_primary=self._is_primary(),
            primary_name=PRIMARY_NAME,
            peers=list(PEER_REPLICAS),
        )

    def _forward_read(self, request):
        with grpc.insecure_channel(f"{PRIMARY_NAME}:{BOOKS_DB_PORT}") as channel:
            stub = books_database_grpc.BooksDatabaseServiceStub(channel)
            return stub.Read(request, timeout=2)

    def _forward_write(self, request):
        with grpc.insecure_channel(f"{PRIMARY_NAME}:{BOOKS_DB_PORT}") as channel:
            stub = books_database_grpc.BooksDatabaseServiceStub(channel)
            return stub.Write(request, timeout=2)

    def _forward_prepare(self, request):
        with grpc.insecure_channel(f"{PRIMARY_NAME}:{BOOKS_DB_PORT}") as channel:
            stub = books_database_grpc.BooksDatabaseServiceStub(channel)
            return stub.Prepare(request, timeout=2)

    def _forward_commit(self, request):
        with grpc.insecure_channel(f"{PRIMARY_NAME}:{BOOKS_DB_PORT}") as channel:
            stub = books_database_grpc.BooksDatabaseServiceStub(channel)
            return stub.Commit(request, timeout=2)

    def _forward_abort(self, request):
        with grpc.insecure_channel(f"{PRIMARY_NAME}:{BOOKS_DB_PORT}") as channel:
            stub = books_database_grpc.BooksDatabaseServiceStub(channel)
            return stub.Abort(request, timeout=2)

    def _apply_replicated_write(self, key, value, request_id):
        acknowledgements = 1

        with self.lock:
            previous_exists = key in self.store
            previous_value = self.store.get(key)
            self.store[key] = value
            self.applied_request_ids.add(request_id)

        for peer in self._backup_replicas():
            try:
                with grpc.insecure_channel(f"{peer}:{BOOKS_DB_PORT}") as channel:
                    stub = books_database_grpc.BooksDatabaseServiceStub(channel)
                    response = stub.ReplicateWrite(
                        books_database.ReplicateWriteRequest(
                            key=key,
                            value=value,
                            request_id=request_id,
                        ),
                        timeout=1.5,
                    )
            except grpc.RpcError:
                continue

            if response.ok:
                acknowledgements += 1

        required_acks = self._required_acknowledgements()
        if acknowledgements < required_acks:
            with self.lock:
                if previous_exists:
                    self.store[key] = previous_value
                else:
                    self.store.pop(key, None)
                self.applied_request_ids.discard(request_id)
            return (
                False,
                (
                    f"Write rejected because only {acknowledgements} replicas acknowledged; "
                    f"{required_acks} required for commit."
                ),
                acknowledgements,
            )

        return True, "Write committed.", acknowledgements

    def _execute_stock_updates(self, transaction_id, writes):
        for key, value in writes.items():
            ok, message, _ = self._apply_replicated_write(
                key,
                value,
                f"{transaction_id}:{key}:{uuid.uuid4()}",
            )
            if not ok:
                return False, message

            print(
                f"INFO: Database commit operation applied for tx {transaction_id}: "
                f"'{key}' stock is now {value}."
            )

        return True, f"Applied {len(writes)} staged stock update(s)."

    def _is_primary(self):
        return REPLICA_NAME == PRIMARY_NAME

    def _backup_replicas(self):
        return [peer for peer in PEER_REPLICAS if peer != PRIMARY_NAME]

    def _required_acknowledgements(self):
        return (len(PEER_REPLICAS) // 2) + 1


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    books_database_grpc.add_BooksDatabaseServiceServicer_to_server(ReplicatedBooksDatabase(), server)
    server.add_insecure_port(f"[::]:{BOOKS_DB_PORT}")
    server.start()
    print(
        f"INFO: Books database replica {REPLICA_NAME} started on port {BOOKS_DB_PORT}. "
        f"Primary replica: {PRIMARY_NAME}."
    )
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
