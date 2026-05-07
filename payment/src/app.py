import json
import os
import sys
import threading
import time
from concurrent import futures

import grpc

FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
payment_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/payment'))
sys.path.insert(0, payment_grpc_path)

import payment_pb2 as payment
import payment_pb2_grpc as payment_grpc

SERVICE_PORT = int(os.getenv("PAYMENT_PORT", "50057"))
DEFAULT_LOG_PATH = os.getenv("PAYMENT_TRANSACTION_LOG_PATH", "/tmp/payment_transactions.jsonl")

class PaymentService(payment_grpc.PaymentServiceServicer):
    def __init__(self, log_path=None):
        self.lock = threading.RLock()
        self.log_path = log_path or DEFAULT_LOG_PATH
        self.prepared_payments = {}
        self.committed_payments = {}
        self.aborted_transactions = set()
        self._recover_from_log()

    def Prepare(self, request, context):
        if request.amount <= 0:
            return payment.PrepareResponse(ready=False, message="Payment amount must be positive.")

        transaction_id = request.transaction_id
        staged_payment = {
            "order_id": request.order_id,
            "amount": request.amount,
            "user": request.user,
        }

        with self.lock:
            if transaction_id in self.committed_payments:
                return payment.PrepareResponse(ready=True, message="Payment already committed.")
            if transaction_id in self.aborted_transactions:
                return payment.PrepareResponse(ready=False, message="Payment transaction already aborted.")

            existing_payment = self.prepared_payments.get(transaction_id)
            if existing_payment is not None:
                return payment.PrepareResponse(ready=True, message="Payment already staged.")

            self.prepared_payments[transaction_id] = staged_payment
            self._append_log_record(
                {
                    "event": "prepared",
                    "transaction_id": transaction_id,
                    "payment": staged_payment,
                    "logged_at": time.time(),
                }
            )

        print(
            f"INFO: Payment prepared for order {request.order_id}, "
            f"amount {request.amount}, tx {request.transaction_id}"
        )
        return payment.PrepareResponse(ready=True, message="Payment staged.")

    def Commit(self, request, context):
        transaction_id = request.transaction_id
        with self.lock:
            if transaction_id in self.committed_payments:
                return payment.CommitResponse(success=True, message="Payment already committed.")
            if transaction_id in self.aborted_transactions:
                return payment.CommitResponse(success=False, message="Payment transaction was aborted.")

            staged_payment = self.prepared_payments.pop(transaction_id, None)
            if staged_payment is None:
                return payment.CommitResponse(success=False, message="Payment was not prepared.")
            if staged_payment["order_id"] != request.order_id:
                self.prepared_payments[transaction_id] = staged_payment
                return payment.CommitResponse(success=False, message="Order id does not match prepared payment.")

            executed_payment = dict(staged_payment)
            executed_payment["payment_reference"] = f"dummy-payment-{transaction_id}"
            executed_payment["executed_at"] = time.time()
            self._append_log_record(
                {
                    "event": "committed",
                    "transaction_id": transaction_id,
                    "payment": executed_payment,
                    "logged_at": time.time(),
                }
            )
            self.committed_payments[transaction_id] = executed_payment
            self._execute_payment(executed_payment)

        print(f"INFO: Payment committed for order {request.order_id}, tx {request.transaction_id}")
        return payment.CommitResponse(success=True, message="Payment committed.")

    def Abort(self, request, context):
        transaction_id = request.transaction_id
        with self.lock:
            if transaction_id in self.committed_payments:
                return payment.AbortResponse(
                    aborted=False,
                    message="Payment was already committed and cannot be aborted.",
                )

            self.prepared_payments.pop(transaction_id, None)
            if transaction_id not in self.aborted_transactions:
                self._append_log_record(
                    {
                        "event": "aborted",
                        "transaction_id": transaction_id,
                        "order_id": request.order_id,
                        "logged_at": time.time(),
                    }
                )
                self.aborted_transactions.add(transaction_id)

        print(f"INFO: Payment aborted for order {request.order_id}, tx {request.transaction_id}")
        return payment.AbortResponse(aborted=True, message="Payment aborted.")

    def _execute_payment(self, executed_payment):
        print(
            f"INFO: Executed dummy payment for order {executed_payment['order_id']}: "
            f"user={executed_payment['user']}, amount={executed_payment['amount']}, "
            f"reference={executed_payment['payment_reference']}"
        )

    def _recover_from_log(self):
        if not os.path.exists(self.log_path):
            return

        with open(self.log_path, "r", encoding="utf-8") as log_file:
            for line in log_file:
                line = line.strip()
                if not line:
                    continue

                record = json.loads(line)

                transaction_id = record["transaction_id"]
                event = record["event"]

                if event == "prepared":
                    if (
                        transaction_id not in self.committed_payments
                        and transaction_id not in self.aborted_transactions
                    ):
                        self.prepared_payments[transaction_id] = record["payment"]
                elif event == "committed":
                    self.prepared_payments.pop(transaction_id, None)
                    self.aborted_transactions.discard(transaction_id)
                    self.committed_payments[transaction_id] = record["payment"]
                elif event == "aborted" and transaction_id not in self.committed_payments:
                    self.prepared_payments.pop(transaction_id, None)
                    self.aborted_transactions.add(transaction_id)

        print(
            f"INFO: Payment recovery loaded {len(self.prepared_payments)} prepared, "
            f"{len(self.committed_payments)} committed, and "
            f"{len(self.aborted_transactions)} aborted transaction(s)."
        )

    def _append_log_record(self, record):
        log_dir = os.path.dirname(self.log_path)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)

        with open(self.log_path, "a", encoding="utf-8") as log_file:
            log_file.write(json.dumps(record) + "\n")
            log_file.flush()
            os.fsync(log_file.fileno())


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    payment_grpc.add_PaymentServiceServicer_to_server(PaymentService(), server)
    server.add_insecure_port(f"[::]:{SERVICE_PORT}")
    server.start()
    print(f"INFO: Payment service running on {SERVICE_PORT}")
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
