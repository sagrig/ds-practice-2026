import importlib.util
import sys
import tempfile
import types
import unittest
from pathlib import Path
from types import SimpleNamespace


class PrepareResponse:
    def __init__(self, ready=False, message=""):
        self.ready = ready
        self.message = message


class CommitResponse:
    def __init__(self, success=False, message=""):
        self.success = success
        self.message = message


class AbortResponse:
    def __init__(self, aborted=False, message=""):
        self.aborted = aborted
        self.message = message


def load_payment_app():
    sys.modules["grpc"] = types.SimpleNamespace(server=lambda *args, **kwargs: None)
    sys.modules["payment_pb2"] = types.SimpleNamespace(
        PrepareResponse=PrepareResponse,
        CommitResponse=CommitResponse,
        AbortResponse=AbortResponse,
    )
    sys.modules["payment_pb2_grpc"] = types.SimpleNamespace(
        PaymentServiceServicer=object,
        add_PaymentServiceServicer_to_server=lambda service, server: None,
    )

    app_path = Path(__file__).resolve().parents[1] / "src" / "app.py"
    spec = importlib.util.spec_from_file_location("payment_app_under_test", app_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class PaymentRecoveryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.payment_app = load_payment_app()

    def test_prepared_payment_survives_restart_and_later_commits(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            log_path = str(Path(temp_dir) / "payment-transactions.jsonl")
            prepare_request = SimpleNamespace(
                transaction_id="tx-prepared-recovery",
                order_id="order-1",
                amount=12.5,
                user="alice",
            )
            commit_request = SimpleNamespace(
                transaction_id=prepare_request.transaction_id,
                order_id=prepare_request.order_id,
            )

            service = self.payment_app.PaymentService(log_path=log_path)
            prepare_response = service.Prepare(prepare_request, None)
            self.assertTrue(prepare_response.ready)

            recovered_service = self.payment_app.PaymentService(log_path=log_path)
            self.assertIn(prepare_request.transaction_id, recovered_service.prepared_payments)

            commit_response = recovered_service.Commit(commit_request, None)
            self.assertTrue(commit_response.success)
            committed_payment = recovered_service.committed_payments[prepare_request.transaction_id]

            recovered_again = self.payment_app.PaymentService(log_path=log_path)
            retry_response = recovered_again.Commit(commit_request, None)
            self.assertTrue(retry_response.success)
            self.assertEqual(
                committed_payment,
                recovered_again.committed_payments[prepare_request.transaction_id],
            )

    def test_abort_after_prepare_survives_restart_and_blocks_commit(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            log_path = str(Path(temp_dir) / "payment-transactions.jsonl")
            prepare_request = SimpleNamespace(
                transaction_id="tx-abort-recovery",
                order_id="order-2",
                amount=4.0,
                user="bob",
            )
            decision_request = SimpleNamespace(
                transaction_id=prepare_request.transaction_id,
                order_id=prepare_request.order_id,
            )

            service = self.payment_app.PaymentService(log_path=log_path)
            self.assertTrue(service.Prepare(prepare_request, None).ready)
            self.assertTrue(service.Abort(decision_request, None).aborted)

            recovered_service = self.payment_app.PaymentService(log_path=log_path)
            commit_response = recovered_service.Commit(decision_request, None)
            self.assertFalse(commit_response.success)
            self.assertIn(prepare_request.transaction_id, recovered_service.aborted_transactions)


if __name__ == "__main__":
    unittest.main()
