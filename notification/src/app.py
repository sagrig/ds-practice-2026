import os
import sys
import threading
import time
from concurrent import futures

import grpc

FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
notification_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/notification'))
sys.path.insert(0, notification_grpc_path)

import notification_pb2 as notification
import notification_pb2_grpc as notification_grpc

SERVICE_PORT = int(os.getenv("NOTIFICATION_PORT", "50058"))


class NotificationService(notification_grpc.NotificationServiceServicer):
    def __init__(self):
        self.lock = threading.RLock()
        self.prepared_notifications = {}
        self.committed_notifications = {}
        self.aborted_transactions = set()

    def Prepare(self, request, context):
        transaction_id = request.transaction_id
        staged_notification = {
            "order_id": request.order_id,
            "user":     request.user,
            "message":  request.message,
        }

        with self.lock:
            if transaction_id in self.committed_notifications:
                return notification.PrepareResponse(ready=True, message="Notification already committed.")
            if transaction_id in self.aborted_transactions:
                return notification.PrepareResponse(ready=False, message="Notification transaction already aborted.")

            existing_notification = self.prepared_notifications.get(transaction_id)

            if existing_notification is not None:
                return notification.PrepareResponse(ready=True, message="Notification already staged.")

            self.prepared_notifications[transaction_id] = staged_notification

        print(f"INFO: Notification prepared for order {request.order_id}, tx {request.transaction_id}")
        return notification.PrepareResponse(ready=True, message="Notification staged.")

    def Commit(self, request, context):
        transaction_id = request.transaction_id
        with self.lock:
            if transaction_id in self.committed_notifications:
                return notification.CommitResponse(success=True, message="Notification already committed.")
            if transaction_id in self.aborted_transactions:
                return notification.CommitResponse(success=False, message="Notification transaction was aborted.")

            staged_notification = self.prepared_notifications.pop(transaction_id, None)
            if staged_notification is None:
                return notification.CommitResponse(success=False, message="Notification was not prepared.")
            if staged_notification["order_id"] != request.order_id:
                self.prepared_notifications[transaction_id] = staged_notification
                return notification.CommitResponse(
                    success=False,
                    message="Order id does not match prepared notification.",
                )

            self.committed_notifications[transaction_id] = self._send_notification(
                transaction_id,
                staged_notification,
            )

        print(f"INFO: Notification committed for order {request.order_id}, tx {request.transaction_id}")
        return notification.CommitResponse(success=True, message="Notification committed.")

    def Abort(self, request, context):
        transaction_id = request.transaction_id
        with self.lock:
            if transaction_id in self.committed_notifications:
                return notification.AbortResponse(
                    aborted=False,
                    message="Notification was already committed and cannot be aborted.",
                )

            self.prepared_notifications.pop(transaction_id, None)
            self.aborted_transactions.add(transaction_id)

        print(f"INFO: Notification aborted for order {request.order_id}, tx {request.transaction_id}")
        return notification.AbortResponse(aborted=True, message="Notification aborted.")

    def _send_notification(self, transaction_id, staged_notification):
        sent_notification = dict(staged_notification)
        sent_notification["notification_reference"] = f"dummy-notification-{transaction_id}"
        sent_notification["sent_at"] = time.time()

        # dummy implementation (in reality, it could push an email/SMS)
        print(
            f"INFO: Sent dummy notification for order {staged_notification['order_id']} "
            f"to user {staged_notification['user']}: {staged_notification['message']}"
        )
        return sent_notification


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    notification_grpc.add_NotificationServiceServicer_to_server(NotificationService(), server)
    server.add_insecure_port(f"[::]:{SERVICE_PORT}")
    server.start()
    print(f"INFO: Notification service running on {SERVICE_PORT}")
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
