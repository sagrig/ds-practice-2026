import sys
import os
import grpc
import threading
from concurrent import futures
from collections import deque

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
order_queue_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/order_queue'))
sys.path.insert(0, order_queue_grpc_path)

import order_queue_pb2      as order_queue
import order_queue_pb2_grpc as order_queue_grpc

QUEUE     = deque()
LOCK      = threading.Lock()


# -- Order Queue Service API --
class OrderQueueService(order_queue_grpc.OrderQueueServiceServicer):
    def Enqueue(self, request, context):
        print("INFO: OrderQueue Enqueue request received:")
        print(f"Order ID: {request.order_id}")
        print(f"User:     {request.user}")
        print(f"Items:    {[{'title': item.title, 'quantity': item.quantity} for item in request.items]}")


        with LOCK:

            QUEUE.append({
                "order_id":     request.order_id,
                "user":         request.user,
                "items":        [
                    {"title": item.title, "quantity": item.quantity}
                    for item in request.items
                ]
            })

        response = order_queue.EnqueueResponse(
            ok      = True,
            message = "Order enqueued successfully."
        )

        print("INFO: OrderQueue Enqueue response sent.")
        return response

    def Dequeue(self, request, context):
        print("INFO: OrderQueue Dequeue request received.")

        with LOCK:
            if not QUEUE:

                response = order_queue.DequeueResponse(
                    ok      = False,
                    message = "Queue is empty.",
                    order_id = "",
                    user     = ""
                )
                print("INFO: Queue is empty.")
                return response

            order_data = QUEUE.popleft()

        response = order_queue.DequeueResponse(
            ok       = True,
            message  = "Order dequeued successfully.",
            order_id = order_data["order_id"],
            user     = order_data["user"],
        )
        for item in order_data["items"]:
            response.items.add(title=item["title"], quantity=item["quantity"])

        print("INFO: OrderQueue Dequeue response sent.")
        print(f"Order ID: {response.order_id}")
        print(f"User:     {response.user}")
        print(f"Items:    {[{'title': item.title, 'quantity': item.quantity} for item in response.items]}")

        return response

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    order_queue_grpc.add_OrderQueueServiceServicer_to_server(OrderQueueService(), server)

    port = "50054"
    server.add_insecure_port("[::]:" + port)
    server.start()
    print("INFO: Server started. Listening on port 50054.")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
