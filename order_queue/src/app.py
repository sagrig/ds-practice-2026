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

NODES     = ["orchestrator", "transaction", "fraud", "suggestions", "order_queue"]
THIS_NODE = "order_queue"

QUEUE     = deque()
LOCK      = threading.Lock()

# -- VECTOR CLOCK API --
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

# -- Order Queue Service API --
class OrderQueueService(order_queue_grpc.OrderQueueServiceServicer):
    def Enqueue(self, request, context):
        print("INFO: OrderQueue Enqueue request received:")
        print(f"Order ID: {request.order_id}")
        print(f"User:     {request.user}")
        print(f"Items:    {list(request.items)}")

        clock_input = dict(request.vector_clock)

        with LOCK:
            local_clock = merge_clock(zero_clocks(), clock_input)
            local_clock = tick(local_clock, THIS_NODE)

            QUEUE.append({
                "order_id":     request.order_id,
                "user":         request.user,
                "items":        list(request.items),
                "vector_clock": local_clock
            })

        response = order_queue.EnqueueResponse(
            ok      = True,
            message = "Order enqueued successfully."
        )
        response.vector_clock.update(local_clock)

        print("INFO: OrderQueue Enqueue response sent.")
        return response

    def Dequeue(self, request, context):
        print("INFO: OrderQueue Dequeue request received.")

        clock_input = dict(request.vector_clock)

        with LOCK:
            base_clock = merge_clock(zero_clocks(), clock_input)

            if not QUEUE:
                local_clock = tick(base_clock, THIS_NODE)

                response = order_queue.DequeueResponse(
                    ok      = False,
                    message = "Queue is empty.",
                    order_id = "",
                    user     = ""
                )
                response.vector_clock.update(local_clock)

                print("INFO: Queue is empty.")
                return response

            order_data = QUEUE.popleft()

            local_clock = merge_clock(order_data["vector_clock"], base_clock)
            local_clock = tick(local_clock, THIS_NODE)

        response = order_queue.DequeueResponse(
            ok       = True,
            message  = "Order dequeued successfully.",
            order_id = order_data["order_id"],
            user     = order_data["user"],
            items    = order_data["items"]
        )
        response.vector_clock.update(local_clock)

        print("INFO: OrderQueue Dequeue response sent.")
        print(f"Order ID: {response.order_id}")
        print(f"User:     {response.user}")
        print(f"Items:    {list(response.items)}")

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