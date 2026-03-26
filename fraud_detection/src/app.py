import sys
import os
import grpc
import threading
from concurrent import futures

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
fraud_detection_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/fraud_detection'))
sys.path.insert(0, fraud_detection_grpc_path)

import fraud_detection_pb2      as fraud_detection
import fraud_detection_pb2_grpc as fraud_detection_grpc

NODES     = ["orchestrator", "transaction", "fraud", "suggestions"]
THIS_NODE = "fraud"

ORDERS    = {}
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

# -- Fraud Service API --
class FraudService(fraud_detection_grpc.FraudServiceServicer):
    def InitOrder(self, request, context):
        print("INFO: Fraud InitOrder request received:")
        print(f"Order ID:     {request.order_id}")
        print(f"Card:         {request.card_number}")
        print(f"Amount cents: {request.order_amount_cents}")

        clock_input = dict(request.vector_clock)

        with LOCK:
            local_clock = merge_clock(zero_clocks(), clock_input)
            local_clock = tick(local_clock, THIS_NODE)

            ORDERS[request.order_id] = {
                "card_number":        request.card_number,
                "order_amount_cents": request.order_amount_cents,
                "vector_clock":       local_clock
            }

        response = fraud_detection.InitOrderResponse(
            ok      = True,
            message = "Fraud service initialized order."
        )
        response.vector_clock.update(local_clock)

        print("INFO: Fraud InitOrder response sent.")
        return response

    def CheckFraud(self, request, context):
        print("INFO: Fraud CheckFraud request received:")
        print(f"Order ID: {request.order_id}")

        with LOCK:
            if request.order_id not in ORDERS:
                print("ERROR: Unknown order_id in fraud service.")

                response = fraud_detection.FraudResponse(is_fraud=True)
                response.vector_clock.update(zero_clocks())
                return response

            order_state = ORDERS[request.order_id]

            local_clock = merge_clock(order_state["vector_clock"], dict(request.vector_clock))
            local_clock = tick(local_clock, THIS_NODE)
            order_state["vector_clock"] = local_clock

            card_number        = order_state["card_number"]
            order_amount_cents = order_state["order_amount_cents"]

        print(f"Cached card:         {card_number}")
        print(f"Cached amount cents: {order_amount_cents}")

        is_fraud = (
            card_number.startswith("0000")
            or order_amount_cents > 100000
        )

        response = fraud_detection.FraudResponse(is_fraud=is_fraud)
        response.vector_clock.update(local_clock)

        print("INFO: Fraud Service response:")
        print(f"Fraud result: {response.is_fraud}")

        return response

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    fraud_detection_grpc.add_FraudServiceServicer_to_server(FraudService(), server)

    port = "50051"
    server.add_insecure_port("[::]:" + port)
    server.start()
    print("INFO: Server started. Listening on port 50051.")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()