import sys
import os
import grpc
import threading
from concurrent import futures
from google.protobuf import empty_pb2

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
fraud_detection_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/fraud_detection'))
sys.path.insert(0, fraud_detection_grpc_path)

import fraud_detection_pb2      as fraud_detection
import fraud_detection_pb2_grpc as fraud_detection_grpc

# -- Fraud Service API --
class FraudService(fraud_detection_grpc.FraudServiceServicer):
    def __init__(self):
        self.NODES = ["transaction", "fraud", "suggestions"]
        self.THIS_NODE = "fraud"
        self.lock   = threading.Lock()
        self.orders = {}

    def InitOrder(self, request, context):
        print("INFO: Fraud InitOrder request received:")
        print(f"Order ID:     {request.order_id}")
        print(f"User:         {request.user}")
        print(f"Card:         {request.card_number}")
        print(f"Amount cents: {request.order_amount_cents}")

        with self.lock:
            self.orders[request.order_id] = {
                "user":               request.user,
                "card_number":        request.card_number,
                "order_amount_cents": request.order_amount_cents,
                "order_clock":        self.zero_clocks()
            }

        print("INFO: Fraud InitOrder response sent.")
        return empty_pb2.Empty()
    
    # event (d) - check for user fraud – username consists of numbers
    def CheckUserFraud(self, request, context):
        print("INFO: (d) CheckUserFraud request received:")
        with self.lock:
            ingress_clock = dict(request.vector_clock)
            print(f"INFO: (d) CheckUserFraud ingress vector clock: {ingress_clock} for order_id: {request.order_id}")

            order = self.orders.get(request.order_id)
            order["order_clock"] = self.merge_clocks(order["order_clock"], ingress_clock)
            order["order_clock"] = self.tick_clock(order["order_clock"])
            print(f"INFO: (a) CheckUserFraud updated vector clock: {order['order_clock']} for order_id: {request.order_id}")

            user = order["user"]
            failed = user.isdigit() # simple fraud check: username consists of only digits
            print(f"INFO: (d) CheckUserFraud completed for order_id: {request.order_id}, failed: {failed}")
            return fraud_detection.CheckUserFraudResponse(
                order_id = request.order_id,
                vector_clock = order["order_clock"],
                failed   = failed
            )
    
    # event (e) - check for credit card fraud – card number starts with "9999"
    def CheckCardFraud(self, request, context):
        print("INFO: (e) CheckCardFraud request received:")
        with self.lock:
            ingress_clock = dict(request.vector_clock)
            print(f"INFO: (e) CheckCardFraud ingress vector clock: {ingress_clock} for order_id: {request.order_id}")

            order = self.orders.get(request.order_id)
            order["order_clock"] = self.merge_clocks(order["order_clock"], ingress_clock)
            order["order_clock"] = self.tick_clock(order["order_clock"])
            print(f"INFO: (e) CheckCardFraud updated vector clock: {order['order_clock']} for order_id: {request.order_id}")

            card_number = order["card_number"]
            failed = card_number.startswith("9999") # simple fraud check: card number starts with "9999"
            print(f"INFO: (e) CheckCardFraud completed for order_id: {request.order_id}, failed: {failed}")
            return fraud_detection.CheckCardFraudResponse(
                order_id = request.order_id,
                vector_clock = order["order_clock"],
                failed   = failed
            )
        
    def zero_clocks(self):
        return {node: 0 for node in self.NODES}

    def merge_clocks(self, local, ingress):
        merged = {}
        for node in self.NODES:
            merged[node] = max(local.get(node, 0), ingress.get(node, 0))
        return merged
    
    def tick_clock(self, clock):
        new_clock = dict(clock)
        new_clock[self.THIS_NODE] = new_clock.get(self.THIS_NODE, 0) + 1
        return new_clock


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